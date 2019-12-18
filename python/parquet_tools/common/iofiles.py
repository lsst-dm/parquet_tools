# This file is part of parquet_tools.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
"""
This module handles the generation and processing of files to be converted.
It is used as a helper class to control the flow of execution.
"""

import os

from .errhandlers import ErrInfo, FatalError, ParmError


# *****************************************************************************
# *                               I O F i l e s                               *
# *****************************************************************************

class IOFiles:

    # File information
    #
    fIN = []          # List of files to process
    fOUT = []         # List of files to produce (1-to-1 corespondence)

    # get_filepair() processing options
    #
    _blab = False
    _skip = False
    _skipRep = False
    _replace = False

    # *************************************************************************
    # *                      _a p p l y _ t e m p l a t e                     *
    # *************************************************************************
    @classmethod
    def _apply_template(cls, tmplt):
        """Generate output file names from a template.

        Parameters
        ----------
        tmplt : 'string'
            The template to use to generate output file names (see notes).

        Raises
        ------
        ParmError
            Raised when an invalid template is passed to this method.

        Notes
        -----
        - The template is of the form '[=dir/][-sfx][+sfx]'. Where:
            '=dir/' replaces the input file directory path with 'dir'
            '-sfx'  removes 'sfx' from the end of the input file name.
            '+sfx'  adds 'sfx' to the end of the input file name.
        - The input file names are contained in our fIN list and the
          correspodning output files names are placed in our fOUT list
          in 1-to-1 order.
        """

        # Get optional directory
        #
        stmp = tmplt
        xdir = ''
        if tmplt[0:1] == '=':
            n = tmplt.rfind('/')
            if n < 0:
                raise ParmError(3, 'directory specification in template', stmp)
            xdir = tmplt[1:n + 1]
            if n < len(tmplt) - 1:
                tmplt = tmplt[n + 1:]
            else:
                tmplt = ''

        # Get suffixes
        #
        dirsfx = ''
        endsfx = ''
        if tmplt:
            if tmplt[0:1] == '-' and len(tmplt) > 1:
                n = tmplt.find('+')
                if n < 0:
                    dirsfx = tmplt[1:]
                    tmplt = ''
                else:
                    dirsfx = tmplt[1:n]
                    tmplt = tmplt[n:]
            if tmplt[0:1] == '+' and len(tmplt) > 1:
                endsfx = tmplt[1:]
            else:
                if tmplt:
                    raise ParmError(3, 'suffix specification in template',
                                    stmp)

        # Apply template to generate output file names
        #
        for infile in IOFiles.fIN:
            if xdir:
                n = infile.rfind('/')
                if n >= 0:
                    infile = xdir+infile[n + 1:]
                else:
                    infile = xdir + infile

            if dirsfx and infile.endswith(dirsfx):
                infile = infile[0:len(infile) - len(dirsfx)]

            if endsfx:
                infile += endsfx

            IOFiles.fOUT.append(infile)

    # *****************************************************************************
    # *                            _ f i l e _ o k                            *
    # *****************************************************************************
    @classmethod
    def _file_ok(cls, infile):
        """Verify that the given file exists and is an actual file.

        Parameters
        ----------
        infile : 'string'
            The input file path.

        Raises
        ------
        ParmError
            Raised when the infile doesn't exist or is not a file.

        Returns
        -------
        infile : 'string'
        """

        if not os.path.exists(infile):
            raise ParmError(6, 'Input file', infile, 'not found')
        if not os.path.isfile(infile):
            raise ParmError(6, 'Input file', infile, 'is not a file')
        return infile

    # *************************************************************************
    # *                         g e t _ i n f i l e s                         *
    # *************************************************************************
    @classmethod
    def get_infiles(cls, infile):
        """Get all input files. The files are placed in the fIN vector.

        Parameters
        ----------
        infile : 'string'
            The command line input file specification.

        Raises
        ------
        FatalError
            Raised when the input file list cannot be obtained from stdin.

        Returns
        -------
        result : 'bool'
            True is returned if there is at least one input file, False o/w.

        Notes
        -----
        - If infile starts with a dash, then input files are read from stdin.
        """

        if infile != '-':
            if not infile:
                return False
            IOFiles.fIN.append(IOFiles._file_ok(infile))
            return True

        # Get files from stdin
        #
        while True:
            try:
                IOFiles.fIN.append(IOFiles._file_ok(input()))
            except EOFError:
                return True
            except Exception as exc:  # Catch any remaining exceptions
                raise FatalError(exc, 'Unable to get input files from stdin')

    # *************************************************************************
    # *                        g e t _ o u t f i l e s                        *
    # *************************************************************************
    @classmethod
    def get_outfiles(cls, outfile):
        """Get all output files. The files are placed in the fOUT vector.

        Parameters
        ----------
        outfile : 'string'
            The command line output file specification.

        Raises
        ------
        ParmError
            Raised when the outfile is incompatible with the infile template.

        Notes
        -----
        - if outfile is specified, it must be contextually compatible with
          whatever was specified as the input. If it's a template, the
          template is applied against all input file specifications to generate
          correspodning output file names.
        - The resulting list of output files is placed in the fOUT vector.
        """

        if outfile:
            if outfile[0:1] in '+-=':
                IOFiles._apply_template(outfile)
            elif len(IOFiles.fIN) > 1:
                raise ParmError(1, "Non templated output file is",
                                "incompatible with multiple input files")
            else:
                IOFiles.fOUT.append(outfile)

    # *************************************************************************
    # *                        g e t _ f i l e p a i r                        *
    # *************************************************************************
    @classmethod
    def get_filepair(cls):
        """Get an input/output file pair to process.

        Raises
        ------
        FatalError
            Raised when the output file cannot be replaced.
        ParmError
            Raised when the output file already exists and cannot be skipped.

        Returns
        -------
        result : 'vector'
            The vector is the input/output filename pair
            (i.e. ['string', 'string']. If no more pairs exists the vector
            [None, None] is returned.
        """

        # Get an input and the corresponding output file. We may have more
        # input files than output files. That is OK.
        #
        while IOFiles.fIN:
            infile = IOFiles.fIN.pop(0)
            if IOFiles.fOUT:
                outfile = IOFiles.fOUT.pop(0)
            else:
                return [infile, '']

            # Make sure output file does not exist unless --replace
            # or --skip specified
            #
            if os.path.exists(outfile):
                if IOFiles._skipRep:
                    if not IOFiles.fOUT or not os.path.exists(IOFiles.fOUT[0]):
                        IOFiles._skipRep = False
                        IOFiles._replace = True

                if IOFiles._replace:
                    try:
                        os.remove(outfile)
                    except OSError as ex:
                        raise FatalError(ex, 'Unable to replace file', outfile)
                elif IOFiles._skip:
                    if (IOFiles._blab):
                        ErrInfo.say("Skipping file", infile, "output file",
                                    outfile, "exists.")
                    continue

                else:
                    raise ParmError(6, 'Output file', outfile,
                                    'already exists')
            return [infile, outfile]

        # Nothing more to do
        #
        return [None, None]

    # *************************************************************************
    # *                         s e t _ o p t i o n s                         *
    # *************************************************************************
    @classmethod
    def set_options(cls, blab, skip, repl):
        """Set options for get_filepair processing.

        Parameters
        ----------
        blab : 'bool'
            True if additional output is desired.
        skip : 'bool'
            True if --skip option is in effect.
        repl : 'bool'
            True if --replace option is in effect.
        """
        # Set options and check for skip/replace action and prime toggle if so
        #
        IOFiles._blab = blab
        IOFiles._skip = skip
        if skip and repl:
            IOFiles._skipRep = True
            IOFiles._replace = False
        else:
            IOFiles._replace = repl
