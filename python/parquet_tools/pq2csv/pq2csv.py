#!/usr/bin/env python3
#
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
Usage: pq2csv [options] [infile [outfile]]

Specify 'pq2csv --help' to get the a description of what is needed.
"""

# *****************************************************************************
# *                                p q 2 c s v                                *
# *****************************************************************************

__all__ = ['main']

import sys

import pyarrow.parquet as pq

from parquet_tools.pq2csv.syntax import syntax

from parquet_tools.common.cmdinfo import CmdInfo

from parquet_tools.common.iofiles import IOFiles

from parquet_tools.common.schema import Schema

from parquet_tools.common.errhandlers import ErrInfo, FatalError, ParmError


# *****************************************************************************
# *                               c o n v e r t                               *
# *****************************************************************************

def convert(infile, outfile):
    """Convert a parquet file to a csv file.

    Parameters
    ----------
    infile : 'string'
        The path to the parquet file to be converted.
    outfile : 'string'
        The path to use for the output file. This may be a null string.

    Raises
    -------
    FatalError
        Raised when an unknown or unexpected exception occurs.
    ParmError
        Raised when an invalid file is passed to this method.
    """

    # Convert input file to a pandas dataframe
    #
    try:
        df = pq.read_table(infile).to_pandas()
        df.reset_index(inplace=True)
    except Exception as exc:  # We don't care what kind it is
        raise FatalError(exc, 'Unable to create pandas dataframe from', infile)

    # If a schema was specified, then we need to do some additional work
    #
    if CmdInfo.opt.schema:
        outcols = Schema.ver_dataframe(df, CmdInfo.opt.blab)
        if not outcols:
            raise ParmError(1, 'Schema verification failed for', infile)
    else:
        outcols = None
        if CmdInfo.opt.display:
            CmdInfo.opt.display = False
            Schema.set_schema(df)
            Schema.display_pq2csv()

    # If there is an output file, do the conversion. Otherwise, this is only
    # a verification only call. If we are writing out a file normalize the
    # dataframe to correspond to the actual original schema.
    #
    if outfile:
        Schema.normalize(df, CmdInfo.opt.nil)
        try:
            df.to_csv(outfile, sep=CmdInfo.opt.sep, na_rep=CmdInfo.opt.nil,
                      float_format='%.7f', encoding=CmdInfo.opt.enc,
                      index=False, columns=outcols, header=CmdInfo.opt.hdr)
        except Exception as exc:  # We don't care what kind it is
            raise FatalError(exc, 'Unable to create csv file from', infile)


# *****************************************************************************
# *                                c o n f i g                                *
# *****************************************************************************

def config():
    """Parse command line options and verify correctness.

    Parameters
    ----------
    None.

    Raises
    -------
    FatalError
        Raised when an unknown or unexpected exception occurs.
    ParmError
        Raised when an invalid parameter is passed to this method.

    Returns
    -------
    result : 'bool'
        True indicaes that there are filed that to be converted; otherwise,
        there is nothing to convert.

    Notes
    -----
    - This function uses information contained in the CmdInfo class. The
      CmdInfo class public variables are modified to reflect the processing
      that needs to occur.
    """

    # Parse all options and positional arguments
    #
    CmdInfo.parse_commandline(syntax())

    # -fnnfmt: Make sure format, if specified, is valid.
    #
    if CmdInfo.opt.f32fmt is not None:
        try:
            _ = format(float(12.13), CmdInfo.opt.f32fmt)  # noqa: F841
        except ValueError:
            raise ParmError(3, 'f32fmt value', CmdInfo.opt.f32fmt)

    if CmdInfo.opt.f64fmt is not None:
        try:
            _ = format(float(12.13), CmdInfo.opt.f64fmt)  # noqa: F841
        except ValueError:
            raise ParmError(3, 'f64fmt value', CmdInfo.opt.f64fmt)

    Schema.setfp(CmdInfo.opt.f32fmt, CmdInfo.opt.f64fmt)

    # Resolve the input file. If none, then --display must be in effect.
    #
    if not IOFiles.get_infiles(CmdInfo.opt.infile):
        if not CmdInfo.opt.display:
            raise ParmError(2, 'Input file')

    # Process any output file specification. This may raise an exception.
    #
    IOFiles.get_outfiles(CmdInfo.opt.outfile)

    # Process the schema file. If present, apply schema against all input files
    #
    if CmdInfo.opt.schema:
        Schema.get_schema(CmdInfo.opt.schema, False)
        if CmdInfo.opt.display:
            CmdInfo.opt.display = False
            Schema.display_pq2csv()

    # Return indicating whether any conversion is needed
    #
    return len(IOFiles.fIN) > 0


# *****************************************************************************
# *                          M a i n   P r o g r a m                          *
# *****************************************************************************

def main():
    # Make sure we have some arguments.
    #
    if len(sys.argv) < 2:
        ErrInfo.say('No arguments specified; use --help option for',
                    'usage information.')
        exit(1)

    # All of the following either works or raises an exception.
    try:
        # Parse the command line and determine what to do
        #
        if not config():
            exit(0)

        # Prepare for processing
        #
        IOFiles.set_options(CmdInfo.opt.blab, CmdInfo.opt.skp, CmdInfo.opt.rep)

        # Process all input files.
        #
        infile, outfile = IOFiles.get_filepair()
        while infile:

            # Indicate what we are doing
            #
            if CmdInfo.opt.blab:
                if outfile:
                    ErrInfo.say("Converting", infile, "to", outfile, "...")
                else:
                    ErrInfo.say("Processing", infile, '...')

            # Convert or display the file.
            #
            convert(infile, outfile)

            # Get next pair to convert
            #
            infile, outfile = IOFiles.get_filepair()

    # Catch any failures.
    #
    except (FatalError, ParmError) as exc:
        ErrInfo.say(exc)
        exit(99)


# Generally, in the Unix world, one should not use python modules as commands
# but the pythonic world sems to differ on that one. So, we allow it here.
#
if __name__ == '__main__':
    main()
