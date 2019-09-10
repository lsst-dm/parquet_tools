# *****************************************************************************
# *                            h a n d l e r . p y                            *
# *****************************************************************************
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
Module that interfaces between pandas csv contertor and the actual csv file.

The module is used to handle csv files that associated with a scheme that
allows null integer or string values. These cannot be handled by pandas.
This module substitutes a known value for such values and adds a correspoding
bolean column whose value indicates whether the associated column is null
(i.e. true if it is and false otherwise).
"""

import csv

from ..common.errhandlers import FatalError, ParmError


# *****************************************************************************
# *                         C l a s s   C s v 2 C s v                         *
# *****************************************************************************

class Csv2Csv(object):
    """
    Preprocess a csv file record for methods processing csv files.

    Parameters
    ----------
    infile : 'string'
        The path to the file that should be preprocessed.

    Returns
    -------
    Result : 'object'

    Raises
    ------
    FatalError
    ParmError
    """

    _cmtC = '#'    # Character indicating a comment line.
    _nanV = '0'    # The value to substiture for a null indicator.
    _nCol = 0      # Number of columns each record should have.
    _nilC = []     # List ofcolun numbers to check for a null indicator.
    _nilV = '\\N'  # The sequence indicating a null value.
    _sepC = ','    # The field separator character.

    def __init__(self, infile):

        # Open the csv file we will be actually reading. We need not specify
        # the encoding because all we care about is the separator and the
        # null value indicator which will invariably be ascii.
        #
        try:
            self.csvF = open(infile, newline=None)
        except OSError as exc:
            raise FatalError(exc, 'Unable to open input file', infile)

        self.nRow = 0
        self.csvrdr = csv.reader(self.csvF, delimiter=Csv2Csv._sepC,
                                 quoting=csv.QUOTE_NONE)

    def __del__(self):
        self.csvF.close()

    def __getattr__(self, attr):
        return getattr(self.csvF, attr)

    # *************************************************************************
    # *                                r e a d                                *
    # *************************************************************************

    def read(self, size):
        """Mimic the standard file read() function.

        Parameters
        ----------
        size : 'int'
            The number of bytes to read and return (ignored, see notes).

        Returns
        -------
        result : 'string'
            The manipulated next row of the file is returned unless no more
            records remain, in which case, a null string is returned.

        Notes
        -----
        - Since only we read a single row at a time from the csv file, the
          caller (a pandas csv reader) knows how to handle varying length
          records regardless of how much was requested. So, we ignore the
          size argument.
        """

        # Get next row unless we have no more rows, then we are done.
        #
        row = []
        try:
            row = next(self.csvrdr)
        except StopIteration:
            return ''      # On EOF return the null string
        except Exception as exc:  # We don't care what it is
            raise FatalError(exc, 'Unable to convert csv row', self.nRow,
                  'in file', self.csvF.name)

        # Validate we have the correct number of columns.
        #
        n = 0
        for r in row:
            n += 1
        if Csv2Csv._nCol > 0:
            if len(row) != Csv2Csv._nCol:
                raise ParmError(7, 'In file "' + self.csvF.name + '" record',
                      self.nRow, 'has', len(row),
                      'columns but schema defines only',
                      Csv2Csv._nCol, 'columns')

        # Look at all columns that we need to preprocess null values
        #

        for i in Csv2Csv._nilC:
            if row[i] == Csv2Csv._nilV:
                row.append('true')
                row[i] = Csv2Csv._nanV
            else:
                row.append('false')
        rec = Csv2Csv._sepC.join(row)+'\n'

        # We return one row at a time. We could return more but memory
        # reallocation is more costly then a call for another record.
        #
        return rec

    # *************************************************************************
    # *                               s e t u p                               *
    # *************************************************************************
    @classmethod
    def setup(cls, cmdinfo, schema):
        """Setup this class for future use.

        Parameters
        ----------
        cmdinfo : 'object'
            The command information object holding various options.
        schema : 'object'
            The schema object defining the csv file schema.

        Returns
        -------
        result : 'None'
        """

        Csv2Csv._cmtC = cmdinfo.opt.cmt
        Csv2Csv._nanV = str(cmdinfo.opt.nan)
        Csv2Csv._nCol = schema.colOrigs
        Csv2Csv._nilC = schema.colNVChk
        Csv2Csv._nilV = cmdinfo.opt.nil
        Csv2Csv._sepC = cmdinfo.opt.sep
