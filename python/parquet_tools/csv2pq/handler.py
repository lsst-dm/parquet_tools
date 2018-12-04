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

from .cmdinfo import CmdInfo
from ..common.utils import fatal


# *****************************************************************************
# *                         C l a s s   C s v 2 C s v                         *
# *****************************************************************************

class Csv2Csv(object):
    def __init__(self, file_, nvchk_):
        self.csvF = file_
        self.nilC = nvchk_
        self.cmtc = CmdInfo.opt.cmt
        self.nilV = CmdInfo.opt.nil
        self.nanV = CmdInfo.opt.nan
        self.nRow = 0
        self.sepC = CmdInfo.opt.sep
        self.csvrdr = csv.reader(self.csvF, delimiter=self.sepC,
                                 quoting=csv.QUOTE_NONE)

    def __del__(self):
        self.csvF.close()

    def __getattr__(self, attr):
        return getattr(self.csvF, attr)

    def read(self, size):
        """In:  size - number of bytes to read, the argument is ignored. We
                   only read a single row at a time for the csv object as it
                   knows how to handle varying sized reads.
           Out: Returns the row or the null string if no more rows remain.
        """

        # Get next row unless we have no more rows, then we are done.
        #
        row = []
        try:
            row = next(self.csvrdr)
        except StopIteration:
            return ''      # On EOF return the null string
        except Exception:  # We don't care what it is
            fatal(0, 'Unable to convert csv row', self.nRow,
                  'in file', self.csvF.name)

        # Look at all columns that we need to preprocess null values
        #
        for i in self.nilC:
            if row[i] == self.nilV:
                row.append('true')
                row[i] = self.nanV
            else:
                row.append('false')
        rec = self.sepC.join(row)+'\n'

        # We return one row at a time. We could return more but memory
        # reallocation is more costly then a call for another record.
        #
        self.nRow += 1
        return rec


# *****************************************************************************
# *                           g e t _ h a n d l e r                           *
# *****************************************************************************

def get_handler(infile, nvchk):
    """Return an input handler for a csv file.
    In:  infile - the path o the csv file.
         nvchk  - the columns which should be checked for a null value
    Out: Returns an instance of the csv file handling object upon success and
         exits the program upon failure.
    """

    # Open the csv file we will be actually reading
    #
    try:
        csvfile = open(infile, newline=None)
    except OSError:
        fatal(0, 'Unable to open input file', infile)

    # Create a wrapper for this file and return it to be used for
    # reading augmented rows.
    #
    return Csv2Csv(csvfile, nvchk)
