# *****************************************************************************
# *                     c s v 2 p q _ h a n d l e r . p y                     *
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
This module substitutes a known value for such values and adds set the
bollean value to true in the column used to indicate whether the value in
that column is null or not (see csv2pq_schema.py for details).
"""

import csv
import os
import sys

from csv2pq_cmdinfo import CmdInfo # noqa, flake8 issue
from csv2pq_utils import Fatal


# *****************************************************************************
# *                         C l a s s   C s v 2 C s v                         *
# *****************************************************************************

class Csv2Csv(object):
    def __init__(self, file_):
        self.csvF = file_
        self.cmtc = CmdInfo.OPT['cmt']
        self.nilV = CmdInfo.OPT['nil']
        self.nanV = CmdInfo.OPT['nan']
        self.nRow = 0
        self.sepC = CmdInfo.OPT['sep']
        self.csvrdr = csv.reader(self.csvF, delimiter=self.sepC,
                                 quoting=csv.QUOTE_NONE)

    def __del__(self):
        self.csvF.close()

    def __getattr__(self, attr):
        return getattr(self.csvF, attr)

    def read(self, size):

        # The csv reader throws an exception at EOF. Of course, it can throw
        # one at any time. We know we are at EOF if the file pointer is at EOF
        #
        row = []
        try:
            row = next(self.csvrdr)
        except Exception:  # We don't care what it is
            if os.fstat(self.csvF.fileno()).st_size != self.csvF.tell():
                e = sys.exc_info()[1]
                Fatal(0, 'Unable to convert csv row ' + str(self.nRow)
                      + ' in file ' + self.csvF.name, e)

        # On EOF we must return a null string. Returning None (the correct
        # value) causes pandas to crash, sigh. Pass through commented lines.
        #
        if not row: return ''

        # Look at all columns that we need to preprocess null values
        #
        for i in CmdInfo.colNVChk:
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
# *                            g e t H a n d l e r                            *
# *****************************************************************************

def getHandler(infile):
    "Return an input handler for a csv file."

    # Open the csv file we will be actually reading
    #
    try:
        csvfile = open(infile, newline=None)
    except Exception:
        e = sys.exc_info()[1]
        Fatal(0, 'Unable to open input file '+infile, e)

    # Create a wrapper for this file and return it to be used for
    # reading augmented rows.
    #
    return Csv2Csv(csvfile)
