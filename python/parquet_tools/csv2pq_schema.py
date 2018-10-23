# *****************************************************************************
# *                      c s v 2 p q _ s c h e m a . p y                      *
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
This module encapsulates all the code to process schema files and provide
support for schemas that allow null values for non-floating point values.

This module augments schemas that allow null values for data types which
pandas does not support to be null. Specifically, for each such column is
adds a  column with a name of '<cname>_ISNULL', where <cname> is the name
of the column that may contain an unsupported null value. The associated
column will contain true if the original column contains a null value or
false, otherwise. Such a column will contain a well defined value or a
value specified by the command issuer. While this module sets up the
mechanics, the relealiztion of this process is handled by csv2pq_handler.py.
"""

__all__ = ['chkSchema', 'getSchema']

import csv
import sys
import numpy as np

from csv2pq_cmdinfo import CmdInfo
from csv2pq_utils import Fatal


# *****************************************************************************
# *           T y p e   C o n v e r s i o n   I n f o r m a t i o n           *
# *****************************************************************************

class TypeInfo:

    table = {'int':     np.int32,   'short':    np.int16, 'long':  np.int64,
             'bigint':  np.int64,   'smallint': np.int16,
             'int8':    np.int8,    'int32':    np.int32, 'int64': np.int64,
             'float':   np.float32, 'double':   np.float64,
             'float32': np.float32, 'float64':  np.float64,
             'char':    np.bytes_,  'bool':     np.bool_
             }

    isInt = {'int':     True,       'short':    True,     'long':  True,
             'bigint':  True,       'smallint': True,
             'int8':    True,       'int32':    True,     'int64': True
             }


# *****************************************************************************
# *                                a d d C o l                                *
# *****************************************************************************

def addCol(cName):
    "Add a column to the column list and record size of longest name."

    CmdInfo.colNames.append(cName)
    if len(cName) > CmdInfo.cNameMax: CmdInfo.cNameMax = len(cName)


# *****************************************************************************
# *                               g e t D e c T                               *
# *****************************************************************************

def getDecT(dArgs):
    "Convert decimal type to appropriate numpy type."

    # Find the closing paren a strip off the remianing text
    #
    n = dArgs.find(')')
    if n < 1: return ''
    xArgs = dArgs[0:n]

    # Split this into one or two args and convert
    #
    pSpec = xArgs.split(',')
    try:
        pVal1 = int(pSpec[0])
    except Exception:  # We don't care what he exception is
        return ''
    if len(pSpec) == 1:
        pVal2 = 0
    else:
        try:
            pVal2 = int(pSpec[1])
        except Exception:  # We don't care what gthe exception is
            return ''

    # Return the an int type if this has no digits after the decimal point
    #
    if pVal2 == 0:
        if pVal1 < 3:
            return 'int8'
        elif pVal1 < 5:
            return 'int16'
        elif pVal1 < 10:
            return 'int32'
        else:
            return 'int64'

    # Return appropriate float
    #
    if pVal1 < 7: return 'float32'
    return 'float64'


# *****************************************************************************
# *                               g e t T y p e                               *
# *****************************************************************************

def getType(cName, cType, cNull, colNulls):
    "Record correct type for a particular column."

    # We check for types using lower case but use original spec in messages
    #
    xType = cType.lower()

    # Check for character type and decimal type
    #
    if xType[0:4] == 'char':
        xType = 'char'
    elif xType[0:8] == 'decimal(':
        xType = getDecT(xType[8:])

    # All remaining types must be found in our name to numpy dictionary
    #
    CmdInfo.colSpecs.append([cName, cType, xType])
    if xType in TypeInfo.table:
        CmdInfo.colTypes[cName] = TypeInfo.table[xType]
    else:
        Fatal(4, cName, cType)

    # New record whether we should preprocess null values for the column
    #
    if cNull and xType in TypeInfo.isInt:
        cName += '_ISNULL'
        colNulls.append([len(CmdInfo.colNames)-1, cName])
        CmdInfo.colTypes[cName] = np.bool_


# *****************************************************************************
# *                               g e t N u l l                               *
# *****************************************************************************

def getNull(cspec):
    "Return true if this column may have a null value."

    # Convert tokens in the spec to upper case
    #
    ctext = ' '.join(cspec).upper()

    # Check if NULL is even present
    #
    return ' NULL' in ctext and 'NOT ' not in ctext


# *****************************************************************************
# *                             g e t S c h e m a                             *
# *****************************************************************************

def getSchema(schFile, doDisp):
    "Setup colum names and optional data types."
    colNulls = []

    # Check if the first line in infile has column names
    #
    if schFile == 'hdr':
        CmdInfo.colNames = None
        CmdInfo.OPT['hdr'] = 0
        return

    # Open the schfile and get all the records
    #
    try:
        with open(schFile) as sch_file:
            recs = [line.rstrip('\n') for line in sch_file]
    except Exception:
        e = sys.exc_info()[1]
        Fatal(0, 'Unable to open schema file '+schFile, e)

    # Run through each record grabbing the first two tokens (the second one
    # may be missing).
    #
    for line in recs:
        toks = line.split()
        if len(toks) > 0:
            addCol(toks[0])
            if not CmdInfo.OPT['ato'] and len(toks) > 1:
                getType(toks[0], toks[1], getNull(toks[2:]), colNulls)
            elif doDisp:
                CmdInfo.colSpecs.append([toks[0], 'auto', 'auto'])

    # Check if we should augment the schema for integers that may be null
    #
    if colNulls:
        for x in colNulls:
            CmdInfo.colNVChk.append(x[0])
            addCol(x[1])

    # Check if we should display the scheme
    #
    if doDisp and CmdInfo.colSpecs:
        n = len(str(len(CmdInfo.colNames)))
        fmt1 = '{0: >'+str(n)+'} {1: <'+str(CmdInfo.cNameMax)+'} {2}'
        fmt2 = fmt1+' -> {3}'
        n = -1
        for sp in CmdInfo.colSpecs:
            n += 1
            if sp[1] == sp[2].upper():
                print(fmt1.format(n, sp[0], sp[2]))
            else:
                print(fmt2.format(n, sp[0], sp[1], sp[2]))
        for sp in colNulls:
            n += 1
            print(fmt1.format(n, sp[1], "bool"))


# *****************************************************************************
# *                             c h k S c h e m a                             *
# *****************************************************************************

def chkSchema(inFile):
    "Check that the schema coresponds to the data."

    # Read the first line of the cvs file, which may be the header
    #
    try:
        with open(inFile) as f:
            csvrdr = csv.reader(f, delimiter=CmdInfo.OPT['sep'],
                                quoting=csv.QUOTE_NONE)
            row = next(csvrdr)
            if len(row) != len(CmdInfo.colSpecs):
                nR = len(row)
                nC = len(CmdInfo.colSpecs)
                Fatal(1, 'Schema with ' + str(nC) + ' cols '
                      + 'does not match the data with ' + str(nR) + ' cols')
            return
    except Exception:
        e = sys.exc_info()[1]
        Fatal(0, 'Unable to read input file '+inFile, e)
