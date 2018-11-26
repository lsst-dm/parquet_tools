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
pandas does not support to be null. Specifically, for each such column it
adds a  column with a name of '<cname>_ISNULL', where <cname> is the name
of the column that may contain an unsupported null value. The associated
column will contain true if the original column contains a null value or
false, otherwise. Such a column will contain a well defined value or a
value specified by the command issuer. While this module sets up the
mechanics, the realization of this process is handled by csv2pq.handler.py.
"""

__all__ = ['chk_schema', 'get_schema']

import csv
import numpy as np

from parquet_tools.csv2pq.cmdinfo import CmdInfo
from parquet_tools.common.typeinfo import TypeInfo
from parquet_tools.common.utils import fatal


# *****************************************************************************
# *                               a d d _ c o l                               *
# *****************************************************************************

def add_col(cname):
    """Add a column to the column list and record size of longest name.
    In:  The name of the column.
    Out: Add he column to he column name vector and updates the maximum
         column name length in the CmdInfo class. It returns None.
    """

    CmdInfo.colNames.append(cname)
    if len(cname) > CmdInfo.cNameMax:
        CmdInfo.cNameMax = len(cname)


# *****************************************************************************
# *                           c a n _ b e _ n u l l                           *
# *****************************************************************************

def can_be_null(cspec):
    """Return true if this column may have a null value.
    In:  The tokenized line from the schema file corresponding to a column.
    Out: Returns True if the column may contain a null value, False otherwise.
         Note we use the common SQL syntax that the schema line must contain
         'not null' for the column not be able to have a null value. Also, it
         is assumed that comments have been removed from the schema definition.
    """

    # Return True is 'not null' is not present
    #
    return 'NOT NULL' not in ' '.join(cspec).upper()


# *****************************************************************************
# *                           g e t _ d e c t y p e                           *
# *****************************************************************************

def get_dectype(dargs):
    """Convert decimal type to appropriate numpy type.
    In:  the characters after 'decimal(' which should be 'p[,s])'.
    Out: a) If the specification is invalid, a null string is returned.
            We do not check whether or not the type is fully representable
            in the returned type as these will be caught during conversion.
         b) When s is missing or is 0, and integer type is returned whose
            size is dependent on the 'p', as follows:
             0 <= p <  3: int8
             3 <= p <  5: int16
             5 <= p < 10: int32
            10 <= p:      int64 (note max p is 18 but we don't check)
         c) When 's' is specified, then we return float32 when p < 7 and
            float64 otherwise. This is an imperfect data type for decimals
            as it is not very precise but it's the best one we have.
    """

    # Find the closing paren a strip off the remaining text
    #
    n = dargs.find(')')
    if n < 1:
        return ''
    xargs = dargs[0:n]

    # Split this into one or two args and convert
    #
    pspec = xargs.split(',')
    try:
        pval1 = int(pspec[0])
    except Exception:  # We don't care what the exception is
        return ''
    if pval1 < 0:
        return ''
    if len(pspec) == 1:
        pval2 = 0
    else:
        try:
            pval2 = int(pspec[1])
        except Exception:  # We don't care what the exception is
            return ''

    # Do check for valid scale value
    #
    if pval2 < 0:
        return ''

    # Return the an int type if this has no digits after the decimal point
    #
    if pval2 == 0:
        if pval1 < 3:
            return 'int8'
        elif pval1 < 5:
            return 'int16'
        elif pval1 < 10:
            return 'int32'
        else:
            return 'int64'

    # Return appropriate float
    #
    if pval1 < 7:
        return 'float32'
    return 'float64'


# *****************************************************************************
# *                              g e t _ t y p e                              *
# *****************************************************************************

def get_type(cname, ctype, cnull, colnulls):
    """Record correct type for a particular column.
    In:  cname    - The name of the column.
         ctype    - The proposed column data type.
         cnull    - Boolean: True if column may be null.
         colnulls - A list to which null column info is to be appended.
    Out: The [column name, type name, and pandas datatype] is appended
         to the colSpecs and pandas type is is entered into he ColTypes
         dictionary using the column name as the key both in the CmdInfo class.
         If the column may be null, the [column number, cname_ISNULL] is
         appended to the colnulls list passed as an argument. The type for
         this additional column is also added to the column type dictionary.
         This function returns None on success and exits upon error.
    """

    # We check for types using lower case but use original spec in messages
    #
    xtype = ctype.lower()

    # Check for character type and decimal type
    #
    if xtype[0:4] == 'char':
        xtype = 'char'
    elif xtype[0:8] == 'decimal(':
        xtype = get_dectype(xtype[8:])

    # All remaining types must be found in our name to numpy dictionary
    #
    CmdInfo.colSpecs.append([cname, ctype, xtype])
    if xtype in TypeInfo.table:
        CmdInfo.colTypes[cname] = TypeInfo.table[xtype]
    else:
        fatal(4, cname, ctype)

    # New record whether we should preprocess null values for the column
    #
    if cnull and xtype in TypeInfo.isInt:
        cname += '_ISNULL'
        colnulls.append([len(CmdInfo.colNames)-1, cname])
        CmdInfo.colTypes[cname] = np.bool_


# *****************************************************************************
# *                            g e t _ s c h e m a                            *
# *****************************************************************************

def get_schema(schfile, do_display):
    """Setup colum names and optional data types.
    In:  schfile    - Either the Path to the schema file or the token 'hdr' to
                      indicate that the schema is described by the csv file.
         do_display - True if the processing of the schema file is to be
                      displayed.
    Out: This function completes the informtion in the CmdInfo class so that a
         csv file can be correctly be converted to a parquet file.
         Specifically, if the schfile is 'hdr' then then the column names
         are defined by the first row of the csv file. Otherwise, the schema
         file is read, each line toknized, and parsed to obtain the column
         name and ssociated data type. Also, if there are null non-float
         columns those columns are recorded in colNVChk in the CmdInfo class.
         This list will be used to check if the associated column contains a
         null value and if it does, the corresponding _ISNULL column will be
         set to True. This function returns None upon success and exits the
         program when an error is encountered (e.g. invalid data type).
    """

    colnulls = []

    # Check if the first line in infile has column names
    #
    if schfile == 'hdr':
        CmdInfo.colNames = None
        CmdInfo.opt.hdr = 0
        return

    # Open the schfile and get all the records
    #
    try:
        with open(schfile) as sch_file:
            recs = [line.rstrip('\n') for line in sch_file]
    except OSError:
        fatal(0, 'Unable to open schema file', schfile)

    # Run through each record grabbing the first two tokens (the second one
    # may be missing).
    #
    for line in recs:
        toks = line.split()
        if len(toks) > 0:
            add_col(toks[0])
            if not CmdInfo.opt.ato and len(toks) > 1:
                get_type(toks[0], toks[1], can_be_null(toks[2:]), colnulls)
            elif do_display:
                CmdInfo.colSpecs.append([toks[0], 'auto', 'auto'])

    # Check if we should augment the schema for integers that may be null
    #
    if colnulls:
        for x in colnulls:
            CmdInfo.colNVChk.append(x[0])
            add_col(x[1])

    # Check if we should display the scheme
    #
    if do_display and CmdInfo.colSpecs:
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
        for sp in colnulls:
            n += 1
            print(fmt1.format(n, sp[1], "bool"))


# *****************************************************************************
# *                            c h k _ s c h e m a                            *
# *****************************************************************************

def chk_schema(infile):
    """Check that the schema coresponds to the data.
    In:  The path to the csv file.
    Out: Returns None upon success and exits the program upon failure.
    """

    # Read the first line of the cvs file, which may be the header
    #
    try:
        with open(infile) as f:
            csvrdr = csv.reader(f, delimiter=CmdInfo.opt.sep,
                                quoting=csv.QUOTE_NONE)
            row = next(csvrdr)
            if len(row) != len(CmdInfo.colSpecs):
                nr = len(row)
                nc = len(CmdInfo.colSpecs)
                fatal(7, 'Schema with', nc,
                      'cols does not match the data with', nr, 'cols')
            return
    except Exception:
        fatal(0, 'Unable to read input file', infile)
