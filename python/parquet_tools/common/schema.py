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
mechanics, the realization of this process is handled by the caller.
"""

import csv
import os
import numpy as np

from ..common.typeinfo import TypeInfo
from ..common.errhandlers import ErrInfo, FatalError, ParmError


# *****************************************************************************
# *                             S c h e m a . p y                             *
# *****************************************************************************

class Schema:

    # Public column information
    #
    colNames = None   # List of column names (None if not a defined schema)
    colNVChk = []     # The columns that need to be checked for a null value
    colTypes = {}     # Dictionary of column name to pandas type
    colOrigs = 0      # Original number of columns in schema

    # Private members
    #
    _cNameMaw = 0     # The length of the longest column name in original
    _cNameMax = 0     # The length of the longest column name in augmented
    _colFmtfp = ''    # Current format in effect when formating floats
    _colFPFmt = []    # The columns that need to be formatted as fixed point
    _colFP32 = []     # List of column names with type float.
    _colFP64 = []     # List of column names with type double.
    _colNotNull = []  # List of column names that cannot be null
    _colSpecs = []    # List of [column name, SQL type, pandas type]
    _f32fmt = None    # Output format for floats.
    _f64fmt = None    # Output format for doubles.
    _nilSuffx = '_ISNULL'  # Suffix added to column to track null values

    # *************************************************************************
    # *                            _ a d d _ c o l                            *
    # *************************************************************************
    @classmethod
    def _add_col(cls, cname):
        """Add a column to the column list and record size of longest name.

        Parameters
        ----------
        cname : 'string'
            The name of the column.

        Notes
        -----
        - This method simply records the longest column name in the class
          _cNameMax variable and adds the name to the class colNames list.
        """

        Schema.colNames.append(cname)
        if len(cname) > Schema._cNameMax:
            Schema._cNameMax = len(cname)

    # *************************************************************************
    # *                         _ c a n _ b e _ n u l                         *
    # *************************************************************************
    @classmethod
    def _can_be_null(cls, cspec):
        """Return true if this column may have a null value.

        Parameters
        ----------
        cspec : 'string'
            The tokenized line from the schema file corresponding to a column.

        Returns
        -------
        result : 'bool'
             Returns True if the column may contain a null value, False
             otherwise.

        Notes
        -----
        - This method uses the common SQL syntax that a schema line must
          contain 'not null' for the column not be able to have a null
          value. Also, it is assumed that comments have been removed from
          the schema definition.
        """

        # Return True is 'not null' is not present
        #
        return 'NOT NULL' not in ' '.join(cspec).upper()

    # *************************************************************************
    # *                      d i s p l a y _ c s v 2 p q                      *
    # *************************************************************************
    @classmethod
    def display_csv2pq(cls):
        """Display conversion of csv file to this parquet schema.

        Notes
        -----
        - The format used is:
          <colno> <colname> <sqltype> -> <pqtype>
        """

        # Check if we should display the scheme
        #
        if Schema._colSpecs:
            n = len(str(len(Schema.colNames)))
            fmt1 = '{0: >' + str(n) + '} {1: <' + str(Schema._cNameMax) +\
                   '} {2}'
            fmt2 = fmt1 + ' -> {3}'
            n = -1
            for cname, sqltype, nptype in Schema._colSpecs:
                n += 1
                if not sqltype or sqltype == nptype.upper():
                    print(fmt1.format(n, cname, nptype))
                else:
                    print(fmt2.format(n, cname, sqltype, nptype))
            if Schema.colOrigs < len(Schema.colNames):
                for cname in Schema.colNames[Schema.colOrigs:]:
                    n += 1
                    print(fmt1.format(n, cname, "bool"))

    # *************************************************************************
    # *                      d i s p l a y _ p q 2 c s v                      *
    # *************************************************************************
    @classmethod
    def display_pq2csv(cls):
        """Display conversion of parquet file to this csv schema.

        Notes
        -----
        - It is assumed that the caller has verified that the underlying
          parquet file corresponds to this schema.
        - The format used is:
          <colno> <colname> <pqtype> -> <csvtype>
        """

        # Setup to format the schmema conversion
        #
        if Schema.colOrigs:
            pqcols = Schema._colSpecs[0:Schema.colOrigs]
        else:
            return

        # Develop the correct format
        #
        n = len(str(len(Schema.colNames)))
        fmt1 = '{0: >' + str(n) + '} {1: <' + str(Schema._cNameMaw) + '} {2}'
        fmt2 = fmt1 + ' -> {3}'
        n = -1

        # Display conversion
        #
        for cname, sqltype, nptype in pqcols:
            n += 1
            if cname in Schema._colNotNull:
                nntext = ' NOT NULL'
            else:
                nntext = ''
            if not sqltype or sqltype == nptype.upper():
                print(fmt1.format(n, cname, nptype + nntext))
            else:
                print(fmt2.format(n, cname, nptype, sqltype + nntext))

    # *************************************************************************
    # *                        _ g e t _ d e c t y p e                        *
    # *************************************************************************
    @classmethod
    def _get_dectype(cls, dargs, cname):
        """Convert decimal type to appropriate numpy type.

        Parameters
        ----------
        dargs : 'string'
            The characters after 'decimal(' which should be 'p[,s])'. Where
            'p' is the precision and 's' is the scale.
        cname : 'string'
            The associated column name.

        Returns
        -------
        result : 'string'
            The returned string is the generic numpy datatype to be used to
            represent the decimal type. A null string indicates that the
            appropriate type could not be determined.

        Notes
        -----
        - This method does not check whether or not the type is fully
          representable in the returned type as these will be caught during
          actual conversion.
        - When s is missing or is 0, and integer type is returned whose
          size is dependent on the 'p', as follows:
           0 <= p <  3: int8
           3 <= p <  5: int16
           5 <= p < 10: int32
          10 <= p:      int64 (note max p is 18 but we don't check)
        - When 's' is specified, then we return float32 when p < 7 and
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

        # Add column to formating list
        #
        fpfmt = '.{}f'.format(pval2)
        Schema._colFPFmt.append([cname, fpfmt])

        # Return appropriate float
        #
        if pval1 < 7:
            return 'float32'
        return 'float64'

    # *************************************************************************
    # *                           _ g e t _ t y p e                           *
    # *************************************************************************
    @classmethod
    def _get_type(cls, cname, ctype, cnull, colnulls):
        """Record correct type for a particular column.

        Parameters
        ----------
        cname : 'string'
            The name of the column.
        ctype : 'string'
            The column type in SQL format.
        cnull : 'bool'
            True if the column may be null; False otherwise.
        colnulls : 'list'
            A list to which null column info is to be appended (see notes).

        Raises
        ------
        ParmError

        Notes
        -----
        - The [cname, ctype, pandas datatype] is appended to the class
          _colSpecs list and pandas type is entered into the ColTypes
          dictionary with cname as the key.
        - If the column may be null, the [column number, cname_ISNULL] is
          appended to the colnulls list passed as an argument. The type for
          this additional column is also added to the colTypes dictionary.
        """

        # Check for types using lower case but use original spec in messages
        #
        xtype = ctype.lower()

        # Check for character type and decimal type
        #
        if xtype[0:4] == 'char' or xtype[0:7] == 'varchar':
            xtype = 'char'
        elif xtype[0:8] == 'decimal(':
            xtype = Schema._get_dectype(xtype[8:], cname)
        else:
            # Record columns that have float or double types.
            #
            try:
                if TypeInfo.table[xtype] == np.float32:
                    Schema._colFP32.append(cname)
                elif TypeInfo.table[xtype] == np.float64:
                    Schema._colFP64.append(cname)
            except KeyError:
                raise ParmError(4, cname, ctype)

        # All types must be found in our name to numpy dictionary. We have
        # screened for ths but just on case we check again.
        #
        try:
            nptype = TypeInfo.table[xtype]
            Schema.colTypes[cname] = nptype
            Schema._colSpecs.append([cname, ctype, str(np.dtype(nptype))])
        except KeyError:
            raise ParmError(4, cname, ctype)

        # Now record whether we should preprocess null values for the column
        #
        if cnull and xtype in TypeInfo.isInt:
            cname += Schema._nilSuffx
            colnulls.append([len(Schema.colNames)-1, cname])
            Schema.colTypes[cname] = np.bool_

    # *************************************************************************
    # *                          c h k _ s c h e m a                          *
    # *************************************************************************
    @classmethod
    def chk_schema(cls, infile, sepc):
        """Check that the schema coresponds to the data.

        Parameters
        ----------
        infile : 'string'
            The path to the input csv file.
        sepc : 'string'
            The csv column separator character.

        Raises
        -------
        FatalError
        ParmError
        """

        # Screen for the common error here.
        #
        if not os.path.exists(infile):
            raise ParmError(6, 'Schema file', infile, 'not found')

        # Read the first line of the cvs file, which may be the header
        #
        try:
            with open(infile) as f:
                csvrdr = csv.reader(f, delimiter=sepc, quoting=csv.QUOTE_NONE)
                row = next(csvrdr)
                if len(row) != len(Schema._colSpecs):
                    nr = len(row)
                    nc = len(Schema._colSpecs)
                    raise ParmError(1, 'Schema with', nc,
                                    'cols does not match the data with',
                                    nr, 'cols')
                return
        except Exception as ex:
            raise FatalError(ex, 'Unable to read input file', infile)

    # *************************************************************************
    # *                             _ f o r m a t                             *
    # *************************************************************************
    @classmethod
    def _format(cls, x):
        """Format floating point number using desired format.

        Parameters
        ----------
        x : 'float'
             The floating point number to be formatted.

        Returns
        -------
        result : 'string'
             The formatted number.

        Notes
        -----
        - This method is meant to be passed to pandas apply() and map()
          functions and, as such, expects only one parameter.
        - This method uses the implicit argument Schema._colFmtfp which
          should contain the format to be used.
        """

        # Format the number
        #
        if x == 0.0:
            result = '0.0'
        else:
            result = format(x, Schema._colFmtfp).rstrip('0')
            if result.endswith('.'):
                result += '0'
        return result

    # *************************************************************************
    # *                          g e t _ s c h e m a                          *
    # *************************************************************************
    @classmethod
    def get_schema(cls, schfile, do_auto):
        """Setup colum names and optional data types.

        Parameters
        ----------
        schfile : 'string'
             The Path to the schema file.
        do_auto : 'bool'
             True if automatic schema is enabled and this call is to obtain
             a schema for comparison purposes.

        Raises
        ------
        FatalError

        Notes
        -----
        - This method completes the column information so that a csv file
          can be correctly be converted to a parquet file ir vice versa.
        - Each line in schema file is toknized, and parsed to obtain the
          column name and associated data type. Also, if there are null
          non-float columns those columns are recorded in colNVChk. This
          list will be used to check if the associated column contains a
          null value and if it does, the corresponding _ISNULL column will
          be set to True.
        """

        Schema.colNames = []
        colnulls = []

        # Screen for the common error here.
        #
        if not os.path.exists(schfile):
            raise ParmError(6, 'Schema file', schfile, 'not found')

        # Open the schfile and get all the records
        #
        try:
            with open(schfile) as sch_file:
                recs = [line.rstrip('\n') for line in sch_file]
        except OSError as ex:
            raise FatalError(ex, 'Unable to open schema file', schfile)

        # Run through each record grabbing the first two tokens (the second one
        # may be missing).
        #
        for line in recs:
            toks = line.split()
            if len(toks) > 0:
                Schema._add_col(toks[0])
                if not do_auto and len(toks) > 1:
                    nullok = Schema._can_be_null(toks[2:])
                    if not nullok:
                        Schema._colNotNull.append(toks[0])
                    Schema._get_type(toks[0], toks[1], nullok, colnulls)
                elif toks[0]:
                    Schema._colSpecs.append([toks[0], 'auto', 'auto'])

        # Record original number of colums
        #
        Schema.colOrigs = len(Schema.colNames)
        Schema._cNameMaw = Schema._cNameMax

        # Check if we should augment the schema for integers that may be null
        #
        if colnulls:
            for x in colnulls:
                Schema.colNVChk.append(x[0])
                Schema._add_col(x[1])

    # *************************************************************************
    # *                        _ r e v e r t 2 n u l l                        *
    # *************************************************************************
    @classmethod
    def _revert2null(cls, df, nilval):
        """Revert integer fields that originally cotained null to be so.

        Parameters
        ----------
        df : 'PANDAS dataframe'
            This is the dataframe which is to be manipulated into submission.
        nilval : 'string'
            This is the sequence that defines a "null" value.

        Raises
        ------
        FatalError
        """

        # If there are no null values at all, simply return.
        #
        if not Schema.colNVChk:
            return None

        # For each column that may have a null value, convert it to a string.
        # Then replace it's value with a null sequence if it should contain it.
        # Note that we need to get around flake8 as pandas only allows
        # comparisons to values and we need to base execution on a boolean.
        #
        trueval = True
        for cnum in Schema.colNVChk:
            cname = Schema.colNames[cnum]
            try:
                df[cname] = df[cname].apply(str)
            except Exception as ex:
                raise FatalError(ex, 'Unable to convert integer column',
                                 cname, 'to string')
            cisnil = cname + Schema._nilSuffx
            df.loc[df[cisnil] == trueval, cname] = nilval

    # *************************************************************************
    # *                           n o r m a l i z e                           *
    # *************************************************************************
    @classmethod
    def normalize(cls, df, nilval):
        """Normalize a PANDAS dataframe to correspond to the original schema.

        Parameters
        ----------
        df : 'pandas dataframe'
            The pandas dataframe to be normalized.
        nilval : 'string'
            This is the sequence that defines a "null" value.

        Notes
        - Normalization is defined as: a) null values are returned to integer
          that could have contained null values, and b) floating point values
          that represent fixed point data types are rounded to the original
          precision.
        """

        # If we had any integer column that may have contained null values,
        # make sure that rows in which null values occured are present.
        #
        if Schema.colNVChk:
            Schema._revert2null(df, nilval)

        # If we had any fixed point types are are being represented as floats,
        # format the floats using the original precision.
        #
        for cname, Schema._colFmtfp in Schema._colFPFmt:
            df[cname] = df[cname].map(Schema._format, na_action='ignore')

        # Format 32-bit floats as requested.
        #
        if Schema._f32fmt is not None and Schema._colFP32:
            Schema._colFmtfp = Schema._f32fmt
            for cname in Schema._colFP32:
                df[cname] = df[cname].map(Schema._format, na_action='ignore')

        # Format 64-bit floats as requested.
        #
        if Schema._f64fmt is not None and Schema._colFP64:
            Schema._colFmtfp = Schema._f64fmt
            for cname in Schema._colFP64:
                df[cname] = df[cname].map(Schema._format, na_action='ignore')

    # *************************************************************************
    # *                               s e t f p                               *
    # *************************************************************************
    @classmethod
    def setfp(cls, f32fmt, f64fmt):
        """Set output floating point precision.

        Parameters
        ----------
        f32fmt : 'string'
            The precision to use for 32-bit floats.
        f64fmt : 'string'
            The precision to use for 64-bit floats.
        """
        Schema._f32fmt = f32fmt
        Schema._f64fmt = f64fmt

    # *************************************************************************
    # *                          s e t _ s c h e m a                          *
    # *************************************************************************
    @classmethod
    def set_schema(cls, df):
        """Create a schema based on a pandas dataframe.

        Parameters
        ----------
        df : 'pandas dataframe'
            The pandas dataframe to use for the schema.
        """

        # Enter the schema into the _colSpecs array for later display.
        #
        ncols = 0
        Schema.colNames = []
        for cname in df:
            ncols += 1
            Schema._add_col(cname)
            ptype = df[cname].dtype
            if ptype == 'object':
                ptype = 'char'
            else:
                ptype = str(ptype)
            Schema._colSpecs.append([cname, '', ptype])

        # Finish up for display purposes
        #
        Schema._cNameMaw = Schema._cNameMax
        Schema.colOrigs = ncols

    # *************************************************************************
    # *                       v e r _ d a t a f r a m e                       *
    # *************************************************************************
    @classmethod
    def ver_dataframe(cls, df, blab):
        """Verify that a pandas dataframe corresponds to the specified schema.

        Parameters
        ----------
        df : 'pandas dataframe'
            The pandas dataframe to verify against the schema in this class.
        blab : 'bool'
            True if all arrors are to be printed; false prints only the first.

        Returns
        -------
        result : 'list'
            The list of column names in the original schema is returned if
            verification succeeded. Otherwise, a null list is returned.
        """

        # Make sure each column is in the schema with the correct type.
        #
        isaok = True
        ncols = 0
        for cname in df:
            ncols += 1
            try:
                stype = Schema.colTypes[cname]
                ptype = df[cname].dtype
                if ptype == 'object':
                    ptype = np.bytes_
                if stype != ptype:
                    ErrInfo.say("column '" + cname + "' has wrong datatype;",
                                "parquet is", str(ptype), "while schema is",
                                str(stype) + ".")
                    if not blab:
                        return []
                    isaok = False
            except KeyError:
                ErrInfo.say("parquet file contains non-schema column '"
                            + cname + "'.")
                if not blab:
                    return []
                isaok = False

        # Verify that column counts match
        #
        if ncols != len(Schema.colTypes):
            ErrInfo.say('Derived schema with', len(Schema.colTypes),
                        'cols does not match the data with', ncols, 'cols.')
            isaok = False

        # Return result
        #
        if isaok:
            return Schema.colNames[0:Schema.colOrigs]
        return []
