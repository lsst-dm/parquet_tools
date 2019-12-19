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
This module encapsulates valid convertable types.
"""

__all__ = ['TypeInfo']

import numpy as np


# *****************************************************************************
# *                              T y p e I n f o                              *
# *****************************************************************************

class TypeInfo:
    """table: Dictionary mapping SQL types to corresponding pandas types.
       np2sql: Dictionary mapping from pandas type to SQL type.
       isInt: Dictionary indicating whether or not an SQL type is an 'int'.
       okTypes: String of comma separated valied types use for help info.
    """

    table = {'int':     np.int32,   'short':    np.int16, 'long':  np.int64,
             'bigint':  np.int64,   'smallint': np.int16,
             'tinyint': np.int8,
             'int8':    np.int8,    'int32':    np.int32, 'int64': np.int64,
             'float':   np.float32, 'double':   np.float64,
             'float32': np.float32, 'float64':  np.float64,
             'char':    np.bytes_,  'varchar':  np.bytes_,
             'bool':    np.bool_,   'datetime': np.datetime64
             }

    isInt = {'int':     True,       'short':    True,     'long':  True,
             'bigint':  True,       'smallint': True,
             'int8':    True,       'int32':    True,     'int64': True
             }

    okTypes = ', '.join(sorted(list(table.keys())))
