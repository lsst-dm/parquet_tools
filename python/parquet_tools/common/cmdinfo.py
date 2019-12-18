# *****************************************************************************
# *                            c m d i n f o . p y                            *
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
"""This module contains an argparse wrapper class that handles common
options for parquet_tools commands.
"""

__all__ = ['CmdInfo']

from parquet_tools.common.errhandlers import ErrInfo, ParmError


# *****************************************************************************
# *                               C m d I n f o                               *
# *****************************************************************************

class CmdInfo:
    """The CmdInfo class processes the command syntax and parses the
    command line using that syntax.
    """

    # Command specification
    #
    opt = None        # Instance of the argparse object

    # *************************************************************************
    # *                   p a r s e _ c o m m a n d l i n e                   *
    # *************************************************************************
    @classmethod
    def parse_commandline(cls, apobj):
        """Parse the command line using a supplied command definition.

        Parameters
        ----------
        apobj : 'argprase object'
               The argparse object encapsulating the command definition.

        Raises
        ------
        ParmError
            Raised whenever a command syntax error is encountered, including
            invalid parameter values.
        """

        # Parse the command line
        cls.opt = apobj.parse_args()

        # Propogate the debug flag if present.
        if hasattr(cls.opt, 'dbg'):
            ErrInfo.set_debug(cls.opt.dbg)

        # --sep: Make sure the seprator character is a single character
        if hasattr(cls.opt, 'sep') and len(cls.opt.sep) != 1:
            raise ParmError(3, 'sep value', cls.opt.sep)
