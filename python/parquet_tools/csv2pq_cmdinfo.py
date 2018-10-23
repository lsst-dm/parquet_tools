# *****************************************************************************
# *                     c s v 2 p q _ c m d i n f o . p y                     *
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
"""This module contains variables describing the command line invocation.

This module contains all the variables used to communicate the requirements
csv2pq command invocation across all related modules.
"""

from collections import defaultdict


# *****************************************************************************
# *                               C m d I n f o                               *
# *****************************************************************************

class CmdInfo:

    # Column information
    #
    cNameMax = 0
    colNames = []
    colNVChk = []
    colSpecs = []
    colTypes = {}

    # File information
    #
    fIN = []
    fOUT = []

    # Options
    #
    OPT = defaultdict(lambda: '')
    OPT['ato'] = False       # --auto is off
    OPT['cmp'] = 'snappy'    # --compress snappy
    OPT['cmt'] = None        # --cchar is off
    OPT['dct'] = True        # --nodict is off
    OPT['dna'] = False       # --null \N
    OPT['flv'] = None        # --flavor not specified
    OPT['hdr'] = None        # --hdr not specified
    OPT['ign'] = None        # Number of rows to ignore if --hdr specified
    OPT['map'] = False       # --mmap not specified
    OPT['nan'] = '0'         # --intnan value
    OPT['nil'] = '\\N'       # --null value
    OPT['rep'] = False       # --replace not specified
    OPT['rgs'] = None        # --rgsize value
    OPT['sep'] = ','         # --sep value
    OPT['skp'] = False       # --skip not specified
    OPT['skr'] = False       # --skip and --replace not in effect
    OPT['ver'] = False       # --verify not specified

    blab = False             # --verbose not specified
    dbg = False              # --debug not specified
