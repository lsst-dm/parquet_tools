# *****************************************************************************
# *                       c s v 2 p q _ u t i l s . p y                       *
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
The csv2pq_utils.py module contains utility functions used by the csv2pq
command and its assiciated python modules.

This module provides various functions to make life somewhat easier:
ePrint    - print a message to stderr.
Fatal     - print a numbered message to stderr and exit.
getVal    - get the next value from the command line argument vector.
traceBack - print a stack traceback.
"""

from __future__ import print_function

import sys
import traceback


# *****************************************************************************
# *                                e P r i n t                                *
# *****************************************************************************

def ePrint(*args, **kwargs):
    "Print to stderr."

    print(*args, file=sys.stderr, **kwargs)


# *****************************************************************************
# *                             t r a c e B a c k                             *
# *****************************************************************************

def traceBack():
    "Produce a readable track back."

#   exc_type, exc_value, exc_traceback = sys.exc_info()
    traceback.print_stack()


# *****************************************************************************
# *                                 F a t a l                                 *
# *****************************************************************************

def Fatal(enum, p1="", p2="", p3="", p4="", p5=""):
    "Issue a fatal (i.e., we don't return) error message."
#   exc_type, exc_value, exc_traceback = sys.exc_info()
#   traceback.print_stack()
#   print(repr(traceback.extract_stack()))

    # An enum of zero is reserved for exception printing
    #
    if enum == 0:
        ePrint('csv2pq: ' + p1 + '; ', p2)
        exit(99)

    # Format the message
    #
    if enum == 1:
        msg = p1 + '.'
    elif enum == 2:
        msg = p1 + ' not specified.'
    elif enum == 3:
        msg = 'Invalid ' + p1 + ' "' + p2 + '".'
    elif enum == 4:
        msg = 'Column "' + p1 + '" has an unknown type "' + p2+'".'
    elif enum == 5:
        msg = '"' + p1 + '" and "' + p2 + '" are mutaully exclusive.'
    else:
        msg = 'Message ' + str(enum) + ' not found.'

    # Print the message and exit
    #
    ePrint('csv2pq:', msg)
#   print_exception(*sys.exc_info())
    exit(enum)


# *****************************************************************************
# *                                g e t V a l                                *
# *****************************************************************************

def getVal(item, argv):
    "Obtain a value, complain if missing."

    if not argv or argv[0][0] == '-':
        Fatal(2, item+' value')
    return argv.pop(0)
