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
command and its associated python modules.

This module provides various functions to make life somewhat easier:
einfo     - class to control error handling.
eprint    - print a message to stderr.
fatal     - print a numbered message to stderr and exit.
"""

__all__ = ['eprint', 'fatal']

import os
import sys
import traceback


# *****************************************************************************
# *                                 e i n f o                                 *
# *****************************************************************************

class einfo:
    "Simple class to handle various error."

    # Debugging wanted
    #
    doDebug = False   # True if a stack trace need upon fatal error


# *****************************************************************************
# *                                e p r i n t                                *
# *****************************************************************************

def eprint(*args, **kwargs):
    """Print to stderr.
    In:  args   - positional arguments to the print function.
         kwargs - keyword arguments to the print function.
    Out: Returns None.
    """

    # Determine the command name header
    #
    cmdname = os.path.basename(sys.argv[0]) + ':'

    print(cmdname, *args, file=sys.stderr, **kwargs)


# *****************************************************************************
# *                                 q u o t e                                 *
# *****************************************************************************

def quote(val):
    """Surround val with double quotes and return it.
    In:  A string value.
    Out: The string value surrouided by double quotes.
    """

    return '"' + val + '"'


# *****************************************************************************
# *                                 f a t a l                                 *
# *****************************************************************************

def fatal(enum, *argv):
    """Issue a fatal (i.e., we don't return) error message.
    In:  enum - the message number to use as a template.
         argv - substitution arguments.
    Out: This function exits with a non-zero return code.
    """

    # Unpack the substitution list, converting ints to strings.
    #
    tok = []
    for arg in argv:
        if isinstance(arg, int):
            arg = str(arg)
        tok.append(arg)
    tok.extend(('', '', ''))

    # Produce a traceback if debug is set on.
    #
    if einfo.doDebug:
        traceback.print_stack()

    # An enum of zero is reserved for exception printing
    #
    if enum == 0:
        eprint((' '.join(list(filter(bool, tok)))) + ';', sys.exc_info()[1])
        exit(99)

    # Format the message
    #
    if enum == 1:
        eprint(tok[0] + '.')
    elif enum == 2:
        eprint(tok[0], 'not specified.')
    elif enum == 3:
        eprint('Invalid', tok[0] + ',', quote(tok[1]) + '.')
    elif enum == 4:
        eprint('Column', quote(tok[0]), 'has an unknown type',
               quote(tok[1]) + '.')
    elif enum == 5:
        eprint(quote(tok[0]), 'and', quote(tok[1]), ' are mutaully exclusive.')
    elif enum == 6:
        eprint(tok[0], quote(tok[1]), tok[2] + '.')
    elif enum == 7:
        eprint(' '.join(list(filter(bool, tok))) + '.')
    else:
        eprint('Message', str(enum), 'not found.')

    # Exit using the message number as the return code
    #
    exit(enum)
