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
The errhandlers.py module contains utility classes used by parquet_tools
to process various errors that may be encountered:

ErrInfo    - class to control error handling and printing to stderr.
FatalError - Custom exception class to process any exception.
ParmError  - Custom exception class to handle non-exception errors.
"""

__all__ = ['ErrInfo', 'FatalError', 'ParmError']

import os
import sys
import traceback


# *****************************************************************************
# *                               E r r I n f o                               *
# *****************************************************************************

class ErrInfo:
    """Simple class to control error messages and debugging. This class is
    inherited by our custom exception classes and is also be used to print
    messages prefixed by our command name to stderr.
    """

    cmdname = os.path.basename(sys.argv[0]) + ':'
    doDebug = False   # True if a stack trace needed upon fatal error

    # *************************************************************************
    # *                                 s a y                                 *
    # *************************************************************************
    @classmethod
    def say(cls, *args, **kwargs):
        """Print a message to stderr.

        Parameters
        ----------
        args : 'varies'
            The are the positional argumentgs to the print function.
        kwargs : 'varies'
            The are the positional argumentgs to the print function.
        """

        # Simply print the message prefixed by out command name.
        #
        print(cls.cmdname, *args, file=sys.stderr, **kwargs)

    # *************************************************************************
    # *                           s e t _ d e b u g                           *
    # *************************************************************************
    @classmethod
    def set_debug(cls, val):
        """Set the debug parameter for exceptions.

        Parameters
        ----------
        val : 'bool'
            When True, a traceback is printed when a fata exception occurs.
        """

        cls.doDebug = val


# *****************************************************************************
# *                            F a t a l E r r o r                            *
# *****************************************************************************

class FatalError(Exception, ErrInfo):
    """Handle fatal exception errors.

    Parameters
    ----------
    ex : 'exception object'
        The exception object that caused the fatal error.
    args : 'print style arguments"
        Printable tokens providing context for the error.
    """

    def __init__(self, ex, *args):

        # Produce a stack trace if debuggging is enabled.
        #
        if ErrInfo.doDebug:
            traceback.print_exception(type(ex), ex, ex.__traceback__)

        # Construct an informative message.
        #
        msg = ' '.join(str(x) for x in args) + '; fatal ' +\
              type(ex).__name__ + ' exception: ' + str(ex)

        # Initialize the exception object with the message.
        #
        Exception.__init__(self, msg)

    def __context__(self):
        pass


# *****************************************************************************
# *                             P a r m E r r o r                             *
# *****************************************************************************

class ParmError(Exception, ErrInfo):
    """Handle fatal processing errors.

    Parameters
    ----------
    msgnum : 'int'
        The numeric message code to format an error message using args.
    args : 'print style arguments"
        Printable tokens providing context for the error.

    Notes
    _____
    - The following message format numbers are defined:
      1: <joins all tokens with a space>.
      2: :{0} not specified.
      3: Invalid {0}, "{1}".
      4: Column "{0}" has an unsupported type "{1}".
      5: "{0}" and "{1}" are mutually exclusive.
      6: {0} "{1}" {2}.
    = Any number not defined above produces the message:
      Message {msgnum} not found.
    """

    def __init__(self, msgnum, *args):

        # Print a stack trace if debugging is enabled.
        #
        if ErrInfo.doDebug:
            traceback.print_stack()

        # Message format 1 is special. It only stringifies the tokens.
        #
        if msgnum == 1:
            msg = ' '.join(str(x) for x in args)

        # Unpack the message tokens, converting everything to a string.
        #
        else:
            tok = []
            for x in args:
                tok.append(str(x))
            tok.extend(('', '', ''))

            # Format the message according the the number given.
            #
            if msgnum == 2:
                msg = tok[0] + ' not specified'
            elif msgnum == 3:
                msg = 'Invalid ' + tok[0] + ',' + ' "' + tok[1] + '"'
            elif msgnum == 4:
                msg = 'Column "' + tok[0] + '" has an unsupported type "' +\
                       tok[1] + '"'
            elif msgnum == 5:
                msg = '"' + tok[0] + '" and "' + tok[1] + \
                      '" are mutually exclusive'
            elif msgnum == 6:
                msg = tok[0] + ' "' + tok[1] + '" ' + tok[2]
            else:
                msg = 'Message ' + str(msgnum) + ' not found'

        # Initialize the exception object with the message.
        #
        Exception.__init__(self, msg + '.')

    def __context__(self):
        pass
