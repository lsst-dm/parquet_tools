# *****************************************************************************
# *                             s y n t a x . p y                             *
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

__all__ = ['syntax']

import argparse

from ..common.typeinfo import TypeInfo


# *****************************************************************************
# *                                s y n t a x                                *
# *****************************************************************************

def syntax():
    """Initialize the definition of the command and return an instance.

    Returns
    -------
    opt : 'argparse object'
        This is a fully initialized argparse object for the csv2pq command.
    """

    opt = argparse.ArgumentParser(add_help=True, allow_abbrev=False,
                                  usage='csv2pq [options] [infile [outfile]]',
                                  epilog="""The following schema coltypes are
                                  supported: %s. Also, decimal(<p>[,<s>]) is
                                  supported as a compatibly sized float
                                  or int.""" % TypeInfo.okTypes)

    opt.add_argument('--auto', action='store_true', default=False,
                     dest='ato',
                     help="Ignore type in schema file, use auto conversion.")

    opt.add_argument('--cchar', action='store', default=None,
                     metavar='C', dest='cmt',
                     help="Character designating a comment in infile.")

    opt.add_argument('--compress', action='store', default='snappy',
                     metavar='TYPE', dest='cmp', type=str,
                     choices=['none', 'snappy', 'gzip', 'lzo', 'brotli',
                              'lz4', 'zst'],
                     help="""The compression algorithm to use; where TYPE is:
                     brotli, gzip, LZO, LZ4, snappy (default), ZSTD, or none
                     to disable compression.""")

    opt.add_argument('--debug', action='store_true', default=False,
                     dest='dbg',
                     help="""Enable debug output (e.g. a stack trace on fatal
                     errors).""")

    opt.add_argument('--display', action='store_true', default=False,
                     help="""Display schema conversions. When specified,
                     infile and outfile are optional. If infile is present,
                     it is checked for consistency with the schema. When
                     outfile is present, infile is converted as well.""")

    opt.add_argument('--encoding', action='store', default='utf-8',
                     metavar='T', dest='enc',
                     help="""The input encoding: ascii or utf-8
                     (or synonym utf8). The default is utf-8.""")

    opt.add_argument('--flavor', action='store', default=None,
                     dest='flv', type=str, choices=['spark'],
                     help="""Output flavor. For spark compatibility specify
                     'spark'.""")

    opt.add_argument('--header', action='store_true', default=None,
                     dest='hdr',
                     help="""The infile contains a column name header as the
                     the first row.""")

    opt.add_argument('--intnan', action='store', default=0, type=int,
                     metavar='N', dest='nan',
                     help="""The integer value to use for 'null'
                     (default is 0).""")

    opt.add_argument('--nodict', action='store_false', default=True,
                     dest='dct',
                     help="""Do not use dictionary encoding for the
                     output file,""")

    opt.add_argument('--null', action='store', default='\\N',
                     metavar='SEQ', dest='nil',
                     help="""the csv sequence designating a null value
                     (default \\N).""")

    opt.add_argument('--replace', action='store_true', default=False,
                     dest='rep',
                     help="""Replace the existing outfile.""")

    opt.add_argument('--rgsize', action='store', default=None, type=int,
                     metavar='N', dest='rgs',
                     help="""The number of rows per row group
                     (default all rows).""")

    opt.add_argument('--schema', action='store', default=None,
                     metavar='SFN',
                     help="""The file that describes the schema. The format is:
                     <colname> <coltype> [NOT NULL][,]""")

    opt.add_argument('--sep', action='store', default=',',
                     metavar='C', dest='sep',
                     help="""The column field separator character
                     (default comma).""")

    opt.add_argument('--skip', action='store_true', default=False,
                     dest='skp',
                     help="""Skip conversion if the outfile already exists.
                     If --replace is also specified, all but the last existing
                     outfile is skipped, at which point --replace goes into
                     effect.""")

    opt.add_argument('--verbose', action='store_true', default=False,
                     dest='blab',
                     help="Produce additional explanatory output.")

    opt.add_argument('infile', action='store', default='', nargs='?',
                     help="""The file name to be converted. Specifying a single
                     dash reads stdin for a newline separated list of files
                     to be converted.""")

    opt.add_argument('outfile', action='store', default='', nargs='?',
                     help="""The output file name unless infile is a dash;
                     then it must be a template of the form
                     '[=dir/][-sfx]+sfx'. For each infile its directory path
                     is replaced by 'dir', if specified; '-sfx' removes the
                     specified infile suffix and '+sfx' adds the specified
                     suffix to produce the outfile name.""")

    return opt
