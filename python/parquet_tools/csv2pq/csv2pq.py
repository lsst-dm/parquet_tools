#!/usr/bin/env python3
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
'''
Usage: csv2pq [options] [infile [outfile]]

Specify 'csv2pq --help' to get the a description of what is needed.
'''

# *****************************************************************************
# *                                c s v 2 p q                                *
# *****************************************************************************

import sys

import pandas as pd
import pyarrow as pa

import pyarrow.parquet as pq

from parquet_tools.csv2pq.syntax import syntax

from parquet_tools.csv2pq.csv2csv import Csv2Csv

from parquet_tools.common.cmdinfo import CmdInfo

from parquet_tools.common.iofiles import IOFiles

from parquet_tools.common.schema import Schema

from parquet_tools.common.errhandlers import ErrInfo, FatalError, ParmError


# *****************************************************************************
# *                                c o n f i g                                *
# *****************************************************************************

def config():
    """Parse command line options and verify correctness.

    Raises
    ------
    FatalError
    ParmError

    Returns
    -------
    result : 'bool'
        True indicaes that there are filed that to be converted; otherwise,
        there is nothing to convert.

    Notes
    -----
    - This function uses information contained in the CmdInfo class. The
      CmdInfo class public variables are modified to reflect the processing
      that needs to occur.
    """

    # Parse all options and positional arguments
    #
    CmdInfo.parse_commandline(syntax())

    # --cchar: Make sure the character is a single character
    #
    if CmdInfo.opt.cmt and len(CmdInfo.opt.cmt) != 1:
        raise ParmError(3, 'cchar value', CmdInfo.opt.cmt)

    # --compress: Make sure it is all in capital letters.
    #
    CmdInfo.opt.cmp = CmdInfo.opt.cmp.upper()

    # --header: Adjust header option for later processing and add ttribute
    # indicating how many rows need to be skipped.
    #
    if CmdInfo.opt.hdr:
        if CmdInfo.schema is not None:
            CmdInfo.opt.hdr = None
            setattr(CmdInfo, 'skipRows', 1)
        else:
            CmdInfo.opt.hdr = 0
            setattr(CmdInfo, 'skipRows', None)

    # Resolve the input file. If none, then a schema --display must be in
    # effect and no input files are required.
    #
    if not IOFiles.get_infiles(CmdInfo.opt.infile):
        if not CmdInfo.opt.display or not CmdInfo.opt.schema:
            raise ParmError(2, 'Input file')

    # Process any output file specification. This may raise an exception.
    #
    IOFiles.get_outfiles(CmdInfo.opt.outfile)

    # Process the schema file.
    #
    if not CmdInfo.opt.schema:
        if CmdInfo.opt.display:
            raise ParmError(2, 'Unable to do --display because schema')
    else:
        Schema.get_schema(CmdInfo.opt.schema, CmdInfo.opt.ato)
        if CmdInfo.opt.display:
            Schema.display_csv2pq()

    # Return indicating whether any processing is needed
    #
    return len(IOFiles.fIN) > 0


# *****************************************************************************
# *                               c o n v e r t                               *
# *****************************************************************************

def convert(input, outfile):
    """Convert a csv file to a parquet file.

    Parameters
    ----------
    infile : 'string' or 'object'
        Either the path to the input csv file or an IO object that
        contains a read() method. Specifically, it will be a csv2pq
        handler object defined in csv2pq.handler.py.
    outfile : 'string'
        The path to use for the output file. This may be a null string.

    Raises
    ------
    FatalError
    """

    # Read the csv file and convert it to a pandas dataframe.
    #
    try:
        df = pd.read_csv(input, header=CmdInfo.opt.hdr,
                         names=Schema.colNames,
                         sep=CmdInfo.opt.sep, na_values=CmdInfo.opt.nil,
                         keep_default_na=False, encoding=CmdInfo.opt.enc,
                         dtype=Schema.colTypes, skiprows=CmdInfo.skipRows)
    except Exception as exc:  # We don't care what kind it is
        raise FatalError(exc, 'Unable to create pandas dataframe')

    # If there is no output file then user just wanted to make sure we could
    # convert this csv file. So, we are done.
    #
    if not outfile:
        return

    # Convert dataframe to a pyarrow table
    #
    try:
        table = pa.Table.from_pandas(df)
    except Exception as exc:  # We don't care what kind it is
        raise FatalError(exc, 'Unable to create arrow table')

    # Now write out the dataframe as a parquet file (df.to_parquet missing)
    #
    try:
        pq.write_table(table, outfile,
                       compression=CmdInfo.opt.cmp,
                       flavor=CmdInfo.opt.flv,
                       row_group_size=CmdInfo.opt.rgs,
                       use_dictionary=CmdInfo.opt.dct)
    except Exception as exc:  # We don't care what kind it is
        raise FatalError(exc, 'Unable to create parquet file')


# *****************************************************************************
# *                          M a i n   P r o g r a m                          *
# *****************************************************************************

def main():
    """Sequence the execution of csv2pq.

    Notes
    ----_
    - This function exits the from with a non-zero return code upon failure.
    """

    # Make sure we have some arguments
    #
    if len(sys.argv) < 2:
        ErrInfo.say('No arguments specified; use --help option for',
                    'usage information.')
        exit(1)

    # All of the following either works or raises an exception.
    #
    try:
        # Parse the command line and determine what to do.
        #
        if not config():
            exit(0)

        # Prepare for processing
        #
        IOFiles.set_options(CmdInfo.opt.blab, CmdInfo.opt.skp, CmdInfo.opt.rep)
        Csv2Csv.setup(CmdInfo, Schema)
        needproc = len(Schema.colNVChk) > 0 or Schema.colOrigs > 0

        # Process all input files
        #
        infile, outfile = IOFiles.get_filepair()
        while infile:

            # Indicate what we are doing
            #
            if CmdInfo.opt.blab:
                if outfile:
                    ErrInfo.say("Converting", infile, "to", outfile, "...")
                else:
                    ErrInfo.say("Processing", infile, '...')

            # Check if we need to manually handle nulls for integer columns.
            # In any case, simply convert the input be it a file of i/o object.
            #
            if needproc:
                infile = Csv2Csv(infile)
            convert(infile, outfile)

            # Get next pair to convert
            #
            infile, outfile = IOFiles.get_filepair()

    # Catch any failures.
    #
    except (FatalError, ParmError) as exc:
        ErrInfo.say(exc)
        exit(99)


# Generally, in the Unix world, one should not use python modules as commands
# but the pythonic world sems to differ on that one. So, we allow it here.
#
if __name__ == '__main__':
    main()
