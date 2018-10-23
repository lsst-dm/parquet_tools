#!/usr/bin/env python3
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
This program excersises the csv2pq command to make sure it works as
expected.
'''

import os
import subprocess
import unittest


# *****************************************************************************
# *                              T e s t I n f o                              *
# *****************************************************************************

class TestInfo:

    hush = True   # Set to False to see all test output in real time

    inFiles = ["data/testfile1.csv", "data/testfile2.csv"]
    inSchem = "data/test.schema"
    outFile = ["/tmp/testfile1.pq", "/tmp/testfile2.pq"]
    numTest = 1


# *****************************************************************************
# *                              c h k 4 F i l e                              *
# *****************************************************************************

def chk4File(flist, rm=False):
    "Verify that created output files actually exist."

    for file in flist:
        if not os.path.exists(file):
            print('Output file "' + file + '" not found.')
            return False
        if (rm): os.unlink(file)
    return True


# *****************************************************************************
# *                               r u n T e s t                               *
# *****************************************************************************

def runTest(cmd, indata=None):
    "Run a command to test it."

    nTest = str(TestInfo.numTest)
    TestInfo.numTest += 1
    print("Running test", nTest + ':', cmd)

    # We would use the newfangled subprocess options to do this but those
    # are broken in various versions of python 3 so are undependable.
    #
    if indata:
        rPipe, wPipe = os.pipe()
        os.write(wPipe, bytes(indata, 'ascii'))
        os.close(wPipe)
    else:
        rPipe = None

    try:
        status = subprocess.run(cmd.split(), stdin=rPipe)
    except Exception as err:
        print('Test', nTest, 'ended with an exception;', err)
        return None

    print("Test", nTest, 'ended with RC =', status.returncode)
    print('-' * 79)
    return status.returncode == 0


# *****************************************************************************
# *                          s e t u p _ m o d u l e                          *
# *****************************************************************************

def setup_module():
    "Setup environment for testing."

    # Make sure all the files we need are present
    #
    for file in TestInfo.inFiles + [TestInfo.inSchem]:
        if not os.path.exists(file):
            raise Exception('Required file "' + file + '" not found.')
        if not os.path.isfile(file):
            raise Exception('Required file "' + file + '" is not a file.')

    # Start with a clean slate
    #
    for file in TestInfo.outFile:
        try:
            os.unlink(file)
        except Exception:
            pass

    # Get the current working directory for subsequent envar settings
    #
    curDir = os.path.dirname(os.path.abspath(__file__))

    # Augment python path for fidning modules
    #
    if 'PYTHONPATH' in os.environ:
        oldEnv = os.environ['PYTHONPATH']
    else:
        oldEnv = ''
    os.environ['PYTHONPATH'] = curDir + '/../python/parquet_tools:' + oldEnv

    # Augment system path for finding executables
    #
    if 'PATH' in os.environ:
        oldEnv = os.environ['PATH']
    else:
        oldEnv = ''
    os.environ['PATH'] = '../bin.src:' + oldEnv


# *****************************************************************************
# *                             t e s t _ h e l p                             *
# *****************************************************************************

def test_help(capsys):
    "Test the --help option."
    if TestInfo.hush:
        assert runTest('csv2pq --help')
    else:
        with capsys.disabled():
            assert runTest('csv2pq --help')


# *****************************************************************************
# *                              t e s t _ 2 p q                              *
# *****************************************************************************

def test_2pq(capsys):
    "Test converting a csv file to a parquet file using a schema."

    cmd = 'csv2pq --replace --verify --schema ' + TestInfo.inSchem + ' ' +\
          TestInfo.inFiles[0] + ' ' + TestInfo.outFile[0]
    if TestInfo.hush:
        assert runTest(cmd)
    else:
        with capsys.disabled():
            assert runTest(cmd)

    assert chk4File([TestInfo.outFile[0]])


# *****************************************************************************
# *                              t e s t _ 3 p q                              *
# *****************************************************************************

def test_3pq(capsys):
    "Test converting multiple input files."

    inD = '\n'.join(TestInfo.inFiles)
    cmd = 'csv2pq --skip --schema ' + TestInfo.inSchem + ' - =/tmp/-csv+pq'
    if TestInfo.hush:
        assert runTest(cmd, indata=inD)
    else:
        with capsys.disabled():
            assert runTest(cmd, indata=inD)

    assert chk4File(TestInfo.outFile, rm=True)
