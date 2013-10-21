#!/usr/bin/env bash

# Installs and Runs tests on this python project inside a virtual env. The
# tests run include:
#
# * Nose Unit Tests
# * Pyflakes
# * PEP8

set -e

PACKAGE="celery_tasks_ctl"
TEST_RESULTS_DIR=".test_results"
PIP_CACHE_PATH="~/.pip_download_cache"

log() {
    echo -e "`date +'%F %T'` - $@"
}

log "Deactivating virtualenv if in one"
deactivate || true

log "Removing old test results and creating new folder"
rm -rf $TEST_RESULTS_DIR || true
mkdir $TEST_RESULTS_DIR

log "Setting 'PIP_DOWNLOAD_CACHE' to '${PIP_CACHE_PATH}'"
export PIP_DOWNLOAD_CACHE="${PIP_CACHE_PATH}"

log "Creating a virtual environment"
virtualenv .venv

source .venv/bin/activate
# Remove $PACKAGE.egg-info as sometimes zip files get this error:
# zipimport.ZipImportError: bad local file header in...
rm -rf $PACKAGE.egg-info | true

log "Install packages needed for testing"
pip install nose pyflakes pep8

log "Installing project in the virtual environment"
./setup.py install

log "Running unit tests with nose"
UNIT_TEST_RC=0
.venv/bin/python .venv/bin/nosetests || UNIT_TEST_RC=$?

log "Running Pyflakes static code checker"
PYFLAKE_RC=0
.venv/bin/python .venv/bin/pyflakes $PACKAGE 1>$TEST_RESULTS_DIR/pyflakes.txt \
    || PYFLAKE_RC=$?
cat $TEST_RESULTS_DIR/pyflakes.txt

log "Running PEP8 style guide checker"
NUM_PEP8_FAILURES=0
# Count of number of errors goes to std_err so save to tmp file
.venv/bin/python .venv/bin/pep8 --count $PACKAGE 2>.pep8count \
    | tee $TEST_RESULTS_DIR/pep8.txt
NUM_PEP8_FAILURES=`cat .pep8count`
rm .pep8count
if [ -z $NUM_PEP8_FAILURES ]; then
    NUM_PEP8_FAILURES=0
fi

UNIT_TEST_STATUS="Passed"
if [ $UNIT_TEST_RC -ne "0" ]; then
    UNIT_TEST_STATUS="Failed"
fi

PYFLAKE_STATUS="Passed"
if [ $PYFLAKE_RC -ne "0" ]; then
    PYFLAKE_STATUS="Failed"
fi

PEP8_STATUS="Passed"
if [ $NUM_PEP8_FAILURES -ne "0" ]; then
    PEP8_STATUS="Failed with $NUM_PEP8_FAILURES failures"
fi

log "*** Summary ***"
log "Unit Tests: $UNIT_TEST_STATUS"
log "Pyflakes: $PYFLAKE_STATUS"
log "PEP8: $PEP8_STATUS"

exit `expr $UNIT_TEST_RC + $PYFLAKE_RC + $NUM_PEP8_FAILURES`
