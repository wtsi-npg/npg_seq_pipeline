#!/bin/bash

set -e -x

export WTSI_NPG_iRODS_Test_IRODS_ENVIRONMENT_FILE=$HOME/.irods/irods_environment.json

# Perl environment has been already cached for this package.
./Build test --verbose

if [ $? -ne 0 ]; then
    echo ===============================================================================
    cat tests.log
    echo ===============================================================================
fi
