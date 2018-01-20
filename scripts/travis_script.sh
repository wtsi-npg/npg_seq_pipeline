#!/bin/bash

set -e -x

touch t/bin/software/solexa/bin/blat; chmod +x t/bin/software/solexa/bin/blat
touch t/bin/software/solexa/bin/bamtofastq; chmod +x t/bin/software/solexa/bin/bamtofastq
touch t/bin/software/solexa/bin/norm_fit; chmod +x t/bin/software/solexa/bin/norm_fit 

export WTSI_NPG_iRODS_Test_IRODS_ENVIRONMENT_FILE=$HOME/.irods/irods_environment.json

# Perl environment has been already cached for this package.
./Build test --verbose

if [ $? -ne 0 ]; then
    echo ===============================================================================
    cat tests.log
    echo ===============================================================================
fi
