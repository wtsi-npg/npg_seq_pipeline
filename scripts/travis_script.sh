#!/bin/bash

set -e -x

perl Build.PL
./Build clean

touch t/bin/software/solexa/bin/blat; chmod +x t/bin/software/solexa/bin/blat
touch t/bin/software/solexa/bin/bamtofastq; chmod +x t/bin/software/solexa/bin/bamtofastq
touch t/bin/software/solexa/bin/norm_fit; chmod +x t/bin/software/solexa/bin/norm_fit 

./Build test --verbose

if [ $? -ne 0 ]; then
    echo ===============================================================================
    cat tests.log
    echo ===============================================================================
fi
