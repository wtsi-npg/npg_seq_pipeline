#!/bin/bash

set -e -x

# workaround for iRODS buffer overflow
# see https://github.com/travis-ci/travis-ci/issues/5227
sudo hostname "$(hostname | cut -c1-63)"
sed -e "s/^\\(127\\.0\\.0\\.1.*\\)/\\1 $(hostname | cut -c1-63)/" /etc/hosts > /tmp/hosts
sudo mv /tmp/hosts /etc/hosts

sudo apt-get update -qq
sudo addgroup solexa
U=`whoami`
sudo adduser $U solexa

wget "https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh" -O miniconda.sh;
chmod +x miniconda.sh;
./miniconda.sh -b  -p /home/travis/miniconda;

