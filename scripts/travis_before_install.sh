#!/bin/bash

set -e -x

sudo apt-get update -qq
sudo addgroup solexa
U=`whoami`
sudo adduser $U solexa

