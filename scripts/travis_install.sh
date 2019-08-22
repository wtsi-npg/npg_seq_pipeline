#!/bin/bash

set -e -x

# iRODS test server is not set up, so tests that require it will
# be skipped

# install conda client
wget "https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh" -O miniconda.sh;
chmod +x miniconda.sh;
./miniconda.sh -b  -p /home/travis/miniconda;
export PATH=/home/travis/miniconda/bin:$PATH;

# install baton from our conda channel
conda install --yes --channel ${WTSI_NPG_CONDA_REPO} --override-channels --mkdir --prefix /home/travis/miniconda/baton baton;

# install samtools from our conda channel
# this is needed for our basic IRODS Perl wrapper to work
conda install --yes --channel ${WTSI_NPG_CONDA_REPO} --override-channels --mkdir --prefix /home/travis/miniconda/samtools samtools;

# The default build branch for all repositories. This defaults to
# TRAVIS_BRANCH unless set in the Travis build environment.
WTSI_NPG_BUILD_BRANCH=${WTSI_NPG_BUILD_BRANCH:=$TRAVIS_BRANCH}

# CPAN
cpanm --quiet --notest Alien::Tidyp # For npg_tracking
cpanm --quiet --notest Module::Build
cpanm --quiet --notest Net::SSLeay
cpanm --quiet --notest https://github.com/chapmanb/vcftools-cpan/archive/v0.953.tar.gz # for npg_qc

# WTSI NPG Perl repo dependencies
repos=""
for repo in perl-dnap-utilities perl-irods-wrap ml_warehouse npg_ml_warehouse npg_tracking npg_seq_common npg_qc; do
    cd /tmp
    # Always clone master when using depth 1 to get current tag
    git clone --branch master --depth 1 ${WTSI_NPG_GITHUB_URL}/${repo}.git ${repo}.git
    cd /tmp/${repo}.git
    # Shift off master to appropriate branch (if possible)
    git ls-remote --heads --exit-code origin ${WTSI_NPG_BUILD_BRANCH} && git pull origin ${WTSI_NPG_BUILD_BRANCH} && echo "Switched to branch ${WTSI_NPG_BUILD_BRANCH}"
    repos=$repos" /tmp/${repo}.git"
done

# Install CPAN dependencies. The src libs are on PERL5LIB because of
# circular dependencies. The blibs are on PERL5LIB because the package
# version, which cpanm requires, is inserted at build time. They must
# be before the libs for cpanm to pick them up in preference.

for repo in $repos
do
    export PERL5LIB=$repo/blib/lib:$PERL5LIB:$repo/lib
done

for repo in $repos
do
    cd $repo
    cpanm --quiet --notest --installdeps .
    perl Build.PL
    ./Build
done

# Finally, bring any common dependencies up to the latest version and
# install
for repo in $repos
do
    cd $repo
    cpanm --quiet --notest --installdeps .
    ./Build install
done

cd $TRAVIS_BUILD_DIR




