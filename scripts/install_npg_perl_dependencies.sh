#!/bin/bash

set -e -x

# iRODS test server is not set up, so tests that require it will
# be skipped

#setting environment variables
WTSI_NPG_GITHUB_URL=$1
WTSI_NPG_CONDA_REPO=$2
WTSI_NPG_BUILD_BRANCH=$3

eval $(perl -I ~/perl5ext/lib/perl5/ -Mlocal::lib=~/perl5ext)
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
    cpanm  --quiet --notest --installdeps .
    perl Build.PL
    ./Build
done

# Finally, bring any common dependencies up to the latest version and
# install

# to set liblocal for perl5_npg
eval $(perl -I ~/perl5ext/lib/perl5/ -Mlocal::lib=~/perl5npg)

for repo in $repos
do
    cd $repo
    cpanm  --quiet --notest --installdeps .
    ./Build install
done
cd
