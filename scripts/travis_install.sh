#!/bin/bash

set -e -x

# iRODS test server is not set up, so tests that require it will
# be skipped

#setting environment variables
WTSI_NPG_GITHUB_URL=$1
WTSI_NPG_CONDA_REPO=$2
WTSI_NPG_BUILD_BRANCH=$3

#tmp
wget -qO - https://cpanmin.us | /usr/bin/perl - --sudo App::cpanminus #installing cpanminus to install modules

# install conda client
wget "https://repo.continuum.io/miniconda/Miniconda2-4.6.14-Linux-x86_64.sh" -O miniconda.sh;
chmod +x miniconda.sh;
./miniconda.sh -b  -p $HOME/miniconda;
export PATH=$HOME/miniconda/bin:$PATH;

# install baton from our conda channel
conda install --yes --channel ${WTSI_NPG_CONDA_REPO} --channel default --mkdir --prefix $HOME/miniconda/miniconda/baton baton;

# install samtools from our conda channel
# this is needed for our basic IRODS Perl wrapper to work
conda install --yes --channel ${WTSI_NPG_CONDA_REPO} --channel default --mkdir --prefix $HOME/miniconda/miniconda/samtools samtools;
echo "1:1 complete conda install"
# The default build branch for all repositories. This defaults to
# TRAVIS_BRANCH unless set in the Travis build environment.
WTSI_NPG_BUILD_BRANCH=${WTSI_NPG_BUILD_BRANCH:=$TRAVIS_BRANCH} #TODO this is missing a value...current github branch to add here instead of travis

# CPANM install and C compiler
cpanm --local-lib=~/perl5 local::lib && eval $(perl -I ~/perl5/lib/perl5/ -Mlocal::lib)
cpanm --quiet --notest Alien::Tidyp # For npg_tracking
cpanm --quiet --notest Module::Build
cpanm --quiet --notest Net::SSLeay
cpanm --quiet --notest https://github.com/chapmanb/vcftools-cpan/archive/v0.953.tar.gz # for npg_qc
echo "1:2 complete cpanm modules"

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

echo "1:3 complete repo branch clones"
# Install CPAN dependencies. The src libs are on PERL5LIB because of
# circular dependencies. The blibs are on PERL5LIB because the package
# version, which cpanm requires, is inserted at build time. They must
# be before the libs for cpanm to pick them up in preference.

for repo in $repos
do
    export PERL5LIB=$repo/blib/lib:$PERL5LIB:$repo/lib
done

echo "1:4 complete perl5lib export"
for repo in $repos
do
    cd $repo
    echo "2:0: install deps for : $repo"
    #TODO getting cpanm .
    #wget -qO - https://cpanmin.us | /usr/bin/perl - --sudo App::cpanminus # try install cpan for each repo
    
    #cpanm UUID 2>&1 | tee tmp.log
    cpanm  --quiet --notest --installdeps .
    
    #perl -nle 'if (/failed/){ if ( m{(/home\S+/build.log)} ) { print $1; } }' tmp.log | xargs -r cat
    perl Build.PL
    ./Build
done

echo FINISHED FIRST CPAN INSTALLDEPS LOOP

# Finally, bring any common dependencies up to the latest version and
# install
for repo in $repos
do
    cd $repo
    cpanm  --quiet --notest --installdeps .
    ./Build install
done

echo "this is current perl5lib: $PERL5LIB"

cd
echo "This is current locations: \n"
pwd

#seperate miniconda and perl
#break up internal and external perl dependencies
#cache conda + external perl 




