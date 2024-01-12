#!/bin/bash

set -e -u -x

WSI_NPG_GITHUB_URL=${WSI_NPG_GITHUB_URL:=https://github.com/wtsi-npg}
WSI_NPG_BUILD_BRANCH=${WSI_NPG_BUILD_BRANCH:=devel}

# The first argument is the install base for NPG modules, enabling them to be
# installed independently of CPAN dependencies. E.g. for cases where we want
# different caching behaviour.
NPG_ROOT="$1"
shift

repos=""
for repo in "$@" ; do
    cd /tmp

    # Clone deeper than depth 1 to get the tag even if something has been already
    # committed over the tag
    git clone --branch master --depth 3 "$WSI_NPG_GITHUB_URL/${repo}.git" "${repo}.git"
    cd "/tmp/${repo}.git"

    # Shift off master to appropriate branch (if possible)
    git ls-remote --heads --exit-code origin "$WSI_NPG_BUILD_BRANCH" && \
	    git pull origin "$WSI_NPG_BUILD_BRANCH" && \
	    echo "Switched to branch $WSI_NPG_BUILD_BRANCH"
    repos="$repos /tmp/${repo}.git"
done

# Install CPAN dependencies. The src libs are on PERL5LIB because of
# circular dependencies. The blibs are on PERL5LIB because the package
# version, which cpanm requires, is inserted at build time. They must
# be before the libs for cpanm to pick them up in preference.
for repo in $repos
do
    export PERL5LIB="$PERL5LIB:$repo/blib/lib:$repo/lib"
done

for repo in $repos
do
    cd "$repo"
    cpanm --quiet --notest --installdeps .
    perl Build.PL
    ./Build
done

# Finally, bring any common dependencies up to the latest version and
# install
for repo in $repos
do
    cd "$repo"
    cpanm --quiet --notest --installdeps .
    ./Build install --install-base "$NPG_ROOT"
done
