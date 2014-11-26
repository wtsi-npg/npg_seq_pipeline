#########
# Author:        rmp
# Maintainer:    $Author: mg8 $
# Created:       2007-10
# Last Modified: $Date: 2013-04-08 11:03:18 +0100 (Mon, 08 Apr 2013) $
# Id:            $Id: 00-pod.t 16979 2013-04-08 10:03:18Z mg8 $
# $HeadURL: svn+ssh://svn.internal.sanger.ac.uk/repos/svn/new-pipeline-dev/npg-pipeline/trunk/t/00-pod.t $
#
use strict;
use warnings;
use Test::More;

our $VERSION = do { my @r = (q$LastChangedRevision: 16979 $ =~ /\d+/mxg); sprintf '%d.'.'%03d' x $#r, @r };

eval "use Test::Pod 1.00"; ## no critic
plan skip_all => "Test::Pod 1.00 required for testing POD" if $@;
all_pod_files_ok();
