#########
# Author:        rmp
# Last Modified: $Date: 2013-10-28 17:20:03 +0000 (Mon, 28 Oct 2013) $ $Author: kt6 $
# Id:            $Id: 00-distribution.t 17674 2013-10-28 17:20:03Z kt6 $
# Source:        $Source: /cvsroot/Bio-DasLite/Bio-DasLite/t/00-distribution.t,v $
# $HeadURL: svn+ssh://svn.internal.sanger.ac.uk/repos/svn/new-pipeline-dev/npg-pipeline/trunk/t/00-distribution.t $
#
package distribution;
use strict;
use warnings;
use Test::More;
use English qw(-no_match_vars);

our $VERSION = do { my @r = (q$LastChangedRevision: 17674 $ =~ /\d+/mxg); sprintf '%d.'.'%03d' x $#r, @r };
local $ENV{PATH} = join q[:], q[t/bin], $ENV{PATH};

eval {
  require Test::Distribution;
};

if($EVAL_ERROR) {
  plan skip_all => 'Test::Distribution not installed';
} else {
  Test::Distribution->import('not' => 'prereq'); # Having issues with Test::Dist seeing my PREREQ_PM :(
}

1;
