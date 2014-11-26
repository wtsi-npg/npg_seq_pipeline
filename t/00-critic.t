#########
# Author:        rmp
# Last Modified: $Date: 2013-05-17 10:37:25 +0100 (Fri, 17 May 2013) $ $Author: mg8 $
# Id:            $Id: 00-critic.t 17158 2013-05-17 09:37:25Z mg8 $
# Source:        $Source: /cvsroot/Bio-DasLite/Bio-DasLite/t/00-critic.t,v $
# $HeadURL: svn+ssh://svn.internal.sanger.ac.uk/repos/svn/new-pipeline-dev/npg-pipeline/trunk/t/00-critic.t $
#
package critic;
use strict;
use warnings;
use Test::More;
use English qw(-no_match_vars);

our $VERSION = do { my @r = (q$LastChangedRevision: 17158 $ =~ /\d+/mxg); sprintf '%d.'.'%03d' x $#r, @r };

if (!$ENV{TEST_AUTHOR}) {
  my $msg = 'Author test.  Set $ENV{TEST_AUTHOR} to a true value to run.';
  plan( skip_all => $msg );
}

eval {
  require Test::Perl::Critic;
};

if($EVAL_ERROR) {
  plan skip_all => 'Test::Perl::Critic not installed';

} else {
  Test::Perl::Critic->import(
			     -severity => 1,
		             -exclude => [ qw{
		         tidy
		         ValuesAndExpressions::ProhibitImplicitNewlines
		         Documentation::RequirePodAtEnd
		         ValuesAndExpressions::RequireConstantVersion
		         Miscellanea::ProhibitUnrestrictedNoCritic
		         Documentation::PodSpelling
                         RegularExpressions::ProhibitEnumeratedClasses
		       } ],
			     -profile => 't/perlcriticrc',
                             -verbose => "%m at %f line %l, policy %p\n",
			    );
  all_critic_ok();
}

1;
