use strict;
use warnings;
use Test::More;
use English qw(-no_match_vars);

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
                         Documentation::RequirePodSections
		       } ],
			     -profile => 't/perlcriticrc',
                             -verbose => "%m at %f line %l, policy %p\n",
			    );
  all_critic_ok();
}

1;
