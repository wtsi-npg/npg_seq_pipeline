use strict;
use warnings;
use Test::More;
use English qw(-no_match_vars);

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
