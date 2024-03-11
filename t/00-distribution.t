use strict;
use warnings;
use Test::More;
use English qw(-no_match_vars);

eval {
  require Test::Distribution;
};

if($EVAL_ERROR) {
  plan skip_all => 'Test::Distribution not installed';
} else {
  Test::Distribution->import(only => [qw/versions description/], distversion => 1);
}

1;
