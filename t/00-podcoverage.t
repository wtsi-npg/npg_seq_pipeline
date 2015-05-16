use strict;
use warnings;
use Test::More;

eval "use Test::Pod::Coverage 1.00";
plan skip_all => "Test::Pod::Coverage 1.00 required for testing POD coverage" if $@;
local $ENV{'PATH'} = join q[:], 't/bin', $ENV{'PATH'};
all_pod_coverage_ok();

1;
