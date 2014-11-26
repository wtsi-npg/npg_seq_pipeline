# $Id: 00-podcoverage.t 16979 2013-04-08 10:03:18Z mg8 $
use Test::More;
eval "use Test::Pod::Coverage 1.00";
plan skip_all => "Test::Pod::Coverage 1.00 required for testing POD coverage" if $@;
all_pod_coverage_ok();
