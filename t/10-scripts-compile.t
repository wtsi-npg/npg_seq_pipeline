# $Id: 10-base.t 15116 2012-02-06 09:20:07Z mg8 $
use strict;
use warnings;
use Test::More;

eval "use Test::Compile";
plan skip_all => "Test::Compile required for testing compilation"
  if $@;
all_pl_files_ok();

1;