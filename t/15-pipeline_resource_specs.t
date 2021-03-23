use strict;
use warnings;
use Test::More;

my $product_config = q[t/data/release/config/archive_on/product_release.yml];

my $command = <<'CLI';
npg_pipeline_test --product_conf_file_path $product_config
--no_bsub --no_sf_resource --runfolder_path $rf --function_order dodo 2>&1`;
CLI


my $out = `$command`;
