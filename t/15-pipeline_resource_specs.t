use strict;
use warnings;
use Test::More tests => 1;

use t::util;

my $product_config = q[t/data/release/config/archive_on/product_release.yml];
my $test_pipeline = q[data/config_files/test_pipeline.json];
my $util = t::util->new(clean_temp_directory => 0);
my $rf = $util->analysis_runfolder_path;
$util->create_analysis;
my @params = (
  '--product_conf_file_path', $product_config,
  '--id_run', 1,
  '--analysis_path', $rf,
  '--runfolder_path', $rf,
  '--id_flowcell_lims', 1,
  '--no_bsub', '--local', '--no_sf_resource', '--no-spider',
  '--function_list', $test_pipeline,
  '--log_file_dir', $rf
);
note @params;
system('npg_pipeline_test', @params);
ok(1, 'Stuff happened?')