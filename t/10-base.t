use strict;
use warnings;
use Test::More tests => 8;
use Test::Exception;
use File::Temp qw(tempdir tempfile);
use Cwd;
use Log::Log4perl qw(:levels);
use Moose::Util qw(apply_all_roles);
use File::Copy qw(cp);

use t::util;
use npg_tracking::util::abs_path qw(abs_path);

my $util = t::util->new();

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $util->temp_directory(), 'logfile'),
                          utf8   => 1});

my $cwd = abs_path(getcwd());
my $config_dir = $cwd . '/data/config_files';

use_ok(q{npg_pipeline::base});

subtest 'local flag' => sub {
  plan tests => 3;

  my $base = npg_pipeline::base->new();
  isa_ok($base, q{npg_pipeline::base});
  is($base->local, 0, 'local flag is 0');
  $base = npg_pipeline::base->new(local => 1);
  is($base->local, 1, 'local flag is 1 as set');
};

subtest 'timestamp and random string' => sub {
  plan tests => 3;

  my $base = npg_pipeline::base->new();
  ok ($base->timestamp eq $base->timestamp, 'timestamp is cached');
  ok ($base->random_string ne $base->random_string, 'random string is not cached');
  my $t = $base->timestamp;
  ok ($base->random_string =~ /\A$t-\d+/xms, 'random string structure');
};

subtest 'config' => sub {
  plan tests => 2;

  my $base = npg_pipeline::base->new();
  isa_ok( $base->general_values_conf(), q{HASH});
  throws_ok {
   npg_pipeline::base->new(conf_path => q{does/not/exist});
  } qr/Attribute \(conf_path\) does not pass the type constraint/,
    'Croaks for non-esistent config file as expected';
};

subtest 'flowcell id and barcode' => sub {
  plan tests => 7;

  my $bpath = t::util->new()->temp_directory;
  my $path = join q[/], $bpath, '150206_HS29_15467_A_C5WL2ACXX';
  my $base;
  lives_ok { $base = npg_pipeline::base->new(runfolder_path => $path); }
    'can create object without supplying run id';
  is ($base->id_run, 15467, 'id run derived correctly from runfolder_path');
  ok (!defined $base->id_flowcell_lims, 'lims flowcell id undefined');
  is ($base->flowcell_id, 'C5WL2ACXX', 'flowcell barcode derived from runfolder path');
  
  $path = join q[/], $bpath, '150204_MS8_15441_A_MS2806735-300V2';
  $base = npg_pipeline::base->new(runfolder_path => $path, id_flowcell_lims => 45);
  is ($base->id_run, 15441, 'id run derived correctly from runfolder_path');
  is ($base->id_flowcell_lims, 45, 'lims flowcell id returned correctly');
  is ($base->flowcell_id, 'MS2806735-300V2', 'MiSeq reagent kit id derived from runfolder path');
};

subtest 'repository preexec' => sub {
  plan tests => 1;

  my $ref_adapt = npg_pipeline::base->new(repository => q{t/data/sequence});
  apply_all_roles( $ref_adapt, 'npg_pipeline::function::util' );
  is( $ref_adapt->repos_pre_exec_string(),
    q{npg_pipeline_preexec_references --repository t/data/sequence},
    q{correct ref_adapter_pre_exec_string} );
};

subtest 'products' => sub {
  plan tests => 18;

  my $rf_info = $util->create_runfolder();
  my $rf_path = $rf_info->{'runfolder_path'};
  my $products;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/products/samplesheet_novaseq4lanes.csv';
  cp 't/data/run_params/runParameters.novaseq.xml',  "$rf_path/runParameters.xml";
  my $b = npg_pipeline::base->new(runfolder_path => $rf_path, id_run => 999);
  ok ($b->merge_lanes, 'merge_lanes flag is set');
  lives_ok {$products = $b->products} 'products hash created for NovaSeq run';
  ok (exists $products->{'lanes'}, 'products lanes key exists');
  is (scalar @{$products->{'lanes'}}, 4, 'four lane product');
  ok (exists $products->{'data_products'}, 'products data_products key exists');
  is (scalar @{$products->{'data_products'}}, 23, '23 data products'); 

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/products/samplesheet_rapidrun_nopool.csv';
  cp 't/data/run_params/runParameters.hiseq.rr.xml',  "$rf_path/runParameters.xml";
  cp 't/data/run_params/RunInfo.hiseq.rr.xml',  "$rf_path/RunInfo.xml"; 
  $b = npg_pipeline::base->new(runfolder_path => $rf_path, id_run => 999);
  ok (!$b->merge_lanes, 'merge_lanes flag is not set');
  lives_ok {$products = $b->products} 'products hash created for rapid run';
  ok (exists $products->{'lanes'}, 'products lanes key exists');
  is (scalar @{$products->{'lanes'}}, 2, 'two lane products');
  ok (exists $products->{'data_products'}, 'products data_products key exists');
  is (scalar @{$products->{'data_products'}}, 2, 'two data products');

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/miseq/samplesheet_16850.csv';
  cp 't/data/run_params/runParameters.miseq.xml',  "$rf_path/runParameters.xml";
  cp 't/data/miseq/16850_RunInfo.xml',  "$rf_path/RunInfo.xml";
  $b = npg_pipeline::base->new(runfolder_path => $rf_path, id_run => 999);
  ok (!$b->merge_lanes, 'merge_lanes flag is not set');
  lives_ok {$products = $b->products} 'products hash created for rapid run';
  ok (exists $products->{'lanes'}, 'products lanes key exists');
  is (scalar @{$products->{'lanes'}}, 1, 'one lane product');
  ok (exists $products->{'data_products'}, 'products data_products key exists');
  is (scalar @{$products->{'data_products'}}, 3, 'three data products');
};

subtest 'label' => sub {
  plan tests => 4;

  my $base = npg_pipeline::base->new(id_run => 22);
  is ($base->label, '22', 'label defaults to run id');
  $base = npg_pipeline::base->new(id_run => 22, label => '33');
  is ($base->label, '33', 'label as set');
  $base = npg_pipeline::base->new(product_rpt_list => '22:1:33');
  throws_ok { $base->label }
    qr/cannot build 'label' attribute, it should be pre-set/,
    'error if label is not preset';
  $base = npg_pipeline::base->new(product_rpt_list => '22:1:33', label => '33');
  is ($base->label, '33', 'label as set');
};

1;
