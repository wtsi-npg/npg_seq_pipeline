use strict;
use warnings;
use Test::More tests => 8;
use Test::Exception;
use File::Temp qw(tempdir tempfile);
use Cwd qw(getcwd abs_path);
use Log::Log4perl qw(:levels);
use Moose::Util qw(apply_all_roles);
use File::Copy qw(cp);

use t::util;

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

subtest 'repository preexec' => sub {
  plan tests => 1;

  my $ref_adapt = npg_pipeline::base->new(repository => q{t/data/sequence});
  apply_all_roles( $ref_adapt, 'npg_pipeline::function::util' );
  is( $ref_adapt->repos_pre_exec_string(),
    q{npg_pipeline_preexec_references --repository t/data/sequence},
    q{correct ref_adapter_pre_exec_string} );
};

subtest 'products - merging (or not) lanes' => sub {
  plan tests => 22;

  my $rf_path = q[t/data/novaseqx/20231017_LH00210_0012_B22FCNFLT3];
  my $b = npg_pipeline::base->new(runfolder_path => $rf_path, id_run => 47995);
  ok(!$b->merge_lanes, 'merge by lanes is false for NovaSeqX');

  my $rf_info = $util->create_runfolder();
  $rf_path = $rf_info->{'runfolder_path'};
  my $products;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/products/samplesheet_novaseq4lanes.csv';
  cp 't/data/run_params/runParameters.novaseq.xml',  "$rf_path/runParameters.xml";
  cp 't/data/novaseq/210111_A00513_0447_AHJ55JDSXY/RunInfo.xml',  "$rf_path/RunInfo.xml";
  $b = npg_pipeline::base->new(runfolder_path => $rf_path, id_run => 999);
  ok ($b->merge_lanes, 'merge_lanes flag is set');
  ok (!$b->_selected_lanes, 'selected_lanes flag is not set');
  lives_ok {$products = $b->products} 'products hash created for NovaSeq run';
  ok (exists $products->{'lanes'}, 'products lanes key exists');
  is (scalar @{$products->{'lanes'}}, 4, 'four lane product');
  ok (exists $products->{'data_products'}, 'products data_products key exists');
  is (scalar @{$products->{'data_products'}}, 29, '29 data products'); 

  $b = npg_pipeline::base->new(
    runfolder_path => $rf_path,
    id_run => 999,
    merge_lanes => 1,
    process_separately_lanes => [2]
  );
  # 8 products out of previous 29 are tag zero and spiked phiX
  is (scalar @{$b->products->{'data_products'}}, 50, '50 data products');
  ok ($b->_selected_lanes, 'selected_lanes flag is set');

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

subtest 'products - merging (or not) libraries' => sub {
  plan tests => 423;

  my $rf_info = $util->create_runfolder();
  my $rf_path = $rf_info->{'runfolder_path'};
  cp 't/data/run_params/runParameters.novaseq.xml',  "$rf_path/runParameters.xml";
  my $b = npg_pipeline::base->new(runfolder_path => $rf_path, id_run => 999);
  ok(!$b->merge_by_library, 'merge by library is false for NovaSeq');

  $rf_path = q[t/data/novaseqx/20231017_LH00210_0012_B22FCNFLT3];
  my $id_run = 47995;
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = qq[$rf_path/samplesheet_${id_run}.csv];
  # All lanes are spiked. Possible merges:
  # lanes 1, 2 - 17 samples
  # lanes 3, 4 - 10 samples
  # lanes 5, 6 - 22 samples
  # lanes 7, 8 - 38 samples
  $b = npg_pipeline::base->new(runfolder_path => $rf_path, id_run => $id_run);
  ok($b->merge_by_library, 'merge by library is true for NovaSeqX');
  my @lane_products = @{$b->products()->{'lanes'}};
  is (@lane_products, 8, 'eight lane products');
  is_deeply ([map { $_->rpt_list } @lane_products],
    [map { join q[:], $id_run, $_} (1 .. 8)],
    'lane products have correct rpt lists assigned');
  my @products = @{$b->products()->{'data_products'}};
  is (@products, 103, 'number of data products is 103');
  my @expected = map { join q[:], $id_run, $_, 888 } (1 .. 8);
  is_deeply ([map { $_->rpt_list } map { $products[$_] } (0 .. 7)],
    \@expected, 'first eight products are for spiked controls');
  @expected = map { join q[:], $id_run, $_, 0 } (1 .. 8);
  is_deeply ([map { $_->rpt_list } map { $products[$_] } (8 .. 15)],
     \@expected, 'next eight products are for tag zero');
  my $ti = 1;
  foreach my $p (map { $products[$_] } (16 .. 32)) {
    my $expected_rpt_list = _generate_rpt($id_run, [1,2], $ti);
    $ti++;
    is ($p->rpt_list, $expected_rpt_list,
      "product rpt list $expected_rpt_list as expected");
    ok ($p->selected_lanes, 'selected_lanes flag is set to true');
  }
  $ti = 1;
  foreach my $p (map { $products[$_] } (33 .. 42)) {
    my $expected_rpt_list = _generate_rpt($id_run, [3,4], $ti);
    $ti++;
    is ($p->rpt_list, $expected_rpt_list,
      "product rpt list $expected_rpt_list as expected");
    ok ($p->selected_lanes, 'selected_lanes flag is set to true');
  }
  $ti = 1;
  foreach my $p (map { $products[$_] } (43 .. 64)) {
    my $expected_rpt_list = _generate_rpt($id_run, [5,6], $ti);
    $ti++;
    is ($p->rpt_list, $expected_rpt_list,
      "product rpt list $expected_rpt_list as expected");
    ok ($p->selected_lanes, 'selected_lanes flag is set to true');
  }
  $ti = 1;
  foreach my $p (map { $products[$_] } (65 .. 102)) {
    my $expected_rpt_list = _generate_rpt($id_run, [7,8], $ti);
    $ti++;
    is ($p->rpt_list, $expected_rpt_list,
      "product rpt list $expected_rpt_list as expected");
    ok ($p->selected_lanes, 'selected_lanes flag is set to true');
  }

  $b = npg_pipeline::base->new(
    runfolder_path => $rf_path,
    id_run => $id_run,
    process_separately_lanes => [1,2,5,6]
  );
  @products = @{$b->products()->{'data_products'}};
  is (@products, 142, 'number of data products is 142');

  $b = npg_pipeline::base->new(
    runfolder_path => $rf_path,
    id_run => $id_run,
    process_separately_lanes => [1,6]
  );
  @products = @{$b->products()->{'data_products'}};
  is (@products, 142, 'number of data products is 142');
  
  # Expect lanes 3 and 4 merged.
  $b = npg_pipeline::base->new(
    runfolder_path => $rf_path, id_run => $id_run, lanes => [4,8,3]);
  ok($b->merge_by_library, 'merge by library is true for NovaSeqX');

  @lane_products = @{$b->products()->{'lanes'}};
  is (@lane_products, 3, 'three lane products');
  is_deeply ([map { $_->rpt_list } @lane_products],
    [map { join q[:], $id_run, $_} (3,4,8)],
    'lane products have correct rpt lists assigned');

  @products = @{$b->products()->{'data_products'}};
  is (@products, 54, 'number of data products is 54');

  is ($products[0]->rpt_list, "$id_run:3:888",
    'first single product is for spiked control for lane 3');
  is ($products[1]->rpt_list, "$id_run:4:888",
    'second single product is for spiked control for lane 4');
  # Then all single products for lane 8.
  $ti = 1;
  foreach my $p (map { $products[$_] } (2 .. 39)) {
    my $expected_rpt_list = join q[:], $id_run, 8, $ti;
    $ti++;
    is ($p->rpt_list, $expected_rpt_list,
      "product rpt list $expected_rpt_list as expected");
    ok ($p->selected_lanes, 'selected_lanes flag is set to true');
  }
  is ($products[40]->rpt_list, "$id_run:8:888", 'spiked control for lane 8');
  is ($products[41]->rpt_list, "$id_run:3:0", 'tag zero for lane 3');
  is ($products[42]->rpt_list, "$id_run:4:0", 'tag zero for lane 4');
  is ($products[43]->rpt_list, "$id_run:8:0", 'tag zero for lane 8');
  # Then merged data.
  $ti = 1;
  foreach my $p (map { $products[$_] } (44 .. 53)) {
    my $expected_rpt_list = _generate_rpt($id_run, [3,4], $ti);
    $ti++;
    is ($p->rpt_list, $expected_rpt_list,
      "product rpt list $expected_rpt_list as expected");
    ok ($p->selected_lanes, 'selected_lanes flag is set to true');
  }

  # Merge disabled.
  $b = npg_pipeline::base->new(
    runfolder_path => $rf_path,
    id_run => $id_run,
    lanes => [4,8,3],
    merge_by_library => 0
  );
  ok(!$b->merge_by_library, 'merge by library is false');
  @products = @{$b->products()->{'data_products'}};
  is (@products, 64, 'number of data products is 64');
  
  foreach my $position ((3, 4, 8)) {
    my $num_plexes = $position == 8 ? 38 : 10;
    foreach my $index ((1 .. $num_plexes, 888, 0)) {
      my $p = shift @products;
      my $expected_rpt_list = join q[:], $id_run, $position, $index;
      is ($p->rpt_list, $expected_rpt_list,
        "product rpt list $expected_rpt_list as expected");
      ok ($p->selected_lanes, 'selected_lanes flag is set to true');
    }
  }
  is (@products, 0, 'no products are left');

  # remove lane 3 from the merge - no merge will take place
  $b = npg_pipeline::base->new(
    runfolder_path => $rf_path,
    id_run => $id_run,
    lanes => [4,8,3],
    merge_by_library => 1,
    process_separately_lanes => [3]
  );
  @products = @{$b->products()->{'data_products'}};
  is (@products, 64, 'number of data products is 64');
  
  $b = npg_pipeline::base->new(
    runfolder_path => $rf_path,
    id_run => $id_run,
    lanes => [4,8,3],
    merge_by_library => 0,
    process_separately_lanes => [3,8]
  );
  lives_ok { @products = @{$b->products()->{'data_products'}} }
    'process_separately_lanes is compatible with suppressed merge';
  is (@products, 64, 'number of data products is 64');
};

sub _generate_rpt {
  my ($id_run, $lanes, $tag_index) = @_;
  return join q[;], map { join q[:], $id_run, $_, $tag_index  } @{$lanes};
}

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
