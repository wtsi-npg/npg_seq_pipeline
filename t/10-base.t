use strict;
use warnings;
use Test::More tests => 9;
use Test::Exception;
use File::Temp qw(tempdir tempfile);
use Cwd;
use Log::Log4perl qw(:levels);

use t::util;
use npg_tracking::util::abs_path qw(abs_path);

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], t::util->new()->temp_directory(), 'logfile'),
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

subtest 'timestamp andrandom string' => sub {
  plan tests => 3;

  my $base = npg_pipeline::base->new();
  ok ($base->timestamp eq $base->timestamp, 'timestamp is cached');
  ok ($base->random_string ne $base->random_string, 'random string is not cached');
  my $t = $base->timestamp;
  ok ($base->random_string =~ /\A$t-\d+/xms, 'random string structure');
};

subtest 'config' => sub {
  plan tests => 3;

  my $base = npg_pipeline::base->new();
  isa_ok( $base->general_values_conf(), q{HASH});
  lives_ok {
    $base = npg_pipeline::base->new(conf_path => q{does/not/exist});
  } q{base ok};
  throws_ok{ $base->general_values_conf()} qr{does not exist or is not readable},
    'Croaks for non-esistent config file as expected';;
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

subtest 'HiSeq flag' => sub {
  plan tests => 2;

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data/hiseqx];
  my $base = npg_pipeline::base->new(id_run => 13219);
  ok($base->is_hiseqx_run, 'is a HiSeqX instrument run');
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];
  $base = npg_pipeline::base->new(id_run => 1234);
  ok(!$base->is_hiseqx_run, 'is not a HiSeqX instrument run');
};

subtest 'metadata cache directory' => sub {
  plan tests => 8;

  my $dir = tempdir( CLEANUP => 1 );
  my ($fh, $file) = tempfile( 'tmpfileXXXX', DIR => $dir);
  
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = $dir;
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[];
  is (npg_pipeline::base->metadata_cache_dir(), $dir, 'cache dir from webservice cache dir');
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = $file;
  is (npg_pipeline::base->metadata_cache_dir(), $dir, 'cache dir from two consistent caches');
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[];
  is (npg_pipeline::base->metadata_cache_dir(), $dir, 'cache dir from samplesheet path');
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t];
  throws_ok {npg_pipeline::base->metadata_cache_dir()}
    qr/Multiple possible locations for metadata cache directory/,
    'inconsistent locations give an error';
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[some];
  is (npg_pipeline::base->metadata_cache_dir(), $dir, 'one valid and one invalid path is OK');
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[other];
  throws_ok {npg_pipeline::base->metadata_cache_dir()}
    qr/Cannot infer location of cache directory/,
    'error with two invalid paths';
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[];
  throws_ok {npg_pipeline::base->metadata_cache_dir()}
    qr/Cannot infer location of cache directory/,
    'error with one path that is invalid';
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[];
  throws_ok {npg_pipeline::base->metadata_cache_dir()}
    qr/Cannot infer location of cache directory/,
    'error when no env vars are set';
};

subtest 'qc run flag' => sub {
  plan tests => 10;

  package mytest::central;
  use base 'npg_pipeline::base';
  package main;

  my $base = npg_pipeline::base->new(flowcell_id  => 'HBF2DADXX');
  ok( !$base->is_qc_run(), 'looking on flowcell lims id: not qc run');
  ok( !$base->qc_run, 'not qc run');
  ok( $base->is_qc_run('3980331130775'), 'looking on argument - qc run');
  
  $base = npg_pipeline::base->new(id_flowcell_lims => 3456);
  ok( !$base->is_qc_run(), 'looking on flowcell lims id: not qc run');
  ok( !$base->qc_run, 'not qc run');
  ok( !$base->is_qc_run(3456), 'looking on argument: not qc run');

  $base = mytest::central->new(id_flowcell_lims => 3456, qc_run => 1);
  ok( !$base->is_qc_run(), 'looking on flowcell lims id: not qc run');
  
  $base = mytest::central->new(id_flowcell_lims => '3980331130775');
  ok( $base->is_qc_run(), 'looking on flowcell lims id: qc run');
  ok( $base->qc_run, 'qc run');
  ok( $base->is_qc_run('3980331130775'), 'looking on argument: qc run');
};

subtest 'lims driver type' => sub {
  plan tests => 7;

  my $base = npg_pipeline::base->new(id_run => 4);
  is($base->lims_driver_type, 'ml_warehouse');
  $base = npg_pipeline::base->new(id_run => 4,
                                  id_flowcell_lims => 1234567890123);
  is($base->lims_driver_type, 'warehouse');
  $base = npg_pipeline::base->new(id_run => 4,
                                  id_flowcell_lims => 12345678);
  is($base->lims_driver_type, 'ml_warehouse');
  $base = npg_pipeline::base->new(id_run => 4, qc_run=>0);
  is($base->lims_driver_type, 'ml_warehouse');
  $base = npg_pipeline::base->new(id_run => 4,
                                  qc_run => 0,
                                  id_flowcell_lims => 1234567890123);
  is($base->lims_driver_type, 'ml_warehouse');
  $base = npg_pipeline::base->new(id_run => 4,
                                  qc_run=>1,
                                  id_flowcell_lims => 1234567890123);
  is($base->lims_driver_type, 'warehouse');
  $base = npg_pipeline::base->new(id_run => 4,
                                  qc_run=>1,
                                  id_flowcell_lims => 12345678);
  is($base->lims_driver_type, 'ml_warehouse');
};

1;
