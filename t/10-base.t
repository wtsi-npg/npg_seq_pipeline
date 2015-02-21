use strict;
use warnings;
use Test::More tests => 64;
use Test::Exception;
use t::util;
use File::Temp qw(tempdir tempfile);
use Cwd;
use Sys::Filesystem::MountPoint qw(path_to_mount_point);
use Sys::Hostname;
use t::dbic_util;

use_ok(q{npg_pipeline::base});

{
  throws_ok {npg_pipeline::base->new(no_bsub => 3)}
    qr/Validation failed for 'Bool' (failed[ ])?with value 3/,
    'error trying to set boolean flag to 3';
  my $base = npg_pipeline::base->new();
  isa_ok($base, q{npg_pipeline::base});
  is($base->no_bsub, undef, 'no_bsub flag value is undefined if not set');
  ok(!$base->no_bsub, '   ... and it evaluates to false');
  is($base->local, 0, 'local flag is 0');
  $base = npg_pipeline::base->new(no_bsub => q[]);
  is($base->no_bsub, q[], 'no_bsub flag value is empty string as set');
  ok(!$base->no_bsub, '   ... and it evaluates to false');
  $base = npg_pipeline::base->new(no_bsub => 0);
  is($base->no_bsub, 0, 'no_bsub flag value is 0 as set');
  ok(!$base->no_bsub, '   ... and it evaluates to false');
  is($base->local, 0, 'local flag is 0');
  $base = npg_pipeline::base->new(no_bsub => 1);
  is($base->no_bsub, 1, 'no_bsub flag value is 1 as set');
  ok($base->no_bsub, '   ... and it evaluates to true');
  is($base->local, 1, 'local flag is 1');
  $base = npg_pipeline::base->new(local => 1);
  is($base->local, 1, 'local flag is 1 as set');
  $base = npg_pipeline::base->new(no_bsub => 0, local => 1);
  is($base->local, 1, 'local flag is 1 as set');
  is($base->no_bsub, 0, 'no_sub flag is 0 as set');
}

{
  my $base = npg_pipeline::base->new();
  ok(!$base->olb, 'OLB preprocessing is switched off by default');
  $base = npg_pipeline::base->new(olb => 1);
  ok($base->olb, 'OLB preprocessing is switched on as set');
}

{
  my $base;
  lives_ok {
    $base = npg_pipeline::base->new();
  } q{base ok};

  foreach my $config_group ( qw{
    external_script_names_conf
    function_order_conf
    general_values_conf
    illumina_pipeline_conf
    pb_cal_pipeline_conf
  } ) {
    isa_ok( $base->$config_group(), q{HASH}, q{$} . qq{base->$config_group} );
  }
}

{
  my $base;
  lives_ok {
    $base = npg_pipeline::base->new(conf_path => q{does/not/exist});
  } q{base ok};
  throws_ok{ $base->general_values_conf()} qr{cannot find },
    'Croaks for non-esistent config file as expected';;
}

{
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
}

{
  local $ENV{TEST_FS_RESOURCE} = q{nfs_12};
  my $expected_fs_resource =  q{nfs_12};
  my $path = t::util->new()->temp_directory;
  my $base = npg_pipeline::base->new(id_run => 7440, runfolder_path => $path);
  my $arg = q{-R 'select[mem>2500] rusage[mem=2500]' -M2500000};
  is ($base->fs_resource_string({resource_string => $arg,}),
    qq[-R 'select[mem>2500] rusage[mem=2500,$expected_fs_resource=8]' -M2500000],
   'resource string with sf resource');
  is ($base->fs_resource_string({resource_string => $arg, seq_irods => 1,}),
    qq[-R 'select[mem>2500] rusage[mem=2500,$expected_fs_resource=8,seq_irods=1]' -M2500000],
    'resource string with sf and irods resource');
  $base = npg_pipeline::base->new(id_run => 7440, runfolder_path => $path , no_sf_resource => 1);
  is ($base->fs_resource_string({resource_string => $arg,}), $arg,
    'resource string with no sr resource if no_sf_resource is set');

  $arg = q{-R 'select[mem>13800] rusage[mem=13800] span[hosts=1]'};
  is ($base->fs_resource_string({resource_string => $arg, counter_slots_per_job => 8,}), $arg,
    'resource string with no sr resource if no_sf_resource is set');
}

{
  my $host = hostname;
  SKIP: {
    skip 'Not running on a farm node', 1 unless ($host =~ /^sf/);
    my $basedir = tempdir( CLEANUP => 1 );
    my $base = npg_pipeline::base->new(id_run => 1234, runfolder_path => $basedir);
    is($base->_fs_resource, 'tmp', 'fs_resourse as expected');
  }
}

{
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data/hiseqx];
  my $base = npg_pipeline::base->new(id_run => 13219);
  ok($base->is_hiseqx_run, 'is a HiSeqX instrument run');
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];
  $base = npg_pipeline::base->new(id_run => 1234);
  ok(!$base->is_hiseqx_run, 'is not a HiSeqX instrument run');
}

{
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
}

{
  my $clims;

  my $wh_schema = t::dbic_util->new()->test_schema_mlwh('t/data/fixtures/mlwh');
  my $base = npg_pipeline::base->new(
               flowcell_id  => 'HBF2DADXX',
               _mlwh_schema =>  $wh_schema);
  ok( !$base->qc_run, 'not qc run');
  lives_ok {$clims = $base->samplesheet_source_lims() }
    'can retrieve lims objects';
  ok( $clims, 'lims objects returned');
  is( scalar @{$clims}, 2, 'two lims objects returned');
  is( $clims->[0]->driver_type, 'ml_warehouse', 'correct driver type');

  $base = npg_pipeline::base->new(
               flowcell_id  => 'XXXXXXXXX',
               _mlwh_schema =>  $wh_schema);
  throws_ok {$clims = $base->samplesheet_source_lims() }
    qr/No record retrieved for st::api::lims::ml_warehouse flowcell_barcode XXXXXXXXX/,
    'cannot retrieve lims objects';
  
  $base = npg_pipeline::base->new(id_flowcell_lims => 3456);
  ok( !$base->qc_run, 'not qc run');
  ok( !$base->samplesheet_source_lims(), 'ss_source lims undefined');

  $wh_schema = t::dbic_util->new()->test_schema_wh('t/data/fixtures/wh');
  $base = npg_pipeline::base->new(id_flowcell_lims => '3980331130775',
                                  _wh_schema       => $wh_schema);
  ok($base->qc_run, 'qc run');
  lives_ok { $clims = $base->samplesheet_source_lims() }
    'can retrieve lims objects';
  ok( $clims, 'lims objects returned');
  is( scalar @{$clims}, 1, 'one lims object returned');
  is( $clims->[0]->driver_type, 'warehouse', 'correct driver type');

  $base = npg_pipeline::base->new(id_flowcell_lims => '9870331130775',
                                  _wh_schema       => $wh_schema);
  throws_ok { $clims = $base->samplesheet_source_lims() }
    qr/EAN13 barcode checksum fail for code 9870331130775/,
    'cannot retrieve lims objects';

  $base = npg_pipeline::base->new(id_flowcell_lims => '5260271901788',
                                  _wh_schema       => $wh_schema);
  throws_ok { $clims = $base->samplesheet_source_lims() }
    qr/Single tube not found from barcode 271901/,
    'cannot retrieve lims objects';  
}

1;
