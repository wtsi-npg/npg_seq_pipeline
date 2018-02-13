use strict;
use warnings;
use Test::More tests => 60;
use Test::Exception;
use File::Temp qw(tempdir tempfile);
use File::Copy qw(cp);
use Cwd;
use Sys::Filesystem::MountPoint qw(path_to_mount_point);
use Sys::Hostname;
use Log::Log4perl qw(:levels);

use t::util;
use t::dbic_util;
use npg_tracking::util::abs_path qw(abs_path);

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], t::util->new()->temp_directory(), 'logfile'),
                          utf8   => 1});

my $cwd = abs_path(getcwd());
my $config_dir = $cwd . '/data/config_files';

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
  isa_ok( $base->general_values_conf(), q{HASH});
}

{
  my $b = npg_pipeline::base->new(no_bsub => 1);
  lives_ok { $b->submit_bsub_command('bkill -22') }
    'no_bsub mode, can submit bkill command';
  lives_ok { $b->submit_bsub_command('bsub -o out -Jname /bin/true') }
    'no_bsub mode, can submit bsub command';
  
  local $ENV{'NPG_UPSTREAM_LSFJOBS'} = 'some';
  lives_ok { $b->submit_bsub_command('bsub -o out -Jname[1..5] /bin/true') }
    'no_bsub mode, can submit bsub command';
  local $ENV{'NPG_UPSTREAM_LSFJOBS'} = q[];

  local $ENV{'PATH'} = $cwd . '/t/bin:' . $ENV{'PATH'};  
  $b = npg_pipeline::base->new();
  lives_ok { $b->submit_bsub_command('bkill -22') }
    'can submit bkill command';
  lives_ok { $b->submit_bsub_command('bsub -o out -J name[5,6] /bin/true') }
    'can submit bsub command';

  local $ENV{'NPG_UPSTREAM_LSFJOBS'} = 'some';
  lives_ok { $b->submit_bsub_command('bsub -o out -Jname /bin/true') }
    'can submit bsub command';

  $b = npg_pipeline::base->new(job_priority => 80);
  lives_ok { $b->submit_bsub_command('bsub -o out -J name /bin/true') }
    'can submit bsub command';
}

{
  my $base;
  lives_ok {
    $base = npg_pipeline::base->new(conf_path => q{does/not/exist});
  } q{base ok};
  throws_ok{ $base->general_values_conf()} qr{does not exist or is not readable},
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

package mytest::central;
use base 'npg_pipeline::base';
package main;

{
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
}

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
