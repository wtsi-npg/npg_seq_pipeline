use strict;
use warnings;
use Test::More tests => 98;
use Test::Exception;
use File::Temp qw/tempdir/;
use File::Path qw/make_path remove_tree/;
use File::Copy qw/cp/;
use t::dbic_util;

use st::api::lims::ml_warehouse;
use st::api::lims;

use_ok('npg_pipeline::cache');

local $ENV{http_proxy} = 'http://wibble';
local $ENV{no_proxy}   = q[];

is(join(q[ ], npg_pipeline::cache->env_vars()),
  'NPG_WEBSERVICE_CACHE_DIR NPG_CACHED_SAMPLESHEET_FILE',
  'names of env. variables that can be set by the module');

my $wh_schema = t::dbic_util->new()->test_schema_mlwh('t/data/fixtures/mlwh');

my $lims_driver = st::api::lims::ml_warehouse->new(
                     mlwh_schema      => $wh_schema,
                     id_flowcell_lims => undef,
                     flowcell_barcode => 'HBF2DADXX'
                                                  );
 my @lchildren = st::api::lims->new(
                     id_flowcell_lims => undef,
                     flowcell_barcode => 'HBF2DADXX',
                     driver           => $lims_driver,
                     driver_type      => 'ml_warehouse'
                                   )->children;

local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = '';

my $ca = tempdir( CLEANUP => 1);
my $run_path = join q[/], $ca, 'npg', 'run';
my $in_path = join q[/], $ca, 'npg', 'instrument';
make_path $run_path;
make_path $in_path;
cp 't/data/cache/xml/npg/run/12376.xml', $run_path;
cp 't/data/cache/xml/npg/instrument/103.xml', $in_path;

for my $type (qw/warehouse mlwarehouse/) {
  my $method = $type . '_driver_name';
  my $expected = $type eq 'mlwarehouse' ? 'ml_warehouse' : $type;
  is(npg_pipeline::cache->$method, $expected, "driver name for $type");
}

{
  my $tempdir = tempdir( CLEANUP => 1);

  my $cache = npg_pipeline::cache->new(id_run           => 12376,
                                       mlwh_schema      => $wh_schema,
                                       id_flowcell_lims => 'XXXXXXXX',
                                       cache_location   => $tempdir);
  is( $cache->lims_driver_type(), 'ml_warehouse', 'correct default driver');

  $cache = npg_pipeline::cache->new(id_run           => 12376,
                                    mlwh_schema      => $wh_schema,
                                    lims_driver_type => 'ml_warehouse',
                                    id_flowcell_lims => 'XXXXXXXX',
                                    cache_location   => $tempdir);
  throws_ok { $cache->lims }
    qr/No record retrieved for st::api::lims::ml_warehouse id_flowcell_lims XXXXXXXX/,
    'cannot retrieve lims objects';

  $cache = npg_pipeline::cache->new(id_run           => 12376,
                                    mlwh_schema      => $wh_schema,
                                    lims_driver_type => 'ml_warehouse',
                                    cache_location   => $tempdir);
  throws_ok { $cache->lims }
    qr/Neither flowcell barcode nor lims flowcell id is known/,
    'cannot retrieve lims objects';

  my $clims;

  $cache = npg_pipeline::cache->new(id_run           => 12376, 
                                    mlwh_schema      => $wh_schema,
                                    lims_driver_type => 'ml_warehouse',
                                    flowcell_barcode => 'HBF2DADXX',
                                    cache_location   => $tempdir);
  lives_ok { $clims = $cache->lims() } 'can retrieve lims objects';
  ok( $clims, 'lims objects returned');
  is( scalar @{$clims}, 2, 'two lims objects returned');
  is( $clims->[0]->driver_type, 'ml_warehouse', 'correct driver type');

  my $oldwh_schema = t::dbic_util->new()->test_schema_wh('t/data/fixtures/wh');

  $cache = npg_pipeline::cache->new(id_flowcell_lims => '3980331130775',
                                    wh_schema        => $oldwh_schema,
                                    id_run           => 12376,
                                    lims_driver_type => 'warehouse',
                                    cache_location   => $tempdir);
  is( $cache->lims_driver_type(), 'warehouse', 'driver as set');
  lives_ok { $clims = $cache->lims() } 'can retrieve lims objects';
  ok( $clims, 'lims objects returned');
  is( scalar @{$clims}, 1, 'one lims object returned');
  is( $clims->[0]->driver_type, 'warehouse', 'correct driver type');

  $cache = npg_pipeline::cache->new(id_flowcell_lims => '9870331130775',
                                    wh_schema        => $oldwh_schema,
                                    id_run           => 12376,
                                    lims_driver_type => 'warehouse',
                                    cache_location   => $tempdir);
  throws_ok { $clims = $cache->lims() }
    qr/EAN13 barcode checksum fail for code 9870331130775/,
    'cannot retrieve lims objects';

  $cache = npg_pipeline::cache->new(id_flowcell_lims => '5260271901788',
                                    wh_schema        => $oldwh_schema,
                                    id_run           => 12376,
                                    lims_driver_type => 'warehouse',
                                    cache_location   => $tempdir);
  throws_ok { $clims = $cache->lims() }
    qr/Single tube not found from barcode 271901/,
    'cannot retrieve lims objects';
}

{
  my $tempdir = tempdir( CLEANUP => 1);
  my $ss_path = join q[/],$tempdir,'ss.csv';
  my $cache = npg_pipeline::cache->new(
      id_run      => 12376,
      mlwh_schema => $wh_schema,
      lims        => \@lchildren,
      samplesheet_file_path => $ss_path);

  isa_ok($cache, 'npg_pipeline::cache');

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = $ca; # npg xml feeds only
  lives_ok { $cache->_samplesheet() } 'samplesheet generated';
  ok(-e $ss_path, 'samplesheet file exists');
}

{
  my $tempdir = tempdir( CLEANUP => 1);
  my $cache_dir = join q[/], $tempdir, 'metadata_cache_12376';
  my $cache = npg_pipeline::cache->new(id_run         => 12376,
                                       mlwh_schema    => $wh_schema,
                                       lims           => \@lchildren,
                                       cache_location => $tempdir);
  is ($cache->cache_dir_path, $cache_dir, 'cache directory path is correct');
  is ($cache->samplesheet_file_path, join(q[/],$cache_dir,'samplesheet_12376.csv'),
     'samplesheet file path is correct');

  throws_ok { $cache->_deprecate() } qr/$cache_dir\ does\ not\ exist/,
    'error renaming non-existing cache';
  mkdir $cache_dir;
  my $renamed;
  lives_ok { $renamed = $cache->_deprecate() } 'no error renaming existing cache';
  ok (!-e $cache_dir, 'old cache directory does not exist');
  my @found = glob($cache_dir . '_moved_*');
  is (scalar @found, 1, 'one directory found');
  is ($renamed, $found[0], 'name of found directory as reported by _deprecate');

  mkdir $cache_dir;
  throws_ok { $cache->create() }
    qr/$cache_dir\ already\ exists,\ cannot\ create\ a\ new/,
    'cannot create a new cache when there is an existing one';
}

{
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = $ca; # npg xml feeds only
  my $tempdir = tempdir( CLEANUP => 1);
  my $cache_dir = join q[/], $tempdir, 'metadata_cache_12376';
  my $cache = npg_pipeline::cache->new(id_run         => 12376,
                                       mlwh_schema    => $wh_schema,
                                       lims           => \@lchildren,
                                       cache_location => $tempdir);
  isa_ok ($cache, 'npg_pipeline::cache');
  lives_ok {$cache->setup} 'no error creating the cache';
  ok (-d $cache_dir, 'cache directory created');
  ok (-d $cache_dir.'/npg', 'npg cache directory is present');
  ok (-e $cache_dir.'/samplesheet_12376.csv', 'samplesheet is present');

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = '';
  $cache = npg_pipeline::cache->new(id_run           => 12376,
                                    mlwh_schema      => $wh_schema,
                                    flowcell_barcode => 'HBF2DADXX',
                                    cache_location   => $tempdir);
  is ($cache->lims_driver_type, 'ml_warehouse', 'default driver type is set');
  is ($cache->reuse_cache, 1, 'reuse_cache true by default');
  lives_ok {$cache->setup} 'no error reusing existing cache';
  my @messages = @{$cache->messages};
  is (scalar @messages, 2, 'two messages saved') or diag explain $cache->messages;
  is (shift @messages, qq[Found existing cache directory $cache_dir],
    'message to confirm existing cache is found');
  is (shift @messages, q[Will use existing cache directory],
    'message to confirm existing cache will be reused');
  my @found = glob($tempdir);
  is (scalar @found, 1, 'one entry in cache location');

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = $ca; # npg xml feeds only
  $cache = npg_pipeline::cache->new(id_run         => 12376,
                                    mlwh_schema    => $wh_schema,
                                    lims           => \@lchildren,
                                    reuse_cache    => 0,
                                    cache_location => $tempdir);
  is ($cache->reuse_cache, 0, 'reuse_cache set to false');
  is ($cache->set_env_vars, 0, 'set_env_vars is false by default');
  lives_ok {$cache->setup}
    'no error creating a new cache when an existing cache is present';
  @found = glob($tempdir);
  is (scalar @found, 1, 'two entries in cache location'); 

  @messages = @{$cache->messages};
  is (scalar @messages, 5, 'five messages saved') or diag explain $cache->messages;
  is (shift @messages, qq[Found existing cache directory $cache_dir],
    'message to confirm existing cache is found');
  like (shift @messages, qr/Renamed\ existing\ cache\ directory/,
    'message to confirm renaming existing cache');
  is (shift @messages, qq[Will create a new cache directory $cache_dir],
    'message to confirm a new cache will be created');

  ok( !$ENV{NPG_WEBSERVICE_CACHE_DIR},
    'value of NPG_WEBSERVICE_CACHE_DIR env var is not set');
  ok( !$ENV{NPG_CACHED_SAMPLESHEET_FILE},
    'value of NPG_CACHED_SAMPLESHEET_FILE env var is not set');

  sleep(1);
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = $ca; # npg xml feeds only
  $cache = npg_pipeline::cache->new(id_run         => 12376,
                                    mlwh_schema    => $wh_schema,
                                    lims           => \@lchildren,
                                    reuse_cache    => 1,
                                    set_env_vars   => 1,
                                    cache_location => $tempdir);
  is ($cache->set_env_vars, 1, 'set_env_vars is set to true');
  lives_ok {$cache->setup}
    'no error creating a new cache and setting env vars';
  @messages = @{$cache->messages};
  is (scalar @messages, 7, 'seven messages saved') or diag explain $cache->messages;

  my $ss = join q[/], $cache_dir, 'samplesheet_12376.csv';
  is ($ENV{NPG_CACHED_SAMPLESHEET_FILE}, $ss,
    'NPG_CACHED_SAMPLESHEET_FILE is set correctly');
  is (pop @messages, qq[NPG_CACHED_SAMPLESHEET_FILE is set to $ss],
    'message about setting NPG_CACHED_SAMPLESHEET_FILE is saved');
  is ($ENV{NPG_WEBSERVICE_CACHE_DIR}, $cache_dir,
    'NPG_WEBSERVICE_CACHE_DIR is set correctly');
  is (pop @messages, qq[NPG_WEBSERVICE_CACHE_DIR is set to $cache_dir],
    'message about setting NPG_WEBSERVICE_CACHE_DIR is saved');

  unlink $ss;
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = '';
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = '';
  $cache = npg_pipeline::cache->new(id_run         => 12376,
                                    mlwh_schema    => $wh_schema,
                                    lims           => \@lchildren,
                                    reuse_cache    => 1,
                                    set_env_vars   => 1,
                                    cache_location => $tempdir);
  $cache->setup();
  @messages = @{$cache->messages};
  is (scalar @messages, 4, 'four messages saved') or diag explain $cache->messages;

  ok (!$ENV{NPG_CACHED_SAMPLESHEET_FILE},
    'NPG_CACHED_SAMPLESHEET_FILE is unset');
  like (pop @messages, qr/NPG_CACHED_SAMPLESHEET_FILE\ is\ not\ set/,
    'message about not setting NPG_CACHED_SAMPLESHEET_FILE is saved');
  is ($ENV{NPG_WEBSERVICE_CACHE_DIR}, $cache_dir,
    'NPG_WEBSERVICE_CACHE_DIR is set correctly');
  is (pop @messages, qq[NPG_WEBSERVICE_CACHE_DIR is set to $cache_dir],
    'message about setting NPG_WEBSERVICE_CACHE_DIR is saved'); 
}

$lims_driver = st::api::lims::ml_warehouse->new(
                     mlwh_schema      => $wh_schema,
                     id_run           => 12376,
                     id_flowcell_lims => 35053,
                     flowcell_barcode => 'undef'
                                               );
@lchildren = st::api::lims->new(
                     id_run           => 12376,
                     id_flowcell_lims => 35053,
                     flowcell_barcode => undef,
                     driver           => $lims_driver,
                     driver_type      => 'ml_warehouse'
                               )->children;

{
  my $tempdir = tempdir( CLEANUP => 1);
  local $ENV{NPG_WEBSERVICE_CACHE_DIR}    = 't/data/cache/xml';
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/cache/my_samplesheet_12376.csv';
  my $cache = npg_pipeline::cache->new(id_run         => 12376,
                                       mlwh_schema    => $wh_schema,
                                       lims           => \@lchildren,
                                       reuse_cache    => 1,
                                       set_env_vars   => 1,
                                       cache_location => $tempdir);
  lives_ok {$cache->setup();} 'no error when NPG_WEBSERVICE_CACHE_DIR is set';
  my $cache_dir = join q[/], $tempdir, 'metadata_cache_12376';
  my $sh = join(q[/], $cache_dir, 'samplesheet_12376.csv');
  ok (-e $sh, 'renamed samplesheet copied');
  ok (-e join(q[/], $cache_dir, 'npg/instrument/103.xml'), 'instrument xml copied');
  ok (-e join(q[/], $cache_dir, 'npg/run/12376.xml'), 'run xml copied');
  ok (-e join(q[/], $cache_dir, 'npg/run_status_dict.xml'), 'run status xml copied');
  is (scalar @{$cache->messages}, 5, 'five messages saved');
  is ($ENV{NPG_WEBSERVICE_CACHE_DIR}, $cache_dir,
    'NPG_WEBSERVICE_CACHE_DIR is set correctly');
  is ($ENV{NPG_CACHED_SAMPLESHEET_FILE}, $sh,
    'NPG_CACHED_SAMPLESHEET_FILE is set');
}

{
  my $tempdir = tempdir( CLEANUP => 1);
  local $ENV{NPG_WEBSERVICE_CACHE_DIR}    = 't/data/cache/xml';
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/cache/my_samplesheet_12376.csv';
  my $cache = npg_pipeline::cache->new(id_run         => 12376,
                                       mlwh_schema    => $wh_schema,
                                       lims           => \@lchildren,
                                       reuse_cache    => 1,
                                       set_env_vars   => 1,
                                       cache_location => $tempdir);
  lives_ok {$cache->setup();} 'no error when NPG_WEBSERVICE_CACHE_DIR is set but xml caching disabled';
  my $cache_dir = join q[/], $tempdir, 'metadata_cache_12376';
  my $sh = join(q[/], $cache_dir, 'samplesheet_12376.csv');
  ok (-e $sh, 'renamed samplesheet copied');
  ok (-e join(q[/], $cache_dir, 'npg'), 'npg directory inside the cache');
  is ($ENV{NPG_WEBSERVICE_CACHE_DIR}, $cache_dir,
    'NPG_WEBSERVICE_CACHE_DIR is set correctly');
  is ($ENV{NPG_CACHED_SAMPLESHEET_FILE}, $sh,
    'NPG_CACHED_SAMPLESHEET_FILE is set');
}

{
  my $tempdir = tempdir( CLEANUP => 1);
  local $ENV{NPG_WEBSERVICE_CACHE_DIR}    = 't/data/cache/xml';
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = '';
  my $cache = npg_pipeline::cache->new(id_run         => 12376,
                                       mlwh_schema    => $wh_schema,
                                       lims           => \@lchildren,
                                       reuse_cache    => 0,
                                       set_env_vars   => 1,
                                       cache_location => $tempdir);
  lives_ok {$cache->setup();} 'no error when NPG_WEBSERVICE_CACHE_DIR is set, but no NPG_CACHED_SAMPLESHEET_FILE is set';
  my $cache_dir = join q[/], $tempdir, 'metadata_cache_12376';
  my $sh = join(q[/], $cache_dir, 'samplesheet_12376.csv');
  ok (-e $sh, 'samplesheet created');
  ok (-e join(q[/], $cache_dir, 'npg/instrument/103.xml'), 'instrument xml copied');
  ok (-e join(q[/], $cache_dir, 'npg/run/12376.xml'), 'run xml copied');
  ok (-e join(q[/], $cache_dir, 'npg/run_status_dict.xml'), 'run status xml copied');
  is (scalar @{$cache->messages}, 5, 'five messages saved') or diag explain $cache->messages;
  is ($ENV{NPG_WEBSERVICE_CACHE_DIR}, $cache_dir,
    'NPG_WEBSERVICE_CACHE_DIR is set correctly');
  is ($ENV{NPG_CACHED_SAMPLESHEET_FILE}, $sh,
    'NPG_CACHED_SAMPLESHEET_FILE is set');
}

my $tempdir = tempdir( CLEANUP => 1);

{
  local $ENV{NPG_WEBSERVICE_CACHE_DIR}    = 't/data/cache/xml';
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/cache/my_samplesheet_12376.csv';
  my $cache = npg_pipeline::cache->new(id_run         => 12376,
                                       mlwh_schema    => $wh_schema,
                                       reuse_cache    => 1,
                                       set_env_vars   => 1,
                                       cache_location => $tempdir);
  lives_ok {$cache->setup();} 'no error when NPG_WEBSERVICE_CACHE_DIR is set';
  my $cache_dir = join q[/], $tempdir, 'metadata_cache_12376';
  my $sh = join(q[/], $cache_dir, 'samplesheet_12376.csv');
  ok (-e $sh, 'renamed samplesheet copied');
  ok (-e join(q[/], $cache_dir, 'npg/instrument/103.xml'), 'instrument xml copied');
  ok (-e join(q[/], $cache_dir, 'npg/run/12376.xml'), 'run xml copied');
  ok (-e join(q[/], $cache_dir, 'npg/run_status_dict.xml'), 'run status xml copied');
  ok (!-e join(q[/], $cache_dir, 'st/batches/26195.xml'), 'batch xml is not copied');
  is (scalar @{$cache->messages}, 5, 'five messages saved');
  is ($ENV{NPG_WEBSERVICE_CACHE_DIR}, $cache_dir,
    'NPG_WEBSERVICE_CACHE_DIR is set correctly');
  is ($ENV{NPG_CACHED_SAMPLESHEET_FILE}, $sh,
    'NPG_CACHED_SAMPLESHEET_FILE is set');
}

{
  my $cache_dir = join q[/], $tempdir, 'metadata_cache_12376';
  my $sh = join(q[/], $cache_dir, 'samplesheet_12376.csv');
 
  local $ENV{NPG_WEBSERVICE_CACHE_DIR}    = '';
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = '';
  my $cache = npg_pipeline::cache->new(id_run         => 12376,
                                    mlwh_schema       => $wh_schema,
                                    reuse_cache       => 1,
                                    reuse_cache_only  => 1,
                                    set_env_vars      => 1,
                                    cache_location    => $tempdir);
  lives_ok {$cache->setup()} 'no error finding existing cache';
  is ($ENV{NPG_WEBSERVICE_CACHE_DIR}, $cache_dir,
    'NPG_WEBSERVICE_CACHE_DIR is set correctly');
  is ($ENV{NPG_CACHED_SAMPLESHEET_FILE}, $sh,
    'NPG_CACHED_SAMPLESHEET_FILE is set');

  local $ENV{NPG_WEBSERVICE_CACHE_DIR}    = '';
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = '';
  unlink $sh;
  $cache = npg_pipeline::cache->new(id_run            => 12376,
                                    mlwh_schema       => $wh_schema,
                                    reuse_cache       => 1,
                                    reuse_cache_only  => 1,
                                    set_env_vars      => 1,
                                    cache_location    => $tempdir);
  throws_ok {$cache->setup()} qr/Failed to find existing samplesheet/,
    'error when samplesheet is not found';

  local $ENV{NPG_WEBSERVICE_CACHE_DIR}    = '';
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = '';
  remove_tree $cache_dir;
  $cache = npg_pipeline::cache->new(id_run            => 12376,
                                    mlwh_schema       => $wh_schema,
                                    reuse_cache       => 1,
                                    reuse_cache_only  => 1,
                                    set_env_vars      => 1,
                                    cache_location    => $tempdir);
  throws_ok {$cache->setup()} qr/Failed to find existing cache directory/,
    'error when cache directory is not found';
}


1;
