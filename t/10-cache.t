use strict;
use warnings;
use Test::More tests => 26;
use Test::Exception;
use File::Temp qw/tempdir/;
use t::dbic_util;

use st::api::lims::ml_warehouse;
use st::api::lims;

use_ok('npg_pipeline::cache');

is(join(q[ ], npg_pipeline::cache->env_vars()),
  'NPG_CACHED_SAMPLESHEET_FILE',
  'names of env. variables that can be set by the module');

my $wh_schema = t::dbic_util->new()->test_schema_mlwh('t/data/fixtures/mlwh');

my @lchildren = st::api::lims->new(
                     id_run           => 12376,
                     id_flowcell_lims => 35053,
                     mlwh_schema      => $wh_schema,
                     driver_type      => 'ml_warehouse'
                                  )->children;

local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = '';

{
  my $tempdir = tempdir( CLEANUP => 1);

  my $cache = npg_pipeline::cache->new(id_run           => 12376,
                                       mlwh_schema      => $wh_schema,
                                       id_flowcell_lims => 'XXXXXXXX',
                                       cache_location   => $tempdir);
  throws_ok { $cache->lims }
    qr/No record retrieved for st::api::lims::ml_warehouse id_flowcell_lims XXXXXXXX/,
    'cannot retrieve lims objects';

  $cache = npg_pipeline::cache->new(id_run           => 12376,
                                    mlwh_schema      => $wh_schema,
                                    cache_location   => $tempdir);
  throws_ok { $cache->lims }
    qr/id_flowcell_lims \(batch id\) is required/,
    'cannot retrieve lims objects';

  $cache = npg_pipeline::cache->new(id_run           => 12376, 
                                    mlwh_schema      => $wh_schema,
                                    id_flowcell_lims => 35053,
                                    cache_location   => $tempdir);
  my $clims;
  lives_ok { $clims = $cache->lims() } 'can retrieve lims objects';
  ok( $clims, 'lims objects returned');
  is( scalar @{$clims}, 1, 'one lims object returned');
  is( $clims->[0]->driver_type, 'ml_warehouse', 'correct driver type');
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
  lives_ok { $cache->_samplesheet() } 'samplesheet generated';
  ok(-e $ss_path, 'samplesheet file exists');
}

{
  my $tempdir = tempdir( CLEANUP => 1);
  my $cache_dir = join q[/], $tempdir, 'metadata_cache_12376';
  mkdir $cache_dir;
  my $cache = npg_pipeline::cache->new(id_run         => 12376,
                                       mlwh_schema    => $wh_schema,
                                       lims           => \@lchildren,
                                       cache_location => $tempdir);
  isa_ok ($cache, 'npg_pipeline::cache');
  lives_ok {$cache->setup} 'no error creating the cache';
  ok (-e $cache_dir.'/samplesheet_12376.csv', 'samplesheet is present');

  sleep(1);
  $cache = npg_pipeline::cache->new(id_run         => 12376,
                                    mlwh_schema    => $wh_schema,
                                    lims           => \@lchildren,
                                    set_env_vars   => 1,
                                    cache_location => $tempdir);
  is ($cache->set_env_vars, 1, 'set_env_vars is set to true');
  mkdir "$tempdir/metadata_cache_12376";
  lives_ok {$cache->setup}
    'no error creating a new cache and setting env vars';
  my @messages = @{$cache->messages};
  is (scalar @messages, 1, 'one message saved') or diag explain $cache->messages;

  my $ss = join q[/], $cache_dir, 'samplesheet_12376.csv';
  is ($ENV{NPG_CACHED_SAMPLESHEET_FILE}, $ss,
    'NPG_CACHED_SAMPLESHEET_FILE is set correctly');
  is (pop @messages, qq[NPG_CACHED_SAMPLESHEET_FILE is set to $ss],
    'message about setting NPG_CACHED_SAMPLESHEET_FILE is saved');
}

{  
  my $tempdir = tempdir( CLEANUP => 1);
  my $cache_dir = join q[/], $tempdir, 'metadata_cache_12376';
  mkdir $cache_dir;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/cache/my_samplesheet_12376.csv';
  my $cache = npg_pipeline::cache->new(id_run         => 12376,
                                       mlwh_schema    => $wh_schema,
                                       lims           => \@lchildren,
                                       set_env_vars   => 1,
                                       cache_location => $tempdir);
  lives_ok {$cache->setup();} 'no error';
  my $sh = join(q[/], $cache_dir, 'samplesheet_12376.csv');
  ok (-e $sh, 'renamed samplesheet copied');
  is ($ENV{NPG_CACHED_SAMPLESHEET_FILE}, $sh, 'NPG_CACHED_SAMPLESHEET_FILE is set');

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/cache/my_samplesheet_12376.csv';
  $cache = npg_pipeline::cache->new(id_run         => 12376,
                                    mlwh_schema    => $wh_schema,
                                    lims           => \@lchildren,
                                    set_env_vars   => 1,
                                    cache_location => $tempdir);
  lives_ok {$cache->setup();} 'no error';
  ok (-e $sh, 'standard samplesheet exists');
  my @moved = glob($sh . '_moved_*');
  is (scalar @moved, 1, 'moved file exists');
  is ($ENV{NPG_CACHED_SAMPLESHEET_FILE}, $sh, 'NPG_CACHED_SAMPLESHEET_FILE is set');
}

1;
