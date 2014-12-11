use strict;
use warnings;
use Test::More tests => 65;
use Test::Exception;
use File::Temp qw/tempdir/;

use_ok('npg_pipeline::cache');
isa_ok(npg_pipeline::cache->new(id_run => 222), 'npg_pipeline::cache');
is(join(q[ ], npg_pipeline::cache->env_vars()),
  'NPG_WEBSERVICE_CACHE_DIR NPG_CACHED_SAMPLESHEET_FILE',
  'names of env. variables that can be set by the module');

package npg_test_no_xml_cache;
  use Moose;
  extends q{npg_pipeline::cache};
  sub _xml {
    my $self = shift;
    mkdir join(q[/], $self->cache_dir_path, 'st');
    mkdir join(q[/], $self->cache_dir_path, 'npg');
    return;
  }
  override '_samplesheet' => sub {
    my $self = shift;
    local $ENV{NPG_WEBSERVICE_CACHE_DIR} = 't/data/cache/xml';
    super();
    return;
  };
  1;

package main;

local $ENV{NPG_WEBSERVICE_CACHE_DIR} = 'wibble';
local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = '';
{
  my $tempdir = tempdir( CLEANUP => 1);
  my $ss_path = join q[/],$tempdir,'ss.csv';
  my $cache = npg_pipeline::cache->new(id_run => 12376,
                                       samplesheet_file_path => $ss_path);
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = 't/data/cache/xml';
  lives_ok { $cache->_samplesheet() } 'samplesheet generated';
  ok(-e $ss_path, 'samplesheet file exists');
}

{
  my $tempdir = tempdir( CLEANUP => 1);
  my $cache_dir = join q[/], $tempdir, 'metadata_cache_12376';
  my $cache = npg_pipeline::cache->new(id_run => 12376,
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
  my $tempdir = tempdir( CLEANUP => 1);
  my $cache_dir = join q[/], $tempdir, 'metadata_cache_12376';
  my $cache = npg_test_no_xml_cache->new(id_run => 12376,
                                         cache_location => $tempdir);
  isa_ok ($cache, 'npg_test_no_xml_cache');
  lives_ok {$cache->create} 'no error creating the cache';
  ok (-d $cache_dir, 'cache directory created');
  ok (-d $cache_dir.'/npg', 'npg cache directory is present');
  ok (-d $cache_dir.'/st_original', 'renamed st cache directory is present');
  ok (-e $cache_dir.'/samplesheet_12376.csv', 'samplesheet is present');

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = '';
  $cache = npg_test_no_xml_cache->new(id_run => 12376,
                                      cache_location => $tempdir);
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

  $cache = npg_test_no_xml_cache->new(id_run => 12376,
                                      reuse_cache    => 0,
                                      cache_location => $tempdir);
  is ($cache->reuse_cache, 0, 'reuse_cache set to false');
  is ($cache->set_env_vars, 0, 'set_env_vars is false by default');
  lives_ok {$cache->setup}
    'no error creating a new cache when an existing cache is present';
  @found = glob($tempdir);
  is (scalar @found, 1, 'two entries in cache location'); 

  @messages = @{$cache->messages};
  is (scalar @messages, 4, 'four messages saved') or diag explain $cache->messages;
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

  $cache = npg_test_no_xml_cache->new(id_run => 12376,
                                      reuse_cache    => 1,
                                      set_env_vars    => 1,
                                      cache_location => $tempdir);
  is ($cache->set_env_vars, 1, 'set_env_vars is set to true');
  lives_ok {$cache->setup}
    'no error creating a new cache and setting env vars';
  @messages = @{$cache->messages};
  is (scalar @messages, 4, 'four messages saved') or diag explain $cache->messages;

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
  $cache = npg_test_no_xml_cache->new(id_run => 12376,
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

{
  my $tempdir = tempdir( CLEANUP => 1);
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = 't/data/cache/xml';
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/cache/my_samplesheet_12376.csv';
  my $cache = npg_pipeline::cache->new(id_run => 12376,
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
  my $tempdir = tempdir( CLEANUP => 1);
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = 't/data/cache/xml';
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = '';
  my $cache = npg_pipeline::cache->new(id_run => 12376,
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
  ok (!-e join(q[/], $cache_dir, 'st/batches/26195.xml'), 'batch xml is copied');
  is (scalar @{$cache->messages}, 5, 'five messages saved') or diag explain $cache->messages;
  is ($ENV{NPG_WEBSERVICE_CACHE_DIR}, $cache_dir,
    'NPG_WEBSERVICE_CACHE_DIR is set correctly');
  is ($ENV{NPG_CACHED_SAMPLESHEET_FILE}, $sh,
    'NPG_CACHED_SAMPLESHEET_FILE is set');
}

1;
