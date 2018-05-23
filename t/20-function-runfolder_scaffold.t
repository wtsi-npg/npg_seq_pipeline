use strict;
use warnings;
use Test::More tests => 12;
use Test::Exception;
use Log::Log4perl qw(:levels);
use t::util;

use_ok('npg_pipeline::function::runfolder_scaffold');

my $util = t::util->new();
my $tdir = $util->temp_directory();

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $tdir, 'logfile'),
                          utf8   => 1});

local $ENV{NPG_WEBSERVICE_CACHE_DIR} = 't/data';

{
  $util->remove_staging;
  $util->create_analysis({skip_archive_dir => 1});
  my $afg;
  lives_ok {
    $afg = npg_pipeline::function::runfolder_scaffold->new(
      run_folder     => q{123456_IL2_1234},
      runfolder_path => $util->analysis_runfolder_path(),
      is_indexed     => 1,      
    );
  } q{no error on creation when run_folder provided};
  isa_ok ($afg, 'npg_pipeline::function::runfolder_scaffold');

  my $da;
  lives_ok { $da = $afg->create(); } q{no error creating archive dir};
  ok ($da && @{$da} == 1, 'an array with one definitions is returned');
  my $d = $da->[0];
  isa_ok($d, q{npg_pipeline::function::definition});
  is ($d->created_by, q{npg_pipeline::function::runfolder_scaffold},
    'created_by is correct');
  is ($d->created_on, $afg->timestamp, 'created_on is correct');
  is ($d->identifier, 1234, 'identifier is set correctly');
  ok ($d->immediate_mode, 'immediate mode is true');
  ok (!$d->has_queue, 'queue is not set');

  lives_ok { $afg->create() } q{no error creating archive dir which already exists};
}
1;
