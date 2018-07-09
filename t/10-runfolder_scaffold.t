use strict;
use warnings;
use Test::More tests => 8;
use Test::Exception;
use Moose::Meta::Class;

use t::util;

use_ok('npg_pipeline::runfolder_scaffold');
use_ok('npg_tracking::illumina::runfolder');

{
  throws_ok {npg_pipeline::runfolder_scaffold->path_in_outgoing()}
    qr/Path required/,
    'error if argument path is not supplied';
  throws_ok {npg_pipeline::runfolder_scaffold->path_in_outgoing(q[])}
    qr/Path required/,
    'error if argument path is empty';
  my $path = '/tmp/analysis/folder';
  my $opath = '/tmp/outgoing/folder';
  is (npg_pipeline::runfolder_scaffold->path_in_outgoing($path),
    $opath, 'path changed to outgoing');
  is (npg_pipeline::runfolder_scaffold->path_in_outgoing($opath),
    $opath, 'path remains in outgoing');
  $path = '/tmp/incoming/folder';
  is (npg_pipeline::runfolder_scaffold->path_in_outgoing($path),
    $path, 'path is not changed');
}

{
  my $util = t::util->new();
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q{t/data/samplesheet_1234.csv};
  $util->create_analysis({skip_archive_dir => 1});

  my $rfs = Moose::Meta::Class->create_anon_class(
    superclasses => ['npg_tracking::illumina::runfolder'],
    roles        => [qw/npg_pipeline::runfolder_scaffold/],
  )->new_object(
      run_folder     => q{123456_IL2_1234},
      runfolder_path => $util->analysis_runfolder_path(),
      is_indexed     => 1
                );

  lives_ok { $rfs->create_product_level({}) }
    q{no error scaffolding runfolder for no products};
}

1;
