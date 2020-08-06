use strict;
use warnings;
use Test::More tests => 4;
use Test::Exception;

use npg_tracking::util::abs_path qw(abs_path);
use t::util;


local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
        't/data/novaseq/200709_A00948_0157_AHM2J2DRXX/Data/' .
        'Intensities/BAM_basecalls_20200710-105415/metadata_cache_34576/' .
        'samplesheet_34576.csv';

my $runfolder_path = 't/data/novaseq/200709_A00948_0157_AHM2J2DRXX';

my $pkg = 'npg_pipeline::function::pp_data_to_irods_archiver';
use_ok($pkg);

subtest 'local flag' => sub {
  plan tests => 3;

  my $archiver = $pkg->new
      (conf_path      => 't/data/release/config/pp_archival',
       id_run         => 34576,
       runfolder_path => $runfolder_path,
       local          => 1);

  my $ds = $archiver->create;
  is(scalar @{$ds}, 1, 'one definition is returned');
  isa_ok($ds->[0], 'npg_pipeline::function::definition');
  is($ds->[0]->excluded, 1, 'function is excluded');
};

subtest 'no_irods_archival flag' => sub {
  plan tests => 3;

  my $archiver = $pkg->new
    (conf_path         => "t/data/release/config/pp_archival",
     id_run            => 34576,
     runfolder_path    => $runfolder_path,
     no_irods_archival => 1);
  my $ds = $archiver->create;
  is(scalar @{$ds}, 1, 'one definition is returned');
  isa_ok($ds->[0], 'npg_pipeline::function::definition');
  is($ds->[0]->excluded, 1, 'function is excluded');
};

subtest 'run archival' => sub {
  plan tests => 1;

  my $archiver = $pkg->new
    (conf_path         => "t/data/release/config/pp_archival",
     id_run            => 34576,
     runfolder_path    => $runfolder_path);
  my $ds = $archiver->create;

  my $num_expected = 384;
  is(scalar @{$ds}, $num_expected, "expected $num_expected definitions");

  foreach my $d (@{$ds}) {


    diag explain $d;
  }


};
