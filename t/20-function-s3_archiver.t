use strict;
use warnings;

use Digest::MD5;
use File::Copy;
use File::Path qw[make_path];
use File::Temp;
use Log::Log4perl qw[:easy];
use Test::More tests => 4;
use Test::Exception;
use t::util;

Log::Log4perl->easy_init($ERROR);

local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
  't/data/novaseq/180709_A00538_0010_BH3FCMDRXX/' .
  'Data/Intensities/BAM_basecalls_20180723-111241/' .
  'metadata_cache_26291/samplesheet_26291.csv';

my $pkg = 'npg_pipeline::function::s3_archiver';
use_ok($pkg);

my $config_path    = 't/data/novaseq/config';
my $runfolder_path = 't/data/novaseq/180709_A00538_0010_BH3FCMDRXX';
my $timestamp      = '20180701-123456';

subtest 'create' => sub {
  plan tests => 155;

  my $archiver = $pkg->new
    (conf_path           => "$config_path/archive_on",
     runfolder_path      => $runfolder_path,
     timestamp           => $timestamp);

  my $total_num_files = 0;
  foreach my $product (@{$archiver->products->{data_products}}) {
    my $name = $product->file_name_root;
    my @observed = $archiver->expected_files($product);
    my $num_files = scalar @observed;

    my $sample = $product->lims->sample_supplier_name;
    $sample ||= 'unnamed';

    cmp_ok($num_files, '==', 10, "$num_files files for product $sample") or
      diag explain \@observed;
    $total_num_files += $num_files;

    my $path_patt = qr|^$runfolder_path/.*/archive/plex\d+/(qc/)?$name|ms;
    foreach my $path (@observed) {
      like($path, $path_patt, "$path matches $path_patt");
    }
  }

  # 140 includes 10 for tag 0 and 10 for tag 888
  cmp_ok($total_num_files, '==', 140, "$total_num_files files expected");
};

subtest 'create' => sub {
  plan tests => 122;

  my $archiver;
  lives_ok {
    $archiver = $pkg->new
      (conf_path           => "$config_path/archive_on",
       runfolder_path      => $runfolder_path,
       timestamp           => $timestamp);
  } 'archiver created ok';

  my $defs = $archiver->create;
  my $num_defs_observed = scalar @{$defs};
  my $num_defs_expected = 120;
  cmp_ok($num_defs_observed, '==', $num_defs_expected,
         "create returns $num_defs_expected definitions when archiving");

  my $cmd_patt = qr|^aws s3 cp --cli-connect-timeout 300 --acl bucket-owner-full-control $runfolder_path/.*/archive/plex\d+/.* s3://|;

  foreach my $def (@{$defs}) {
    my $cmd = $def->command;
    like($cmd, $cmd_patt, "$cmd matches $cmd_patt");
  }
};

subtest 'no_archive_study' => sub {
  plan tests => 1;

  my $archiver = $pkg->new
    (conf_path           => "$config_path/archive_off",
     runfolder_path      => $runfolder_path,
     timestamp           => $timestamp);

  my $defs = $archiver->create;
  my $num_defs_observed = scalar @{$defs};
  my $num_defs_expected = 1;
  cmp_ok($num_defs_observed, '==', $num_defs_expected,
         "create returns $num_defs_expected definitions when not archiving");
};
