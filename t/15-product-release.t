use strict;
use warnings;

use Digest::MD5;
use File::Copy;
use File::Path qw[make_path];
use File::Temp;
use Log::Log4perl qw[:levels];
use File::Temp qw[tempdir];
use Test::More tests => 2;
use Test::Exception;
use Moose::Meta::Class;
use t::util;

my $temp_dir = tempdir(CLEANUP => 1);
Log::Log4perl->easy_init({level  => $INFO,
                          layout => '%d %p %m %n',
                          file   => join(q[/], $temp_dir, 'logfile')});

{
  package TestDB;
  use Moose;

  with 'npg_testing::db';
}

local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
  't/data/novaseq/180709_A00538_0010_BH3FCMDRXX/' .
  'Data/Intensities/BAM_basecalls_20180805-013153/' .
  'metadata_cache_26291/samplesheet_26291.csv';

my $cls = Moose::Meta::Class->create_anon_class(
            superclasses => ['npg_pipeline::base'],
            roles => ['npg_pipeline::product::release']
          );

my $runfolder_path = 't/data/novaseq/180709_A00538_0010_BH3FCMDRXX';
my $timestamp      = '20180701-123456';

subtest 'expected_files' => sub {
  plan tests => 2;

  my $archiver = $cls->new_object
    (conf_path      => "t/data/release/config/archive_on",
     runfolder_path => $runfolder_path,
     id_run         => 26291,
     timestamp      => $timestamp);

  my $path = "$runfolder_path/Data/Intensities/" .
             'BAM_basecalls_20180805-013153/no_cal/archive/plex1';
  my $product = $archiver->products->{data_products}->[4];
  is ($product->rpt_list, '26291:1:1;26291:2:1', 'correct product');

  my @expected = sort map { "$path/$_" }
    ('26291#1_F0x900.stats',
     '26291#1_F0xB00.stats',
     '26291#1_F0xF04_target.stats',
     '26291#1_F0xF04_target_autosome.stats',
     '26291#1.bcfstats',
     '26291#1.cram',
     '26291#1.cram.crai',
     '26291#1.cram.md5',
     '26291#1.seqchksum',
     '26291#1.sha512primesums512.seqchksum',
     'qc/26291#1.verify_bam_id.json');

  my @observed = $archiver->expected_files($product);
  is_deeply(\@observed, \@expected, 'Expected files listed') or
    diag explain \@observed;
};

subtest 'expected_unaligned_files' => sub {
  plan tests => 1;
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
    't/data/novaseq/180709_A00538_0010_BH3FCMDRXX/' .
    'Data/Intensities/BAM_basecalls_20180805-013153/' .
    'metadata_cache_26291/samplesheet_no_align_26291.csv';

  my $archiver = $cls->new_object
      (conf_path      => "t/data/release/config/archive_on",
       runfolder_path => $runfolder_path,
       id_run         => 26291,
       timestamp      => $timestamp);

  my $product = $archiver->products->{data_products}->[4];
  my $path = "$runfolder_path/Data/Intensities/" .
      'BAM_basecalls_20180805-013153/no_cal/archive/plex1';
  my @expected = sort map { "$path/$_" }
    ('26291#1_F0x900.stats',
     '26291#1_F0xB00.stats',
     '26291#1.cram',
     '26291#1.cram.md5',
     '26291#1.seqchksum',
     '26291#1.sha512primesums512.seqchksum');

  my @observed = $archiver->expected_files($product);
  is_deeply(\@observed, \@expected, 'Expected files listed') or
      diag explain \@observed;
};

1;

