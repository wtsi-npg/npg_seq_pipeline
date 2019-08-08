use strict;
use warnings;

use Digest::MD5;
use File::Copy;
use File::Path qw[make_path];
use File::Temp;
use Log::Log4perl qw[:levels];
use File::Temp qw[tempdir];
use Test::More tests => 1;
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

# See README in fixtures for a description of the test data.
my $qc = TestDB->new
  (sqlite_utf8_enabled => 1,
   verbose             => 0)->create_test_db('npg_qc::Schema',
                                             't/data/qc_outcomes/fixtures');

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
  plan tests => 1;

  my $archiver = $cls->new_object
    (conf_path      => "t/data/release/config/archive_on",
     runfolder_path => $runfolder_path,
     id_run         => 26291,
     timestamp      => $timestamp,
     qc_schema      => $qc);

  my $product = shift @{$archiver->products->{data_products}};

  my $path = "$runfolder_path/Data/Intensities/" .
             'BAM_basecalls_20180805-013153/no_cal/archive/plex1';
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
