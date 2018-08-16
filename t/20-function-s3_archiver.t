use strict;
use warnings;

use Digest::MD5;
use File::Copy;
use File::Path qw[make_path];
use File::Temp;
use Log::Log4perl qw[:levels];
use Test::More tests => 4;
use Test::Exception;
use t::util;

Log::Log4perl->easy_init({level  => $INFO,
                          layout => '%d %p %m %n'});

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

my $pkg = 'npg_pipeline::function::s3_archiver';
use_ok($pkg);

my $runfolder_path = 't/data/novaseq/180709_A00538_0010_BH3FCMDRXX';
my $timestamp      = '20180701-123456';

subtest 'expected_files' => sub {
  plan tests => 1;

  my $archiver = $pkg->new
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

subtest 'create' => sub {
  plan tests => 27;

  my $archiver;
  lives_ok {
    $archiver = $pkg->new
      (conf_path      => "t/data/release/config/archive_on",
       runfolder_path => $runfolder_path,
       id_run         => 26291,
       timestamp      => $timestamp,
       qc_schema      => $qc);
  } 'archiver created ok';

  my @defs = @{$archiver->create};
  my $num_defs_observed = scalar @defs;
  my $num_defs_expected = 2; # Only 2 pass manual QC, tag index 3 and 9
  cmp_ok($num_defs_observed, '==', $num_defs_expected,
         "create returns $num_defs_expected definitions when archiving");

  my @archived_rpts;
  foreach my $def (@defs) {
    push @archived_rpts,
      [map { [$_->id_run, $_->position, $_->tag_index] }
       $def->composition->components_list];
  }

  is_deeply(\@archived_rpts,
            [[[26291, 1, 3], [26291, 2, 3]],
             [[26291, 1, 9], [26291, 2, 9]]],
            'Only "26291:1:3;26291:2:3" and "26291:1:9;26291:2:9" archived')
    or diag explain \@archived_rpts;

  my $cmd_patt = qr|^aws s3 cp --cli-connect-timeout 300 --acl bucket-owner-full-control $runfolder_path/.*/archive/plex\d+/.* s3://|;

  foreach my $def (@defs) {
    is($def->created_by, $pkg, "created_by is $pkg");
    is($def->identifier, 26291, "identifier is set correctly");

    my $cmd = $def->command;
    my @parts = split / && /, $cmd; # Deconstruct the command
    foreach my $part (@parts) {
      like($cmd, $cmd_patt, "$cmd matches $cmd_patt");
    }
  }
};

subtest 'no_archive_study' => sub {
  plan tests => 2;

  my $archiver = $pkg->new
    (conf_path      => "t/data/release/config/archive_off",
     runfolder_path => $runfolder_path,
     id_run         => 26291,
     timestamp      => $timestamp,
     qc_schema      => $qc);

  my @defs = @{$archiver->create};
  my $num_defs_observed = scalar @defs;
  my $num_defs_expected = 1;
  cmp_ok($num_defs_observed, '==', $num_defs_expected,
         "create returns $num_defs_expected definitions when not archiving") or
           diag explain \@defs;

  is($defs[0]->composition, undef, 'definition has no composition') or
    diag explain \@defs;
};
