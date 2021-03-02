use strict;
use warnings;

use Digest::MD5;
use File::Copy;
use File::Path qw[make_path];
use File::Temp;
use Log::Log4perl qw[:levels];
use File::Temp qw[tempdir];
use Test::More tests => 18;
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
       timestamp      => $timestamp,
       qc_schema      => $qc);

  my $product = shift @{$archiver->products->{data_products}};

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


subtest '1:1 is_s3_releasable when accept_undef_qc_outcome: undef' => sub {
  plan tests => 4;
  my $lane_qc_rs = $qc->resultset('MqcOutcomeEnt');
  $lane_qc_rs->delete;#seqqc is undef
  my $libqc_rs = $qc->resultset('MqcLibraryOutcomeEnt');
  $libqc_rs->update({id_mqc_outcome=>3});#libqc is final accepted

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
      't/data/novaseq/180709_A00538_0010_BH3FCMDRXX/' .
      'Data/Intensities/BAM_basecalls_20180805-013153/' .
      'metadata_cache_26291/samplesheet_no_align_26291.csv';

  my $archiver = $cls->new_object
        (conf_path      => "t/data/release/config/archive_on",
         runfolder_path => $runfolder_path,
         id_run         => 26291,
         timestamp      => $timestamp,
         qc_schema      => $qc);

  my $product = shift @{$archiver->products->{data_products}};
  my $rpt  = $product->rpt_list();
  my $name = $product->file_name_root();

  is($archiver->qc_outcome_matters($product,'s3'),1,'qc_outcome_matters is set to true');
  is($archiver->accept_undef_qc_outcome($product,'s3'),undef,'accept_undef_qc_outcome is set to false');
  throws_ok{$archiver->has_qc_for_release($product)} qr/Product $name, $rpt are not all Final seq QC values/, 'throws error when accept_undef_qc_outcome is false and seq_qc is undefined for has_qc_for_release';
  throws_ok{$archiver->is_s3_releasable($product)} qr/Product $name, $rpt are not all Final seq QC values/, 'throws error when accept_undef_qc_outcome is false and seq_qc is undefined for is_s3_releasable';

};

#resetting fixture after deletion
$qc = TestDB->new
  (sqlite_utf8_enabled => 1,
   verbose             => 0)->create_test_db('npg_qc::Schema',
                                             't/data/qc_outcomes/fixtures');

subtest '1:2 is_s3_releasable when accept_undef_qc_outcome: undef and seq_qc not final' => sub {
  plan tests => 4;
  my $lane_qc_rs = $qc->resultset('MqcOutcomeEnt');
  $lane_qc_rs->update({id_mqc_outcome=>1});#seqqc not final
  my $libqc_rs = $qc->resultset('MqcLibraryOutcomeEnt');
  $libqc_rs->update({id_mqc_outcome=>3});#libqc is final accepted

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
      't/data/novaseq/180709_A00538_0010_BH3FCMDRXX/' .
      'Data/Intensities/BAM_basecalls_20180805-013153/' .
      'metadata_cache_26291/samplesheet_no_align_26291.csv';

  my $archiver = $cls->new_object
        (conf_path      => "t/data/release/config/archive_on",
         runfolder_path => $runfolder_path,
         id_run         => 26291,
         timestamp      => $timestamp,
         qc_schema      => $qc);

  my $product = shift @{$archiver->products->{data_products}};
  my $rpt  = $product->rpt_list();
  my $name = $product->file_name_root();

  is($archiver->qc_outcome_matters($product,'s3'),1,'qc_outcome_matters is set to true');
  is($archiver->accept_undef_qc_outcome($product,'s3'),undef,'accept_undef_qc_outcome is set to false');
  throws_ok{$archiver->has_qc_for_release($product)} qr/Product $name, $rpt are not all Final seq QC values/, 'throws error when accept_undef_qc_outcome is false and seq_qc is not final for has_qc_for_release';
  throws_ok{$archiver->is_s3_releasable($product)} qr/Product $name, $rpt are not all Final seq QC values/, 'throws error when accept_undef_qc_outcome is false and seq_qc is not final for is_s3_releasable';

};



subtest '1:3 is_s3_releasable when accept_undef_qc_outcome: undef, seq_qc outcome: final rejected' => sub {
  plan tests => 4;
  my $lane_qc_rs = $qc->resultset('MqcOutcomeEnt');
  $lane_qc_rs->update({id_mqc_outcome=>4});#seqqc is final rejected
  my $libqc_rs = $qc->resultset('MqcLibraryOutcomeEnt');
  $libqc_rs->update({id_mqc_outcome=>4});#libqc is final rejected

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
      't/data/novaseq/180709_A00538_0010_BH3FCMDRXX/' .
      'Data/Intensities/BAM_basecalls_20180805-013153/' .
      'metadata_cache_26291/samplesheet_no_align_26291.csv';

  my $archiver = $cls->new_object
        (conf_path      => "t/data/release/config/archive_on",
         runfolder_path => $runfolder_path,
         id_run         => 26291,
         timestamp      => $timestamp,
         qc_schema      => $qc);

  my $product = shift @{$archiver->products->{data_products}};
  my $rpt  = $product->rpt_list();
  my $name = $product->file_name_root();

  is($archiver->qc_outcome_matters($product,'s3'),1,'qc_outcome_matters is set to true');
  is($archiver->accept_undef_qc_outcome($product,'s3'),undef,'accept_undef_qc_outcome is set to false');
  is($archiver->has_qc_for_release($product),0,'has_qc_for_release returns 0 when seq_qc outcome is final rejected');
  is($archiver->is_s3_releasable($product),0,'is_s3_releasable returns 0 when qc_outcome_matters is set to true and seq_qc is final rejected');

};

subtest '1:4 has_qc_for_release when accept_undef_qc_outcome: undef, seq_qc outcome: final accepted, lib_qc outcome: undef' => sub {
  plan tests => 3;
  my $lane_qc_rs = $qc->resultset('MqcOutcomeEnt');
  $lane_qc_rs->update({id_mqc_outcome=>3});#seqqc is final accepted
  my $libqc_rs = $qc->resultset('MqcLibraryOutcomeEnt');
  $libqc_rs->delete;#libqc is undef

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
      't/data/novaseq/180709_A00538_0010_BH3FCMDRXX/' .
      'Data/Intensities/BAM_basecalls_20180805-013153/' .
      'metadata_cache_26291/samplesheet_no_align_26291.csv';

  my $archiver = $cls->new_object
        (conf_path      => "t/data/release/config/archive_on",
         runfolder_path => $runfolder_path,
         id_run         => 26291,
         timestamp      => $timestamp,
         qc_schema      => $qc);

  my $product = shift @{$archiver->products->{data_products}};
  my $rpt  = $product->rpt_list();
  my $name = $product->file_name_root();

  is($archiver->qc_outcome_matters($product,'s3'),1,'qc_outcome_matters is set to true');
  is($archiver->accept_undef_qc_outcome($product,'s3'),undef,'accept_undef_qc_outcome is set to false');
  throws_ok{$archiver->has_qc_for_release($product)} qr/Product $name, $rpt is not Final lib QC value/, 'throws error when accept_undef_qc_outcome is false and seq_qc is final accepted and lib_qc is undef for has_qc_for_release';

};

#resetting fixture after deletion
$qc = TestDB->new
  (sqlite_utf8_enabled => 1,
   verbose             => 0)->create_test_db('npg_qc::Schema',
                                             't/data/qc_outcomes/fixtures');

subtest '1:5 is_s3_releasable when accept_undef_qc_outcome: undef, seq_qc outcome: final accepted, lib_qc outcome: not final' => sub {
  plan tests => 4;
  my $lane_qc_rs = $qc->resultset('MqcOutcomeEnt');
  $lane_qc_rs->update({id_mqc_outcome=>3});#seqqc is final accepted
  my $libqc_rs = $qc->resultset('MqcLibraryOutcomeEnt');
  $libqc_rs->update({id_mqc_outcome=>1});#libqc is not final

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
      't/data/novaseq/180709_A00538_0010_BH3FCMDRXX/' .
      'Data/Intensities/BAM_basecalls_20180805-013153/' .
      'metadata_cache_26291/samplesheet_no_align_26291.csv';

  my $archiver = $cls->new_object
        (conf_path      => "t/data/release/config/archive_on",
         runfolder_path => $runfolder_path,
         id_run         => 26291,
         timestamp      => $timestamp,
         qc_schema      => $qc);

  my $product = shift @{$archiver->products->{data_products}};
  my $rpt  = $product->rpt_list();
  my $name = $product->file_name_root();

  is($archiver->qc_outcome_matters($product,'s3'),1,'qc_outcome_matters is set to true');
  is($archiver->accept_undef_qc_outcome($product,'s3'),undef,'accept_undef_qc_outcome is set to false');
  throws_ok{$archiver->has_qc_for_release($product)} qr/Product $name, $rpt is not Final lib QC value/, 'throws error when accept_undef_qc_outcome is false and seq_qc is final accepted and lib_qc is not final for has_qc_for_release';
  throws_ok{$archiver->is_s3_releasable($product)} qr/Product $name, $rpt is not Final lib QC value/, 'throws error when accept_undef_qc_outcome is false and seq_qc is final accepted and lib_qc is not final for is_s3_releasable';
};



subtest '1:6 is_s3_releasable when accept_undef_qc_outcome: undef, seq_qc outcome: final accepted, lib_qc outcome: final undecided' => sub {
  plan tests => 4;
  my $lane_qc_rs = $qc->resultset('MqcOutcomeEnt');
  $lane_qc_rs->update({id_mqc_outcome=>3});#seqqc is final accepted
  my $libqc_rs = $qc->resultset('MqcLibraryOutcomeEnt');
  $libqc_rs->update({id_mqc_outcome=>6});#libqc is final undecided

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
      't/data/novaseq/180709_A00538_0010_BH3FCMDRXX/' .
      'Data/Intensities/BAM_basecalls_20180805-013153/' .
      'metadata_cache_26291/samplesheet_no_align_26291.csv';

  my $archiver = $cls->new_object
        (conf_path      => "t/data/release/config/archive_on",
         runfolder_path => $runfolder_path,
         id_run         => 26291,
         timestamp      => $timestamp,
         qc_schema      => $qc);

  my $product = shift @{$archiver->products->{data_products}};
  my $rpt  = $product->rpt_list();
  my $name = $product->file_name_root();

  is($archiver->qc_outcome_matters($product,'s3'),1,'qc_outcome_matters is set to true');
  is($archiver->accept_undef_qc_outcome($product,'s3'),undef,'accept_undef_qc_outcome is set to false');
  is($archiver->has_qc_for_release($product),0,'has_qc_for_release returns 0 when seq_qc is final accepted and lib_qc is final undecided');
  is($archiver->is_s3_releasable($product),0,'is_s3_releasable returns 0 when seq_qc is final accepted and lib_qc is final undecided');
};


subtest '1:7 is_s3_releasable when accept_undef_qc_outcome: undef, seq_qc outcome: final accepted, lib_qc outcome: final rejected' => sub {
  plan tests => 4;
  my $lane_qc_rs = $qc->resultset('MqcOutcomeEnt');
  $lane_qc_rs->update({id_mqc_outcome=>3});#seqqc is final accepted
  my $libqc_rs = $qc->resultset('MqcLibraryOutcomeEnt');
  $libqc_rs->update({id_mqc_outcome=>4});#libqqc is final rejected

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
      't/data/novaseq/180709_A00538_0010_BH3FCMDRXX/' .
      'Data/Intensities/BAM_basecalls_20180805-013153/' .
      'metadata_cache_26291/samplesheet_no_align_26291.csv';

  my $archiver = $cls->new_object
        (conf_path      => "t/data/release/config/archive_on",
         runfolder_path => $runfolder_path,
         id_run         => 26291,
         timestamp      => $timestamp,
         qc_schema      => $qc);

  my $product = shift @{$archiver->products->{data_products}};
  my $rpt  = $product->rpt_list();
  my $name = $product->file_name_root();

  is($archiver->qc_outcome_matters($product,'s3'),1,'qc_outcome_matters is set to true');
  is($archiver->accept_undef_qc_outcome($product,'s3'),undef,'accept_undef_qc_outcome is set to false');
  is($archiver->has_qc_for_release($product),0,'has_qc_for_release returns 0 when seq_qc is final accepted and lib_qc is final rejected');
  is($archiver->is_s3_releasable($product),0,'is_s3_releasable returns 0 when seq_qc is final accepted and lib_qc is final rejected');
};

subtest '1:8 is_s3_releasable when accept_undef_qc_outcome: undef, seq_qc outcome: final accepted, lib_qc outcome: final accepted' => sub {
  plan tests => 4;
  my $lane_qc_rs = $qc->resultset('MqcOutcomeEnt');
  $lane_qc_rs->update({id_mqc_outcome=>3});#seqqc is final accepted
  my $libqc_rs = $qc->resultset('MqcLibraryOutcomeEnt');
  $libqc_rs->update({id_mqc_outcome=>3});#libqc is final accepted

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
      't/data/novaseq/180709_A00538_0010_BH3FCMDRXX/' .
      'Data/Intensities/BAM_basecalls_20180805-013153/' .
      'metadata_cache_26291/samplesheet_no_align_26291.csv';

  my $archiver = $cls->new_object
        (conf_path      => "t/data/release/config/archive_on",
         runfolder_path => $runfolder_path,
         id_run         => 26291,
         timestamp      => $timestamp,
         qc_schema      => $qc);

  my $product = shift @{$archiver->products->{data_products}};
  my $rpt  = $product->rpt_list();
  my $name = $product->file_name_root();

  is($archiver->qc_outcome_matters($product,'s3'),1,'qc_outcome_matters is set to true');
  is($archiver->accept_undef_qc_outcome($product,'s3'),undef,'accept_undef_qc_outcome is set to false');
  is($archiver->has_qc_for_release($product),1,'has_qc_for_release returns 1 when seq_qc is final accepted and lib_qc is final accepted');
  is($archiver->is_s3_releasable($product),1,'is_s3_releasable returns 1 when seq_qc is final accepted and lib_qc is final accepted');
};

#accept_undef_qc_outcome is TRUE

subtest '2:1 is_s3_releasable when accept_undef_qc_outcome: true and seq_qc is undefined' => sub {
  plan tests => 4;
  ## setting up schema with values
  my $lane_qc_rs = $qc->resultset('MqcOutcomeEnt');
  $lane_qc_rs->delete;# seqqc is undef
  my $libqc_rs = $qc->resultset('MqcLibraryOutcomeEnt');
  $libqc_rs->update({id_mqc_outcome=>3});#libqc is final accepted

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
      't/data/novaseq/180709_A00538_0010_BH3FCMDRXX/' .
      'Data/Intensities/BAM_basecalls_20180805-013153/' .
      'metadata_cache_26291/samplesheet_no_align_26291.csv';

  my $archiver = $cls->new_object
        (conf_path      => "t/data/release/config/undef_qc_accept",
         runfolder_path => $runfolder_path,
         id_run         => 26291,
         timestamp      => $timestamp,
         qc_schema      => $qc);

  my $product = shift @{$archiver->products->{data_products}};
  my $rpt  = $product->rpt_list();
  my $name = $product->file_name_root();

  is($archiver->qc_outcome_matters($product,'s3'),1,'qc_outcome_matters is set to true');
  is($archiver->accept_undef_qc_outcome($product,'s3'),1,'accept_undef_qc_outcome is set to true');
  is($archiver->has_qc_for_release($product),1,'returns 1 when accept_undef_qc_outcome is true and seq_qc is undefind for has_qc_for_release');
  is($archiver->is_s3_releasable($product),1,'returns 1 when accept_undef_qc_outcome is true and seq_qc is undefined for is_s3_releasable');
};

#resetting fixture after deletion
$qc = TestDB->new
  (sqlite_utf8_enabled => 1,
   verbose             => 0)->create_test_db('npg_qc::Schema',
                                             't/data/qc_outcomes/fixtures');
subtest '2:2 is_s3_releasable when accept_undef_qc_outcome: true and seq_qc not final' => sub {
  plan tests => 4;
  my $lane_qc_rs = $qc->resultset('MqcOutcomeEnt');
  $lane_qc_rs->update({id_mqc_outcome=>1});#seq_qc not final
  my $libqc_rs = $qc->resultset('MqcLibraryOutcomeEnt');
  $libqc_rs->update({id_mqc_outcome=>3});#libqc is final accepted

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
      't/data/novaseq/180709_A00538_0010_BH3FCMDRXX/' .
      'Data/Intensities/BAM_basecalls_20180805-013153/' .
      'metadata_cache_26291/samplesheet_no_align_26291.csv';

  my $archiver = $cls->new_object
        (conf_path      => "t/data/release/config/undef_qc_accept",
         runfolder_path => $runfolder_path,
         id_run         => 26291,
         timestamp      => $timestamp,
         qc_schema      => $qc);

  my $product = shift @{$archiver->products->{data_products}};
  my $rpt  = $product->rpt_list();
  my $name = $product->file_name_root();

  is($archiver->qc_outcome_matters($product,'s3'),1,'qc_outcome_matters is set to true');
  is($archiver->accept_undef_qc_outcome($product,'s3'),1,'accept_undef_qc_outcome is set to true');
  throws_ok{$archiver->has_qc_for_release($product)} qr/Product $name, $rpt are not all Final seq QC values/, 'throws error when accept_undef_qc_outcome is true and seq_qc is not final for has_qc_for_release';
  throws_ok{$archiver->is_s3_releasable($product)} qr/Product $name, $rpt are not all Final seq QC values/, 'throws error when accept_undef_qc_outcome is true and seq_qc is not final for is_s3_releasable';

};



subtest '2:3 is_s3_releasable when accept_undef_qc_outcome: true, seq_qc outcome: final rejected' => sub {
  plan tests => 4;
  my $lane_qc_rs = $qc->resultset('MqcOutcomeEnt');
  $lane_qc_rs->update({id_mqc_outcome=>4});#seqqc is final rejected
  my $libqc_rs = $qc->resultset('MqcLibraryOutcomeEnt');
  $libqc_rs->update({id_mqc_outcome=>4});#libqc is final rejected

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
      't/data/novaseq/180709_A00538_0010_BH3FCMDRXX/' .
      'Data/Intensities/BAM_basecalls_20180805-013153/' .
      'metadata_cache_26291/samplesheet_no_align_26291.csv';

  my $archiver = $cls->new_object
        (conf_path      => "t/data/release/config/undef_qc_accept",
         runfolder_path => $runfolder_path,
         id_run         => 26291,
         timestamp      => $timestamp,
         qc_schema      => $qc);

  my $product = shift @{$archiver->products->{data_products}};
  my $rpt  = $product->rpt_list();
  my $name = $product->file_name_root();

  is($archiver->qc_outcome_matters($product,'s3'),1,'qc_outcome_matters is set to true');
  is($archiver->accept_undef_qc_outcome($product,'s3'),1,'accept_undef_qc_outcome is set to true');
  is($archiver->has_qc_for_release($product),0,'has_qc_for_release returns 0 when seq_qc outcome is final rejected');
  is($archiver->is_s3_releasable($product),0,'is_s3_releasable returns 0 when qc_outcome_matters is set to true and seq_qc is final rejected');

};

subtest '2:4 has_qc_for_release when accept_undef_qc_outcome: true, seq_qc outcome: final accepted, lib_qc outcome: undef' => sub {
  plan tests => 4;
  my $lane_qc_rs = $qc->resultset('MqcOutcomeEnt');
  $lane_qc_rs->update({id_mqc_outcome=>3});#seqqc is final accepted
  my $libqc_rs = $qc->resultset('MqcLibraryOutcomeEnt');
  $libqc_rs->delete;#libqc is undef

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
      't/data/novaseq/180709_A00538_0010_BH3FCMDRXX/' .
      'Data/Intensities/BAM_basecalls_20180805-013153/' .
      'metadata_cache_26291/samplesheet_no_align_26291.csv';

  my $archiver = $cls->new_object
        (conf_path      => "t/data/release/config/undef_qc_accept",
         runfolder_path => $runfolder_path,
         id_run         => 26291,
         timestamp      => $timestamp,
         qc_schema      => $qc);

  my $product = shift @{$archiver->products->{data_products}};
  my $rpt  = $product->rpt_list();
  my $name = $product->file_name_root();

  is($archiver->qc_outcome_matters($product,'s3'),1,'qc_outcome_matters is set to true');
  is($archiver->accept_undef_qc_outcome($product,'s3'),1,'accept_undef_qc_outcome is set to true');
  throws_ok{$archiver->has_qc_for_release($product)} qr/Product $name, $rpt is not Final lib QC value/, 'throws error when accept_undef_qc_outcome is true and seq_qc is final accepted and lib_qc is undef for has_qc_for_release';
throws_ok{$archiver->is_s3_releasable($product)} qr/Product $name, $rpt is not Final lib QC value/, 'throws error when accept_undef_qc_outcome is true and seq_qc is final accepted and lib_qc is undef for has_qc_for_release';

};

#resetting fixture after deletion
$qc = TestDB->new
  (sqlite_utf8_enabled => 1,
   verbose             => 0)->create_test_db('npg_qc::Schema',
                                             't/data/qc_outcomes/fixtures');

subtest '2:5 is_s3_releasable when accept_undef_qc_outcome: true, seq_qc outcome: final accepted, lib_qc outcome: not final' => sub {
  plan tests => 4;
  my $lane_qc_rs = $qc->resultset('MqcOutcomeEnt');
  $lane_qc_rs->update({id_mqc_outcome=>3});#seqqc is final accepted
  my $libqc_rs = $qc->resultset('MqcLibraryOutcomeEnt');
  $libqc_rs->update({id_mqc_outcome=>1});#libqc is not final

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
      't/data/novaseq/180709_A00538_0010_BH3FCMDRXX/' .
      'Data/Intensities/BAM_basecalls_20180805-013153/' .
      'metadata_cache_26291/samplesheet_no_align_26291.csv';

  my $archiver = $cls->new_object
        (conf_path      => "t/data/release/config/undef_qc_accept",
         runfolder_path => $runfolder_path,
         id_run         => 26291,
         timestamp      => $timestamp,
         qc_schema      => $qc);

  my $product = shift @{$archiver->products->{data_products}};
  my $rpt  = $product->rpt_list();
  my $name = $product->file_name_root();

  is($archiver->qc_outcome_matters($product,'s3'),1,'qc_outcome_matters is set to true');
  is($archiver->accept_undef_qc_outcome($product,'s3'),1,'accept_undef_qc_outcome is set to true');
  throws_ok{$archiver->has_qc_for_release($product)} qr/Product $name, $rpt is not Final lib QC value/, 'throws error when accept_undef_qc_outcome is true and seq_qc is final accepted and lib_qc is not final for has_qc_for_release';
  throws_ok{$archiver->is_s3_releasable($product)} qr/Product $name, $rpt is not Final lib QC value/, 'throws error when accept_undef_qc_outcome is true and seq_qc is final accepted and lib_qc is not final for is_s3_releasable';
};


# should return 1
subtest '2:6 is_s3_releasable when accept_undef_qc_outcome: true, seq_qc outcome: final accepted, lib_qc outcome: final undecided' => sub {
  plan tests => 4;
  my $lane_qc_rs = $qc->resultset('MqcOutcomeEnt');
  $lane_qc_rs->update({id_mqc_outcome=>3});#seqqc is final accepted
  my $libqc_rs = $qc->resultset('MqcLibraryOutcomeEnt');
  $libqc_rs->update({id_mqc_outcome=>6});#libqc is final undecided

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
      't/data/novaseq/180709_A00538_0010_BH3FCMDRXX/' .
      'Data/Intensities/BAM_basecalls_20180805-013153/' .
      'metadata_cache_26291/samplesheet_no_align_26291.csv';

  my $archiver = $cls->new_object
        (conf_path      => "t/data/release/config/undef_qc_accept",
         runfolder_path => $runfolder_path,
         id_run         => 26291,
         timestamp      => $timestamp,
         qc_schema      => $qc);

  my $product = shift @{$archiver->products->{data_products}};
  my $rpt  = $product->rpt_list();
  my $name = $product->file_name_root();

  is($archiver->qc_outcome_matters($product,'s3'),1,'qc_outcome_matters is set to true');
  is($archiver->accept_undef_qc_outcome($product,'s3'),1,'accept_undef_qc_outcome is set to true');
  is($archiver->has_qc_for_release($product),1,'has_qc_for_release returns 1 when seq_qc is final accepted and lib_qc is final undecided');
  is($archiver->is_s3_releasable($product),1,'is_s3_releasable returns 1 when seq_qc is final accepted and lib_qc is final undecided');
};


subtest '2:7 is_s3_releasable when accept_undef_qc_outcome: true, seq_qc outcome: final accepted, lib_qc outcome: final rejected' => sub {
  plan tests => 4;
  my $lane_qc_rs = $qc->resultset('MqcOutcomeEnt');
  $lane_qc_rs->update({id_mqc_outcome=>3});#seqqc is final accepted
  my $libqc_rs = $qc->resultset('MqcLibraryOutcomeEnt');
  $libqc_rs->update({id_mqc_outcome=>4});#libqc is final rejected

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
      't/data/novaseq/180709_A00538_0010_BH3FCMDRXX/' .
      'Data/Intensities/BAM_basecalls_20180805-013153/' .
      'metadata_cache_26291/samplesheet_no_align_26291.csv';

  my $archiver = $cls->new_object
        (conf_path      => "t/data/release/config/undef_qc_accept",
         runfolder_path => $runfolder_path,
         id_run         => 26291,
         timestamp      => $timestamp,
         qc_schema      => $qc);

  my $product = shift @{$archiver->products->{data_products}};
  my $rpt  = $product->rpt_list();
  my $name = $product->file_name_root();

  is($archiver->qc_outcome_matters($product,'s3'),1,'qc_outcome_matters is set to true');
  is($archiver->accept_undef_qc_outcome($product,'s3'),1,'accept_undef_qc_outcome is set to true');
  is($archiver->has_qc_for_release($product),0,'has_qc_for_release returns 0 when seq_qc is final accepted and lib_qc is final rejected');
  is($archiver->is_s3_releasable($product),0,'is_s3_releasable returns 0 when seq_qc is final accepted and lib_qc is final rejected');
};

subtest '2:8 is_s3_releasable when accept_undef_qc_outcome: true, seq_qc outcome: final accepted, lib_qc outcome: final accepted' => sub {
  plan tests => 4;
  my $lane_qc_rs = $qc->resultset('MqcOutcomeEnt');
  $lane_qc_rs->update({id_mqc_outcome=>3});#seqqc is final accepted
  my $libqc_rs = $qc->resultset('MqcLibraryOutcomeEnt');
  $libqc_rs->update({id_mqc_outcome=>3});#libqc is final accepted

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
      't/data/novaseq/180709_A00538_0010_BH3FCMDRXX/' .
      'Data/Intensities/BAM_basecalls_20180805-013153/' .
      'metadata_cache_26291/samplesheet_no_align_26291.csv';

  my $archiver = $cls->new_object
        (conf_path      => "t/data/release/config/undef_qc_accept",
         runfolder_path => $runfolder_path,
         id_run         => 26291,
         timestamp      => $timestamp,
         qc_schema      => $qc);

  my $product = shift @{$archiver->products->{data_products}};
  my $rpt  = $product->rpt_list();
  my $name = $product->file_name_root();

  is($archiver->qc_outcome_matters($product,'s3'),1,'qc_outcome_matters is set to true');
  is($archiver->accept_undef_qc_outcome($product,'s3'),1,'accept_undef_qc_outcome is set to true');
  is($archiver->has_qc_for_release($product),1,'has_qc_for_release returns 1 when seq_qc is final accepted and lib_qc is final accepted');
  is($archiver->is_s3_releasable($product),1,'is_s3_releasable returns 1 when seq_qc is final accepted and lib_qc is final accepted');
};
