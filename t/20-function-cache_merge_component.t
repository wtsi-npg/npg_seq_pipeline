use strict;
use warnings;

use Digest::MD5;
use File::Copy;
use File::Path qw[make_path];
use File::Temp;
use Log::Log4perl qw[:levels];
use File::Temp qw[tempdir];
use Test::More tests => 5;
use Test::Exception;
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

my $pkg = 'npg_pipeline::function::cache_merge_component';
use_ok($pkg);

my $runfolder_path = 't/data/novaseq/180709_A00538_0010_BH3FCMDRXX';
my $timestamp      = '20180701-123456';

subtest 'local and no_cache_merge_component' => sub {
  plan tests => 7;

  my $cacher = $pkg->new
    (conf_path      => "t/data/release/config/archive_on",
     runfolder_path => $runfolder_path,
     id_run         => 26291,
     timestamp      => $timestamp,
     qc_schema      => $qc,
     local          => 1);
  ok($cacher->no_cache_merge_component, 'no_cache_merge_component flag is set to true');
  my $ds = $cacher->create;
  is(scalar @{$ds}, 1, 'one definition is returned');
  isa_ok($ds->[0], 'npg_pipeline::function::definition');
  is($ds->[0]->excluded, 1, 'function is excluded');

  $cacher = $pkg->new
    (conf_path      => "t/data/release/config/archive_on",
     runfolder_path => $runfolder_path,
     id_run         => 26291,
     timestamp      => $timestamp,
     qc_schema      => $qc,
     no_cache_merge_component => 1);
  ok(!$cacher->local, 'local flag is false');
  $ds = $cacher->create;
  is(scalar @{$ds}, 1, 'one definition is returned');
  is($ds->[0]->excluded, 1, 'function is excluded');
};

subtest 'expected_files' => sub {
  plan tests => 1;

  my $cacher = $pkg->new
    (conf_path      => "t/data/release/config/archive_on",
     runfolder_path => $runfolder_path,
     id_run         => 26291,
     timestamp      => $timestamp,
     qc_schema      => $qc);

  my $product = shift @{$cacher->products->{data_products}};

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

  my @observed = $cacher->expected_files($product);
  is_deeply(\@observed, \@expected, 'Expected files listed') or
    diag explain \@observed;
};

subtest 'create' => sub {
  plan tests => 3 + 12 * 8;

  my $cacher;
  lives_ok {
    $cacher = $pkg->new
      (conf_path      => "t/data/release/config/archive_on",
       runfolder_path => $runfolder_path,
       id_run         => 26291,
       timestamp      => $timestamp,
       qc_schema      => $qc);
  } 'cacher created ok';

  my @defs = @{$cacher->create};
  my $num_defs_observed = scalar @defs;
  my $num_defs_expected = 8; #  12 total - 2 final accepted - 2 final rejected = 9 to cache
  cmp_ok($num_defs_observed, '==', $num_defs_expected,
         "create returns $num_defs_expected definitions when caching");

  my @archived_rpts;
  foreach my $def (@defs) {
    push @archived_rpts,
      [map { [$_->id_run, $_->position, $_->tag_index] }
       $def->composition->components_list];
  }

  is_deeply(\@archived_rpts,
            [
             [[26291, 1, 1], [26291, 2, 1]],
             [[26291, 1, 2], [26291, 2, 2]],
             [[26291, 1, 5], [26291, 2, 5]],
             [[26291, 1, 6], [26291, 2, 6]],
             [[26291, 1, 7], [26291, 2, 7]],
             [[26291, 1, 8], [26291, 2, 8]],
             [[26291, 1,11], [26291, 2,11]],
             [[26291, 1,12], [26291, 2,12]]
                                           ],
            '9 non-final accepted or rejected cached')
    or diag explain \@archived_rpts;

  my $cmd_patt = qr|^ln $runfolder_path/.*/archive/plex\d+/.* /tmp/npg_seq_pipeline/cache_merge_component_test/|;

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

subtest 'no_cache_study' => sub {
  plan tests => 2;

  my $cacher = $pkg->new
    (conf_path      => "t/data/release/config/archive_off",
     runfolder_path => $runfolder_path,
     id_run         => 26291,
     timestamp      => $timestamp,
     qc_schema      => $qc);

  my @defs = @{$cacher->create};
  my $num_defs_observed = scalar @defs;
  my $num_defs_expected = 1;
  cmp_ok($num_defs_observed, '==', $num_defs_expected,
         "create returns $num_defs_expected definitions when not archiving") or
           diag explain \@defs;

  is($defs[0]->composition, undef, 'definition has no composition') or
    diag explain \@defs;
};
