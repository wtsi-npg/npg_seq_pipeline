use strict;
use warnings;

use Digest::MD5;
use File::Copy;
use File::Path qw[make_path];
use File::Temp;
use Log::Log4perl qw[:levels];
use File::Temp qw[tempdir];
use Test::More tests => 6;
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

my $pkg = 'npg_pipeline::function::s3_archiver';
use_ok($pkg);

my $runfolder_path = 't/data/novaseq/180709_A00538_0010_BH3FCMDRXX';
my $timestamp      = '20180701-123456';

subtest 'local and no_s3_archival flag' => sub {
  plan tests => 7;

  my $archiver = $pkg->new
    (conf_path      => "t/data/release/config/archive_on",
     runfolder_path => $runfolder_path,
     id_run         => 26291,
     timestamp      => $timestamp,
     qc_schema      => $qc,
     local          => 1);
  ok($archiver->no_s3_archival, 'no_s3_archival flag is set to true');
  my $ds = $archiver->create;
  is(scalar @{$ds}, 1, 'one definition is returned');
  isa_ok($ds->[0], 'npg_pipeline::function::definition');
  is($ds->[0]->excluded, 1, 'function is excluded');

  $archiver = $pkg->new
    (conf_path      => "t/data/release/config/archive_on",
     runfolder_path => $runfolder_path,
     id_run         => 26291,
     timestamp      => $timestamp,
     qc_schema      => $qc,
     no_s3_archival => 1);
  ok(!$archiver->local, 'local flag is false');
  $ds = $archiver->create;
  is(scalar @{$ds}, 1, 'one definition is returned');
  is($ds->[0]->excluded, 1, 'function is excluded');
};

subtest 'create' => sub {
  plan tests => 28;

  my $archiver;
  lives_ok {
    $archiver = $pkg->new
      (conf_path      => "t/data/release/config/archive_on",
       runfolder_path => $runfolder_path,
       id_run         => 26291,
       timestamp      => $timestamp,
       qc_schema      => $qc);
  } 'archiver created ok';

  dies_ok {$archiver->create} 'preliminary results present - error';

  my $rs = $qc->resultset('MqcLibraryOutcomeEnt');
  # Make all outcomes either a rejected or undecided final result
  while (my $row = $rs->next) {
    if (!$row->has_final_outcome) {
      my $shift = $row->is_undecided ? 1 : ($row->is_accepted ? 3 : 2);
      $row->update({id_mqc_outcome => $row->id_mqc_outcome + $shift});
    }
  }

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

  my $cmd_patt = qr|^aws s3 cp --cli-connect-timeout 300 --acl bucket-owner-full-control --quiet --profile s3_profile_name $runfolder_path/.*/archive/plex\d+/.* s3://\S+$|;

  foreach my $def (@defs) {
    is($def->created_by, $pkg, "created_by is $pkg");
    is($def->identifier, 26291, "identifier is set correctly");

    my $cmd = $def->command;
    my @parts = split / && /, $cmd; # Deconstruct the command
    foreach my $part (@parts) {
      like($part, $cmd_patt, "$cmd matches $cmd_patt");
    }
  }
};

subtest 'configure_s3_endpoint' => sub {
  plan tests => 21;

  my $archiver;
  lives_ok {
    $archiver = $pkg->new
      (conf_path       => "t/data/release/config/alternative_s3_endpoint",
       runfolder_path => $runfolder_path,
       id_run         => 26291,
       timestamp      => $timestamp,
       qc_schema      => $qc);
  } 'archiver created ok';

  my $cmd_patt = qr|^aws s3 cp --cli-connect-timeout 300 --acl bucket-owner-full-control --quiet --profile s3_profile_name --endpoint-url https://alternative[.]endpoint[.]net $runfolder_path/.*/archive/plex\d+/.* s3://\S+$|;

  my @defs = @{$archiver->create};
  foreach my $def (@defs) {
    my $cmd = $def->command;
    my @parts = split / && /, $cmd; # Deconstruct the command
    foreach my $part (@parts) {
      like($part, $cmd_patt, "$cmd matches $cmd_patt");
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

subtest 'multiple or no study configs' => sub {
  plan tests => 2;

  my $archiver = $pkg->new
    (conf_path      => 't/data/release/config/multiple_configs',
     runfolder_path => $runfolder_path,
     id_run         => 26291,
     timestamp      => $timestamp,
     qc_schema      => $qc);

  throws_ok {$archiver->create}
    qr/Multiple configurations for study 5290/,
    'error if multiple study configs are found';

  $archiver = $pkg->new
    (conf_path      => 't/data/release/config/no_config',
     runfolder_path => $runfolder_path,
     id_run         => 26291,
     timestamp      => $timestamp,
     qc_schema      => $qc);

  throws_ok {$archiver->create}
    qr/No release configuration was defined for study 5290/,
    'error if neither study no default config is found';
};
