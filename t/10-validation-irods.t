use strict;
use warnings;
use Test::More tests => 6;
use Test::Warn;
use Test::Exception;
use File::Temp qw/ tempdir /;
use Log::Log4perl;
use File::Slurp qw/ write_file read_file/;
use File::Path qw/ make_path /;
use File::Basename;
use File::Copy;
use File::Which qw(which);
use Digest::MD5 qw/ md5_hex /;

use WTSI::NPG::iRODS;
use st::api::lims;

use_ok('npg_pipeline::product');
use_ok('npg_pipeline::validation::entity');
use_ok('npg_pipeline::validation::irods');

my @file_list = (
  '20405_6#0', '20405_6#4',  '20405_6#5',
  '20405_3#0', '20405_3#12',               '20405_3#888',
  '20405_7#0', '20405_7#1',  '20405_7#12', '20405_7#888',
  '20405_2#0', '20405_2#12',               '20405_2#888',
  '20405_8#0', '20405_8#7',  '20405_8#8',  '20405_8#888',
  '20405_1#0', '20405_1#11', '20405_1#12',
  '20405_4#0', '20405_4#12',               '20405_4#888',
  '20405_5#0', '20405_5#6',                '20405_5#888'
);

my $dir = tempdir( CLEANUP => 1 );
my @comp = split '/', $dir;
my $dname = pop @comp;
my $IRODS_TEST_AREA1 = "$dname";

my $have_irods_execs = exist_irods_executables();
my $env_file = $ENV{'WTSI_NPG_iRODS_Test_IRODS_ENVIRONMENT_FILE'} || q[];
local $ENV{'IRODS_ENVIRONMENT_FILE'} = $env_file || 'DUMMY_VALUE';
my $test_area_created = ($env_file && $have_irods_execs) ? create_irods_test_area() : 0;

Log::Log4perl::init_once('./t/log4perl_test.conf');
my $logger = Log::Log4perl->get_logger(q[]);

my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0, logger => $logger);

my $irrelevant_entity =  npg_pipeline::validation::entity->new(
  staging_archive_root => q[t],
  target_product       => npg_pipeline::product->new(rpt_list => q[5174:1:0])
);

sub exist_irods_executables {
  return 0 unless `which ienv`;
  return 0 unless `which imkdir`;
  return 1;
}

sub create_irods_test_area {
  diag "creating $IRODS_TEST_AREA1 iRODS test area";
  return !system("imkdir $IRODS_TEST_AREA1");
}

END {
  if($test_area_created) {
    local $ENV{'IRODS_ENVIRONMENT_FILE'} = $env_file;
    eval {system("irm -r $IRODS_TEST_AREA1")};
  }
}

subtest 'object construction, file extensions, file names' => sub {
  plan tests => 6;

  my $ref = {
    irods_destination_collection => "${IRODS_TEST_AREA1}",
    product_entities  => [$irrelevant_entity],
    staging_files     => {'5174:1:0' => ['5174_1#0.cram', '5174_1#0.cram.crai']},
    logger            => $logger,
    irods             => $irods,
    file_extension    => 'cram'
  };

  my $v = npg_pipeline::validation::irods->new($ref);
  is( $v->index_file_extension, 'crai', 'index file extension is crai');
  is( $v->index_file_path('5174_1#0.cram'), '5174_1#0.cram.crai',
    'index file name for a cram file');
  is($v->index_path2seq_path('/tmp/5174_1#0.cram.crai'), '/tmp/5174_1#0.cram',
   'sequence file path from index file path');

  $ref->{file_extension} = 'bam';
  $v = npg_pipeline::validation::irods->new($ref);
  is( $v->index_file_extension, 'bai', 'index file extension is bai');
  is( $v->index_file_path('5174_1#0.bam'), '5174_1#0.bai',
    'index file name for a bam file');
  is($v->index_path2seq_path('/tmp/5174_1#0.bai'), '/tmp/5174_1#0.bam',
   'sequence file path from index file path');
};

subtest 'eligible product entities' => sub {
  plan tests => 4;

  my $config_dir = join q[/], $dir, 'config';
  mkdir $config_dir or die "Failed to create $config_dir";
  copy 't/data/release/config/archive_on/product_release.yml', $config_dir;
  copy 'data/config_files/general_values.ini', $config_dir;

  my $pconfig_content = read_file join(q[/], $config_dir, 'product_release.yml');
  my $study_id = 3573;
  ok ($pconfig_content !~ /study_id: \"$study_id\"/xms,
    'no product release config for this run study');

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q{t/data/miseq/samplesheet_16850.csv};

  my $v = npg_pipeline::validation::irods->new(
    irods_destination_collection => "${IRODS_TEST_AREA1}",
    product_entities  => [],
    staging_files     => {'16850:1:0' => ['16850_1#0.cram', '16850_1#0.cram.crai']},
    logger            => $logger,
    irods             => $irods,
    file_extension    => 'cram',
    conf_path         => $config_dir,
  );
  throws_ok { $v->eligible_product_entities() }
    qr/product_entities array cannot be empty/,
    'error if product entities array is empty';

  my @ets = map {
    npg_pipeline::validation::entity->new(
      staging_archive_root => q[t],
      target_product => npg_pipeline::product->new(
        rpt_list => $_,
        lims     => st::api::lims->new(rpt_list => $_)
      )
    )
  } map { qq[16850:1:$_] } (0 .. 2);

  $v = npg_pipeline::validation::irods->new(
    irods_destination_collection => "${IRODS_TEST_AREA1}",
    product_entities  => \@ets,
    staging_files     => {'16850:1:0' => ['16850_1#0.cram', '16850_1#0.cram.crai']},
    logger            => $logger,
    irods             => $irods,
    file_extension    => 'cram',
    conf_path         => $config_dir,
  );
  is (scalar @ets, scalar @{$v->eligible_product_entities},
    'all product entities are eligible for archival to iRODS');

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
    q{t/data/novaseq/180709_A00538_0010_BH3FCMDRXX/Data/Intensities/} .
    q{BAM_basecalls_20180805-013153/metadata_cache_26291/samplesheet_26291.csv};

  @ets = map {
    npg_pipeline::validation::entity->new(
      staging_archive_root => q[t],
      target_product => npg_pipeline::product->new(
        rpt_list => $_,
        lims     => st::api::lims->new(rpt_list => $_)
      )
    )
  } map { qq[26291:1:$_;26291:2:$_] } (0 .. 12,888);  

  $v = npg_pipeline::validation::irods->new(
    irods_destination_collection => "${IRODS_TEST_AREA1}",
    product_entities  => \@ets,
    staging_files     => {'26291:0' => ['26291#0.cram', '26291#0.cram.crai']},
    logger            => $logger,
    irods             => $irods,
    file_extension    => 'cram',
    conf_path         => $config_dir,
  );

  is (scalar @{$v->eligible_product_entities}, 0, 'no entities to archive to iRODS');
};

subtest 'deletable or not' => sub {
  my $num_tests = 21;
  plan tests => $num_tests;

  my $archive               = join q[/], $dir, '20405';
  my @letters               = (q(a)..q(z));

  SKIP: {
    skip 'Test iRODS not available (WTSI_NPG_iRODS_Test_IRODS_ENVIRONMENT_FILE not set?)',
         $num_tests unless $test_area_created;

   my $ref = {
      irods_destination_collection => "${IRODS_TEST_AREA1}",
      product_entities  => [$irrelevant_entity],
      staging_files     => {},
      eligible_product_entities => [],
      logger            => $logger,
      irods             => $irods,
      file_extension    => 'cram'
    };

    my $v = npg_pipeline::validation::irods->new($ref);
    throws_ok { $v->_eligible_staging_files() }
      qr/staging_files hash cannot be empty/,
      'error if no staging files are defined';

    $ref->{staging_files} = {'5174:1:0' => ['5174_1#0.cram', '5174_1#0.cram.crai']};
    $v = npg_pipeline::validation::irods->new($ref);

    my $result;   
      warnings_like { $result = $v->archived_for_deletion() }
      [qr/No entity is eligible for archival to iRODS/,
       qr/Empty list of iRODS files/],
      'nothing to archive warnings';
    is ($result, 1, 'deletable - nothing in iRODS');
 
    my $file_map = {};

    # Create test data
    for my $file_root (@file_list) {

      my ($lane) = $file_root =~ /^\d+_(\d)/;
      my $lane_archive = join q[/], $archive, 'lane'.$lane;
      make_path $lane_archive;
      
      for my $e (qw/cram cram.crai/) {

        my $file_name = join q[.], $file_root, $e;
        my $p  = join q[/], $lane_archive, $file_name;
        $file_map->{$file_name} = $p;

        my $content = join(q[,], map {$letters[rand(26)]} (1 .. 30));
        write_file($p, $content);
        my $md5_path = $p . q[.md5];
        write_file($md5_path, md5_hex($content));
        my $ipath = join q[/], $IRODS_TEST_AREA1, $file_name;
        $irods->add_object($p, $ipath,$WTSI::NPG::iRODS::CALC_CHECKSUM);
      } 
    }

    my $staging_files = {};
    my @p_entities = ();
    foreach my $name ( grep {$_ =~ /cram\Z/} keys %{$file_map}) {
      my $rpt = $name;
      $rpt =~ s/_/:/;
      $rpt =~ s/\#/:/;
      $rpt =~ s/\.cram//;
      push @p_entities, npg_pipeline::validation::entity->new(
        staging_archive_root => q[t],
        target_product       => npg_pipeline::product->new(rpt_list => $rpt)
      );
      my $path = $file_map->{$name};
      $staging_files->{$rpt} = [$path, $path . '.crai'];
    }

    $ref = {
      irods_destination_collection => "${IRODS_TEST_AREA1}",
      product_entities  => \@p_entities ,
      staging_files     => $staging_files,
      eligible_product_entities => [],
      logger            => $logger,
      irods             => $irods,
      file_extension    => 'cram'
    };
    $v = npg_pipeline::validation::irods->new($ref);
  
    warnings_like { $result = $v->archived_for_deletion() }
      [qr/No entity is eligible for archival to iRODS/,
       qr/Found product files in iRODS where there should be none/],
      'nothing to archive warnings';
    is ($result, 0, 'not deletable - nothing should be in iRODS');

    $ref->{eligible_product_entities} = \@p_entities;

    $v = npg_pipeline::validation::irods->new($ref);
    ok($v->archived_for_deletion(), 'deletable');

    # Remove a cram iRODS files
    my $to_remove = '20405_1#12.cram';
    my $ito_remove = join q[/], $IRODS_TEST_AREA1, $to_remove;
    $irods->remove_object($ito_remove);
    my $trpath = $file_map->{$to_remove};

    $v = npg_pipeline::validation::irods->new($ref);
    warning_like { $result = $v->archived_for_deletion() }
      qr/$trpath is not in iRODS/, 'warning - cram file missing in iRODS';
    is($result, 0, 'not deletable - cram file missing in iRODS');
    # Restore previously removed file
    $irods->add_object($trpath, $ito_remove, $WTSI::NPG::iRODS::CALC_CHECKSUM);

    $v = npg_pipeline::validation::irods->new($ref);
    is($v->archived_for_deletion(), 1, 'deletable');

    # Remove one of the staging md5 files for an index file
    unlink $file_map->{'20405_1#12.cram.crai'} . q[.md5] or
      die 'Failed to delete a file';
    ok ($v->archived_for_deletion(), 'deletable with missing md5 for an index file');

    # Remove one of the staging md5 files for a cram file
    my $sfile = '20405_1#12.cram';
    my $md5path = $file_map->{$sfile} . q[.md5];
    my $moved = $md5path . '_moved';
    rename($md5path, $moved) or die "Could not rename $md5path: $!";
    $v = npg_pipeline::validation::irods->new($ref);
    warning_like { $result = $v->archived_for_deletion() }
      qr/$md5path is absent/, 'warning - md5 missing on staging';
    ok(!$result, 'not deletable - cram md5 missing on staging');

    # Create md5 file with wrong md5 value
    write_file($md5path, q[aaaa]);
    $v = npg_pipeline::validation::irods->new($ref);
    warning_like { $result = $v->archived_for_deletion() }
      qr/Checksums do not match/, 'warning - md5 mismatch';
    ok(!$result, 'not deletable - md5 mismatch');
    # Move back correct md5 file
    unlink $md5path;
    rename($moved, $md5path) or die "Could not rename $moved: $!";

    # Create an extra cram file in iRODS
    my $extra = join q[/], $IRODS_TEST_AREA1, 'extra.cram';
    $irods->add_object($trpath, $extra, $WTSI::NPG::iRODS::CALC_CHECKSUM);
    $v = npg_pipeline::validation::irods->new($ref);
    warning_like { $result = $v->archived_for_deletion() }
      qr/$extra is in iRODS, but not on staging/, 'warning - unexpected file in iRODS';
    is($result, 0, 'not deletable - unexpected file in iRODS');

    # Assign alt_process metadata attr to the extra file
    $irods->add_object_avu($extra, 'alt_process', 'some');
    $v = npg_pipeline::validation::irods->new($ref);
    is($v->archived_for_deletion(), 1, 'deletable');

    $trpath = $file_map->{'20405_6#4.cram'};
    # Need a real file, since it will be inspected by samtools.
    copy 't/data/eight_reads.cram', $trpath or die 'Failed to copy';
    # Remove an index iRODS file
    $to_remove = '20405_6#4.cram.crai';
    $ito_remove = join q[/], $IRODS_TEST_AREA1, $to_remove;
    $irods->remove_object($ito_remove);
    $trpath = $file_map->{$to_remove};
    $v = npg_pipeline::validation::irods->new($ref);
    warning_like { $result = $v->archived_for_deletion() }
      qr/$trpath is not in iRODS/, 'warning - index file is missing';
    ok(!$result, 'not deletable - index file is missing');

    SKIP: {
      skip 'samtools executable not available', 2 unless which('samtools');

      # Create invalid parent file, which will trigger samtools error
      # for any samtools version.
      $trpath = $file_map->{'20405_6#4.cram'};
      open my $fh, q[>], $trpath or die "Failed to open file handle to $trpath";
      print $fh 'hgkdghkdghkdgh';
      close $fh or warn "Failed to close file handle to $trpath\n";
      $v = npg_pipeline::validation::irods->new($ref);
      ok(!$v->archived_for_deletion(), 'not deletable - file with no data');

      # Make the parent sequence file to have zero reads.
      copy 't/data/no_reads.cram', $trpath or die Failed to copy;
      $v = npg_pipeline::validation::irods->new($ref);
      ok($v->archived_for_deletion(), 'deletable - absence of an index ' .
        'file when the main file has zero reads is OK');
    }
  };
};

1;
