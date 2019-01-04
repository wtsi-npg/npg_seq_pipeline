use strict;
use warnings;
use Test::More tests => 3;
use Test::Warn;
use Test::Exception;
use File::Temp qw/ tempdir /;
use Log::Log4perl;
use File::Slurp qw/ write_file prepend_file /;
use File::Path qw/ make_path /;
use List::MoreUtils qw/ none /;
use Digest::MD5 qw/ md5_hex /;

use WTSI::NPG::iRODS;

use_ok('npg_pipeline::validation::sequence_files');

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
@file_list = sort @file_list;

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

subtest 'file extensions, file names' => sub {
  plan tests => 4;

  my $v = npg_pipeline::validation::sequence_files->new(
     collection     => q[/my/c],
     staging_files  => {},
     file_extension => 'cram',
     logger         => $logger,
     _irods         => $irods);
  is( $v->index_file_extension, 'crai', 'index file extension is crai');
  is( $v->_index_file_name('5174_1#0.cram'), '5174_1#0.cram.crai',
    'index file name for a cram file');

  $v = npg_pipeline::validation::sequence_files->new(
     collection     => q[/my/c],
     staging_files  => {},
     file_extension => 'bam',
     logger         => $logger,
     _irods         => $irods);
  is( $v->index_file_extension, 'bai', 'index file extension is bai');
  is( $v->_index_file_name('5174_1#0.bam'), '5174_1#0.bam.bai',
    'index file name for a bam file');
};

subtest 'deletable or not' => sub {
  my $num_tests = 11;
  plan tests => $num_tests;

  SKIP: {

    skip 'Test iRODS not available (WTSI_NPG_iRODS_Test_IRODS_ENVIRONMENT_FILE not set?)',
         $num_tests unless $test_area_created;

    my $archive = join q[/], $dir, '20405';
    my @letters     = (q(a)..q(z));
    my $num_files   = scalar @file_list;
    my $empty                 =  5;
    my $not_aligned           = 15;
    my $empty_and_not_aligned = 20;

    ok($num_files > $empty_and_not_aligned, 'number of files is sufficiently large');
 
    my $i = 0;
    my $file_map = {};
    while ($i < $num_files) {
      my $file_root = $file_list[$i];
      my ($lane) = $file_root =~ /^\d+_(\d)/;
      my $lane_archive = join q[/], $archive, 'lane'.$lane;
      make_path $lane_archive;
      my $file_name = join q[.], $file_root, 'cram';
      my $path  = join q[/], $lane_archive, $file_name;
      $file_map->{$file_name} = $path;
      my $content = join(q[,], map {$letters[rand(26)]} (1 .. 30));
      write_file($path, $content);
      my $md5_path = $path . q[.md5];
      write_file($md5_path, md5_hex($content)) ;
      
      my $ipath = join q[/], $IRODS_TEST_AREA1, $file_name;
      $irods->add_object($path, $ipath, 1);
      $irods->add_object($md5_path, $ipath . q[.md5], 1);
      my $num_reads  = ($i == $empty || $i == $empty_and_not_aligned) ? 0 : int(rand(100));
      my $align_flag = ($i == $not_aligned || $i == $empty_and_not_aligned) ? 0 : 1;

      if (none {$i == $_} ($empty, $not_aligned, $empty_and_not_aligned)) {
        $irods->add_object($path, $ipath . q[.crai], 0); 
      }
      $irods->add_object_avu($ipath, 'alignment', $align_flag);
      $irods->add_object_avu($ipath, 'total_reads', $num_reads);

      $i++;
    }

    my $ref = {
      collection     => "${IRODS_TEST_AREA1}",
      staging_files  => {sequence_files => [values %{$file_map}]},
      logger         => $logger,
      _irods         => $irods,
      file_extension => 'cram'
    };

    my $v = npg_pipeline::validation::sequence_files->new($ref);
    ok($v->archived_for_deletion(), 'deletable');

    # Remove one of iRODS files
    my $temp = $file_list[$empty];
    my $to_remove = join q[/], $IRODS_TEST_AREA1, $temp . '.cram';
    $irods->remove_object($to_remove);

    $v = npg_pipeline::validation::sequence_files->new($ref);
    my $result;
    warning_like { $result = $v->archived_for_deletion() }
      qr/Number of sequence files is different/,
      'not deletable - number of files check';
    ok(!$result, 'not deletable');
  
    # Restore previously removed file, excluding metadata
    $irods->add_object($file_map->{$temp . '.cram'}, $to_remove, 1);

    $ref->{'_irods'} = WTSI::NPG::iRODS->new(strict_baton_version => 0, logger => $logger);
    $v = npg_pipeline::validation::sequence_files->new($ref);
    throws_ok { $v->archived_for_deletion() }
      qr/No or too many 'alignment' meta data for .+\/$to_remove/,
      'alignment metadata missing - error';
    $irods->add_object_avu($to_remove, 'alignment', 1);

    $ref->{'_irods'} = WTSI::NPG::iRODS->new(strict_baton_version => 0, logger => $logger);
    $v = npg_pipeline::validation::sequence_files->new($ref);
    throws_ok { $v->archived_for_deletion() }
      qr/No or too many 'total_reads' meta data for .+\/$to_remove/,
      'total_reads metadata missing - error';
    $irods->add_object_avu($to_remove, 'total_reads', 0);

    $to_remove = $file_list[$num_files - 1];
    my $ito_remove = join q[/], $IRODS_TEST_AREA1, $to_remove . '.cram.crai';
    # Remove an index file
    $irods->remove_object($ito_remove);

    $ref->{'_irods'} = WTSI::NPG::iRODS->new(strict_baton_version => 0, logger => $logger);
    $v = npg_pipeline::validation::sequence_files->new($ref);
    warning_like { $result = $v->archived_for_deletion() }
      qr/Index file 20405_8\#888\.cram\.crai for 20405_8\#888\.cram does not exist/,
      'not deletable - index file is missing';
    ok(!$result, 'not deletable');
    # Put it back
    my $path = $file_map->{$to_remove . '.cram'}; # . q[.crai];
    $irods->add_object($path, $ito_remove, 1);

    $ref->{'_irods'} = WTSI::NPG::iRODS->new(strict_baton_version => 0, logger => $logger);
    my $sfile = '20405_1#12.cram';
    $path = $file_map->{$sfile} . q[.md5];
    unlink $path or warn "Could not unlink $path: $!";
    $v = npg_pipeline::validation::sequence_files->new($ref);
    throws_ok { $v->archived_for_deletion() }
      qr/Can't open '$path'/,
      'md5 file missing on staging - error';

    write_file($path, q[aaaa]);
    $v = npg_pipeline::validation::sequence_files->new($ref);
    warning_like { $result = $v->archived_for_deletion() }
      qr/md5 wrong for $sfile/,
      'not deletable - md5 mismatch';
    ok(!$result, 'not deletable');
  };
};

1;

