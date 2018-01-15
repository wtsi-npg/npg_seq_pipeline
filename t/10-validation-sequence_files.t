use strict;
use warnings;
use Test::More tests => 3;
use Test::Warn;
use Test::Exception;
use File::Temp qw/ tempdir /;
use Log::Log4perl;
use File::Slurp qw/ read_file write_file prepend_file /;
use File::Path qw/ make_path /;
use List::MoreUtils qw/ none /;
use Digest::MD5 qw/ md5_hex /;

use WTSI::NPG::iRODS;

use_ok('npg_pipeline::validation::sequence_files');

local $ENV{'http_proxy'} = 'http://wibble.com';

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
  plan tests => 9;

  my $v = npg_pipeline::validation::sequence_files->new(
     archive_path   => q[t/data/validation],
     collection     => q[/my/c],
     id_run         => 5174,
     file_extension => 'cram',
     logger         => $logger,
     irods          => $irods);
  is( $v->index_file_extension, 'crai', 'index file extension is crai');
  is( $v->_file_name({position=>1}), '5174_1.cram', 'lane 1 file name');
  is( $v->_file_name({position=>1, tag_index=>0}), '5174_1#0.cram',
    'lane 1 tag_index 0 target file name');
  is( $v->_index_file_name('5174_1#0.cram'), '5174_1#0.cram.crai',
    'index file name for a cram file');

  $v = npg_pipeline::validation::sequence_files->new(
     archive_path   => q[t/data/validation],
     collection     => q[/my/c],
     id_run         => 5174,
     file_extension => 'bam',
     logger         => $logger,
     irods          => $irods);
  is( $v->index_file_extension, 'bai', 'index file extension is bai');
  is( $v->_file_name({position=>1}), '5174_1.bam', 'lane 1 file name');
  is( $v->_file_name({position=>1, tag_index=>0}), '5174_1#0.bam',
    'lane 1 tag_index 0 target file name');
  is( $v->_file_name({position=>1, tag_index=>1}), '5174_1#1.bam',
    'lane 1 tag_index 1 target file name');
  is( $v->_index_file_name('5174_1#0.bam'), '5174_1#0.bam.bai',
    'index file name for a bam file');
};

subtest 'deletable or not' => sub {
  my $num_tests = 15;
  plan tests => $num_tests;

  SKIP: {

    skip 'Test iRODS not available (WTSI_NPG_iRODS_Test_IRODS_ENVIRONMENT_FILE not set?)',
         $num_tests unless $test_area_created;

    my $irods_do = sub {
      my $command = shift;
      if (system($command)) {
        die "Failed to execute command";
      }
    };
  
    local $ENV{'NPG_CACHED_SAMPLESHEET_FILE'} =
      't/data/validation/20405/samplesheet_20405.csv';

    my @file_list = read_file('t/data/validation/20405/file_list');
    @file_list = sort @file_list;
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
      my $file = $file_list[$i];
      $file =~ s/\s+$//;
      $file_list[$i] = $file;
      my ($lane) = $file =~ /^\d+_(\d)/;
      my $lane_archive = join q[/], $archive, 'lane'.$lane;
      make_path $lane_archive;

      my $path  = join q[/], $lane_archive, $file;
      $file_map->{$file} = $path;
      my $content = join(q[,], map {$letters[rand(26)]} (1 .. 30));
      write_file($path, $content);
      my $md5_path = $path . q[.md5];
      write_file($md5_path, md5_hex($content)) ;
      
      my $ipath = join q[/], $IRODS_TEST_AREA1, $file;
      $irods_do->("iput -K $path $ipath");
      $irods_do->("iput -K $md5_path $ipath" . q[.md5]);
      my $num_reads = ($i == $empty || $i == $empty_and_not_aligned) ? 0 : int(rand(100));
      my $align_flag = ($i == $not_aligned || $i == $empty_and_not_aligned) ? 0 : 1;

      if (none {$i == $_} ($empty, $not_aligned, $empty_and_not_aligned)) {
        $irods_do->("iput $path ${ipath}.crai"); 
      }
      $irods_do->("imeta add -d $ipath alignment $align_flag");
      $irods_do->("imeta add -d $ipath total_reads $num_reads");

      $i++;
    }  

    my $ref = {
      archive_path   => $archive,
      collection     => "${IRODS_TEST_AREA1}",
      logger         => $logger,
      id_run         => 20405,
      irods          => $irods,
      file_extension => 'cram',
      is_indexed     => 1,
    };

    my $v = npg_pipeline::validation::sequence_files->new($ref);
    ok($v->archived_for_deletion(), 'deletable');

    # Remove one of iRODS files
    my $temp = $file_list[$empty];
    my $to_remove = join q[/], $IRODS_TEST_AREA1, $temp; 
    $irods_do->("irm $to_remove");
    $v = npg_pipeline::validation::sequence_files->new($ref);
    my $result;
    warning_like { $result = $v->archived_for_deletion() }
      qr/Number of sequence files is different/,
      'not deletable - number of files check';
    ok(!$result, 'not deletable');

    # Replace it with a file with a random name,
    # so that the number of files is correct.
    my $wrong = "$IRODS_TEST_AREA1/wrong.cram";
    my ($f, $path) = each %{$file_map};
    $irods_do->("iput $path $wrong");
    $v = npg_pipeline::validation::sequence_files->new($ref);
    warning_like { $result = $v->archived_for_deletion() }
      qr/According to LIMS, file $temp is missing/,
      'not deletable - mismatch with lims data';
    ok(!$result, 'not deletable');
    $irods_do->("irm $wrong");
  
    # Restore previously removed file, excluding metadata
    $irods_do->(join q[ ], 'iput',  '-K', $file_map->{$temp}, $to_remove);
    $ref->{'irods'} = WTSI::NPG::iRODS->new(strict_baton_version => 0, logger => $logger);
    $v = npg_pipeline::validation::sequence_files->new($ref);
    throws_ok { $v->archived_for_deletion() }
      qr/No or too many 'alignment' meta data for .+\/$to_remove/,
      'alignment metadata missing - error';
    $irods_do->("imeta add -d $to_remove alignment 1");
    $ref->{'irods'} = WTSI::NPG::iRODS->new(strict_baton_version => 0, logger => $logger);
    $v = npg_pipeline::validation::sequence_files->new($ref);
    throws_ok { $v->archived_for_deletion() }
      qr/No or too many 'total_reads' meta data for .+\/$to_remove/,
      'total_reads metadata missing - error';
    $irods_do->("imeta add -d $to_remove total_reads 0");

    $to_remove = $file_list[$num_files - 1];
    my $ito_remove = join q[/], $IRODS_TEST_AREA1, $to_remove . '.crai';
    # Remove an index file
    $irods_do->("irm $ito_remove");
    $ref->{'irods'} = WTSI::NPG::iRODS->new(strict_baton_version => 0, logger => $logger);
    $v = npg_pipeline::validation::sequence_files->new($ref);
    warning_like { $result = $v->archived_for_deletion() }
      qr/Index file for $to_remove does not exist/,
      'not deletable - index file is missing';
    ok(!$result, 'not deletable');
    # Put it back
    $irods_do->("iput -K $path $ito_remove");

    $ref->{'irods'} = WTSI::NPG::iRODS->new(strict_baton_version => 0, logger => $logger);
    my $sfile = '20405_1#12.cram';
    my $file  = "${dir}/20405/lane1/${sfile}.md5";
    unlink $file or warn "Could not unlink $file: $!";
    $v = npg_pipeline::validation::sequence_files->new($ref);
    throws_ok { $v->archived_for_deletion() }
      qr/Can't open '$file'/,
      'md5 file missing on staging - error';

    write_file($file, q[aaaa]);
    $v = npg_pipeline::validation::sequence_files->new($ref);
    warning_like { $result = $v->archived_for_deletion() }
      qr/md5 wrong for $sfile/,
      'not deletable - md5 mismatch';
    ok(!$result, 'not deletable');

    unlink $file or warn "Could not unlink $file: $!";
    write_file($file, q[]) ;
    $v = npg_pipeline::validation::sequence_files->new($ref);
    warning_like { $result = $v->archived_for_deletion() }
      qr/md5 wrong for $sfile/,
      'not deletable - md5 mismatch';
    ok(!$result, 'not deletable');
  };
};

1;
__END__
