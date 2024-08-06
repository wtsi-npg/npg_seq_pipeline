use strict;
use warnings;
use Test::More tests => 15;
use Test::Exception;
use Log::Log4perl qw(:levels);
use File::Path qw(make_path);
use File::Copy;

use t::util;

my $util = t::util->new();
my $tmp_dir = $util->temp_directory();

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $tmp_dir, 'logfile'),
                          utf8   => 1});

my $test_data_dir_47995 = 't/data/novaseqx/20231017_LH00210_0012_B22FCNFLT3';

sub _setup_runfolder_47995 {
  my $timestamp = shift;
  my @dirs = split q[/], $test_data_dir_47995;
  my $rf_name = pop @dirs;
  my $rf_info = $util->create_runfolder($tmp_dir, {'runfolder_name' => $rf_name});
  my $rf = $rf_info->{'runfolder_path'};
  for my $file (qw(RunInfo.xml RunParameters.xml)) {
    if (copy("$test_data_dir_47995/$file", "$rf/$file") == 0) {
      die "Failed to copy $file";
    }
  }
  my $bam_basecall_path = $rf . "/Data/Intensities/BAM_basecalls_$timestamp";
  my $archive_path = $bam_basecall_path . q{/no_cal/archive};
  make_path($archive_path);
  $rf_info->{'bam_basecall_path'} = $bam_basecall_path;
  $rf_info->{'archive_path'} = $archive_path;
  return $rf_info;
}

use_ok( q{npg_pipeline::function::seqchksum_comparator} );

my $timestamp = q{09-07-2009};
my $rf_info = _setup_runfolder_47995($timestamp);
my $archive_path = $rf_info->{'archive_path'};
my $bam_basecall_path = $rf_info->{'bam_basecall_path'};

my %init = (
  runfolder_path    => $rf_info->{'runfolder_path'},
  archive_path      => $archive_path,
  bam_basecall_path => $bam_basecall_path,
  id_run            => 47995,
  is_indexed        => 0,
  timestamp         => $timestamp,
  lanes             => [1,2],
  resource          => {
    default => {
      minimum_cpu => 1,
      memory => 2
    }
  }
);

{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = "$test_data_dir_47995/samplesheet_47995.csv";

  my $object;
  lives_ok {
    $object = npg_pipeline::function::seqchksum_comparator->new(%init);
  } q{object ok};

  isa_ok( $object, q{npg_pipeline::function::seqchksum_comparator});
  my $da = $object->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  my $d = $da->[0];
  isa_ok($d, q{npg_pipeline::function::definition});
  is ($d->created_by, q{npg_pipeline::function::seqchksum_comparator},
    'created_by is correct');
  is ($d->created_on, $object->timestamp, 'created_on is correct');
  is ($d->identifier, 47995, 'identifier is set correctly');
  ok (!$d->has_composition, 'composition is not set');
  is ($d->job_name, q{seqchksum_comparator_47995_09-07-2009},
    'job_name is correct');
  my $rp = $object->recalibrated_path;
  is ($d->command,
    q{npg_pipeline_seqchksum_comparator --id_run=47995 --lanes=1 --lanes=2} .
    qq{ --archive_path=$archive_path --bam_basecall_path=$bam_basecall_path} .
    qq{ --input_fofg_name=$rp/47995_input_fofn.txt},
    'command is correct');
  ok (!$d->excluded, 'step not excluded');
  is ($d->queue, 'default', 'default queue');
  lives_ok {$d->freeze()} 'definition can be serialized to JSON';

  throws_ok{$object->do_comparison()} qr/Failed to run command seqchksum_merge.pl/,
    q{Doing a comparison with no files throws an exception};
}

1;
