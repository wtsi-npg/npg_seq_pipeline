use strict;
use warnings;
use English qw(-no_match_vars);
use Test::More tests => 4;
use Test::Exception;
use File::Copy;
use Cwd;
use File::Temp qw(tempdir);
use File::Path qw(make_path);

use t::util;

my $util    = t::util->new();
my $tmp_dir = $util->temp_directory();
my $bin     = cwd() . q[/bin];

my @tools = map { "$tmp_dir/$_" } qw/bamtofastq blat norm_fit/;
foreach my $tool (@tools) {
  open my $fh, '>', $tool or die 'cannot open file for writing';
  print $fh $tool or die 'cannot print';
  close $fh or warn 'failed to close file handle';
}
chmod 0755, @tools;

local $ENV{'PATH'} = join q[:], $tmp_dir, $bin, $ENV{'PATH'};
local $ENV{'HOME'} = 't';

my $product_config = q[t/data/release/config/archive_on/product_release.yml];
my $test_data_dir_47995 = 't/data/novaseqx/20231017_LH00210_0012_B22FCNFLT3';

sub _setup_runfolder_47995 {
  my $tmp_dir = tempdir(CLEANUP => 1);
  my @dirs = split q[/], $test_data_dir_47995;
  my $rf_name = pop @dirs;
  my $rf_info = $util->create_runfolder($tmp_dir, {'runfolder_name' => $rf_name});
  my $rf = $rf_info->{'runfolder_path'};
  for my $file (qw(RunInfo.xml RunParameters.xml)) {
    if (copy("$test_data_dir_47995/$file", "$rf/$file") == 0) {
      die "Failed to copy $file";
    }
  }
  return $rf_info;
}

{
  my $rf_info = _setup_runfolder_47995();
  my $rf = $rf_info->{'runfolder_path'};
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q{/does/not/exist.csv};
  my $command = "$bin/npg_pipeline_central " .
    "--product_conf_file_path $product_config --spider --no_bsub " .
    "--runfolder_path $rf --function_order dodo 2>&1";
  note "Executing $command";
  like(`$command`, qr/Error initializing pipeline: Error while spidering/,
    'error in spidering when pre-set samplesheet does not exist');
}

subtest 'test analysis and archival pipeline scripts' => sub {
  plan tests => 5;

  # A full run folder is scaffolded by the analysis pipeline.
  # The archival pipeline is using teh same run folder.

  my $rf_info = _setup_runfolder_47995();
  my $rf = $rf_info->{'runfolder_path'};
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = "$test_data_dir_47995/samplesheet_47995.csv";
  my $command = "$bin/npg_pipeline_central " .
    "--product_conf_file_path $product_config --no-spider --no_bsub " .
    "--runfolder_path $rf --function_order create_summary_link_analysis " .
    "--function_order dodo 2>&1";
  note "Executing $command";
  like(`$command`, qr/Function dodo cannot be found in the graph/,
    'error when function does not exist');

  my $config_dir = join q[/], $tmp_dir, 'config';
  make_path($config_dir);
  my @files = glob 'data/config_files/*.{json,ini}';
  push @files, 't/data/release/config/archive_on/product_release.yml';
  for (@files) {
    copy $_, $config_dir;
  }

  $command = "$bin/npg_pipeline_post_qc_review --no_bsub --runfolder_path $rf " .
    "--conf_path $config_dir";
  note "Executing $command";
  lives_ok { `$command` } 'ran bin/npg_pipeline_post_qc_review';
  ok(!$CHILD_ERROR, 'No error running command');

  $command = "$bin/npg_pipeline_post_qc_review --no_bsub --runfolder_path $rf " .
    "--conf_path $config_dir --function_list some";
  note "Executing $command";
  lives_ok { `$command` }
    'ran bin/npg_pipeline_post_qc_review with non-exisiting function list';
  ok($CHILD_ERROR, "Child error $CHILD_ERROR");
};

subtest 'test npg_pipeline_seqchksum_comparator script' => sub {
  plan tests => 2;
  my $rf_info = _setup_runfolder_47995();
  my $rf = $rf_info->{'runfolder_path'};
  my $bbc = "$rf/Data/Intensities/BAM_basecalls";
  my $apath = "$bbc/no_cal/archive";
  make_path($apath);
  my $command = "$bin/npg_pipeline_seqchksum_comparator --id_run=1234 " . 
    "--archive_path=$apath --bam_basecall_path=$bbc --lanes=1";
  note "Executing $command";
  lives_ok { `$command` }
    'ran npg_pipeline_seqchksum_comparator with analysis and bam_basecall_path';
  ok($CHILD_ERROR, qq{Return code of $CHILD_ERROR as no files found});
};

subtest 'test npg_pipeline_preexec_references script' => sub {
  plan tests => 2;
  `bin/npg_pipeline_preexec_references --repository t/data/sequence/refs 2>/dev/null`;
  ok($CHILD_ERROR, "failed as could not locate references directory - $CHILD_ERROR");
  `bin/npg_pipeline_preexec_references --repository t/data/sequence`;
  ok(! $CHILD_ERROR, 'script runs OK');
};

1;
