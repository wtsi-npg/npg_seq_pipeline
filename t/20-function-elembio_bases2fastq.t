use strict;
use warnings;
use Test::More tests => 3;
use Test::Exception;
use File::Copy qw(cp);
use File::Slurp qw(read_file);
use File::Temp qw(tempdir);

use t::util;

use_ok('npg_pipeline::function::elembio_bases2fastq');

my $default = {
  default => {
    minimum_cpu => 16,
    memory      => 20,
    fs_slots_num => 4,
    queue       => q{p4stage1},
  }
};

my $util = t::util->new(clean_temp_directory => 1);

sub _create_elembio_function {
  my $rf_name = q[20250127_AV244103_1234_NT1850075L];
  my $tmp_dir = tempdir(CLEANUP => 1);
  my $rf_info = $util->create_runfolder(
    $tmp_dir, {runfolder_name => $rf_name, analysis_path => q[BAM_basecalls_20260316-setup]}
  );
  my $runfolder = $rf_info->{runfolder_path};
  my $bam_basecall_path = $rf_info->{analysis_path};

  cp("t/data/elembio/$rf_name/RunParameters.json", "$runfolder/RunParameters.json")
    or die 'Failed to copy Elembio RunParameters.json';

  my $f = npg_pipeline::function::elembio_bases2fastq->new(
    id_run            => 51922,
    runfolder_path    => $runfolder,
    bam_basecall_path => $bam_basecall_path,
    resource          => $default,
    timestamp         => q{20260316-101010},
  );

  return ($f, $rf_info);
}

subtest 'generate lane-level Elembio bases2fastq definitions' => sub {
  plan tests => 20;

  my ($f, $rf_info) = _create_elembio_function();
  my $runfolder = $rf_info->{runfolder_path};
  my $bam_basecall_path = $rf_info->{analysis_path};
  my $lane1_dir = "$bam_basecall_path/fastq/lane1";
  my $lane2_dir = "$bam_basecall_path/fastq/lane2";
  my $lane1_log_dir = "$lane1_dir/log";
  my $lane2_log_dir = "$lane2_dir/log";
  my $lane1_samples_path = "$lane1_dir/samples.csv";
  my $lane2_samples_path = "$lane2_dir/samples.csv";

  isa_ok($f, q{npg_pipeline::function::elembio_bases2fastq});
  is($f->manufacturer, q{Element Biosciences}, 'Elembio manufacturer detected');

  my $definitions;
  lives_ok { $definitions = $f->generate() } 'definition generation succeeds';
  ok(-d $lane1_log_dir, 'lane1 log directory created');
  ok(-d $lane2_log_dir, 'lane2 log directory created');
  ok(-f $lane1_samples_path, 'lane1 samples csv created');
  ok(-f $lane2_samples_path, 'lane2 samples csv created');
  is(scalar @{$definitions}, 2, 'two definitions returned');

  my $d1 = $definitions->[0];
  my $d2 = $definitions->[1];
  isa_ok($d1, q{npg_pipeline::function::definition});
  isa_ok($d2, q{npg_pipeline::function::definition});
  is($d1->job_name, q{elembio_bases2fastq_51922_20260316-101010},
    'shared array job name for lane1');
  is($d2->job_name, q{elembio_bases2fastq_51922_20260316-101010},
    'shared array job name for lane2');
  is($d1->queue, q{p4stage1}, 'lane1 queue');
  is($d2->queue, q{p4stage1}, 'lane2 queue');
  ok(!$d1->has_command_preexec, 'lane1 has no preexec command');
  ok(!$d2->has_command_preexec, 'lane2 has no preexec command');
  is(
    $d1->command,
    join(q{ },
      q{bases2fastq},
      q{-e}, q{'.*'},
      q{-i}, q{'L1R..C..S.'},
      q{--settings}, q{"I1Fastq,True"},
      q{--no-projects},
      q{-p `npg_pipeline_job_env_to_threads --num_threads 16`},
      $runfolder, $lane1_dir,
      q{--skip-qc-report},
      q{--force-index-orientation},
      q{--group-fastq},
      q{--split-lanes},
      q{--skip-multi-qc},
      q{--settings}, q{"SpikeInAsUnassigned,False"},
      q{-r}, $lane1_samples_path,
      q{--settings}, q{"ReadType,Paired"},
    ),
    'lane1 command is correct',
  );
  is(
    $d2->command,
    join(q{ },
      q{bases2fastq},
      q{-e}, q{'.*'},
      q{-i}, q{'L2R..C..S.'},
      q{--settings}, q{"I1Fastq,True"},
      q{--no-projects},
      q{-p `npg_pipeline_job_env_to_threads --num_threads 16`},
      $runfolder, $lane2_dir,
      q{--skip-qc-report},
      q{--force-index-orientation},
      q{--group-fastq},
      q{--split-lanes},
      q{--skip-multi-qc},
      q{--settings}, q{"SpikeInAsUnassigned,False"},
      q{-r}, $lane2_samples_path,
      q{--settings}, q{"ReadType,Paired"},
    ),
    'lane2 command is correct',
  );

  is(read_file($lane1_samples_path), "[Samples]\nSampleName\nWholeLane\n",
    'lane1 samples csv content');
  is(read_file($lane2_samples_path), "[Samples]\nSampleName\nWholeLane\n",
    'lane2 samples csv content');
};

subtest 'skip non-Elembio runfolders via excluded definition' => sub {
  plan tests => 5;

  my $tmp_dir = tempdir(CLEANUP => 1);
  my $rf_info = $util->create_runfolder(
    $tmp_dir, {runfolder_name => q[171114_MS6_24347_A_MS5534842-300V2], analysis_path => q[BAM_basecalls]}
  );
  my $runfolder = $rf_info->{runfolder_path};
  cp(q[t/data/miseq/24347_RunInfo.xml], qq[$runfolder/RunInfo.xml])
    or die 'Failed to copy RunInfo.xml';
  cp(q[t/data/run_params/runParameters.miseq.xml], qq[$runfolder/runParameters.xml])
    or die 'Failed to copy runParameters.xml';

  my $f = npg_pipeline::function::elembio_bases2fastq->new(
    id_run            => 24347,
    runfolder_path    => $runfolder,
    bam_basecall_path => $rf_info->{analysis_path},
    resource          => $default,
  );

  my $definitions;
  lives_ok { $definitions = $f->generate() } 'non-Elembio generation succeeds';
  is(scalar @{$definitions}, 1, 'one excluded definition returned');
  isa_ok($definitions->[0], q{npg_pipeline::function::definition});
  ok($definitions->[0]->excluded, 'definition is excluded');
  ok(!$definitions->[0]->has_command, 'excluded definition has no command');
};

1;
