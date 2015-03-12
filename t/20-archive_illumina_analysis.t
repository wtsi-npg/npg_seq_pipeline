use strict;
use warnings;
use Test::More tests => 7;
use Test::Exception;
use t::util;

local $ENV{PATH} = join q[:], q[t/bin], q[t/bin/software/solexa/bin], $ENV{PATH};

use_ok('npg_pipeline::archive::qc::illumina_analysis');

my $util = t::util->new();
$ENV{TEST_DIR} = $util->temp_directory();
$ENV{TEST_FS_RESOURCE} = q{nfs_12};
my $tmp_dir = $util->temp_directory();

my $analysis_runfolder_path = $util->analysis_runfolder_path();
my $pbcal = q{Data/Intensities/Bustard1.3.4_09-07-2009_auto/PB_cal};
my $nfs_pbcal = q{/nfs/sf45/IL2/analysis/123456_IL2_1234/} . $pbcal;
sub create_analysis {
  `rm -rf $tmp_dir/nfs/sf45`;
  `mkdir -p $analysis_runfolder_path/$pbcal/archive`;
  `mkdir $analysis_runfolder_path/Config`;
  `cp t/data/Recipes/TileLayout.xml $analysis_runfolder_path/Config/`;
  `cp t/data/Recipes/Recipe_GA2_37Cycle_PE_v6.1.xml $analysis_runfolder_path/`;
  `ln -s $pbcal $analysis_runfolder_path/Latest_Summary`;

  return 1;
}

{
  my $aia;

  lives_ok { $aia = npg_pipeline::archive::qc::illumina_analysis->new(
    run_folder => q{123456_IL2_1234},
    runfolder_path => $analysis_runfolder_path,
    recalibrated_path => "$analysis_runfolder_path/$pbcal",
    timestamp => q{20090709-123456},
    verbose => 1,
  ); } q{created with run_folder ok};
  isa_ok($aia, q{npg_pipeline::archive::qc::illumina_analysis}, q{$aia});
  $util->create_analysis();

  my $arg_refs = {
    required_job_completion => q{-w'done(123) && done(321)'},
  };

  my @jids;
  lives_ok { @jids = $aia->submit_to_lsf($arg_refs); } q{no croak submitting job to lsf};
  is(scalar@jids, 1, q{only one job submitted});

  my $bsub_command = $util->drop_temp_part_from_paths( $aia->_generate_bsub_command( $arg_refs ) );
  my $expected_command = qq{bsub -q srpipeline -w'done(123) && done(321)' -J illumina_analysis_loader_1234_20090709-123456 -R 'rusage[nfs_12=1]' -E 'script_must_be_unique_runner -job_name="illumina_analysis_loader" -own_job_name="illumina_analysis_loader_1234_20090709-123456"' -o $nfs_pbcal/log/illumina_analysis_loader_1234_20090709-123456.out 'npg_qc_illumina_analysis_loader  --id_run 1234  --run_folder 123456_IL2_1234  --runfolder_path /nfs/sf45/IL2/analysis/123456_IL2_1234  --basecall_path /nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BaseCalls  --verbose'};

  is( $bsub_command, $expected_command, q{generated bsub command is correct} );
}

{
  my $aia = npg_pipeline::archive::qc::illumina_analysis->new(
    run_folder => q{123456_IL2_1234},
    runfolder_path => $analysis_runfolder_path,
    recalibrated_path => "$analysis_runfolder_path/$pbcal",
    timestamp => q{20090709-123456},
    bam_basecall_path => $analysis_runfolder_path,
  );
  $util->create_analysis();

  my $arg_refs = {
    required_job_completion => q{-w'done(123) && done(321)'},
  };

  my $bsub_command = $util->drop_temp_part_from_paths( $aia->_generate_bsub_command( $arg_refs ) );
  my $expected_command = qq{bsub -q srpipeline -w'done(123) && done(321)' -J illumina_analysis_loader_1234_20090709-123456 -R 'rusage[nfs_12=1]' -E 'script_must_be_unique_runner -job_name="illumina_analysis_loader" -own_job_name="illumina_analysis_loader_1234_20090709-123456"' -o $nfs_pbcal/log/illumina_analysis_loader_1234_20090709-123456.out 'npg_qc_illumina_analysis_loader  --id_run 1234  --run_folder 123456_IL2_1234  --runfolder_path /nfs/sf45/IL2/analysis/123456_IL2_1234  --bam_basecall_path /nfs/sf45/IL2/analysis/123456_IL2_1234  --basecall_path /nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BaseCalls'};
  is( $bsub_command, $expected_command, q{generated bsub command is correct} );
}
1;
