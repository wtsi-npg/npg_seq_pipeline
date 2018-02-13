use strict;
use warnings;
use Test::More tests => 15;
use Test::Exception;
use t::util;

use_ok('npg_pipeline::function::seq_to_irods_archiver');

my $util = t::util->new();

$ENV{TEST_FS_RESOURCE} = q{nfs_12};
local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];
local $ENV{PATH} = join q[:], q[t/bin], q[t/bin/software/solexa/bin], $ENV{PATH};

my $tmp_dir = $util->temp_directory();

my $analysis_runfolder_path = $util->analysis_runfolder_path();
my $pb_cal = q[/Data/Intensities/Bustard1.3.4_09-07-2009_auto/PB_cal];
my $pb_cal_path = $analysis_runfolder_path . $pb_cal;

sub create_analysis {
  `mkdir -p $analysis_runfolder_path/$pb_cal/archive`;
  `mkdir -p $analysis_runfolder_path/Config`;
  `cp t/data/Recipes/Recipe_GA2_37Cycle_PE_v6.1.xml $analysis_runfolder_path/`;
  `cp t/data/Recipes/TileLayout.xml $analysis_runfolder_path/Config/`;
  `ln -s $pb_cal $analysis_runfolder_path/Latest_Summary`;

  my $archive_root = qq{$analysis_runfolder_path/$pb_cal/archive/};
  foreach my $i (1..7) {
    foreach my $extension (qw{bam}) {
      my $file = $archive_root . qq{1234_$i}.qq{.$extension};
      `touch $file`;
    }
  }

  `touch $archive_root/1234_8_human.cram`;
  `touch $archive_root/1234_8.cram`;

  return 1;
}

create_analysis();

{
  my $bam_irods;
  lives_ok { $bam_irods = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder        => q{123456_IL2_1234},
    runfolder_path    => $analysis_runfolder_path,
    timestamp         => q{20090709-123456},
    verbose           => 0,
    recalibrated_path => $pb_cal_path,
  ); } q{created with run_folder ok};
  isa_ok($bam_irods , q{npg_pipeline::function::seq_to_irods_archiver}, q{object test});
  ok (!$bam_irods->no_irods_archival, 'no_irods_archival flag is unset');

  my @jids;
  lives_ok { @jids = $bam_irods->submit_to_lsf(); } q{no croak submitting job to lsf};

  is(scalar@jids, 1, q{only one job submitted});
  my $archive_path = "$pb_cal_path/archive";
  my $bsub_command = $bam_irods ->_generate_bsub_command();
  my $expected_command = qq[bsub -q lowload -J npg_publish_illumina_run.pl_1234_20090709-123456 -R 'rusage[nfs_12=1,seq_irods=15]' -E 'npg_pipeline_script_must_be_unique_runner -job_name="npg_publish_illumina_run.pl_1234"' -o $pb_cal_path/log/npg_publish_illumina_run.pl_1234_20090709-123456.out 'npg_publish_illumina_run.pl --archive_path $archive_path --runfolder_path $analysis_runfolder_path --restart_file ${archive_path}/process_publish_\${LSB_JOBID}.json --max_errors 10'];
  is( $bsub_command, $expected_command, q{generated bsub command is correct});
  
  $bam_irods = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder        => q{123456_IL2_1234},
    runfolder_path    => $analysis_runfolder_path,
    verbose           => 0,
    no_irods_archival => 1,
    recalibrated_path => $pb_cal_path,
  );
  ok ($bam_irods->no_irods_archival, 'no_irods_archival flag is set');
  ok (!$bam_irods->submit_to_lsf(), 'no jobs created');
  
  $bam_irods = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder        => q{123456_IL2_1234},
    runfolder_path    => $analysis_runfolder_path,
    verbose           => 0,
    local              => 1,
    recalibrated_path => $pb_cal_path,
  );
  ok ($bam_irods->no_irods_archival, 'no_irods_archival flag is set');
  ok (!$bam_irods->submit_to_lsf(), 'no jobs created');

  $bam_irods = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder        => q{123456_IL2_1234},
    runfolder_path    => $analysis_runfolder_path,
    recalibrated_path => $pb_cal_path,
    timestamp         => q{20090709-123456},
    verbose           => 0,
    lanes             => [8],
  );

  lives_ok { @jids = $bam_irods->submit_to_lsf(); } q{no croak submitting job to lsf};
  is(scalar@jids, 1, q{only one job submitted});
  $bsub_command = $bam_irods ->_generate_bsub_command();
  $expected_command = qq[bsub -q lowload -J npg_publish_illumina_run.pl_1234_20090709-123456 -R 'rusage[nfs_12=1,seq_irods=15]' -E 'npg_pipeline_script_must_be_unique_runner -job_name="npg_publish_illumina_run.pl_1234"' -o $pb_cal_path/log/npg_publish_illumina_run.pl_1234_20090709-123456.out 'npg_publish_illumina_run.pl --archive_path $archive_path --runfolder_path $analysis_runfolder_path --restart_file ${archive_path}/process_publish_\${LSB_JOBID}.json --max_errors 10 --positions 8'];
  is( $bsub_command, $expected_command, q{generated bsub command is correct} );

  $bam_irods = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder        => q{123456_IL2_1234},
    runfolder_path    => $analysis_runfolder_path,
    id_flowcell_lims  => q{1023456789111},
    recalibrated_path => $pb_cal_path,
    timestamp         => q{20090709-123456},
    verbose           => 0,
  );

  $bsub_command = $bam_irods ->_generate_bsub_command();
  $expected_command = qq[bsub -q lowload -J npg_publish_illumina_run.pl_1234_20090709-123456 -R 'rusage[nfs_12=1,seq_irods=15]' -E 'npg_pipeline_script_must_be_unique_runner -job_name="npg_publish_illumina_run.pl_1234"' -o $pb_cal_path/log/npg_publish_illumina_run.pl_1234_20090709-123456.out 'npg_publish_illumina_run.pl --archive_path $archive_path --runfolder_path $analysis_runfolder_path --restart_file ${archive_path}/process_publish_\${LSB_JOBID}.json --max_errors 10 --alt_process qc_run'];
  is( $bsub_command, $expected_command, q{generated bsub command is correct} );
}

1;
__END__
