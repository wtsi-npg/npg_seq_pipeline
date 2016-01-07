use strict;
use warnings;
use Test::More tests => 13;
use Test::Exception;
use t::util;

use_ok('npg_pipeline::archive::file::logs');

my $util = t::util->new();

$ENV{TEST_DIR} = $util->temp_directory();
$ENV{TEST_FS_RESOURCE} = q{nfs_12};
local $ENV{PATH} = join q[:], q[t/bin], $ENV{PATH};

my $tmp_dir = $util->temp_directory();

my $analysis_runfolder_path = $util->analysis_runfolder_path();
my $pb_cal = q[/Data/Intensities/Bustard1.3.4_09-07-2009_auto/PB_cal];
my $pb_cal_path = $analysis_runfolder_path . $pb_cal;

my $rfpath = '/nfs/sf45/IL2/outgoing/123456_IL2_1234';

sub create_analysis {
  `rm -rf $tmp_dir/nfs/sf45`;
  `mkdir -p $analysis_runfolder_path/$pb_cal/archive`;
  `mkdir $analysis_runfolder_path/Config`;
  `cp t/data/Recipes/Recipe_GA2_37Cycle_PE_v6.1.xml $analysis_runfolder_path/`;
  `cp t/data/Recipes/TileLayout.xml $analysis_runfolder_path/Config/`;
  `ln -s $pb_cal $analysis_runfolder_path/Latest_Summary`;

  return 1;
}

{
  my $bam_irods;

  lives_ok { $bam_irods = npg_pipeline::archive::file::logs->new(
    function_list => q{post_qc_review},
    run_folder => q{123456_IL2_1234},
    runfolder_path => $analysis_runfolder_path,
    timestamp => q{20090709-123456},
    verbose => 0,
    recalibrated_path => $pb_cal_path,
  ); } q{created with run_folder ok};
  isa_ok($bam_irods , q{npg_pipeline::archive::file::logs}, q{object test});
  create_analysis();

  my $arg_refs = {
    required_job_completion => q{-w'done(123) && done(321)'},
  };

  my @jids;
  lives_ok { @jids = $bam_irods->submit_to_lsf($arg_refs); } q{no croak submitting job to lsf};

  is(scalar@jids, 1, q{only one job submitted});

  my $bsub_command = $util->drop_temp_part_from_paths( $bam_irods ->_generate_bsub_command($arg_refs) );
  my $expected_command = qq{bsub -q lowload -w'done(123) && done(321)' -J npg_irods_log_loader.pl_1234_20090709-123456 -R 'rusage[nfs_12=1,seq_irods=15]' -o ${rfpath}/Data/Intensities/Bustard1.3.4_09-07-2009_auto/PB_cal/log/npg_irods_log_loader.pl_1234_20090709-123456.out -E "[ -d '$rfpath' ]" 'npg_irods_log_loader.pl --runfolder_path $rfpath --id_run 1234 --irods_root /seq/'};
  is( $bsub_command, $expected_command, q{generated bsub command is correct});
}

{
  my $bam_irods;

  lives_ok { $bam_irods = npg_pipeline::archive::file::logs->new(
    function_list => q{post_qc_review},
    run_folder => q{123456_IL2_1234},
    runfolder_path => $analysis_runfolder_path,
    recalibrated_path => $pb_cal_path,
    timestamp => q{20090709-123456},
    verbose => 0,
    lanes      => [8],
    irods_root => q{/test/},
  ); } q{created with run_folder ok};
  isa_ok($bam_irods , q{npg_pipeline::archive::file::logs}, q{object test});
  create_analysis();

  my $arg_refs = {
    required_job_completion => q{-w'done(123) && done(321)'},
  };

  my @jids;
  lives_ok { @jids = $bam_irods->submit_to_lsf($arg_refs); } q{no croak submitting job to lsf};

  is(scalar@jids, 1, q{only one job submitted});

  my $bsub_command = $util->drop_temp_part_from_paths( $bam_irods ->_generate_bsub_command($arg_refs) );
  my $expected_command = qq{bsub -q lowload -w'done(123) && done(321)' -J npg_irods_log_loader.pl_1234_20090709-123456 -R 'rusage[nfs_12=1,seq_irods=15]' -o ${rfpath}/Data/Intensities/Bustard1.3.4_09-07-2009_auto/PB_cal/log/npg_irods_log_loader.pl_1234_20090709-123456.out -E "[ -d '$rfpath' ]" 'npg_irods_log_loader.pl --runfolder_path $rfpath --id_run 1234 --irods_root /test/'};
  is( $bsub_command, $expected_command, q{generated bsub command is correct} );
}

{
  my $bam_irods;

  lives_ok { $bam_irods = npg_pipeline::archive::file::logs->new(
    function_list => q{post_qc_review_gclp},
    run_folder => q{123456_IL2_1234},
    runfolder_path => $analysis_runfolder_path,
    recalibrated_path => $pb_cal_path,
    timestamp => q{20090709-123456},
    verbose => 0,
  ); } q{created with run_folder ok};

  my $arg_refs = {
    required_job_completion => q{-w'done(123) && done(321)'},
  };

  my $bsub_command = $util->drop_temp_part_from_paths( $bam_irods ->_generate_bsub_command($arg_refs) );
  my $expected_command = qq{bsub -q lowload -w'done(123) && done(321)' -J npg_irods_log_loader.pl_1234_20090709-123456 -R 'rusage[nfs_12=1,seq_irods=15]' -o ${rfpath}/Data/Intensities/Bustard1.3.4_09-07-2009_auto/PB_cal/log/npg_irods_log_loader.pl_1234_20090709-123456.out -E "[ -d '$rfpath' ]" 'irodsEnvFile=\$HOME/.irods/.irodsEnv-gclp-iseq-logs npg_irods_log_loader.pl --runfolder_path $rfpath --id_run 1234 --irods_root /gseq/'};
  is( $bsub_command, $expected_command, q{generated bsub command is correct} );
}

1;
__END__
