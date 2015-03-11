use strict;
use warnings;
use Test::More tests => 10;
use Test::Exception;
use t::util;

use_ok(q{npg_pipeline::archive::qc::fastqcheck_loader});

my $util = t::util->new();

$ENV{TEST_DIR} = $util->temp_directory();
$ENV{TEST_FS_RESOURCE} = q{nfs_12};
local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];
local $ENV{PATH} = join q[:], q[t/bin], q[t/bin/software/solexa/bin], $ENV{PATH};
my $pbcal_path = q{/nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/Bustard1.3.4_09-07-2009_auto/PB_cal};
{
  $util->create_multiplex_analysis( { qc_dir => [1..7], } );

  my $fq_loader;
  lives_ok {
    $fq_loader = npg_pipeline::archive::qc::fastqcheck_loader->new({
      run_folder => q{123456_IL2_1234},
      runfolder_path => $util->analysis_runfolder_path(),
      timestamp => q{20090709-123456},
      verbose => 0,
    });
  } q{fq_loader created ok};
  isa_ok( $fq_loader, q{npg_pipeline::archive::qc::fastqcheck_loader}, q{$fq_loader} );

  my $arg_refs = {
    required_job_completion => q{-w'done(123) && done(321)'},
  };

  my @jids;
  lives_ok { @jids = $fq_loader->submit_to_lsf($arg_refs); } q{no croak submitting job to lsf};

  is( scalar @jids, 1, q{1 job id returned} );

  my $command = $util->drop_temp_part_from_paths( $fq_loader->_generate_bsub_command($arg_refs) );
  my $expected_cmd = qq{bsub -q srpipeline -w'done(123) && done(321)' -J fastqcheck_loader_1234_20090709-123456 -R 'rusage[nfs_12=1]' -o $pbcal_path/log/fastqcheck_loader_1234_20090709-123456.out 'npg_qc_save_files.pl --path=$pbcal_path/archive --path=$pbcal_path/archive/lane1 --path=$pbcal_path/archive/lane2 --path=$pbcal_path/archive/lane3 --path=$pbcal_path/archive/lane4 --path=$pbcal_path/archive/lane5 --path=$pbcal_path/archive/lane6 --path=$pbcal_path/archive/lane7'};
  is( $command, $expected_cmd, q{generated bsub command is correct} );
}

{
  $util->create_analysis( { qc_dir => 1, } );
  my $fq_loader;
  lives_ok {
    $fq_loader = npg_pipeline::archive::qc::fastqcheck_loader->new({
      run_folder => q{123456_IL2_1234},
      runfolder_path => $util->analysis_runfolder_path(),
      timestamp => q{20090709-123456},
      verbose => 0,
    });
  } q{fq_loader created ok};

  my $arg_refs = {
    required_job_completion => q{-w'done(123) && done(321)'},
  };

  my @jids;
  lives_ok { @jids = $fq_loader->submit_to_lsf($arg_refs); } q{no croak submitting job to lsf};
  is( scalar @jids, 1, q{1 job id returned} );

  my $command = $util->drop_temp_part_from_paths( $fq_loader->_generate_bsub_command($arg_refs) );
  my $expected_cmd = qq{bsub -q srpipeline -w'done(123) && done(321)' -J fastqcheck_loader_1234_20090709-123456 -R 'rusage[nfs_12=1]' -o $pbcal_path/log/fastqcheck_loader_1234_20090709-123456.out 'npg_qc_save_files.pl --path=$pbcal_path/archive'};
  is( $command, $expected_cmd, q{generated bsub command is correct} );
}

1;
