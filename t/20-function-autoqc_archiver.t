use strict;
use warnings;
use Test::More tests => 10;
use Test::Exception;
use t::util;

use_ok('npg_pipeline::function::autoqc_archiver');

my $util = t::util->new();

$ENV{TEST_DIR} = $util->temp_directory();
$ENV{TEST_FS_RESOURCE} = q{nfs_12};
local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];
local $ENV{PATH} = join q[:], q[t/bin], q[t/bin/software/solexa/bin], $ENV{PATH};

my $analysis_runfolder_path = $util->analysis_runfolder_path();
my $rel_pbcal = 'Data/Intensities/Bustard1.3.4_09-07-2009_auto/PB_cal';
my $nfs_pbcal = "/nfs/sf45/IL2/analysis/123456_IL2_1234/$rel_pbcal";
my $pbcal = "$analysis_runfolder_path/$rel_pbcal";
sub create_analysis {
  `rm -rf /tmp/nfs/sf45`;
  `mkdir -p $pbcal/archive`;
  `mkdir $analysis_runfolder_path/Config`;
  `cp t/data/Recipes/Recipe_GA2_37Cycle_PE_v6.1.xml $analysis_runfolder_path/`;
  `ln -s $rel_pbcal $analysis_runfolder_path/Latest_Summary`;

  return 1;
}

{
  my $aaq;

  lives_ok { $aaq = npg_pipeline::function::autoqc_archiver->new(
    run_folder => q{123456_IL2_1234},
    runfolder_path => $analysis_runfolder_path,
    timestamp => q{20090709-123456},
    verbose => 0,
  ); } q{created with run_folder ok};
  isa_ok($aaq, q{npg_pipeline::function::autoqc_archiver}, q{$aaq});
  create_analysis();

  my @jids;
  lives_ok { @jids = $aaq->submit_to_lsf(); } q{no croak submitting job to lsf};

  is(scalar@jids, 1, q{only one job submitted});

  my $bsub_command = $util->drop_temp_part_from_paths( $aaq->_generate_bsub_command() );
  my $expected_cmd = qq{bsub -q lowload -J autoqc_loader_1234_20090709-123456 -R 'rusage[nfs_12=1]' -o $nfs_pbcal/log/autoqc_loader_1234_20090709-123456.out 'npg_qc_autoqc_data.pl --id_run=1234 --path=$nfs_pbcal/archive/qc'};

  is( $bsub_command, $expected_cmd, q{generated bsub command is correct} );
}

{
  my $args = {};
  $args->{qc_dir} = [2,3,5];

  $util->create_multiplex_analysis($args);

  my $aaq;
  lives_ok { $aaq = npg_pipeline::function::autoqc_archiver->new(
    run_folder => q{123456_IL2_1234},
    runfolder_path => $analysis_runfolder_path,
    timestamp => q{20090709-123456},
    verbose   => 0,
  ); } q{created with run_folder ok};

  my @jids;
  lives_ok { @jids = $aaq->submit_to_lsf(); } q{no croak submitting job to lsf};

  is(scalar@jids, 1, q{only one job submitted});

  my $bsub_command = $util->drop_temp_part_from_paths( $aaq->_generate_bsub_command() );
  my $expected_cmd = qq{bsub -q lowload -J autoqc_loader_1234_20090709-123456 -R 'rusage[nfs_12=1]' -o $nfs_pbcal/log/autoqc_loader_1234_20090709-123456.out 'npg_qc_autoqc_data.pl --id_run=1234 --path=$nfs_pbcal/archive/qc --path=$nfs_pbcal/archive/lane2/qc --path=$nfs_pbcal/archive/lane3/qc --path=$nfs_pbcal/archive/lane5/qc'};

  is( $bsub_command, $expected_cmd, q{generated bsub command is correct} );
}

1;
