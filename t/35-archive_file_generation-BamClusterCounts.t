use strict;
use warnings;
use English qw{-no_match_vars};
use Test::More tests => 20;
use Test::Exception;
use Log::Log4perl qw(:levels);
use t::util;

use_ok( q{npg_pipeline::archive::file::BamClusterCounts} );

my $util = t::util->new({});
my $dir = $util->temp_directory();
$ENV{TEST_DIR} = $dir;
local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];
local $ENV{PATH} = join q[:], q[t/bin], q[t/bin/software/solexa/bin], $ENV{PATH};

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $dir, 'logfile'),
                          utf8   => 1});

$util->create_multiplex_analysis();
my $analysis_runfolder_path = $util->analysis_runfolder_path();
my $bam_basecall_path = $util->standard_analysis_bustard_path();
qx{cp t/data/summary_files/BustardSummary_mp.xml $bam_basecall_path/BustardSummary.xml};
my $recalibrated_path = $util->standard_analysis_recalibrated_path();
my $archive_path = $recalibrated_path . q{/archive};

{
  my $object;
  lives_ok {
    $object = npg_pipeline::archive::file::BamClusterCounts->new(
      run_folder => q{123456_IL2_1234},
      runfolder_path => $analysis_runfolder_path,
      bam_basecall_path => $bam_basecall_path,
      id_run => 1234,
      timestamp => q{20100907-142417},
      no_bsub => 1,
    );
  } q{obtain object ok};

  isa_ok( $object, q{npg_pipeline::archive::file::BamClusterCounts}, q{$object} );

  my $arg_refs = {
    required_job_completion => q{-w'done(123) && done(321)'},
    array_string => q{[1-8]},
  };

  my $bsub_command = $util->drop_temp_part_from_paths( qq{bsub -q srpipeline -w'done(123) && done(321)' -J 'npg_pipeline_check_bam_file_cluster_count_1234_20100907-142417[1-8]' -o $archive_path/log/npg_pipeline_check_bam_file_cluster_count_1234_20100907-142417.} . q{%I.%J.out 'npg_pipeline_check_bam_file_cluster_count --id_run=1234 --position=`echo $LSB_JOBINDEX` --runfolder_path=} . qq{$analysis_runfolder_path --qc_path=$archive_path/qc --bam_basecall_path=$bam_basecall_path'} );
  is( $util->drop_temp_part_from_paths( $object->_generate_bsub_command( $arg_refs ) ), $bsub_command, q{generated bsub command is correct} );

  my @jids = $object->launch( $arg_refs );
  is( scalar @jids, 1, q{1 job id returned} );
}

{
  my $analysis_runfolder_path = 't/data/example_runfolder/121103_HS29_08747_B_C1BV5ACXX';
  my $bam_basecall_path = "$analysis_runfolder_path/Data/Intensities/BAM_basecalls_20130122-085552";
  my $archive_path = "$bam_basecall_path/PB_cal_bam/archive";

  my $object;
  lives_ok{
    $object = npg_pipeline::archive::file::BamClusterCounts->new(
      id_run => 8747,
      position => 1,
      runfolder_path => $analysis_runfolder_path,
      bam_basecall_path => $bam_basecall_path,
      archive_path => $archive_path,
    );
  } q{obtain object ok};

  is( $object->_bustard_pf_cluster_count(),  150694669, q{correct pf_cluster_count obtained from TileMetricsOut.bin}  );
  is( $object->_bustard_raw_cluster_count(), 158436062, q{correct raw_cluster_count obtained from TileMetricsOut.bin} );

  lives_ok {
    $object->run_cluster_count_check();
  } qr{check returns ok};
}

{
  my $object;
  lives_ok{
    $object = npg_pipeline::archive::file::BamClusterCounts->new(
      id_run => 1234,
      runfolder_path => $analysis_runfolder_path,
      position => 3,
      bam_basecall_path => $bam_basecall_path,
      archive_path => $archive_path,
    );
  } q{obtain object ok};
  
  ok( !$object->_bam_cluster_count_total({}), 'no bam cluster count total returned');
  
  qx{mkdir $archive_path/qc};
  qx{cp t/data/bam_flagstats/1234_3_bam_flagstats.json $archive_path/qc };
  qx{cp t/data/bam_flagstats/1234_3_phix_bam_flagstats.json $archive_path//qc/1234_3_phix_bam_flagstats.json};

  my $is_indexed = 1;
  qx{mkdir -p $archive_path/lane3/qc};
  qx{cp t/data/bam_flagstats/1234_3_bam_flagstats.json $archive_path/lane3/qc/1234_3#0_bam_flagstats.json};
  qx{cp t/data/bam_flagstats/1234_3_bam_flagstats.json $archive_path/lane3/qc/1234_3#1_bam_flagstats.json};
  
  is( $object->_bam_cluster_count_total( {plex=>$is_indexed} ), 32, 'correct bam cluster count total for plexes');

  qx{cp t/data/bam_flagstats/1234_3_phix_bam_flagstats.json $archive_path/lane3/qc/1234_3#0_phix_bam_flagstats.json};
  
  is( $object->_bam_cluster_count_total( {plex=>$is_indexed} ), 46, 'correct bam cluster count total for plexes');

}

{
  my $analysis_runfolder_path = 't/data/example_runfolder/121103_HS29_08747_B_C1BV5ACXX';
  my $bam_basecall_path = "$analysis_runfolder_path/Data/Intensities/BAM_basecalls_20130122-085552";
  my $archive_path = "$bam_basecall_path/PB_cal_bam/archive";

  my $object;
  lives_ok{
    $object = npg_pipeline::archive::file::BamClusterCounts->new(
      id_run => 8747,
      position => 1,
      runfolder_path => $analysis_runfolder_path,
      bam_basecall_path => $bam_basecall_path,
      archive_path => $archive_path,
    );
  } q{obtain object ok};

  is( $object->_bam_cluster_count_total({plex=>1}), 301389338, 'correct bam cluster count total');
  rename "$archive_path/lane1/qc/8747_1#0_bam_flagstats.json", "$archive_path/lane1/qc/8747_1#0_bam_flagstats.json.RENAMED";
  throws_ok {$object->run_cluster_count_check()}  qr{Cluster count in bam files not as expected}, 'Cluster count in bam files not as expected';
  rename "$archive_path/lane1/qc/8747_1#0_bam_flagstats.json.RENAMED", "$archive_path/lane1/qc/8747_1#0_bam_flagstats.json";
  ok($object->run_cluster_count_check(), 'Cluster count in bam files as expected');
}

{
  my $analysis_runfolder_path = 't/data/example_runfolder/121103_HS29_08747_B_C1BV5ACXX';
  my $bam_basecall_path = "$analysis_runfolder_path/Data/Intensities/BAM_basecalls_20130122-085552";
  my $qc_path = "$bam_basecall_path/PB_cal_bam/archive/qc";

  my $common_command = "$EXECUTABLE_NAME bin/npg_pipeline_check_bam_file_cluster_count --id_run 8747 --bam_basecall_path $bam_basecall_path --qc_path $qc_path --position ";
  note `$common_command 1 2>&1`;
  ok( ! $CHILD_ERROR, q{script runs ok when no spatial filter json} );
  note `$common_command 4 2>&1`;
  ok( ! $CHILD_ERROR, q{script runs ok when spatial filter has failed reads} );
  note `$common_command 6 2>&1`;
  ok( ! $CHILD_ERROR, q{script runs ok when no spatial filter has no PF reads} );
}

1;
