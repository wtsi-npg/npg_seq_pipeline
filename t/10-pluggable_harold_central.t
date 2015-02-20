use strict;
use warnings;
use Test::More tests => 30;
use Test::Exception;
use Test::Deep;
use t::util;
use Cwd qw/getcwd/;

my $util = t::util->new();
my $conf_path = $util->conf_path();

my $cwd = getcwd();
my $tdir = $util->temp_directory();
local $ENV{TEST_DIR} = $tdir;
local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];
local $ENV{TEST_FS_RESOURCE} = q{nfs_12};

my $sp = join q[/], $tdir, 'spatial_filter';
my $java = join q[/], $tdir, 'java';
foreach my $tool (($sp, $java)) {
  `touch $tool`;
  `chmod +x $tool`;
}
local $ENV{PATH} = join q[:], $tdir, qq[$cwd/t/bin], $ENV{PATH};

my $central = q{npg_pipeline::pluggable::harold::central};
use_ok($central);

my $runfolder_path = $util->analysis_runfolder_path();

{
  $util->set_staging_analysis_area();
  my $pipeline;
  lives_ok {
    $pipeline = $central->new({
      script_name => q{test},
      id_run => 1234,
      run_folder => q{123456_IL2_1234},
      runfolder_path => $runfolder_path,
      conf_path => $conf_path,
      domain => q{test},
    });
  } q{no croak creating new object};
  isa_ok($pipeline, $central);

  my $expected_function_order = [ qw{
          lsf_start
          spider
          create_archive_directory
          create_empty_fastq
          create_summary_link_analysis
          bustard_matrix_lanes
          bustard_matrix_all
          bustard_phasing_lanes
          bustard_phasing_all
          bustard_basecalls_lanes
          bustard_basecalls_all
          illumina_basecall_stats
          illumina2bam
          qc_tag_metrics
          harold_alignment_files
          harold_calibration_tables
          harold_recalibration
          split_bam_by_tag
          bam2fastqcheck_and_cached_fastq
          qc_qX_yield
          qc_adapter
          qc_insert_size
          qc_sequence_error
          qc_gc_fraction
          qc_ref_match
          seq_alignment
          bam_cluster_counter_check
          seqchksum_comparator
          qc_gc_bias
          qc_pulldown_metrics
          qc_genotype
          qc_verify_bam_id
          qc_upstream_tags
          lsf_end     
  }];
  is_deeply( $pipeline->function_order() , $expected_function_order, q{Function order correct} );
}

{
  local $ENV{CLASSPATH} = q{t/bin/software/solexa/bin/aligners/illumina2bam/current};
  my $pipeline;
  lives_ok {
    $pipeline = $central->new({
      script_name => q{test},
      id_run => 1234,
      run_folder => q{123456_IL2_1234},
      runfolder_path => $runfolder_path,
      conf_path => $conf_path,
      domain => q{test},
      recalibration => 0,
      no_bsub => 1,
    });
  } q{no croak creating new object};

  ok( !scalar $pipeline->harold_calibration_tables(),  q{no calibration tables launched} );
 
  lives_ok { $pipeline->prepare() } 'prepare lives';
  ok( $pipeline->illumina_basecall_stats(),  q{olb false - illumina_basecall_stats job launched} );
  ok( !$pipeline->bustard_matrix_lanes(),  q{olb false - bustard_matrix_lanes job is not launhed} );
}

{
  my $pipeline = $central->new(
      script_name => q{test},
      id_run => 1234,
      run_folder => q{123456_IL2_1234},
      runfolder_path => $runfolder_path,
      conf_path => $conf_path,
      domain => q{test},
      no_bsub => 1,
      olb => 1
  );
  ok( !$pipeline->illumina_basecall_stats(),  q{olb true - illumina_basecall_stats job is not launched} );
}

{
  my $pb;
  lives_ok {
    $pb = $central->new({
      id_run => 1234,
      function_order => [qw(qc_qX_yield illumina2bam qc_insert_size)],
      run_folder => q{123456_IL2_1234},
      runfolder_path => $runfolder_path,
      conf_path => $conf_path,
      domain => q{test},
    });
  } q{no croak on creation};
  $util->set_staging_analysis_area({with_latest_summary => 1});
  is(join(q[ ], @{$pb->function_order()}), 'lsf_start qc_qX_yield illumina2bam qc_insert_size lsf_end', 'function_order set on creation');
}

{
  local $ENV{CLASSPATH} = undef;
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];

  my $pb;
  $util->set_staging_analysis_area();
  my $init = {
      script_name => q{test},
      id_run => 1234,
      run_folder => q{123456_IL2_1234},
      function_order => [qw{illumina2bam qc_qX_yield qc_adapter update_warehouse qc_insert_size archive_to_irods}],
      lanes => [4],
      runfolder_path => $runfolder_path,
      conf_path => $conf_path,
      domain => q{test},
      no_bsub => 1,
      repository => 't/data/sequence',
  };
 
  lives_ok { $pb = $central->new($init); } q{no croak on new creation};
  mkdir $pb->archive_path;
  mkdir $pb->qc_path;
  
  throws_ok { $pb->main() }
    qr/Error submitting jobs: Can\'t find \'BamAdapterFinder\.jar\' because CLASSPATH is not set/, 
    q{error running qc->main() when CLASSPATH is not set for illumina2bam job};

  local $ENV{CLASSPATH} = q[t/bin/software];
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];

  throws_ok { $pb->main() }
    qr/Error submitting jobs: no such file on CLASSPATH: BamAdapterFinder\.jar/, 
    q{error running qc->main() when CLASSPATH is not set correctly for illumina2bam job};

  local $ENV{CLASSPATH} = q[t/bin/software/solexa/bin/aligners/illumina2bam/current];
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];

  lives_ok { $pb->main() } q{no croak running qc->main() when CLASSPATH is set correctly for illumina2bam job};
  my $timestamp = $pb->timestamp;
  my $recalibrated_path = $pb->recalibrated_path();
  my $log_dir = $pb->make_log_dir( $recalibrated_path );
  my $expected_command =  qq[bsub -q test  -J whupdate_1234_central -o $log_dir/whupdate_1234_central_] . $timestamp .
  q[.out 'unset NPG_WEBSERVICE_CACHE_DIR; unset NPG_CACHED_SAMPLESHEET_FILE; warehouse_loader --id_run 1234'];
  is($pb->_update_warehouse_command, $expected_command, 'update warehouse command');
}

my $rf = join q[/], $tdir, 'myfolder';
mkdir $rf;
{
  my $init = {
      id_run => 1234,
      run_folder => 'myfolder',
      runfolder_path => $rf,
      no_bsub => 1,
      timestamp => '22-May',
  };
  my $pb;
  lives_ok { $pb = $central->new($init); }
    q{no croak on creation of a flattened runfolder};
  is ($pb->intensity_path, $rf, 'intensities path is set to runfolder');
  is ($pb->basecall_path, $rf, 'basecall path is set to runfolder');
  is ($pb->bam_basecall_path, join(q[/],$rf,q{BAM_basecalls_22-May}), 'bam basecall path is created');
  is ($pb->pb_cal_path, join(q[/],$pb->bam_basecall_path, 'no_cal'), 'pb_cal path set');
  is ($pb->recalibrated_path, $pb->pb_cal_path, 'recalibrated directory set');
  my $status_path = $pb->status_files_path();
  is ($status_path, join(q[/],$rf,q{BAM_basecalls_22-May}, q{status}), 'status directory path');
  ok(-d $status_path, 'status directory created');
  ok(-d "$status_path/log", 'log directory for status jobs created');

  my $expected = qq[bsub -q srpipeline -R 'rusage[nfs_12=1]' -w'done(462362)' -J 'bam2fastqcheck_and_cached_fastq_1234_22-May[1-8]' -o $rf/BAM_basecalls_22-May/no_cal/log/bam2fastqcheck_and_cached_fastq_1234_22-May.%I.%J.out 'generate_cached_fastq --path $rf/BAM_basecalls_22-May/no_cal/archive --file $rf/BAM_basecalls_22-May/no_cal/1234_`echo ] . q[$LSB_JOBINDEX`.bam'];
  is ($pb->_bam2fastqcheck_and_cached_fastq_command(q[-w'done(462362)']),
    $expected, 'command for bam2fastqcheck_and_cached_fastq');
  my @ids = $pb->bam2fastqcheck_and_cached_fastq();
  is (scalar @ids, 1, 'one bam2fastqcheck_and_cached_fastq job submitted');
}

{
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data/hiseqx];
  my $pb;
  lives_ok { $pb = $central->new(
                     id_run => 13219,
                     no_bsub => 1,
                     run_folder => 'myfolder',
                     runfolder_path => $rf,
                   )
           }
     q{no croak on creation of an object};
  ok (!$pb->illumina_basecall_stats, 'illumina_basecall_stats step is skipped for HiSeqX run');
}

1;
