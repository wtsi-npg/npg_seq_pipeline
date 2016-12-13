use strict;
use warnings;
use Test::More tests => 32;
use Test::Exception;
use Cwd qw/getcwd/;
use List::MoreUtils qw/ any none /;
use Log::Log4perl qw(:levels);

use t::util;

local $ENV{http_proxy} = 'http://wibble';
local $ENV{no_proxy} = q[];

my $util = t::util->new();
my $cwd = getcwd();
my $tdir = $util->temp_directory();

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $tdir, 'logfile'),
                          utf8   => 1});

local $ENV{TEST_DIR} = $tdir;
local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];
local $ENV{TEST_FS_RESOURCE} = q{nfs_12};

my $sp = join q[/], $tdir, 'spatial_filter';
my $java = join q[/], $tdir, 'java';
foreach my $tool (($sp, $java)) {
  `touch $tool`;
  `chmod +x $tool`;
}
local $ENV{PATH} = join q[:], $tdir, qq[$cwd/t/bin],  qq[$cwd/t/bin/software/solexa/bin], $ENV{PATH};

my $central = q{npg_pipeline::pluggable::harold::central};
use_ok($central);

my $runfolder_path = $util->analysis_runfolder_path();

{
  $util->set_staging_analysis_area();
  my $pipeline;
  lives_ok {
    $pipeline = $central->new(
      runfolder_path => $runfolder_path,
    );
  } q{no croak creating new object};
  isa_ok($pipeline, $central);

  my $expected_function_order = [ qw{
    lsf_start
    create_archive_directory
    create_empty_fastq
    create_summary_link_analysis
    run_analysis_in_progress
    lane_analysis_in_progress
    illumina_basecall_stats
    p4_stage1_analysis
    update_warehouse
    update_ml_warehouse
    run_secondary_analysis_in_progress
    bam2fastqcheck_and_cached_fastq
    qc_qX_yield
    qc_adapter
    qc_insert_size
    qc_sequence_error
    qc_gc_fraction
    qc_ref_match
    seq_alignment
    update_ml_warehouse
    bam_cluster_counter_check
    seqchksum_comparator
    qc_gc_bias
    qc_pulldown_metrics
    qc_genotype
    qc_verify_bam_id
    qc_upstream_tags
    qc_rna_seqc
    run_analysis_complete
    update_ml_warehouse
    archive_to_irods_samplesheet
    run_qc_review_pending
    lsf_end
  }];
  is_deeply( $pipeline->function_order() , $expected_function_order, q{Function order correct} );
}

{
  local $ENV{CLASSPATH} = q{t/bin/software/solexa/bin/aligners/illumina2bam/current};
  my $pipeline;
  lives_ok {
    $pipeline = $central->new(
      id_run => 1234,
      runfolder_path => $runfolder_path,
      recalibration => 0,
      no_bsub => 1,
      spider  => 0,
    );
  } q{no croak creating new object};

  ok( !scalar $pipeline->harold_calibration_tables(),  q{no calibration tables launched} );
  ok(!$pipeline->olb, 'not olb pipeline');
  lives_ok { $pipeline->prepare() } 'prepare lives';
  ok( $pipeline->illumina_basecall_stats(),  q{olb false - illumina_basecall_stats job launched} );
  my $bool = none {$_ =~ /bustard/} @{$pipeline->function_order()};
  ok( $bool, 'bustard functions are out');

  $pipeline = $central->new(
    runfolder_path => $runfolder_path,
    no_bsub => 1,
    olb     => 1,
  );
  is ($pipeline->function_list, getcwd() . '/data/config_files/function_list_central_olb.yml',
    'olb function list');
  $bool = any {$_ =~ /bustard/} @{$pipeline->function_order()};
  ok( $bool, 'bustard functions are in');
}

{
  my $pb;
  lives_ok {
    $pb = $central->new(
      function_order => [qw(qc_qX_yield illumina2bam qc_insert_size)],
      runfolder_path => $runfolder_path,
    );
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
      function_order => [qw{illumina2bam qc_qX_yield qc_adapter update_warehouse qc_insert_size archive_to_irods}],
      lanes => [4],
      runfolder_path => $runfolder_path,
      no_bsub => 1,
      repository => 't/data/sequence',
      spider  => 0,
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
  my $unset_string = 'unset NPG_WEBSERVICE_CACHE_DIR;unset NPG_CACHED_SAMPLESHEET_FILE;';
  my $expected_command = q[bsub -q lowload 50 -J warehouse_loader_1234_central ] .
                        qq[-o $log_dir/warehouse_loader_1234_central_] . $timestamp .
                        qq[.out  '${unset_string}warehouse_loader --verbose --id_run 1234'];
  is($pb->_update_warehouse_command('warehouse_loader', (50)),
    $expected_command, 'update warehouse command');
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
  lives_ok { $pb = $central->new($init); $pb->_set_paths() }
    q{no error on object creation and analysis paths set for a flattened runfolder};
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
                   );
             $pb->_set_paths();
           } q{no error on object creation and analysis paths set};
  ok (!$pb->illumina_basecall_stats, 'illumina_basecall_stats step is skipped for HiSeqX run');
}

1;
