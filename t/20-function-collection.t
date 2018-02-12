use strict;
use warnings;
use Test::More tests => 5;
use Test::Exception;
use Log::Log4perl qw(:levels);

use t::util;

my $util = t::util->new();
my $runfolder_path = $util->analysis_runfolder_path();
local $ENV{PATH} = join q[:], q[t/bin], $ENV{PATH};
$ENV{TEST_FS_RESOURCE} = q{nfs_12};
my $temp = $util->temp_directory();

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $temp, 'logfile'),
                          utf8   => 1});

use_ok('npg_pipeline::function::collection');

subtest 'warehouse updates' => sub {
  plan tests => 5;

  my $c = npg_pipeline::function::collection->new(
      run_folder          => q{123456_IL2_1234},
      runfolder_path      => $runfolder_path,
      recalibrated_path   => $runfolder_path,
      no_irods_archival   => 1,
      no_warehouse_update => 1
  );
  isa_ok ($c, 'npg_pipeline::function::collection');
  my $timestamp = $c->timestamp;
  my $recalibrated_path = $c->recalibrated_path();
  my $log_dir = $c->make_log_dir($recalibrated_path);
  my $log_dir_in_outgoing = $log_dir;
  $log_dir_in_outgoing =~ s{/analysis/}{/outgoing/}smx;
  my $job_name = 'warehouse_loader_1234_collection';
  my $prefix = qq[bsub -q lowload -J $job_name ] .
    qq[-o $log_dir/${job_name}_${timestamp}.out];
  my $command = q['warehouse_loader --verbose --id_run 1234 --lims_driver_type samplesheet'];
  is($c->_update_warehouse_command('warehouse_loader'), qq[$prefix  $command],
    'update warehouse command');

  $job_name .= '_postqccomplete';
  $prefix = qq[bsub -q lowload -J $job_name ] .
    qq[-o $log_dir_in_outgoing/${job_name}_${timestamp}.out];
  my $preexec = qq(-E "[ -d '${log_dir_in_outgoing}' ]");
  $command = q['warehouse_loader --verbose --id_run 1234 --lims_driver_type ml_warehouse_fc_cache'];
  is($c->_update_warehouse_command('warehouse_loader', 'post_qc_complete'),
    join(q[ ],$prefix,$preexec,$command),
    'update warehouse command with preexec and change to outgoing');

  $job_name = 'npg_runs2mlwarehouse_1234_collection';
  $prefix = qq[bsub -q lowload -J $job_name ] .
            qq[-o $log_dir/${job_name}_${timestamp}.out];
  $command = q['npg_runs2mlwarehouse --verbose --id_run 1234'];
  is($c->_update_warehouse_command('npg_runs2mlwarehouse'),
    qq[$prefix  $command], 'update ml_warehouse command');

  $job_name .= '_postqccomplete';
  $prefix = qq[bsub -q lowload -J $job_name ] .
            qq[-o $log_dir_in_outgoing/${job_name}_${timestamp}.out];
  is($c->_update_warehouse_command('npg_runs2mlwarehouse', 'post_qc_complete'),
    join(q[ ],$prefix,$preexec,$command),
    'update ml_warehouse command with preexec and change to outgoing');
};

subtest 'updates disabled' => sub {
  plan tests => 3;

  my $c = npg_pipeline::function::collection->new(
    runfolder_path      => $runfolder_path,
    recalibrated_path   => $runfolder_path,
    no_irods_archival   => 1,
    no_warehouse_update => 1
  );
  ok(!$c->update_warehouse(), 'update to warehouse switched off');

  $c = npg_pipeline::function::collection->new(
    runfolder_path => $runfolder_path,
    recalibrated_path   => $runfolder_path,
    local          => 1,
  );
  ok(!$c->update_warehouse(), 'update to warehouse switched off');

  $c = npg_pipeline::function::collection->new(
    runfolder_path      => $runfolder_path,
    recalibrated_path   => $runfolder_path,
    local               => 1,
    no_warehouse_update => 0,
  );
  ok($c->update_warehouse(), 'update to warehouse switched on');
};

subtest 'start and stop jobs' => sub {
  plan tests => 4;

  my $c = npg_pipeline::function::collection->new(
    id_run         => 1234,
    runfolder_path => $runfolder_path,
    no_bsub        => 1,
  );
  my @ids;
  lives_ok { @ids = $c->pipeline_start() } q{no error submitting start job};
  is(join(q[ ], @ids), '50', 'test start job id is correct');
  lives_ok { @ids = $c->pipeline_end() } q{no error submitting end job};
  is(join(q[ ], @ids), '50', 'test start job id is correct');
};

subtest ' bam2fastqcheck_and_cached_fastq' => sub {
  plan tests => 2;

  my $c = npg_pipeline::function::collection->new(
    id_run            => 1234,
    recalibrated_path => $runfolder_path,
    runfolder_path    => $runfolder_path,
    timestamp         => q{22-May},
    no_bsub          => 1,
  );
  my $expected = qq[bsub -q srpipeline -R 'rusage[nfs_12=1]' -J 'bam2fastqcheck_and_cached_fastq_1234_22-May[1-8]' -o $runfolder_path/log/bam2fastqcheck_and_cached_fastq_1234_22-May.%I.%J.out 'generate_cached_fastq --path $runfolder_path/archive --file $runfolder_path/1234_`echo ] . q[$LSB_JOBINDEX`.bam'];
  is ($c->_bam2fastqcheck_and_cached_fastq_command(),
    $expected, 'command for bam2fastqcheck_and_cached_fastq');
  my @ids = $c->bam2fastqcheck_and_cached_fastq();
  is (scalar @ids, 1, 'one bam2fastqcheck_and_cached_fastq job submitted');
};
