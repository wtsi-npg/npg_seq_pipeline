use strict;
use warnings;
use Test::More tests => 6;
use Test::Exception;
use JSON;

use npg_tracking::glossary::composition::component::illumina;
use npg_tracking::glossary::composition;

use_ok ('npg_pipeline::function::definition');
use_ok ('npg_pipeline::executor::lsf::job');

my $conf = {default_queue              => 'srpipeline',
            small_queue                => 'small',
            lowload_queue              => 'lowload',
            job_name_prefix            => 'prod',
            array_cpu_limit            => 64};

my $fs_resource = q[nfs-sf44];

sub _create_definition_args {
  my %h = (created_by   => 'test',
           created_on   => '19 March 2018',
           job_name     => 'my_job',
           identifier   => '12345',
           command      => '/bin/true',
           log_file_dir => '/tmp');
  return \%h;
}

subtest 'object creation, default values of attributes' => sub {
  plan tests => 7;

  throws_ok {npg_pipeline::executor::lsf::job->new(definitions => [])}
    qr/Array of definitions cannot be empty/,
    'error if definitions array is empty';

  my $args = _create_definition_args();
  my $d = npg_pipeline::function::definition->new($args);
  my $j = npg_pipeline::executor::lsf::job->new(definitions => [$d]);
  isa_ok ($j, 'npg_pipeline::executor::lsf::job');
  ok (!$j->is_array, 'job should not be an array');

  $j = npg_pipeline::executor::lsf::job->new(definitions =>[$d, $d]);
  ok ($j->is_array, 'job should be an array');

  $args->{'command'} = '/bin/false';
  my $d1 = npg_pipeline::function::definition->new($args);
  lives_ok {npg_pipeline::executor::lsf::job->new(definitions =>[$d, $d1])}
    'mismatch in values returned by the command attribute is allowed';

  $args->{'num_cpus'} = [2];
  my $d2 = npg_pipeline::function::definition->new($args);
  throws_ok {npg_pipeline::executor::lsf::job->new(definitions =>[$d1, $d2])}
    qr/Inconsistent values for definition predicate method has_num_cpus/,
    'mismatch in values returned by the predicate - error';

  $args->{'num_cpus'} = [2,6];
  my $d3 = npg_pipeline::function::definition->new($args);
  throws_ok {npg_pipeline::executor::lsf::job->new(definitions =>[$d2, $d3])}
    qr/Inconsistent values for definition attribute num_cpus/,
    'mismatch in values returned by an attribute - error';
};

subtest 'delegated methods' => sub {
  plan tests => 24;

  my $args = _create_definition_args();
  my $d = npg_pipeline::function::definition->new($args);
  my $j = npg_pipeline::executor::lsf::job->new(definitions => [$d]);

  my @methods = map {q[j].$_ } qw/    job_name
                                      command_preexec
                                      log_file_dir
                                      num_cpus
                                      num_hosts
                                      memory
                                      queue
                                      fs_slots_num
                                      reserve_irods_slots
                                      array_cpu_limit
                                      apply_array_cpu_limit
                                /;
  for my $m (@methods) {
    ok ($j->can($m), "method $m is defined");
  }
  ok (!$j->can('jcommand'), 'command method is not accessible via jcommand');
  ok (!$j->can('command'),  'command method is not available');

  is ($j->jjob_name, 'my_job', 'job name');
  is ($j->jlog_file_dir, '/tmp', 'log file dir');
  is ($j->jqueue, 'default', 'queue');
  ok (!$j->jcommand_preexec, 'preexec not set');
  ok (!$j->japply_array_cpu_limit, 'apply_array_cpu_limit is false');
  ok (!$j->jmemory,'memory is not set');
  ok (!$j->jnum_cpus,'num_cpus is not set');
  ok (!$j->jnum_hosts,'num_hosts is not set');
  ok (!$j->jfs_slots_num,'fs_slots_num is not set');
  ok (!$j->jreserve_irods_slots,'reserve_irods_slots is not set');
  ok (!$j->jarray_cpu_limit,'array_cpu_limit is not set');
};

subtest 'run level job' => sub {
  plan tests => 3;

  my $json =  '{
         "__CLASS__" : "npg_pipeline::function::definition",
         "command" : "cd /nfs/sf55 && setupBclToQseq.py",
         "created_by" : "npg_pipeline::function::illumina_basecall_stats",
         "created_on" : "20180322-093445",
         "fs_slots_num" : 4,
         "identifier" : 25438,
         "job_name" : "basecall_stats_25438_20180322-093445",
         "log_file_dir" : "/tmp/BAM_basecalls_20180321-075511/log",
         "memory" : 350,
         "num_cpus" : [
            4
         ],
         "num_hosts" : 1,
         "queue" : "default"
               }';
  my $d = npg_pipeline::function::definition->thaw($json);
  my $j = npg_pipeline::executor::lsf::job->new(
                  upstream_job_ids => [40, 50],
                  definitions      => [$d],
                  fs_resource      => $fs_resource,
                  lsf_conf         => $conf);
  ok (!$j->is_array(), 'not an array');
  is_deeply ($j->commands(), {25438 => "cd /nfs/sf55 && setupBclToQseq.py"},
    'commands');
  is ($j->params,
    q[-w'done(40) && done(50)' -q srpipeline -J 'prod_basecall_stats_25438_20180322-093445' -M 350 -R 'select[mem>350] rusage[mem=350]' -n 4 -R 'span[hosts=1]' -R 'rusage[nfs-sf44=4]' -o /tmp/BAM_basecalls_20180321-075511/log/basecall_stats_25438_20180322-093445.%J.out],
    'params');
};

subtest 'arrays' => sub {
  plan tests => 10;

  my $json = '{
         "__CLASS__" : "npg_pipeline::function::definition",
         "apply_array_cpu_limit" : "1",
         "command" : "qc --check=adapter --id_run=25438 --position=1 --file_type=bam --qc_in=/tmp/BAM_basecalls_20180321-075511/no_cal --qc_out=/tmp/BAM_basecalls_20180321-075511/no_cal/archive/qc",
         "command_preexec" : "npg_pipeline_preexec_references",
         "composition" : {
            "__CLASS__" : "npg_tracking::glossary::composition",
            "components" : [
               {
                  "__CLASS__" : "npg_tracking::glossary::composition::component::illumina",
                  "id_run" : 25438,
                  "position" : 1
               }
            ]
         },
         "created_by" : "npg_pipeline::function::autoqc",
         "created_on" : "20180322-093445",
         "fs_slots_num" : 1,
         "reserve_irods_slots": 1,
         "identifier" : 25438,
         "job_name" : "qc_adapter_25438_20180322-093445",
         "log_file_dir" : "/tmp/BAM_basecalls_20180321-075511/no_cal/archive/qc/log",
         "memory" : 1500,
         "num_cpus" : [
            "2","6"
         ],
         "num_hosts" : 1,
         "queue" : "default"
             }';

  my $d = npg_pipeline::function::definition->thaw($json);
  my $j = npg_pipeline::executor::lsf::job->new(
                  upstream_job_ids => [],
                  definitions      => [$d],
                  fs_resource      => $fs_resource,
                  lsf_conf         => $conf);
  throws_ok {$j->params} qr/default_lsf_irods_resource not set in the LSF conf file/,
    'error when default_lsf_irods_resource is not set in the conf file';

  $conf->{'default_lsf_irods_resource'} = 15;

  $j = npg_pipeline::executor::lsf::job->new(
                  upstream_job_ids => [],
                  definitions      => [$d],
                  fs_resource      => $fs_resource,
                  lsf_conf         => $conf);
  ok ($j->is_array(), 'is an array');
  my $command = q[qc --check=adapter --id_run=25438 --position=1 --file_type=bam --qc_in=/tmp/BAM_basecalls_20180321-075511/no_cal --qc_out=/tmp/BAM_basecalls_20180321-075511/no_cal/archive/qc];
  is_deeply ($j->commands(), {1 => $command}, 'commands');
  my $params = q(-q srpipeline -J 'prod_qc_adapter_25438_20180322-093445[1]%64' -M 1500 -R 'select[mem>1500] rusage[mem=1500]' -n 2,6 -R 'span[hosts=1]' -R 'rusage[nfs-sf44=1]' -R 'rusage[seq_irods=15]' -o /tmp/BAM_basecalls_20180321-075511/no_cal/archive/qc/log/qc_adapter_25438_20180322-093445.%J.%I.out -E 'npg_pipeline_preexec_references');
  is ($j->params, $params, 'params');

  $j = npg_pipeline::executor::lsf::job->new(
                  upstream_job_ids   => [],
                  definitions        => [$d],
                  no_array_cpu_limit => 1,
                  lsf_conf           => $conf,
                  fs_resource        => undef);
  is_deeply ($j->commands(), {1 => $command}, 'commands');
  $params = q(-q srpipeline -J 'prod_qc_adapter_25438_20180322-093445[1]' -M 1500 -R 'select[mem>1500] rusage[mem=1500]' -n 2,6 -R 'span[hosts=1]' -R 'rusage[seq_irods=15]' -o /tmp/BAM_basecalls_20180321-075511/no_cal/archive/qc/log/qc_adapter_25438_20180322-093445.%J.%I.out -E 'npg_pipeline_preexec_references'); 
  is ($j->params, $params, 'params');

  delete $conf->{'job_name_prefix'};
  delete $conf->{'array_cpu_limit'};
  $j = npg_pipeline::executor::lsf::job->new(
                  upstream_job_ids => [],
                  definitions      => [$d],
                  lsf_conf         => $conf,
                  fs_resource      => undef,
                  job_priority     => 80);
  is_deeply ($j->commands(), {1 => $command}, 'commands');
  $params = q(-sp 80 -q srpipeline -J 'qc_adapter_25438_20180322-093445[1]' -M 1500 -R 'select[mem>1500] rusage[mem=1500]' -n 2,6 -R 'span[hosts=1]' -R 'rusage[seq_irods=15]' -o /tmp/BAM_basecalls_20180321-075511/no_cal/archive/qc/log/qc_adapter_25438_20180322-093445.%J.%I.out -E 'npg_pipeline_preexec_references');
  is ($j->params, $params, 'params'); 

  $json = '{
         "__CLASS__" : "npg_pipeline::function::definition",
         "apply_array_cpu_limit" : "1",
         "command" : "qc --check=adapter --id_run=25438 --position=1 --tag_index=0 --file_type=bam --qc_in=/tmp/BAM_basecalls_20180321-075511/no_cal/lane1 --qc_out=/tmp/BAM_basecalls_20180321-075511/no_cal/archive/lane1/qc",
         "command_preexec" : "npg_pipeline_preexec_references",
         "composition" : {
            "__CLASS__" : "npg_tracking::glossary::composition",
            "components" : [
               {
                  "__CLASS__" : "npg_tracking::glossary::composition::component::illumina",
                  "id_run" : 25438,
                  "position" : 1,
                  "tag_index" : 0
               }
            ]
         },
         "created_by" : "npg_pipeline::function::autoqc",
         "created_on" : "20180322-093445",
         "fs_slots_num" : 1,
         "reserve_irods_slots" : 1,
         "identifier" : 25438,
         "job_name" : "qc_adapter_25438_20180322-093445",
         "log_file_dir" : "/tmp/BAM_basecalls_20180321-075511/no_cal/archive/qc/log",
         "memory" : 1500,
         "num_cpus" : [
            "2", "6"
         ],
         "num_hosts" : 1,
         "queue" : "default"
          }';

  my $d1 = npg_pipeline::function::definition->thaw($json);

  $j = npg_pipeline::executor::lsf::job->new(
                  upstream_job_ids => [],
                  definitions      => [$d, $d1],
                  lsf_conf         => $conf,
                  fs_resource      => undef);

  is_deeply ($j->commands(),
    { '1'     => 'qc --check=adapter --id_run=25438 --position=1 --file_type=bam --qc_in=/tmp/BAM_basecalls_20180321-075511/no_cal --qc_out=/tmp/BAM_basecalls_20180321-075511/no_cal/archive/qc',
      '10000' => 'qc --check=adapter --id_run=25438 --position=1 --tag_index=0 --file_type=bam --qc_in=/tmp/BAM_basecalls_20180321-075511/no_cal/lane1 --qc_out=/tmp/BAM_basecalls_20180321-075511/no_cal/archive/lane1/qc'},
    'commands');
  $params = q(-q srpipeline -J 'qc_adapter_25438_20180322-093445[1,10000]' -M 1500 -R 'select[mem>1500] rusage[mem=1500]' -n 2,6 -R 'span[hosts=1]' -R 'rusage[seq_irods=15]' -o /tmp/BAM_basecalls_20180321-075511/no_cal/archive/qc/log/qc_adapter_25438_20180322-093445.%J.%I.out -E 'npg_pipeline_preexec_references');
  is ($j->params, $params, 'params');
};

1;
