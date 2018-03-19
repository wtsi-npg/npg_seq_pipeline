use strict;
use warnings;
use Test::More tests => 4;
use Test::Exception;

use_ok ('npg_pipeline::function::definition');
use_ok ('npg_pipeline::executor::lsf::job');

sub _create_definition {
  return npg_pipeline::function::definition->new(
           created_by   => 'test',
           created_on   => '19 March 2018',
           job_name     => 'my_job',
           identifier   => '12345',
           command      => '/bin/true',
           log_file_dir => '/tmp'
         );
}

subtest 'object creation, default values of attributes' => sub {
  plan tests => 4;

  throws_ok {npg_pipeline::executor::lsf::job->new(definitions => [])}
    qr/Array of definitions cannot be empty/,
    'error if definitions array is empty';

  my $j = npg_pipeline::executor::lsf::job->new(definitions => [_create_definition()]);
  isa_ok ($j, 'npg_pipeline::executor::lsf::job');
  ok (!$j->is_array, 'job should not be an array');

  $j = npg_pipeline::executor::lsf::job->new(
    definitions =>[_create_definition(), _create_definition()]);
  ok ($j->is_array, 'job should be an array');
};

subtest 'delegated methods' => sub {
  plan tests => 24;

  my $j = npg_pipeline::executor::lsf::job->new(definitions => [_create_definition()]);

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

1;
