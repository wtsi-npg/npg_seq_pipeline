use strict;
use warnings;
use Test::More tests => 9;

#LSB_MCPU_HOSTS\n$LSB_BIND_CPU_LIST
{
  is(`bin/npg_pipeline_job_env_to_threads`, '1', 'default to 1 if no suitable LSB_ environment variables set');
  is(`bin/npg_pipeline_job_env_to_threads --num_threads 7`, '7',
    'no suitable LSB_ environment variables set and value available on command line');
  is(`bin/npg_pipeline_job_env_to_threads --num_threads 8 --maximum 3`, '3',
    'no suitable LSB_ environment variables set and value available on command line');
  $ENV{'LSB_MCPU_HOSTS'}='bc-19-3-16 3';
  is(`bin/npg_pipeline_job_env_to_threads`, '3', '3 from LSB_MCPU_HOSTS');
  $ENV{'LSB_BIND_CPU_LIST'}='3,4,5,15,16,17';
  is(`bin/npg_pipeline_job_env_to_threads`, '6', '6 from LSB_BIND_CPU_LIST');
  is(`bin/npg_pipeline_job_env_to_threads --maximum 3`, '3', '6 from LSB_BIND_CPU_LIST, with maximum 3');
  is(`bin/npg_pipeline_job_env_to_threads --exclude 2`, '4', '6 from LSB_BIND_CPU_LIST, with exclude 2');
  is(`bin/npg_pipeline_job_env_to_threads --exclude 1 --divide 2`, '2', '6 from LSB_BIND_CPU_LIST, with exclude 1 and divide 2');
  is(`bin/npg_pipeline_job_env_to_threads --maximum 3 --num_threads 1`, '3',
    '6 from LSB_BIND_CPU_LIST, with maximum 3, command line option disregarded');
}
1;
