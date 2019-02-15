use strict;
use warnings;
use Test::More tests => 3;
use Test::Exception;
use File::Copy;
use File::Basename;

use t::util;

my $util = t::util->new();
my $runfolder_path = $util->analysis_runfolder_path();

use_ok('npg_pipeline::function::start_stop');

subtest 'start and stop functions' => sub {
  plan tests => 19;

  my $ss = npg_pipeline::function::start_stop->new(
    id_run         => 1234,
    runfolder_path => $runfolder_path,
  );
  isa_ok ($ss, 'npg_pipeline::function::start_stop');
  
  foreach my $m (qw/pipeline_start pipeline_end/) {
    my $ds = $ss->$m('pname');
    ok ($ds && scalar @{$ds} == 1 && !$ds->[0]->excluded, "$m is enabled");
    my $d = $ds->[0];
    isa_ok ($d, 'npg_pipeline::function::definition');
    is ($d->identifier, '1234', 'identifier set to run id');
    is ($d->created_by, 'npg_pipeline::function::start_stop', 'created_by');
    is ($d->command, '/bin/true', 'command');
    is ($d->job_name, $m . '_1234_pname', "job name for $m");
    is ($d->queue, 'small', 'small queue');
    ok ($d->has_num_cpus, 'number of cpus is set');
    is_deeply ($d->num_cpus, [0], 'zero cpus');
  }
};

subtest 'wait4path function' => sub {
  plan tests => 13;

  my $f = npg_pipeline::function::start_stop->new(
    id_run         => 1234,
    runfolder_path => $runfolder_path,
  );

  my $path = $runfolder_path;
  $path =~ s/analysis/outgoing/;
  ok ($path =~ /outgoing/, 'future path is in outgoing'); 

  my $ds = $f->pipeline_wait4path();
  ok ($ds && scalar @{$ds} == 1, 'one definition is created');
  my $d = $ds->[0];
  ok (!$d->excluded, 'function is enabled');
  is ($d->identifier, '1234', 'identifier set to run id');
  is ($d->created_by, 'npg_pipeline::function::start_stop', 'created_by');
  is ($d->job_name, 'wait4path_in_outgoing_1234', 'job name');
  is ($d->queue, 'small', 'small queue');
  ok ($d->has_num_cpus, 'number of cpus is set');
  is ($d->command_preexec, qq{[ -d '$path' ]}, 'preexec command');
  is_deeply ($d->num_cpus, [0], 'zero cpus');
  my $command = q{bash -c '}
    . qq{COUNTER=0; NUM_ITERATIONS=20; DIR=$path; STIME=60; }
    .  q{while [ $COUNTER -lt $NUM_ITERATIONS ] && ! [ -d $DIR ] ; }
    .  q{do echo $DIR not available; COUNTER=$(($COUNTER+1)); sleep $STIME; done; }
    .  q{EXIT_CODE=0; if [ $COUNTER == $NUM_ITERATIONS ] ; then EXIT_CODE=1; fi; exit $EXIT_CODE;}
    .  q{'};
  is ($d->command, $command, 'command is correct');

  $f = npg_pipeline::function::start_stop->new(
    id_run         => 1234,
    runfolder_path => $path,
  );
  $ds = $f->pipeline_wait4path();
  is ($ds->[0]->command, $command, 'command is correct');
  is ($d->command_preexec, qq{[ -d '$path' ]}, 'preexec command');
};

1;
