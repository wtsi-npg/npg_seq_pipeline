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
  plan tests => 30;

  my $ss = npg_pipeline::function::start_stop->new(
    id_run         => 1234,
    runfolder_path => $runfolder_path,
  );
  isa_ok ($ss, 'npg_pipeline::function::start_stop');
  is ($ss->label, '1234', 'label');

  my $ss1 = npg_pipeline::function::start_stop->new(
    id_run         => 1234,
    runfolder_path => $runfolder_path,
    label          => 'my_label',
  );

  my $ss2 = npg_pipeline::function::start_stop->new(
    runfolder_path   => $runfolder_path,
    product_rpt_list => '123:4:5;124:3:6',
    label            => 'your_label',
  );
  
  foreach my $m (qw/pipeline_start pipeline_end/) {

    my $ds = $ss->$m('pipeline_name');
    ok ($ds && scalar @{$ds} == 1 && !$ds->[0]->excluded, "$m is enabled");
    my $d = $ds->[0];
    isa_ok ($d, 'npg_pipeline::function::definition');
    is ($d->identifier, '1234', 'identifier set to run id');
    is ($d->created_by, 'npg_pipeline::function::start_stop', 'created_by');
    is ($d->command, '/bin/true', 'command');
    is ($d->job_name, $m . '_1234_pipeline_name', "job name for $m");
    is ($d->queue, 'small', 'small queue');
    ok ($d->has_num_cpus, 'number of cpus is set');
    is_deeply ($d->num_cpus, [0], 'zero cpus');

    $ds = $ss1->$m('pipeline_name');
    ok ($ds && scalar @{$ds} == 1 && !$ds->[0]->excluded, "$m is enabled");
    $d = $ds->[0];
    is ($d->identifier, 'my_label', 'identifier set to label');
    is ($d->job_name, $m . '_my_label_pipeline_name', "job name for $m includes the label");

    $ds = $ss2->$m('pipeline_name');
    $d = $ds->[0];
    is ($d->identifier, 'your_label', 'identifier set to label');
    is ($d->job_name, $m . '_your_label_pipeline_name', "job name for $m includes the label");
  }
};

subtest 'wait4path function' => sub {
  plan tests => 17;

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

  my $command =
      qq{ COUNTER=0; NUM_ITERATIONS=20; DIR=$path; STIME=60; }
    .  q{while [ $COUNTER -lt $NUM_ITERATIONS ] && ! [ -d $DIR ] ; }
    .  q{do echo $DIR not available; COUNTER=$(($COUNTER+1)); sleep $STIME; done; }
    .  q{EXIT_CODE=0; if [ $COUNTER == $NUM_ITERATIONS ] ; then EXIT_CODE=1; fi; exit $EXIT_CODE;}
    .  q{'};
  my @command_components = split q[;], $d->command;
  my $start = shift @command_components;
  my $start_re = qr/bash -c 'echo \d+/;
  like ($start, $start_re, 'first part of the command is correct');
  is (join(q[;], @command_components), $command,
    'second part of the command is correct');

  $f = npg_pipeline::function::start_stop->new(
    label          => 'my_label',
    runfolder_path => $path,
  );
  $ds = $f->pipeline_wait4path();
  $d = $ds->[0];
  @command_components = split q[;], $d->command;
  $start = shift @command_components;
  like ($start, $start_re, 'first part of the command is correct');
  is (join(q[;], @command_components), $command,
    'second part of the command is correct');
  is ($d->command_preexec, qq{[ -d '$path' ]}, 'preexec command');
  is ($d->job_name, 'wait4path_in_outgoing_my_label', 'job name');
  is ($d->identifier, 'my_label', 'identifier set to the value of label');
};

1;
