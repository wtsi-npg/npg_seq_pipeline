use strict;
use warnings;
use Test::More tests => 5;
use Test::Exception;
use Log::Log4perl qw(:levels);

use t::util;

my $util = t::util->new();
my $runfolder_path = $util->analysis_runfolder_path();
$ENV{TEST_FS_RESOURCE} = q{nfs_12};
my $temp = $util->temp_directory();

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $temp, 'logfile'),
                          utf8   => 1});

my $pqq_suffix = q[_post_qc_complete];
my @wh_methods = qw/update_warehouse update_ml_warehouse/;
@wh_methods = map {$_, $_ . $pqq_suffix} @wh_methods;

use_ok('npg_pipeline::function::collection');

subtest 'warehouse updates' => sub {
  plan tests => 41;

  my $c = npg_pipeline::function::collection->new(
    run_folder          => q{123456_IL2_1234},
    runfolder_path      => $runfolder_path,
    recalibrated_path   => $runfolder_path,
  );
  isa_ok ($c, 'npg_pipeline::function::collection');
  
  my $recalibrated_path = $c->recalibrated_path();
  my $log_dir = $c->make_log_dir($recalibrated_path);
  my $log_dir_in_outgoing = $log_dir;
  $log_dir_in_outgoing =~ s{/analysis/}{/outgoing/}smx;

  foreach my $m (@wh_methods) {

    my $postqcc  = $m =~ /$pqq_suffix/smx;
    my $ml       = $m =~ /_ml_/smx;
    my $command  = $ml ? 'npg_runs2mlwarehouse' : 'warehouse_loader';    
    my $job_name = $command . '_1234_collection';
    if ($postqcc) {
      $job_name .= '_postqccomplete';
    }  
    $command    .= ' --verbose --id_run 1234';
    if (!$ml) {
      $command  .= ' --lims_driver_type ' . ($postqcc ?
                   'ml_warehouse_fc_cache' : 'samplesheet');
    }
    my $log_directory = $postqcc ? $log_dir_in_outgoing : $log_dir;

    my $ds = $c->$m();
    ok ($ds && scalar @{$ds} == 1 && !$ds->[0]->excluded,
      'update to warehouse is enabled');
    my $d = $ds->[0];
    isa_ok ($d, 'npg_pipeline::function::definition');    

    is ($d->identifier, '1234', 'identifier set to run id');
    is ($d->created_by, 'npg_pipeline::function::collection', 'created_by');
    ok (!$d->immediate_mode, 'mode is not immediate');
    is ($d->command, $command, "command for $m");
    is ($d->log_file_dir, $log_directory, "log dir for $m");
    is ($d->job_name, $job_name, "job name for $m");
    is ($d->queue, 'small', 'small queue');
    if ($postqcc) {
      is ($d->command_preexec, "[ -d '${log_dir_in_outgoing}' ]",
        "preexec command for $m");
    } else {
      ok (!$d->has_command_preexec, "preexec command not defined for $m");
    }
  }
};

subtest 'warehouse updates disabled' => sub {
  plan tests => 12;

  my $test_method = sub {
    my ($f, $method, $switch) = @_;
    my $d = $f->$method();
    ok($d && scalar @{$d} == 1 &&
      ($switch eq 'off' ? $d->[0]->excluded : !$d->[0]->excluded),
      $method . ': update to warehouse switched ' . $switch);
  };

  foreach my $m (@wh_methods) {
    my $c = npg_pipeline::function::collection->new(
      runfolder_path      => $runfolder_path,
      recalibrated_path   => $runfolder_path,
      no_warehouse_update => 1
    );
    $test_method->($c, $m, 'off');

    $c = npg_pipeline::function::collection->new(
      runfolder_path    => $runfolder_path,
      recalibrated_path => $runfolder_path,
      local             => 1,
    );
    $test_method->($c, $m, 'off');    

    $c = npg_pipeline::function::collection->new(
      runfolder_path      => $runfolder_path,
      recalibrated_path   => $runfolder_path,
      local               => 1,
      no_warehouse_update => 0,
    );
    $test_method->($c, $m, 'on');
  }
};

subtest 'start and stop functions' => sub {
  plan tests => 18;

  my $c = npg_pipeline::function::collection->new(
    id_run         => 1234,
    runfolder_path => $runfolder_path,
  );
  my @methods = map {'pipeline_' . $_} qw/start end/;
  
  foreach my $m (@methods) {
    my $ds = $c->$m();
    ok ($ds && scalar @{$ds} == 1 && !$ds->[0]->excluded, "$m is enabled");
    my $d = $ds->[0];
    isa_ok ($d, 'npg_pipeline::function::definition');
    is ($d->identifier, '1234', 'identifier set to run id');
    is ($d->created_by, 'npg_pipeline::function::collection', 'created_by');
    ok (!$d->immediate_mode, 'mode is not immediate');
    is ($d->command, '/bin/true', 'command');
    is ($d->log_file_dir, $runfolder_path, "log dir for $m");
    is ($d->job_name, $m . '_1234_collection', "job name for $m");
    is ($d->queue, 'small', 'small queue');
  }
};

subtest ' bam2fastqcheck_and_cached_fastq' => sub {
  plan tests => 128;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q{t/data/samplesheet_1234.csv};

  my $ds = npg_pipeline::function::collection->new(
    id_run            => 1234,
    recalibrated_path => $runfolder_path,
    runfolder_path    => $runfolder_path,
    timestamp         => q{22-May},
  )->bam2fastqcheck_and_cached_fastq();
  ok ($ds && scalar @{$ds} == 8, 'eight definitions returned');

  my $count = 0;
  foreach my $d (@{$ds}) {
    $count++;
    my $command = "generate_cached_fastq --path ${runfolder_path}/archive" .
                  " --file ${runfolder_path}/1234_${count}.bam";
    isa_ok ($d, 'npg_pipeline::function::definition');
    is ($d->identifier, '1234', 'identifier set to run id');
    is ($d->created_by, 'npg_pipeline::function::collection', 'created_by');
    ok (!$d->immediate_mode, 'mode is not immediate');
    ok (!$d->excluded, 'function is not excluded');
    is ($d->command, $command, 'command');
    is ($d->log_file_dir, $runfolder_path, "log dir");
    is ($d->job_name, 'bam2fastqcheck_and_cached_fastq_1234_22-May', 'job name');
    is ($d->fs_slots_num, 1, 'one fs slot');
    ok ($d->has_composition, 'composition is set');
    is ($d->composition->num_components, 1, 'one componet in a composition');
    is ($d->composition->get_component(0)->position, $count, 'correct position');
    ok (!defined $d->composition->get_component(0)->tag_index,
      'tag index is not defined');
    ok (!defined $d->composition->get_component(0)->subset,
      'subset is not defined');
    is ($d->queue, 'default', 'default queue');  
  }

  $ds = npg_pipeline::function::collection->new(
    id_run            => 1234,
    recalibrated_path => $runfolder_path,
    runfolder_path    => $runfolder_path,
    lanes             => [2, 5]
  )->bam2fastqcheck_and_cached_fastq();
  ok ($ds && scalar @{$ds} == 2, 'two definitions returned');
  is ($ds->[0]->composition->get_component(0)->position, 2,
    'definition for position 2');
  is ($ds->[1]->composition->get_component(0)->position, 5,
    'definition for position 5');

  $ds = npg_pipeline::function::collection->new(
    id_run            => 1234,
    recalibrated_path => $runfolder_path,
    runfolder_path    => $runfolder_path,
    lanes             => [5, 8, 3]
  )->bam2fastqcheck_and_cached_fastq();
  ok ($ds && scalar @{$ds} == 3, 'two definitions returned');
  is ($ds->[0]->composition->get_component(0)->position, 3,
    'definition for position 3');
  is ($ds->[1]->composition->get_component(0)->position, 5,
    'definition for position 5');
  is ($ds->[2]->composition->get_component(0)->position, 8,
    'definition for position 8');
};
