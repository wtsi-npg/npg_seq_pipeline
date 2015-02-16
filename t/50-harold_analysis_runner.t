use strict;
use warnings;
use Test::More tests => 46;
use Test::Exception;
use Cwd;
use List::MoreUtils qw{any};
use t::util;
use t::dbic_util;

use_ok('npg_pipeline::daemons::harold_analysis_runner');

my $script_name = q[npg_pipeline_PB_cal_bam];
is (npg_pipeline::daemons::harold_analysis_runner->pipeline_script_name(),
            $script_name, 'pipeline script name correct'); 

my $util = t::util->new();
my $temp_directory = $util->temp_directory();
my $script = join q[/],  $temp_directory, $script_name;
`touch $script`;
`chmod +x $script`;
my $current_dir = getcwd();
local $ENV{PATH} = join q[:], $temp_directory, $current_dir.'/t/bin', $ENV{PATH};

my $dbic_util = t::dbic_util->new();
my $schema = $dbic_util->test_schema();
my $test_run = $schema->resultset(q[Run])->find(1234);
$test_run->update_run_status('analysis pending', 'pipeline',);
is($test_run->current_run_status_description, 'analysis pending', 'test run is analysis pending');

my $folder_path_glob = $test_run->folder_path_glob();
like($folder_path_glob, qr/\/sf33\//, 'folder path glob correct for a test run');

my $rf_path = '/some/path';

########test class definition start########

package test_analysis_runner;
use Moose;
extends 'npg_pipeline::daemons::harold_analysis_runner';
sub _check_lims_link{ return 0; }
sub _runfolder_path { return '/some/path' };

########test class definition end########

package main;

{
  my $path49 = '/{export,nfs}/sf49/ILorHSany_sf49/*/';
  my $path32 = '/{export,nfs}/sf32/ILorHSany_sf32/*/';
  my $runner;
  lives_ok { $runner = test_analysis_runner->new(
      log_file_path        => $temp_directory,
      log_file_name       => q{npg_pipeline_daemon.log} ,
      npg_tracking_schema => $schema,
  ) } q{object creation ok};
  isa_ok($runner, q{test_analysis_runner}, q{$runner});
  
  like($runner->_generate_command( {
    rf_path => $rf_path,
    script => q{npg_pipeline_PB_cal},
    job_priority=> 50,
  } ), qr/npg_pipeline_PB_cal --job_priority 50 --verbose --runfolder_path $rf_path/,
    q{generated command is correct});
  ok($runner->green_host, 'running on a host in a green datacentre');
  ok($runner->staging_host_match($path49), 'staging matches host');
  ok(!$runner->staging_host_match($path32), 'staging does not match host');
  throws_ok {$runner->staging_host_match()}
    qr/Need folder_path_glob to decide whether the run folder and daemon host are co-located/,
    'error if folder_path_glob is not defined';
  ok(!$runner->staging_host_match($folder_path_glob), 'staging does not match host for a test run');
  
  $schema->resultset(q[Run])->find(2)->update_run_status('analysis pending', 'pipeline',);
  $schema->resultset(q[Run])->find(3)->update_run_status('analysis pending', 'pipeline',);
  my @test_runs = ();
  lives_ok { @test_runs = $runner->runs_with_status('analysis pending') } 'can get runs with analysys pending status';
  ok(scalar @test_runs >= 3, 'at least three runs are analysis pending');
  foreach my $id (qw/2 3 1234/) {
    ok((any {$_->id_run == $id} @test_runs), "run $id correctly identified as analysis pending");
  }


  $runner = test_analysis_runner->new(
      pipeline_script_name => '/bin/true',
      log_file_path        => $temp_directory,
      log_file_name        => q{npg_pipeline_daemon.log} ,
      npg_tracking_schema  => $schema,
  );
  lives_ok { $runner->run(); } q{no croak on $runner->run()};

  local $ENV{PATH} = join q[:], $current_dir.'/t/bin/red', $ENV{PATH};
  lives_ok { $runner = test_analysis_runner->new(
      log_file_path => $temp_directory,
      log_file_name => q{npg_pipeline_daemon.log} ,
      npg_tracking_schema => $schema,
  ) } q{object creation ok};
  like($runner->_generate_command( {
    rf_path => $rf_path,
    script => q{npg_pipeline_PB_cal},
    job_priority=> 50,
    batch_id => 56,
  } ), qr/npg_pipeline_PB_cal --job_priority 50 --verbose --runfolder_path $rf_path --id_flowcell_lims 56/,
    q{generated command is correct});
  ok(!$runner->green_host, 'host is not in green datacentre');
  ok(!$runner->staging_host_match($path49), 'staging does not match host');
  ok($runner->staging_host_match($path32), 'staging matches host');
  ok($runner->staging_host_match($folder_path_glob), 'staging matches host for a test run');

  local $ENV{PATH} = join q[:], $current_dir.'/t/bin/dodo', $ENV{PATH};
  $runner = test_analysis_runner->new(
      log_file_path => $temp_directory,
      log_file_name => q{npg_pipeline_daemon.log} ,
      npg_tracking_schema => $schema,
  );
  ok(!$runner->green_host, 'host is not in green datacentre');
}

{
  local $ENV{PATH} = join q[:], $current_dir.'/t/bin/red', $ENV{PATH};
  my $runner = test_analysis_runner->new(
    pipeline_script_name => '/bin/true',
    log_file_path        => $temp_directory,
    log_file_name        => q{npg_pipeline_daemon.log} ,
    npg_tracking_schema  => $schema,
  );
  ok($runner->staging_host_match($folder_path_glob), 'staging matches host for a test run');

  lives_ok {
    $runner->run();
    sleep 1;
    $runner->run();
  } q{no croak running through twice - potentially as a daemon process};

  is (join(q[ ],sort keys %{$runner->seen}), '1234', 'correct list of seen runs');
}

########test class definition start########

package test_analysis_anotherrunner;
use Moose;
extends 'npg_pipeline::daemons::harold_analysis_runner';
sub _check_lims_link{ return -1; }
sub _runfolder_path { return '/some/path' };

########test class definition end########

package main;
{
  local $ENV{PATH} = join q[:], $current_dir.'/t/bin/red', $ENV{PATH};
  my $runner = test_analysis_anotherrunner->new(
    pipeline_script_name => '/bin/true',
    log_file_path        => $temp_directory,
    log_file_name        => q{npg_pipeline_daemon.log} ,
    npg_tracking_schema  => $schema,
  );

  lives_ok { $runner->run(); } 'one run is processed';
  is(scalar keys %{$runner->seen}, 0, 'no lims link - run is not listed as seen');
}

{
  my $wh_schema = $dbic_util->test_schema_mlwh('t/data/fixtures/mlwh');
  my $fc_row = $wh_schema->resultset('IseqFlowcell')->search->next;

  my $runner;
  lives_ok { $runner = npg_pipeline::daemons::harold_analysis_runner->new(
               log_file_path       => $temp_directory,
               log_file_name       => q{npg_pipeline_daemon.log} ,
               npg_tracking_schema => $schema,
              _iseq_flowcell       => $wh_schema->resultset('IseqFlowcell')
             );
  } 'object created';

  my $fc = 'dummy_flowcell1';
  is ($test_run->flowcell_id, $fc, 'test prereq. - tracking flowcell id');
  is ($test_run->batch_id, 55, 'test prereq. - tracking batch id');
  is ($wh_schema->resultset('IseqFlowcell')->search('flowcell_barcode' => $fc )->count,
    0, 'test prereq. - no rows with this barcode in the lims table');

  my $message = 'initial';

  is ($runner->_check_lims_link($test_run, \$message), 55, 'batch id returned');
  is ($message, 'initial', 'no message');

  $fc_row->update({'flowcell_barcode' => $fc});

  is ($runner->_check_lims_link($test_run, \$message), 55, 'batch id is returned');
  is ($message, 'initial', 'no message');

  $test_run->update({batch_id => undef,});
  ok (!defined $test_run->batch_id, 'test prereq. - tracking batch id undefined');

  is ($runner->_check_lims_link($test_run, \$message), 0, 'no batch id - no problem');
  is ($message, 'initial', 'no message');

  $fc_row->update({flowcell_barcode => 'some value'});

  is ($runner->_check_lims_link($test_run, \$message), -1,
    'correct return value when neither batch id nor flowcell barcode can be used');
  is ($message, 'Neither batch id nor matching flowcell barcode is found', 'correct message');

  $test_run->update({flowcell_id => undef,});
  ok (!defined $test_run->batch_id, 'test prereq. - tracking flowcell id undefined');
  is ($runner->_check_lims_link($test_run, \$message), -1,
    'correct return value when tracking does not have flowcell barcode');
  is ($message, 'No flowcell barcode', 'correct message');
}

1;
