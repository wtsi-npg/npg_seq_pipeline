# $Id: 50-harold_analysis_runner.t 17862 2013-12-04 10:26:28Z mg8 $
use strict;
use warnings;
use Test::More tests => 29;
use Test::Exception;
use Cwd;
use List::MoreUtils qw{any};
use t::util;
use t::dbic_util;

BEGIN {
  use_ok('npg_pipeline::daemons::harold_analysis_runner');
}
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

my $schema = t::dbic_util->new->test_schema();
my $test_run = $schema->resultset(q[Run])->find(1234);
$test_run->update_run_status('analysis pending', 'pipeline',);
is($test_run->current_run_status_description, 'analysis pending', 'test run is analysis pending');
$test_run->set_tag('pipeline', 'rta');
ok($test_run->is_tag_set('rta'), 'RTA tag set for the test run');

my $folder_path_glob = $test_run->folder_path_glob();
like($folder_path_glob, qr/\/sf33\//, 'folder path glob correct for a test run');

{
  my $path49 = '/{export,nfs}/sf49/ILorHSany_sf49/*/';
  my $path32 = '/{export,nfs}/sf32/ILorHSany_sf32/*/';
  my $runner;
  lives_ok { $runner = npg_pipeline::daemons::harold_analysis_runner->new(
      log_file_path => $temp_directory,
      log_file_name => q{npg_pipeline_daemon.log} ,
      npg_tracking_schema => $schema,
  ) } q{object creation ok};
  isa_ok($runner, q{npg_pipeline::daemons::harold_analysis_runner}, q{$runner});
  
  like($runner->_generate_command( {
    id_run => 1234,
    script => q{npg_pipeline_PB_cal},
    options => q{},
    job_priority=> 50,
  } ), qr/npg_pipeline_PB_cal --job_priority 50 --verbose --id_run 1234/,
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

  lives_ok { $runner->run(); } q{no croak on $runner->run()};

  local $ENV{PATH} = join q[:], $current_dir.'/t/bin/red', $ENV{PATH};
  lives_ok { $runner = npg_pipeline::daemons::harold_analysis_runner->new(
      log_file_path => $temp_directory,
      log_file_name => q{npg_pipeline_daemon.log} ,
      npg_tracking_schema => $schema,
  ) } q{object creation ok};
  like($runner->_generate_command( {
    id_run => 1234,
    script => q{npg_pipeline_PB_cal},
    options => q{},
    job_priority=> 50,
  } ), qr/npg_pipeline_PB_cal --job_priority 50 --verbose --id_run 1234/,
    q{generated command is correct});
  ok(!$runner->green_host, 'host is not in green datacentre');
  ok(!$runner->staging_host_match($path49), 'staging does not match host');
  ok($runner->staging_host_match($path32), 'staging matches host');
  ok($runner->staging_host_match($folder_path_glob), 'staging matches host for a test run');

  local $ENV{PATH} = join q[:], $current_dir.'/t/bin/dodo', $ENV{PATH};
  $runner = npg_pipeline::daemons::harold_analysis_runner->new(
      log_file_path => $temp_directory,
      log_file_name => q{npg_pipeline_daemon.log} ,
      npg_tracking_schema => $schema,
  );
  ok(!$runner->green_host, 'host is not in green datacentre');
}

{
  local $ENV{PATH} = join q[:], $current_dir.'/t/bin/red', $ENV{PATH};
  my $runner = npg_pipeline::daemons::harold_analysis_runner->new({
    log_file_path => $temp_directory,
    log_file_name => q{npg_pipeline_daemon.log} ,
    npg_tracking_schema => $schema,
  });
  ok($runner->staging_host_match($folder_path_glob), 'staging matches host for a test run');

  lives_ok {
    $runner->run();
    sleep 1;
    $runner->run();
  } q{no croak running through twice - potentially as a daemon process};

  is (join(q[ ],sort keys %{$runner->seen}), '1234', 'correct list of seen runs');
}

1;
