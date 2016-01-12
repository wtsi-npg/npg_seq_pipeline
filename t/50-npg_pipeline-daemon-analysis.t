use strict;
use warnings;
use Test::More tests => 59;
use Test::Exception;
use Cwd;
use File::Path qw/make_path/;
use List::MoreUtils qw{any};
use Log::Log4perl qw(:easy);

use t::util;
use t::dbic_util;

$ENV{'http_proxy'} = 'http://wibble.com'.

Log::Log4perl->easy_init($INFO);

my $package = 'npg_pipeline::daemons::analysis';

use_ok($package);

my $script_name = q[npg_pipeline_central];
is ($package->pipeline_script_name(),
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
my $wh_schema = $dbic_util->test_schema_mlwh('t/data/fixtures/mlwh');
my $test_run = $schema->resultset(q[Run])->find(1234);
$test_run->update_run_status('analysis pending', 'pipeline',);
is($test_run->current_run_status_description,
  'analysis pending', 'test run is analysis pending');

my $folder_path_glob = $test_run->folder_path_glob();
like($folder_path_glob, qr/\/sf33\//, 'folder path glob correct for a test run');

my $rf_path = '/some/path';

########test class definition start########

package test_analysis_runner;
use Moose;
extends 'npg_pipeline::daemons::analysis';
sub _runfolder_path { return '/some/path' };

########test class definition end########

package main;

{
  my $path49 = '/{export,nfs}/sf49/ILorHSany_sf49/*/';
  my $path32 = '/{export,nfs}/sf32/ILorHSany_sf32/*/';
  my $runner;
  lives_ok { $runner = test_analysis_runner->new(
      npg_tracking_schema => $schema,
      iseq_flowcell       => $wh_schema->resultset('IseqFlowcell')
  ) } q{object creation ok};
  isa_ok($runner, q{test_analysis_runner}, q{$runner});
  ok(!$runner->dry_run, 'dry_run mode switched off by default');

  my $command_start = 'npg_pipeline_central --verbose --job_priority 50 --runfolder_path';

  throws_ok { $runner->_generate_command( {
                     rf_path      => $rf_path,
                     job_priority => 50,
                                       } )
            } qr/Lims flowcell id is missing/,
    'non-gclp run and lims flowcell id is missing - error';

  like($runner->_generate_command( {
    rf_path      => $rf_path,
    job_priority => 50,
    id           => 1480,
  } ), qr/$command_start $rf_path/,
    q{generated command is correct});

  like($runner->_generate_command( {
    rf_path      => $rf_path,
    job_priority => 50,
    gclp         => 1,
  } ), qr/$command_start $rf_path --function_list gclp/,
    q{generated command is correct});

  like($runner->_generate_command( {
    rf_path      => $rf_path,
    job_priority => 50,
    gclp         => 1,
    id           => 22,
  } ), qr/$command_start $rf_path --function_list gclp/,
    q{generated command is correct});

  ok($runner->green_host,'running on a host in a green datacentre');
  ok($runner->staging_host_match($path49), 'staging matches host');
  ok(!$runner->staging_host_match($path32), 'staging does not match host');
  throws_ok {$runner->staging_host_match()}
    qr/Need folder_path_glob to decide whether the run folder and the daemon host are co-located/,
    'error if folder_path_glob is not defined';
  ok(!$runner->staging_host_match($folder_path_glob),
    'staging does not match host for a test run');
  
  $schema->resultset(q[Run])->find(2)->update_run_status('analysis pending', 'pipeline',);
  $schema->resultset(q[Run])->find(3)->update_run_status('analysis pending', 'pipeline',);
  my @test_runs = ();
  lives_ok { @test_runs = $runner->runs_with_status('analysis pending') }
    'can get runs with analysys pending status';
  ok(scalar @test_runs >= 3, 'at least three runs are analysis pending');
  foreach my $id (qw/2 3 1234/) {
    ok((any {$_->id_run == $id} @test_runs),
      "run $id correctly identified as analysis pending");
  }

  $runner = test_analysis_runner->new(
      pipeline_script_name => '/bin/true',
      npg_tracking_schema  => $schema,
      iseq_flowcell        => $wh_schema->resultset('IseqFlowcell')
  );
  lives_ok { $runner->run(); } q{no croak on $runner->run()};

  local $ENV{PATH} = join q[:], $current_dir.'/t/bin/red', $ENV{PATH};
  lives_ok { $runner = test_analysis_runner->new(
      npg_tracking_schema => $schema,
      iseq_flowcell       => $wh_schema->resultset('IseqFlowcell')
  ) } q{object creation ok};
  like($runner->_generate_command( {
    rf_path      => $rf_path,
    job_priority => 50,
    id           => 56,
  } ), qr/$command_start $rf_path --id_flowcell_lims 56/,
    q{generated command is correct});
  ok(!$runner->green_host, 'host is not in green datacentre');
  ok(!$runner->staging_host_match($path49), 'staging does not match host');
  ok($runner->staging_host_match($path32), 'staging matches host');
  ok($runner->staging_host_match($folder_path_glob),
    'staging matches host for a test run');

  local $ENV{PATH} = join q[:], $current_dir.'/t/bin/dodo', $ENV{PATH};
  $runner = test_analysis_runner->new(
      npg_tracking_schema => $schema,
      iseq_flowcell       => $wh_schema->resultset('IseqFlowcell')
  );
  ok(!$runner->green_host, 'host is not in green datacentre');
}

########test class definition start########

package test_analysis_anotherrunner;
use Moose;
use Carp;
extends 'npg_pipeline::daemons::analysis';
sub check_lims_link{ croak 'No LIMs link'; }
sub _runfolder_path { return '/some/path'; }

########test class definition end########

package main;
{
  local $ENV{PATH} = join q[:], $current_dir.'/t/bin/red', $ENV{PATH};
  my $runner = test_analysis_anotherrunner->new(
    pipeline_script_name => '/bin/true',
    npg_tracking_schema  => $schema,
    iseq_flowcell        => $wh_schema->resultset('IseqFlowcell')
  );

  lives_ok { $runner->run(); } 'one run is processed';
  is(scalar keys %{$runner->seen}, 0,
    'no lims link - run is not listed as seen');
}

{
  my $runner;
  lives_ok { $runner = $package->new(
               npg_tracking_schema => $schema,
               iseq_flowcell       => $wh_schema->resultset('IseqFlowcell')
             );
  } 'object created';

  $test_run->update({'flowcell_id' => undef});
  throws_ok {$runner->check_lims_link($test_run)}
    qr/No flowcell barcode/,
    'no barcode in tracking db - error';

  my $fc = 'dummy_flowcell1';
 
  $test_run->update({'flowcell_id' => $fc});
  $test_run->update({'batch_id' => 0});
  throws_ok {$runner->check_lims_link($test_run)}
    qr/No matching flowcell LIMs record is found/,
    'no batch id and no mlwh record - error';

  $test_run->update({'batch_id' => 1234567891234});
  my $lims_data = $runner->check_lims_link($test_run);
  is ($lims_data->{'id'}, '1234567891234', 'lims id');
  is ($lims_data->{'qc_run'}, 1, 'is qc run');
  ok(!$lims_data->{'gclp'}, 'gclp flag is false');

  $test_run->update({'batch_id' => 55});
  is ($wh_schema->resultset('IseqFlowcell')->search({'flowcell_barcode' => $fc})->count,
    0, 'test prereq. - no rows with this barcode in the lims table');
  throws_ok {$runner->check_lims_link($test_run)}
    qr/Not QC run and not in the ml warehouse/,
    'non-qc run not in mlwh - error';

  $test_run->update({'batch_id' => undef});
  my $fc_row = $wh_schema->resultset('IseqFlowcell')->search()->next;
  $fc_row->update({'flowcell_barcode' => $fc});
  is ($wh_schema->resultset('IseqFlowcell')->search({'flowcell_barcode' => $fc})->count,
    1, 'test prereq. - one row with this barcode in the lims table');

  $fc_row->update({'id_lims' => 'SSCAPE'});
  $lims_data = $runner->check_lims_link($test_run);
  is ($lims_data->{'id'}, undef, 'lims id is undefined');
  ok(!$lims_data->{'gclp'}, 'gclp flag is false');
  is ($lims_data->{'qc_run'}, undef, 'qc run flag is not set');

  $fc_row->update({'id_lims' => 'C_GCLP'});
  $lims_data = $runner->check_lims_link($test_run);
  is ($lims_data->{'id'}, undef, 'lims id is undefined');
  is ($lims_data->{'gclp'}, 1, 'gclp flag is set to true');
  is ($lims_data->{'qc_run'}, undef, 'qc run flag is not set');

  $test_run->update({'batch_id' => 55});
  $fc_row->update({'id_flowcell_lims' => 55});
  $lims_data = $runner->check_lims_link($test_run);
  is ($lims_data->{'id'}, 55, 'lims id is set');
  is ($lims_data->{'gclp'}, 1, 'gclp flag is set to true');
  is ($lims_data->{'qc_run'}, undef, 'qc run flag is not set');

  $fc_row->update({'id_lims' => 'SSCAPE'});
  $lims_data = $runner->check_lims_link($test_run);
  is ($lims_data->{'id'}, 55, 'lims id is set');
  ok (!$lims_data->{'gclp'}, 'gclp flag is false');
  is ($lims_data->{'qc_run'}, undef, 'qc run flag is not set');
}

{
  local $ENV{PATH} = join q[:], $current_dir.'/t/bin/red', $ENV{PATH};
  my $runner = test_analysis_runner->new(
    pipeline_script_name => '/bin/true',
    npg_tracking_schema  => $schema,
    iseq_flowcell        => $wh_schema->resultset('IseqFlowcell')
  );
  ok($runner->staging_host_match($folder_path_glob),
    'staging matches host for a test run');
  
  lives_ok {
    $runner->run();
    sleep 1;
    $runner->run();
  } q{no croak running through twice - potentially as a daemon process};

  is (join(q[ ],sort keys %{$runner->seen}), '1234', 'correct list of seen runs');

  $runner = test_analysis_runner->new(
    pipeline_script_name => '/bin/true',
    npg_tracking_schema  => $schema,
    iseq_flowcell        => $wh_schema->resultset('IseqFlowcell'),
    dry_run              => 1
  );
  ok($runner->dry_run, 'dry_run mode switched on');
  lives_ok {
    $runner->run();
    sleep 1;
    $runner->run();
  } q{no croak running (dry) through twice};

  is (join(q[ ],sort keys %{$runner->seen}), '1234', 'correct list of seen runs');
}

{
  my $temp = t::util->new()->temp_directory();
  my $name = '150227_HS35_1234_A_HBFJ3ADXX';
  my $rf = join q[/], $temp, 'sf33/ILorHSany_sf33/outgoing', $name;
  make_path $rf;
  
  my $row = $schema->resultset('Run')->find(1234);
  $row->set_tag('pipeline','staging');
  $row->update(
    {folder_path_glob => $temp . q[/sf33/ILorHSany_sf33/*/], folder_name => $name});

  my $runner = $package->new(
               npg_tracking_schema => $schema,
               iseq_flowcell       => $wh_schema->resultset('IseqFlowcell')
             );
  is( $runner->_runfolder_path(1234), $rf, 'runfolder path is correct');
}

1;
