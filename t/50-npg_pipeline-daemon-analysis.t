use strict;
use warnings;
use Test::More tests => 9;
use Test::Exception;
use Cwd;
use File::Path qw{ make_path };
use List::MoreUtils qw{ any };
use Log::Log4perl qw{ :levels };
use English qw{ -no_match_vars };

use t::util;
use t::dbic_util;
use npg_tracking::util::abs_path qw(abs_path);

$ENV{'http_proxy'} = 'http://wibble.com';
my $util = t::util->new();
my $temp_directory = $util->temp_directory();

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $temp_directory, 'logfile'),
                          utf8   => 1});

my $package = 'npg_pipeline::daemon::analysis';
my $script_name = q[npg_pipeline_central];

use_ok($package);

my $script = join q[/],  $temp_directory, $script_name;
`touch $script`;
`chmod +x $script`;
my $current_dir = abs_path(getcwd());
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
extends 'npg_pipeline::daemon::analysis';
sub runfolder_path4run { return '/some/path' };

########test class definition end########

package main;

subtest 'staging host matching' => sub {
  plan tests => 24;

  my $path49 = '/{export,nfs}/sf49/ILorHSany_sf49/*/';
  my $path32 = '/{export,nfs}/sf32/ILorHSany_sf32/*/';
  my $runner;
  lives_ok { $runner = test_analysis_runner->new(
      npg_tracking_schema => $schema,
      iseq_flowcell       => $wh_schema->resultset('IseqFlowcell')
  )} q{object creation ok};
  isa_ok($runner, q{test_analysis_runner}, q{$runner});
  is($runner->pipeline_script_name, $script_name, 'script name');
  ok(!$runner->dry_run, 'dry_run mode switched off by default');

  my $command_start = 'npg_pipeline_central --verbose --job_priority 50 --runfolder_path';

  throws_ok { $runner->_generate_command( {
                     rf_path      => $rf_path,
                     job_priority => 50,
  }) } qr/Lims flowcell id is missing/, 'lims flowcell id is missing - error';

  like($runner->_generate_command( {
    rf_path      => $rf_path,
    job_priority => 50,
    id           => 1480,
  } ), qr/$command_start $rf_path/, q{generated command is correct});

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
  )} q{object creation ok};
  like($runner->_generate_command( {
    rf_path      => $rf_path,
    job_priority => 50,
    id           => 56,
  }), qr/$command_start $rf_path --id_flowcell_lims 56/,
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
};

########test class definition start########

package test_analysis_anotherrunner;
use Moose;
use Carp;
extends 'npg_pipeline::daemon::analysis';
sub check_lims_link{ croak 'No LIMs link'; }
sub runfolder_path4run { return '/some/path'; }

########test class definition end########

package main;

subtest 'failure to retrive lims data' => sub {
  plan tests => 2;

  local $ENV{PATH} = join q[:], $current_dir.'/t/bin/red', $ENV{PATH};
  my $runner = test_analysis_anotherrunner->new(
    pipeline_script_name => '/bin/true',
    npg_tracking_schema  => $schema,
    iseq_flowcell        => $wh_schema->resultset('IseqFlowcell')
  );

  lives_ok { $runner->run(); } 'one run is processed';
  is(scalar keys %{$runner->seen}, 0,
    'no lims link - run is not listed as seen');
};

subtest 'retrieve lims data' => sub {
  plan tests => 19;

  my $runner;
  lives_ok { $runner = $package->new(
               npg_tracking_schema => $schema,
               mlwh_schema         => $wh_schema,
  )} 'object created';

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
  is_deeply($lims_data->{'studies'}, [], 'studies not retrieved');

  $test_run->update({'batch_id' => 55});
  is ($wh_schema->resultset('IseqFlowcell')->search({'flowcell_barcode' => $fc})->count,
    0, 'test prereq. - no rows with this barcode in the lims table');
  throws_ok {$runner->check_lims_link($test_run)}
    qr/Not QC run and not in the ml warehouse/,
    'non-qc run not in mlwh - error';

  $test_run->update({'batch_id' => undef});
  my $rs = $wh_schema->resultset('IseqFlowcell')->search();
  $rs->next;
  my $fc_row = $rs->next;
  $fc_row->update({'flowcell_barcode' => $fc});
  is ($wh_schema->resultset('IseqFlowcell')->search({'flowcell_barcode' => $fc})->count,
    1, 'test prereq. - one row with this barcode in the lims table');

  $fc_row->update({'id_lims' => 'SSCAPE'});
  $lims_data = $runner->check_lims_link($test_run);
  is ($lims_data->{'id'}, undef, 'lims id is undefined');
  is ($lims_data->{'qc_run'}, undef, 'qc run flag is not set');
  is(join(q[:], @{$lims_data->{'studies'}}), '2967', 'studies retrieved');

  $test_run->update({'batch_id' => 55});
  $fc_row->update({'id_flowcell_lims' => 55});
  $lims_data = $runner->check_lims_link($test_run);
  is ($lims_data->{'id'}, 55, 'lims id is set');
  is ($lims_data->{'qc_run'}, undef, 'qc run flag is not set');
  is(join(q[:], @{$lims_data->{'studies'}}), '2967', 'studies retrieved');

  $fc_row->update({'id_lims' => 'SSCAPE'});
  $lims_data = $runner->check_lims_link($test_run);
  is ($lims_data->{'id'}, 55, 'lims id is set');
  is ($lims_data->{'qc_run'}, undef, 'qc run flag is not set');

  $fc_row->update({'id_lims' => 'SSCAPE'});
  $fc_row->update({'purpose' => 'qc'});
  $lims_data = $runner->check_lims_link($test_run);
  is ($lims_data->{'id'}, 55, 'lims id is set');
  is ($lims_data->{'qc_run'}, 1, 'qc run flag is set');
};

subtest 'generate command' => sub {
  plan tests => 1;

  my $runner  = $package->new(
               pipeline_script_name => '/bin/true',
               npg_tracking_schema  => $schema,
               mlwh_schema          => $wh_schema,
  );
  my $lims_data = $runner->check_lims_link($test_run);
  $lims_data->{'job_priority'} = 4;
  $lims_data->{'rf_path'} = 't';
  my $original_path = $ENV{'PATH'};
  my $perl_bin = abs_path($EXECUTABLE_NAME);
  $perl_bin =~ s/\/perl\Z//smx;
  my $path = join q[:], "${current_dir}/t", $perl_bin, $original_path;
  my $command = q[/bin/true --verbose --job_priority 4 --runfolder_path t --qc_run --id_flowcell_lims 55];
  is($runner->_generate_command($lims_data),
    qq[export PATH=${path}; $command], 'command');
};

subtest 'mock continious running' => sub {
  plan tests => 6;

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
};

subtest 'compute runfolder path' => sub {
  plan tests => 1;

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
  is( $runner->runfolder_path4run(1234), $rf, 'runfolder path is correct');
};

1;
