use strict;
use warnings;
use Test::More tests => 12;
use Test::Exception;
use Cwd;
use File::Path qw{ make_path };
use Log::Log4perl qw{ :levels };
use English qw{ -no_match_vars };

use t::util;
use t::dbic_util;
use npg_tracking::util::abs_path qw(abs_path);

my $util = t::util->new();
my $temp_directory = $util->temp_directory();

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $temp_directory, 'logfile'),
                          utf8   => 1});

my $package = 'npg_pipeline::daemon::analysis';

use_ok($package);

my $script = join q[/],  $temp_directory, 'npg_pipeline_central';
`touch $script`;
`chmod +x $script`;
my $current_dir = abs_path(getcwd());
local $ENV{PATH} = join q[:], $temp_directory, $current_dir.'/t/bin', $ENV{PATH};

my $dbic_util = t::dbic_util->new();
my $schema = $dbic_util->test_schema();
my $test_run = $schema->resultset(q[Run])->find(1234);
$test_run->update_run_status('analysis pending', 'pipeline',);

my %h = map { $_ => 1 } (1234, 4330, 4999, 5222);
my @statuses = ();
for my $r ($schema->resultset(q[Run])->search({})->all()) {
  my $id = $r->id_run;
  if ($h{$id}) {
    cmp_ok ($r->current_run_status_description, 'eq',
      'analysis pending', "test run $id is analysis pending");
  } else {
    push @statuses, $r->current_run_status_description;
  }
}
is (scalar(grep { $_ eq 'analysis pending' } @statuses), 0,
  'other test runs are not analysis pending');

my $rf_path = '/some/path';

########test class definition start########

package test_analysis_runner;
use Moose;
extends 'npg_pipeline::daemon::analysis';
sub runfolder_path4run { return '/some/path' };

########test class definition end########

########test class definition start########

package test_analysis_anotherrunner;
use Moose;
use Carp;
extends 'npg_pipeline::daemon::analysis';
sub _get_batch_id{ croak 'No LIMs link'; }
sub runfolder_path4run { return '/some/path'; }

########test class definition end########

package main;

subtest 'failure to retrive lims data' => sub {
  plan tests => 4;

  my $runner = test_analysis_anotherrunner->new(
    npg_tracking_schema  => $schema,
  );
  is($runner->pipeline_script_name, 'npg_pipeline_central', 'script name');
  ok(!$runner->dry_run, 'dry_run mode switched off by default');
  lives_ok { $runner->run(); } 'one run is processed';
  is(scalar keys %{$runner->seen}, 0,
    'no lims link - run is not listed as seen');
};

subtest 'retrieve lims data' => sub {
  plan tests => 2;

  my $runner = $package->new(npg_tracking_schema => $schema);

  $test_run->update({'batch_id' => 0});
  throws_ok {$runner->_get_batch_id($test_run)}
    qr/No batch id/, 'no batch id - error';

  $test_run->update({'batch_id' => 567891234});
  is ($runner->_get_batch_id($test_run), '567891234', 'batch id');
};

subtest 'generate command' => sub {
  plan tests => 1;

  $test_run->update({'batch_id' => 55});
  my $runner  = $package->new(
               pipeline_script_name => '/bin/true',
               npg_tracking_schema  => $schema,
  );
  my $data = {batch_id => $runner->_get_batch_id($test_run)};
  $data->{'job_priority'} = 4;
  $data->{'rf_path'} = 't';
  my $original_path = $ENV{'PATH'};
  my $perl_bin = abs_path($EXECUTABLE_NAME);
  $perl_bin =~ s/\/perl\Z//smx;
  my $path = join q[:], "${current_dir}/t", $perl_bin, $original_path;
  my $command = q[/bin/true --verbose --job_priority 4 --runfolder_path t --id_flowcell_lims 55];
  is($runner->_generate_command($data),
    qq[export PATH=${path}; $command], 'command');
};

subtest 'mock continious running' => sub {
  plan tests => 5;

  my $runner = test_analysis_runner->new(
    pipeline_script_name => '/bin/true',
    npg_tracking_schema  => $schema,
  );
  
  lives_ok {
    $runner->run();
    sleep 1;
    $runner->run();
  } q{no croak running through twice - potentially as a daemon process};

  is (join(q[ ],sort keys %{$runner->seen}), '1234 4330 4999 5222',
    'correct list of seen runs');

  $runner = test_analysis_runner->new(
    pipeline_script_name => '/bin/true',
    npg_tracking_schema  => $schema,
    dry_run              => 1
  );
  ok($runner->dry_run, 'dry_run mode switched on');
  lives_ok {
    $runner->run();
    sleep 1;
    $runner->run();
  } q{no croak running (dry) through twice};

  is (join(q[ ],sort keys %{$runner->seen}), '1234 4330 4999 5222',
    'correct list of seen runs');
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

  my $runner = $package->new(npg_tracking_schema => $schema);
  is( $runner->runfolder_path4run(1234), $rf, 'runfolder path is correct');
};

subtest 'no_auto* tags' => sub {
  plan tests => 3;

  my $test_run;
  $test_run = $schema->resultset(q[Run])->find(1234);
  $test_run->update_run_status('analysis pending', 'pipeline',);
  $test_run = $schema->resultset(q[Run])->find(1235);
  ok ($test_run->is_tag_set('no_auto_analysis'), 'tag set');
  $test_run->update_run_status('analysis pending', 'pipeline',);
  $test_run = $schema->resultset(q[Run])->find(1236);
  ok ($test_run->is_tag_set('no_auto'), 'tag set');
  $test_run->update_run_status('analysis pending', 'pipeline',);

  my $runner = test_analysis_runner->new(
    pipeline_script_name => '/bin/true',
    npg_tracking_schema  => $schema,
  );

  $runner->run();

  is (join(q[ ],sort keys %{$runner->seen}), '1234 4330 4999 5222',
    'runs with no_auto and no_auto_analysis not seen');
};

1;
