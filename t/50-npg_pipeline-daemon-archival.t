use strict;
use warnings;
use Test::More tests => 21;
use Test::Exception;
use Cwd;
use List::MoreUtils qw{any};
use Log::Log4perl qw(:levels);

use t::dbic_util;
use t::util;

my $util = t::util->new();
my $temp_directory = $util->temp_directory();

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $temp_directory, 'logfile'),
                          utf8   => 1});

my $script_name = q[npg_pipeline_post_qc_review];

use_ok('npg_pipeline::daemon::archival');

my $script = join q[/],  $temp_directory, $script_name;
`touch $script`;
`chmod +x $script`;
my $current_dir = getcwd();
local $ENV{PATH} = join q[:], $temp_directory, $current_dir.'/t/bin/red', $ENV{PATH};

my $schema = t::dbic_util->new->test_schema();
my $test_run = $schema->resultset(q[Run])->find(1234);
$test_run->update_run_status('archival pending', 'pipeline',);
is($test_run->current_run_status_description, 'archival pending', 'test run is archival pending');

########test class definition start########

package test_archival_runner;
use Moose;
extends 'npg_pipeline::daemon::archival';
sub runfolder_path4run { return '/some/path' }

########test class definition end########

package main;

{
  my $runner;
  lives_ok { $runner = test_archival_runner->new(
    npg_tracking_schema     => $schema) } q{object creation ok};
  isa_ok($runner, q{test_archival_runner});

  $runner = test_archival_runner->new(
    npg_tracking_schema     => $schema
  );
  is($runner->pipeline_script_name(), $script_name, 'pipeline script name correct'); 
  lives_ok { $runner->run(); } q{no croak on $runner->run()};
  like($runner->_generate_command(1234), qr/;\s*npg_pipeline_/,
    q{generated command is correct});
  like($runner->_generate_command(1234),
    qr/npg_pipeline_post_qc_review --verbose --runfolder_path \/some\/path/,
    q{generated command is correct});
  ok(!$runner->green_host, 'host is not in green datacentre');

  $schema->resultset(q[Run])->find(2)->update_run_status('archival pending', 'pipeline');
  $schema->resultset(q[Run])->find(3)->update_run_status('archival pending', 'pipeline');
  my @test_runs = ();
  lives_ok { @test_runs = $runner->runs_with_status('archival pending') }
    'can get runs with analysys pending status';
  ok(scalar @test_runs >= 3, 'at least three runs are archival pending');
  foreach my $id (qw/2 3 1234/) {
    ok((any {$_->id_run == $id} @test_runs), "run $id correctly identified as archival pending");
  }
}

{
  $schema->resultset(q[Run])->find(2)->update({'folder_path_glob'=> 'sf26',});
  is($schema->resultset(q[Run])->find(2)->folder_path_glob(), 'sf26',
    'run 2 updated to be in red room');
  is($schema->resultset(q[Run])->find(3)->folder_path_glob(), undef,
    'run 3 folder path glob undefined, will never match any host');
  my $runner = test_archival_runner->new(
    pipeline_script_name    => '/bin/true',
    npg_tracking_schema     => $schema
  );

  my $s1 = 0;
  my $s2 = 0;
  lives_ok {
    $s1 = $runner->run();
    sleep 1;
    $s2 = $runner->run();
  } q{no croak running through twice - potentially as a daemon process};
  is (join(q[ ],sort {$a <=> $b} keys %{$runner->seen}), '2 1234', 'correct list of seen runs');
  is ($s1, 1, 'one run submittted on the first attempt');
  is ($s2, 1, 'one run submittted on the second attempt');
}

subtest 'propagate failure of the command to run to the caller' => sub {
  plan tests => 4;

  my $runner = test_archival_runner->new(npg_tracking_schema => $schema);
  my $id_run = 22;
  my $command = '/bin/true';
  is ($runner->run_command($id_run, $command), 1, 'command succeeded');
  ok ($runner->seen->{$id_run}, 'run cached');
  $command = '/bin/false';
  $id_run = 33;
  is ($runner->run_command($id_run, $command), 0, 'command failed');
  ok (!$runner->seen->{$id_run}, 'run not cached');
};

1;
