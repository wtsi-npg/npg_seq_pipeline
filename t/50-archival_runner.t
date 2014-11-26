# $Id: 50-archival_runner.t 17862 2013-12-04 10:26:28Z mg8 $
use strict;
use warnings;
use Test::More tests => 17;
use Test::Exception;
use Cwd;
use List::MoreUtils qw{any};
use t::dbic_util;
use t::util;

BEGIN {
  use_ok('npg_pipeline::daemons::archival_runner');
}
my $script_name = q[npg_pipeline_post_qc_review];
is (npg_pipeline::daemons::archival_runner->pipeline_script_name(),
            $script_name, 'pipeline script name correct'); 

my $util = t::util->new();
my $temp_directory = $util->temp_directory();

my $script = join q[/],  $temp_directory, $script_name;
`touch $script`;
`chmod +x $script`;
my $current_dir = getcwd();
local $ENV{PATH} = join q[:], $temp_directory, $current_dir.'/t/bin/red', $ENV{PATH};

my $schema = t::dbic_util->new->test_schema();
my $test_run = $schema->resultset(q[Run])->find(1234);
$test_run->update_run_status('archival pending', 'pipeline',);
is($test_run->current_run_status_description, 'archival pending', 'test run is archival pending');

{
  my $runner;
  lives_ok { $runner = npg_pipeline::daemons::archival_runner->new(
    log_file_path => $temp_directory,
    log_file_name => q{npg_pipeline_archival_daemon.log},
    npg_tracking_schema => $schema, 
  ); } q{object creation ok};
  isa_ok($runner, q{npg_pipeline::daemons::archival_runner}, q{$runner});
  lives_ok { $runner->run(); } q{no croak on $runner->run()};
  like($runner->_generate_command(1234), qr/npg_pipeline_post_qc_review --verbose --id_run=1234/,
    q{generated command is correct});
  ok(!$runner->green_host, 'host is not in green datacentre');

  $schema->resultset(q[Run])->find(2)->update_run_status('archival pending', 'pipeline');
  $schema->resultset(q[Run])->find(3)->update_run_status('archival pending', 'pipeline');
  my @test_runs = ();
  lives_ok { @test_runs = $runner->runs_with_status('archival pending') } 'can get runs with analysys pending status';
  ok(scalar @test_runs >= 3, 'at least three runs are archival pending');
  foreach my $id (qw/2 3 1234/) {
    ok((any {$_->id_run == $id} @test_runs), "run $id correctly identified as archival pending");
  }
}

{
  $schema->resultset(q[Run])->find(2)->update({'folder_path_glob'=> 'sf26',});
  is($schema->resultset(q[Run])->find(2)->folder_path_glob(), 'sf26', 'run 2 updated to be in red room');
  is($schema->resultset(q[Run])->find(3)->folder_path_glob(), undef,
    'run 3 folder path glob undefined, will never match any host');
  my $runner = npg_pipeline::daemons::archival_runner->new(
    log_file_path => $temp_directory,
    log_file_name => q{npg_pipeline_archival_daemon.log},
    npg_tracking_schema => $schema,
  );
  lives_ok {
    $runner->run();
    sleep 1;
    $runner->run();
  } q{no croak running through twice - potentially as a daemon process};
  is (join(q[ ],sort {$a <=> $b} keys %{$runner->seen}), '2 1234', 'correct list of seen runs');
}

1;
