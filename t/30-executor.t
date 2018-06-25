use strict;
use warnings;
use Test::More tests => 5;
use Test::Exception;
use File::Temp qw(tempdir);
use Graph::Directed;
use Log::Log4perl qw(:levels);
use Perl6::Slurp;

use npg_pipeline::function::definition;

use_ok('npg_pipeline::executor');

my $tmp = tempdir(CLEANUP => 1);

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $tmp, 'logfile'),
                          utf8   => 1});

subtest 'constructor and basic consistency checking' => sub {
  plan tests => 2;

  my $ref = {
    function_definitions => {},
    function_graph       => Graph::Directed->new(),
          };
  my $e = npg_pipeline::executor->new($ref);
  isa_ok ($e, 'npg_pipeline::executor');
  lives_ok {$e->execute()} 'execute command runs anyway';
};

subtest 'builder for commands4jobs_file_path' => sub {
  plan tests => 4;

  my $ref = {
    function_definitions => {},
    function_graph       => Graph::Directed->new(),
          };
  my $e = npg_pipeline::executor->new($ref);
  throws_ok {$e->commands4jobs_file_path()}
    qr/analysis_path attribute is not set/,
    'error when analysis_path is not set';

  $ref->{'analysis_path'} = $tmp;
  $e = npg_pipeline::executor->new($ref);
  throws_ok {$e->commands4jobs_file_path()}
    qr/Definition hash is empty/,
    'error when analysis_path definition hash is empty';

  $ref->{'function_definitions'} = {'one' => []};
  $e = npg_pipeline::executor->new($ref);
  throws_ok {$e->commands4jobs_file_path()}
    qr/Empty definition array for one/,
    'error when analysis_path definitions array is empty';

  my $init = { created_by   => 'module',
               created_on   => 'June 25th',
               job_name     => 'name',
               identifier   => 2345,
               command      => 'command'};
  my $d = npg_pipeline::function::definition->new($init);
  $ref->{'function_definitions'} = {'one' => [$d]};
  $e = npg_pipeline::executor->new($ref);
  is ($e->commands4jobs_file_path(), "$tmp/commands4jobs_2345_June 25th",
    'correct path');
};

subtest 'saving commands for jobs' => sub {
 plan tests => 13;

  my $path = "$tmp/commands.txt";
  my $ref = {
    function_definitions    => {},
    function_graph          => Graph::Directed->new(),
    commands4jobs_file_path => $path
            };
  my $e = npg_pipeline::executor->new($ref);
  throws_ok { $e->save_commands4jobs() }
    qr/List of commands cannot be empty/, 'error if no data to save';
  
  ok (!-e $path, 'prerequisite - file does not exist');
  lives_ok { $e->save_commands4jobs('some data to save') } 'command saved';
  ok (-f $path, 'file created');
  my @lines = slurp $path;
  is (scalar @lines, 1, 'file contains one line');
  is ($lines[0], "some data to save\n", 'correct line content');
  unlink $path or die "Failed to delete file $path";
  lives_ok { $e->save_commands4jobs(qw/some data to save/) } 'commands saved';
  ok (-f $path, 'file created');
  @lines = slurp $path;
  is (scalar @lines, 4, 'file contains four lines');
  is ($lines[0], "some\n", 'correct line content');
  is ($lines[1], "data\n", 'correct line content');
  is ($lines[2], "to\n", 'correct line content');
  is ($lines[3], "save\n", 'correct line content');
};

subtest 'builder for function_graph4jobs' => sub {
  plan tests => 17;

  my $e = npg_pipeline::executor->new(
    function_definitions => {},
    function_graph       => Graph::Directed->new()
  );
  throws_ok {$e->function_graph4jobs()}
    qr/Empty function graph/,
    'error when function graph is empty';

  my $g =  Graph::Directed->new();
  $g->add_edge('node_one', 'node_two');
  is ($g->vertices(), 2, 'two nodes in the graph');
  is ($g->edges(), 1, 'one edge in the graph');

  $e = npg_pipeline::executor->new(function_definitions => {},
                                   function_graph       => $g);
  throws_ok { $e->function_graph4jobs() }
    qr/Function node_one is not defined/,
    'error if no definition for a function';

  $e = npg_pipeline::executor->new(
         function_definitions => {node_one => undef},
         function_graph       => $g);
  throws_ok { $e->function_graph4jobs() }
    qr/No definition array for function node_one/,
    'error if definition array is not defined';

  $e = npg_pipeline::executor->new(
         function_definitions => {node_one => []},
         function_graph       => $g);
  throws_ok { $e->function_graph4jobs() }
    qr/Definition array for function node_one is empty/,
    'error if definition array is empty';

  my $init = { created_by   => 'module',
               created_on   => 'June 25th',
               job_name     => 'name',
               identifier   => 2345,
               command      => 'command'};

  my $d1 = npg_pipeline::function::definition->new($init);
  my $d2 = npg_pipeline::function::definition->new($init);

  $e = npg_pipeline::executor->new(
         function_definitions => {node_one => [$d1], node_two => [$d2]},
         function_graph       => $g);
  my $g4jobs = $e->function_graph4jobs();
  isa_ok ($g4jobs, 'Graph::Directed');
  is ($g4jobs->vertices(), 2, 'two nodes in the new graph');
  is ($g4jobs->edges(), 1, 'one edge in the new graph');

  $init->{'excluded'} = 1;
  $d1 = npg_pipeline::function::definition->new($init);
  $e = npg_pipeline::executor->new(
         function_definitions => {node_one => [$d2], node_two => [$d1]},
         function_graph       => $g);
  $g4jobs = $e->function_graph4jobs();
  is ($g4jobs->vertices(), 1, 'one node in the new graph');
  is ($g4jobs->edges(), 0, 'zero edges in the new graph');

  $e = npg_pipeline::executor->new(
         function_definitions => {node_one => [$d1], node_two => [$d1]},
         function_graph       => $g);
  throws_ok {$e->function_graph4jobs()} qr/New function graph is empty/,
    'error if the new graph is empty';

  $g =  Graph::Directed->new();
  $g->add_edge('node_one', 'node_two');
  $g->add_edge('node_two', 'node_two_a');
  $g->add_edge('node_one', 'node_three');
  $g->add_edge('node_three', 'node_three_a');
  $g->add_edge('node_three', 'node_three_b');
  $g->add_edge('node_two', 'node_three_b');
  is ($g->vertices(), 6, 'seven nodes in the graph');
  is ($g->edges(), 6, 'six edges in the graph');

  #####
  # graph $g
  # node_one-node_three,node_one-node_two,
  # node_three-node_three_a,node_three-node_three_b,
  # node_two-node_three_b,node_two-node_two_a

  my $definitions = {
    node_one => [$d2],
    node_two => [$d1],
    node_two_a => [$d2, $d2, $d2],
    node_three => [$d2, $d2],
    node_three_a => [$d2],
    node_three_b => [$d2],
  };
  
  $e = npg_pipeline::executor->new(
         function_definitions => $definitions,
         function_graph       => $g);
  $g4jobs = $e->function_graph4jobs();
  #####
  # New graph $g4jobs
  # node_one-node_three,node_one-node_three_b,
  # node_one-node_two_a,node_three-node_three_a,
  # node_three-node_three_b
  #
  is ($g4jobs->vertices(), 5, 'five nodes in the new graph');
  is ($g4jobs->edges(), 5, 'five edges in the new graph');

  my @dependencies = sort $e->dependencies('node_three_b', 'num_definitions');
  is_deeply (\@dependencies, [1, 2], 'correct dependencies');
};

1;
