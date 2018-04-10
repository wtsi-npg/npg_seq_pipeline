use strict;
use warnings;
use Test::More tests => 4;
use Test::Exception;
use File::Temp qw(tempdir);
use Graph::Directed;
use Log::Log4perl qw(:levels);

use npg_pipeline::function::definition;

use_ok('npg_pipeline::executor');

my $tmp = tempdir(CLEANUP => 1);

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $tmp, 'logfile'),
                          utf8   => 1});

subtest 'constructor and error conditions for an empty graph' => sub {
  plan tests => 3;

  my $ref = {
    function_definitions => {},
    function_graph       => Graph::Directed->new(),
          };

  my $e = npg_pipeline::executor->new($ref);
  isa_ok ($e, 'npg_pipeline::executor');

  throws_ok {$e->function_loop()} qr/Empty function graph/,
    'error iterating an empty graph';

  throws_ok {$e->execute()} qr/Empty function graph/,
    'error executing function for an empty graph';
};

subtest 'simple tests for the execute method' => sub {
  plan tests => 6;

  package npg::test::derived;
  use Moose;
  extends 'npg_pipeline::executor';
  sub executor4function {
    my ($self, $function) = @_;
    $self->info("Function $function");
    return;
  }
  1;

  package main;

  my $g =  Graph::Directed->new();
  $g->add_edge('node_one', 'node_two');

  my $e1 = npg::test::derived->new({function_definitions => {},
                                  function_graph       => $g});
  throws_ok { $e1->execute() } qr/Function node_one is not defined/,
    'error if no definition for a function';

  $e1 = npg::test::derived->new({function_definitions => {node_one => undef},
                                 function_graph       => $g});
  throws_ok { $e1->execute() } qr/No definition array for function node_one/,
    'error if definition array is not defined';

  $e1 = npg::test::derived->new({function_definitions => {node_one => []},
                                 function_graph       => $g});
  throws_ok { $e1->execute() } qr/Definition array for function node_one is empty/,
    'error if definition array is empty';

  my $init = { created_by   => 'module',
               created_on   => 'June 25th',
               job_name     => 'name',
               identifier   => 2345,
               command      => 'command',
               log_file_dir => '/some/dir' };

  my $d1 = npg_pipeline::function::definition->new($init);
  my $d2 = npg_pipeline::function::definition->new($init);

  $e1 = npg::test::derived->new(
       {function_definitions => {node_one => [$d1], node_two => [$d2]},
        function_graph       => $g});
  lives_ok { $e1->execute() } 'two jobs processed';

  $init->{'immediate_mode'} = 1;
  $d1 = npg_pipeline::function::definition->new($init);
  $e1 = npg::test::derived->new(
        {function_definitions => {node_one => [$d1], node_two => [$d2]},
         function_graph       => $g});
  lives_ok { $e1->execute() } 'two jobs processed, one of them in the immediate mode';

  $init->{'immediate_mode'} = 0;
  $init->{'excluded'} = 1;
  $d1 = npg_pipeline::function::definition->new($init);
  $e1 = npg::test::derived->new(
    {function_definitions => {node_one => [$d1], node_two => [$d2]},
     function_graph       => $g});
  lives_ok { $e1->execute() } 'two jobs processed, one of them skipped';
};

subtest 'generated path for files with commands' => sub {
  plan tests => 1;

  my $g =  Graph::Directed->new();
  $g->add_edge('node_one', 'node_two');

  my $init = { created_by   => 'module',
               created_on   => 'June 25th',
               job_name     => 'name',
               identifier   => 2345,
               command      => 'command',
               log_file_dir => '/some/dir' };
  my $d = npg_pipeline::function::definition->new($init);

  my $e = npg_pipeline::executor->new({function_definitions => {node_one => [$d, $d]},
                                       function_graph       => $g,
                                       analysis_path        => '/tmp/data'});
  is ($e->commands4jobs_file_path(), '/tmp/data/commands4jobs_2345_June 25th',
    'path for files with commands');
};

1;
