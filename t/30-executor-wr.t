use strict;
use warnings;
use Test::More tests => 3;
use Test::Exception;
use File::Temp qw(tempdir);
use Graph::Directed;
use Log::Log4perl qw(:levels);

use_ok('npg_pipeline::executor::wr');

my $tmp = tempdir(CLEANUP => 1);

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $tmp, 'logfile'),
                          utf8   => 1});

my $ref = {
  function_definitions => {},
  function_graph       => Graph::Directed->new(),
          };

subtest 'object creation' => sub {
  plan tests => 1;

  my $e = npg_pipeline::executor::wr->new(
    function_definitions => {},
    function_graph       => Graph::Directed->new());
  isa_ok ($e, 'npg_pipeline::executor::wr');
};

subtest 'wr add command' => sub {
  plan tests => 1; 
  
  my $file = "$tmp/commands.txt";  
  my $e = npg_pipeline::executor::wr->new(
    function_definitions    => {},
    function_graph          => Graph::Directed->new(),
    commands4jobs_file_path => $file);
  is ($e->_wr_add_command(),
    "wr add --cwd /tmp --disk 0 --override 2 --priority 50 --retries 1 -f $file",
    'wr command');
}; 

1;
