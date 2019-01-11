use strict;
use warnings;
use Test::More tests => 5;
use Test::Exception;
use File::Temp qw(tempdir);
use Graph::Directed;
use Log::Log4perl qw(:levels);

use_ok('npg_pipeline::function::definition');
use_ok('npg_pipeline::executor::wr');

my $tmp = tempdir(CLEANUP => 1);

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $tmp, 'logfile'),
                          utf8   => 1});

subtest 'object creation' => sub {
  plan tests => 1;

  my $e = npg_pipeline::executor::wr->new(
    function_definitions => {},
    function_graph       => Graph::Directed->new());
  isa_ok ($e, 'npg_pipeline::executor::wr');
};

subtest 'wr conf file' => sub {
  plan tests => 5;
  my $e = npg_pipeline::executor::wr->new(
    function_definitions => {},
    function_graph       => Graph::Directed->new());
  my $conf = $e->wr_conf;
  is (ref $conf, 'HASH', 'configuration is a hash ref');
  while (my ($key, $value) = each %{$conf}) {
    if ( $key =~ /queue\Z/ ) {
      is_deeply ($value,
      $key =~ /\Ap4stage1/ ? {'cloud_flavor' => 'ukb1.2xlarge'} : {},
      "correct settings for $key");
    }
  }
};

subtest 'wr add command' => sub {
  plan tests => 6;

  my $get_env = sub {
    my @env = ();
    for my $name (sort qw/PATH PERL5LIB
                          CLASSPATH NPG_CACHED_SAMPLESHEET_FILE
                          NPG_REPOSITORY_ROOT/) {
      my $v = $ENV{$name};
      if ($v) {
        push @env, join(q[=], $name, $v);
      }
    }
    my $env_string = join q[,], @env;
    return q['] . $env_string . q['];
  }; 
 
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[];
  my $env_string = $get_env->();
  unlike  ($env_string, qr/NPG_CACHED_SAMPLESHEET_FILE/,
    'env does not contain samplesheet');
  my $file = "$tmp/commands.txt";
  my $e = npg_pipeline::executor::wr->new(
    function_definitions    => {},
    function_graph          => Graph::Directed->new(),
    commands4jobs_file_path => $file);
  is ($e->_wr_add_command(),
    "wr add --cwd /tmp --disk 0 --override 2 --retries 1 --env $env_string -f $file",
    'wr command');

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/samplesheet_1234.csv';
  $env_string = $get_env->();
  like  ($env_string, qr/NPG_CACHED_SAMPLESHEET_FILE/,
    'env contains samplesheet');
  is ($e->_wr_add_command(),
    "wr add --cwd /tmp --disk 0 --override 2 --retries 1 --env $env_string -f $file",
    'wr command');
  local $ENV{NPG_REPOSITORY_ROOT} = 't/data';
  $env_string = $get_env->();
  like  ($env_string, qr/NPG_REPOSITORY_ROOT/, 'env contains ref repository');
  is ($e->_wr_add_command(),
    "wr add --cwd /tmp --disk 0 --override 2 --retries 1 --env $env_string -f $file",
    'wr command');
}; 

1;
