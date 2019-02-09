use strict;
use warnings;
use Test::More tests => 6;
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
    for my $name (sort qw/PATH PERL5LIB IRODS_ENVIRONMENT_FILE
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

subtest 'definition for a job' => sub {
  plan tests => 2;

  my $ref = {
    created_by    => __PACKAGE__,
    created_on    => 'today',
    identifier    => 1234,
    job_name      => 'job_name',
    command       => '/bin/true',
    num_cpus      => [1],
    queue         => 'small'
  };
  my $fd = npg_pipeline::function::definition->new($ref);

  my $g = Graph::Directed->new();
  $g->add_edge('pipeline_wait4path', 'pipeline_start');
  my $e = npg_pipeline::executor::wr->new(
    function_definitions => {
      'pipeline_wait4path' => [$fd], 'pipeline_start' => [$fd]},
    function_graph       => $g
  );

  my $job_def = $e->_definition4job('pipeline_wait4path', 'some_dir', $fd);
  my $expected = { 'cmd' => '( /bin/true ) 2>&1',
                   'cpus' => 1,
                   'priority' => 0,
                   'memory' => '2000M' };
  is_deeply ($job_def, $expected, 'job definition without tee-ing to a log file');

  $ref->{'num_cpus'} = [0];
  $ref->{'memory'}   = 100;
  $fd = npg_pipeline::function::definition->new($ref);
  $expected = {
    'cmd' => '( /bin/true ) 2>&1 | tee -a "some_dir/pipeline_start-today-1234.out"',
    'cpus' => 0,
    'priority' => 0,
    'memory'   => '100M' };
  $job_def = $e->_definition4job('pipeline_start', 'some_dir', $fd);
  is_deeply ($job_def, $expected, 'job definition with tee-ing to a log file');
}; 

1;
