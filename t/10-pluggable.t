use strict;
use warnings;
use Test::More tests => 4;
use Test::Exception;
use Cwd;
use Log::Log4perl qw(:levels);

use npg_tracking::util::abs_path qw(abs_path);
use t::util;

local $ENV{PATH} = join q[:], q[t/bin], q[t/bin/software/solexa/bin], $ENV{PATH};

use_ok('npg_pipeline::pluggable');

my $util = t::util->new();
my $test_dir = $util->temp_directory();
$ENV{TEST_DIR} = $test_dir;
$ENV{OWNING_GROUP} = q{staff};
local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $test_dir, 'logfile'),
                          utf8   => 1});

my $config_dir = 'data/config_files';

subtest 'object with no function order set - simple methods' => sub {
  plan tests => 11;

  my $pluggable = npg_pipeline::pluggable->new(
    id_run         => 1234,
    runfolder_path => $test_dir,
    no_bsub        => 1,
  );
  isa_ok($pluggable, q{npg_pipeline::pluggable});
  is($pluggable->pipeline_name, 'pluggable', 'pipeline name');
  is($pluggable->interactive, 0, 'interactive false');
  ok(!$pluggable->has_function_order, 'function order is not set');
  is($pluggable->id_run(), 1234, q{id_run attribute populated});
  is($pluggable->script_name(), q{t/10-pluggable.t}, q{script_name obtained});
  is($pluggable->conf_path, abs_path(join(q[/], getcwd, $config_dir)),
    'local conf path is built');
  my @ids;
  lives_ok { @ids = $pluggable->pipeline_start() } q{no error submitting start job};
  is(join(q[ ], @ids), '50', 'test start job id is correct');
  lives_ok { @ids = $pluggable->pipeline_end() } q{no error submitting end job};
  is(join(q[ ], @ids), '50', 'test start job id is correct');
};

subtest 'graph creation from jgf files' => sub {
  plan tests => 3;

  my $obj = npg_pipeline::pluggable->new(
    id_run         => 1234,
    runfolder_path => $test_dir,
    no_bsub        => 1,
    function_list  => "$config_dir/function_list_central.json"
  );
  lives_ok {$obj->function_graph()}
   'no error creating a graph for default analysis';

  $obj = npg_pipeline::pluggable->new(
    id_run         => 1234,
    runfolder_path => $test_dir,
    no_bsub        => 1,
    function_list  => "$config_dir/function_list_central_qc_run.json"
  );
  lives_ok {  $obj->function_graph() }
    'no error creating a graph for analysis of a qc run';

  $obj = npg_pipeline::pluggable->new(
    id_run         => 1234,
    runfolder_path => $test_dir,
    no_bsub        => 1,
    function_list  => "$config_dir/function_list_post_qc_review.json"
  );
  lives_ok {  $obj->function_graph() }
    'no error creating a graph for default archival';
};

subtest 'graph creation from explicitly given function list' => sub {
  plan tests => 18;

  my $obj = npg_pipeline::pluggable->new(
    id_run         => 1234,
    runfolder_path => $test_dir,
    no_bsub        => 1,
    function_order => ['my_function', 'your_function'],
  );
  ok($obj->has_function_order(), 'function order is set');
  is(join(q[ ], @{$obj->function_order}), 'my_function your_function',
   'function order as set');
  lives_ok {  $obj->function_graph() }
    'no error creating a graph for a preset function order list';
  throws_ok { $obj->_schedule_functions() }
    qr/Can't locate object method "my_function"/,
    'canot schedule non-existing function';

  my $g = $obj->function_graph();
  is($g->vertices(), 4, 'four graph nodes');

  my @p = $g->predecessors('my_function');
  is (scalar @p, 1, 'one predecessor');
  is ($p[0], 'pipeline_start', 'pipeline_start is before my_function');
  ok ($g->is_source_vertex('pipeline_start'), 'pipeline_start is source vertex');
  my @s = $g->successors('my_function');
  is (scalar @s, 1, 'one successor');
  is ($s[0], 'your_function', 'your_function is after my_function');

  @p = $g->predecessors('your_function');
  is (scalar @p, 1, 'one predecessor');
  is ($p[0], 'my_function', 'my_function is before your_function');
  @s = $g->successors('your_function');
  is (scalar @s, 1, 'one successor');
  is ($s[0], 'pipeline_end', 'your_function is before pipeline_end');

  ok ($g->is_sink_vertex('pipeline_end'), 'pipeline_end is sink vertex');
  @p = $g->predecessors('pipeline_end');
  is (scalar @p, 1, 'one predecessor');

  $obj = npg_pipeline::pluggable->new(
    id_run         => 1234,
    function_order => [qw/pipeline_end/],
    runfolder_path => $test_dir,
    no_bsub        => 1
  );
  throws_ok { $obj->function_graph() }
    qr/Graph is not DAG/,
    'pipeline_end cannot be specified in function order';

  $obj = npg_pipeline::pluggable->new(
    id_run         => 1234,
    function_order => [qw/pipeline_start/],
    runfolder_path => $test_dir,
    no_bsub        => 1
  );
  throws_ok { $obj->function_graph() }
    qr/Graph is not DAG/,
    'pipeline_start cannot be specified in function order';
};

1;
