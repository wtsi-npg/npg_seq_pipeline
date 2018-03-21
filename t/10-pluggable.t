use strict;
use warnings;
use Test::More tests => 10;
use Test::Exception;
use Cwd;
use Log::Log4perl qw(:levels);
use File::Copy qw(cp);
use English;

use npg_tracking::util::abs_path qw(abs_path);
use t::util;

use_ok('npg_pipeline::pluggable');

my $util = t::util->new();
my $test_dir = $util->temp_directory();

local $ENV{OWNING_GROUP} = q{staff};
local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $ENV{PWD}, 'logfile'),
                          utf8   => 1});

my $config_dir = 'data/config_files';
my $runfolder_path = $util->analysis_runfolder_path;
$util->set_staging_analysis_area({with_latest_summary => 1});

subtest 'object with no function order set - simple methods' => sub {
  plan tests => 7;

  my $pluggable = npg_pipeline::pluggable->new(
    id_run         => 1234,
    runfolder_path => $test_dir
  );
  isa_ok($pluggable, q{npg_pipeline::pluggable});
  is($pluggable->pipeline_name, 'pluggable', 'pipeline name');
  is($pluggable->interactive, 0, 'interactive false');
  ok(!$pluggable->has_function_order, 'function order is not set');
  is($pluggable->id_run(), 1234, q{id_run attribute populated});
  is($pluggable->_script_name(), q{t/10-pluggable.t}, q{script_name obtained});
  is($pluggable->conf_path, abs_path(join(q[/], getcwd, $config_dir)),
    'local conf path is built');
};

subtest 'graph creation from jgf files' => sub {
  plan tests => 3;

  my $obj = npg_pipeline::pluggable->new(
    id_run         => 1234,
    runfolder_path => $test_dir,
    function_list  => "$config_dir/function_list_central.json"
  );
  lives_ok {$obj->function_graph()}
   'no error creating a graph for default analysis';

  $obj = npg_pipeline::pluggable->new(
    id_run         => 1234,
    runfolder_path => $test_dir,
    function_list  => "$config_dir/function_list_central_qc_run.json"
  );
  lives_ok {  $obj->function_graph() }
    'no error creating a graph for analysis of a qc run';

  $obj = npg_pipeline::pluggable->new(
    id_run         => 1234,
    runfolder_path => $test_dir,
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
    function_order => ['my_function', 'your_function'],
  );
  ok($obj->has_function_order(), 'function order is set');
  is(join(q[ ], @{$obj->function_order}), 'my_function your_function',
   'function order as set');
  lives_ok {  $obj->function_graph() }
    'no error creating a graph for a preset function order list';
  throws_ok { $obj->_schedule_functions() }
    qr/Handler for 'my_function' is not registered/,
    'cannot schedule non-existing function';

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
    runfolder_path => $test_dir
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

subtest 'switching off functions' => sub {
  plan tests => 8;

  my $p = npg_pipeline::pluggable->new(
    runfolder_path      => $runfolder_path,
    no_irods_archival   => 1,
    no_warehouse_update => 1
  );
  ok(($p->_run_function('archive_to_irods_samplesheet')->[0]->excluded &&
      $p->_run_function('archive_to_irods_ml_warehouse')->[0]->excluded),
    'archival to irods switched off');
  ok($p->_run_function('update_warehouse')->[0]->excluded,
    'update to warehouse switched off');

  $p = npg_pipeline::pluggable->new(
    runfolder_path => $runfolder_path,
    local          => 1,
  );
  ok(($p->_run_function('archive_to_irods_samplesheet')->[0]->excluded &&
      $p->_run_function('archive_to_irods_ml_warehouse')->[0]->excluded),
    'archival to irods switched off');
  ok($p->_run_function('update_warehouse')->[0]->excluded,
    'update to warehouse switched off');
  ok($p->no_summary_link, 'summary_link switched off');

  $p = npg_pipeline::pluggable->new(
    runfolder_path      => $runfolder_path,
    local               => 1,
    no_warehouse_update => 0,
  );
  ok(($p->_run_function('archive_to_irods_samplesheet')->[0]->excluded &&
      $p->_run_function('archive_to_irods_ml_warehouse')->[0]->excluded),
    'archival to irods switched off');
  ok(!$p->_run_function('update_warehouse')->[0]->excluded,
    'update to warehouse switched on');
  ok($p->no_summary_link, 'summary_link switched off');
};

subtest 'specifying functions via function_order' => sub {
  plan tests => 3;

  my @functions_in_order = qw(
    run_archival_in_progress
    upload_auto_qc_to_qc_database
    run_run_archived
    run_qc_complete
    update_warehouse_post_qc_complete
  );

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[/t/data];
  local $ENV{'PATH'} = join q[:], 't/bin', $ENV{'PATH'}; # mock LSF clients
  my $p = npg_pipeline::pluggable->new(
    function_order        => \@functions_in_order,
    runfolder_path        => $runfolder_path,
    spider                => 0,
    no_sf_resource        => 1,
    no_bsub               => 0
  );
  is($p->id_run, 1234, 'run id set correctly');
  is(join(q[ ], @{$p->function_order()}), join(q[ ], @functions_in_order),
    q{function_order set on creation});
  lives_ok { $p->main() } q{no error running main};
};

subtest 'propagating options to the executor' => sub {
  plan tests => 21;

  my @functions_in_order = qw(
    run_archival_in_progress
    upload_auto_qc_to_qc_database
    run_run_archived
    run_qc_complete
    update_warehouse_post_qc_complete
  );

  my $ref = {
    function_order        => \@functions_in_order,
    runfolder_path        => $runfolder_path,
    spider                => 0,
  };

  my $p = npg_pipeline::pluggable->new($ref);
  is ($p->executor_type(), 'lsf', 'default executor type is "lsf"');
  ok ($p->execute, '"execute" option is true by default');
  my $e = $p->executor();
  isa_ok ($e, 'npg_pipeline::executor::lsf');
  
  $ref->{'executor_type'} = 'some';
  $p = npg_pipeline::pluggable->new($ref);
  is ($p->executor_type(), 'some', 'executor type is "some" as set');
  throws_ok { $p->executor() }
    qr/Can't locate npg_pipeline\/executor\/some\.pm/,
    'error if executor modules does not exist';
  
  $ref->{'executor_type'} = 'lsf';
  $p = npg_pipeline::pluggable->new($ref);
  is ($p->executor_type(), 'lsf', 'executor type is "lsf" as set');
  $p->function_definitions();
  $p->function_graph();

  $e = $p->executor();
  isa_ok ($e, 'npg_pipeline::executor::lsf');

  my @boolean_attrs = qw/interactive no_sf_resource no_bsub no_array_cpu_limit/;
  for my $attr (@boolean_attrs) {
    ok (!$e->$attr, "executor: $attr value is false");
  }

  for my $attr (qw/job_name_prefix job_priority array_cpu_limit/) {
    my $predicate = "has_$attr";
    ok (!$e->$predicate, "executor: $attr is not set");
  }

  for my $attr (@boolean_attrs) {
    $ref->{$attr} = 1;
  }
  $ref->{'job_name_prefix'} = 'my';
  $ref->{'job_priority'} = 80;
  $ref->{'array_cpu_limit'} = 4;

  $p = npg_pipeline::pluggable->new($ref);
  $e = $p->executor();
  for my $attr (@boolean_attrs) {
    ok ($e->$attr, "executor: $attr value is true");
  }
  is ($e->job_name_prefix, 'my', 'job_name_prefix set correctly');
  is ($e->job_priority, 80, 'job_priority set correctly');
  is ($e->array_cpu_limit, 4, 'array_cpu_limit set correctly');
};

subtest 'options and error capture' => sub {
  plan tests => 6;

  my @functions_in_order = qw(
    run_archival_in_progress
    upload_auto_qc_to_qc_database
    run_run_archived
    run_qc_complete
    update_warehouse_post_qc_complete
  );

  my $ref = {
    function_order        => \@functions_in_order,
    runfolder_path        => $runfolder_path,
    spider                => 0,
    execute               => 0,
    no_sf_resource        => 1,
  };

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[/t/data];
  my $p = npg_pipeline::pluggable->new($ref);
  lives_ok { $p->main(); } q{no error running main without execution };
  
  $ref->{'execute'} = 1;
  $ref->{'no_bsub'} = 1;
  $p = npg_pipeline::pluggable->new($ref);
  lives_ok { $p->main(); } q{no error running main in no_bsub mode};

  $ref->{'no_bsub'} = 0;
  local $ENV{'PATH'} = join q[:], 't/bin', $ENV{'PATH'}; # mock LSF clients
  $p = npg_pipeline::pluggable->new($ref);
  lives_ok { $p->main(); } q{no error running main with mock LSF client};

  # soft-link bresume command to /bin/false so that it fails
  my $bin = "$test_dir/bin";
  mkdir $bin;
  symlink '/bin/false', "$bin/bresume";
  local $ENV{'PATH'} = join q[:], $bin, $ENV{'PATH'};
  throws_ok { npg_pipeline::pluggable->new($ref)->main() }
    qr/Failed to submit command to LSF/, q{error running main};

  $ref->{'interactive'} = 1;
  lives_ok { npg_pipeline::pluggable->new($ref)->main() }
    'no failure in interactive mode';

  $ref->{'interactive'} = 0;
  # soft-link bkill command to /bin/false so that it fails
  symlink '/bin/false', "$bin/bkill";
  throws_ok { npg_pipeline::pluggable->new($ref)->main() }
    qr/Failed to submit command to LSF/, q{error running main};
};

subtest 'positions and spidering' => sub {
  plan tests => 12;

 local $ENV{'PATH'} = join q[:], 't/bin', $ENV{'PATH'}; # mock LSF clients 

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];
  my $p = npg_pipeline::pluggable->new(
      id_run         => 1234,
      run_folder     => q{123456_IL2_1234},
      runfolder_path => $runfolder_path,
      spider         => 0
  );
  ok(!$p->spider, 'spidering is off');
  is (join( q[ ], $p->positions), '1 2 3 4 5 6 7 8', 'positions array');
  is (join( q[ ], $p->all_positions), '1 2 3 4 5 6 7 8', 'all positions array');

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];
  my $function = 'run_analysis_complete';

  $p = npg_pipeline::pluggable->new(
      id_run         => 1234,
      run_folder     => q{123456_IL2_1234},
      function_order => [$function],
      runfolder_path => $runfolder_path,
      lanes          => [1,2],
      spider         => 0,
      no_sf_resource => 1,
  );
  is (join( q[ ], $p->positions), '1 2', 'positions array');
  is (join( q[ ], $p->all_positions), '1 2 3 4 5 6 7 8', 'all positions array');
  ok(!$p->interactive, 'start job will be resumed');
  lives_ok { $p->main() } "running main for $function, non-interactively";

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];
  $p = npg_pipeline::pluggable->new(
      id_run         => 1234,
      run_folder     => q{123456_IL2_1234},
      function_order => [$function],
      runfolder_path => $runfolder_path,
      lanes          => [1,2],
      interactive    => 1,
      spider         => 0,
      no_sf_resource => 1,
  );
  ok($p->interactive, 'start job will not be resumed');
  lives_ok { $p->main() } "running main for $function, interactively";

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];
  $util->set_staging_analysis_area();
  $p = npg_pipeline::pluggable->new(
      id_run           => 1234,
      run_folder       => q{123456_IL2_1234},
      function_order   => [qw{qc_qX_yield qc_adapter qc_insert_size}],
      lanes            => [4],
      runfolder_path   => $runfolder_path,
      no_bsub          => 1,
      repository       => q{t/data/sequence},
      id_flowcell_lims => 2015,
      spider           => 0,
      no_sf_resource   => 1,
  );
  mkdir $p->archive_path;
  mkdir $p->qc_path;
  is (join( q[ ], $p->positions), '4', 'positions array');
  is (join( q[ ], $p->all_positions), '1 2 3 4 5 6 7 8', 'all positions array');
  lives_ok { $p->main() } q{running main for three qc functions};
};

subtest 'script name and function list' => sub {
  plan tests => 20;

  my $base = npg_pipeline::pluggable->new();
  my $path = join q[/], getcwd(), $config_dir, 'function_list_pluggable.json';
  is ($base->_script_name, $PROGRAM_NAME, 'script name');
  throws_ok { $base->function_list }
    qr/File $path does not exist or is not readable/,
    'error when default function list does not exist';

  $base = npg_pipeline::pluggable->new(function_list => 'base');
  $path = join q[/], getcwd(), $config_dir, 'function_list_pluggable_base.json';
  throws_ok { $base->function_list }
    qr/File $path does not exist or is not readable/,
    'error when function list does not exist';

  $path = join q[/], getcwd(), $config_dir, 'function_list_central.json';
  $base = npg_pipeline::pluggable->new(function_list => $path);
  is( $base->function_list, $path, 'function list path as given');
  isa_ok( $base->_function_list_conf(), q{HASH}, 'function list is read into a hash');
  
  $base = npg_pipeline::pluggable->new(function_list => 'data/config_files/function_list_central.json');
  is( $base->function_list, $path, 'function list absolute path from relative path');
  isa_ok( $base->_function_list_conf(), q{HASH}, 'function list is read into an array');

  $base = npg_pipeline::pluggable->new(function_list => 'central');
  is( $base->function_list, $path, 'function list absolute path from list name');
  isa_ok( $base->_function_list_conf(), q{HASH}, 'function list is read into an array');

  $path =~ s/function_list_central/function_list_post_qc_review/;

  $base = npg_pipeline::pluggable->new(function_list => 'post_qc_review');
  is( $base->function_list, $path, 'function list absolute path from list name');
  isa_ok( $base->_function_list_conf(), q{HASH}, 'function list is read into an array');

  my $test_path = '/some/test/path.json';
  $base = npg_pipeline::pluggable->new(function_list => $test_path);
  throws_ok { $base->function_list }
    qr/Bad function list name: $test_path/,
    'error when function list does not exist, neither it can be interpreted as a function list name';
  
  cp $path, $test_dir;
  $path = $test_dir . '/function_list_post_qc_review.json';

  $base = npg_pipeline::pluggable->new(function_list => $path);
  is( $base->function_list, $path, 'function list absolute');
  isa_ok( $base->_function_list_conf(), q{HASH}, 'function list is read into an array');

  $base = npg_pipeline::pluggable->new(
    conf_path     => $test_dir,
    function_list => 'post_qc_review');
  is( $base->function_list, $path, 'function list absolute path from list name');

  $path =~ s/function_list_post_qc_review/function_list_pluggable/;
  $base = npg_pipeline::pluggable->new(conf_path => $test_dir);
  throws_ok { $base->function_list }
    qr/File $path does not exist or is not readable/,
    'error when default function list does not exist';

  $base = npg_pipeline::pluggable->new(function_list => 'some+other:');
  throws_ok { $base->function_list }
    qr/Bad function list name: some\+other:/,
    'error when function list name contains illegal characters';
  
  $base = npg_pipeline::pluggable->new(qc_run => 1);
  $path = join q[/], getcwd(), $config_dir, 'function_list_pluggable_qc_run.json';
  throws_ok { $base->function_list }
    qr/File $path does not exist or is not readable/,
    'error when default function list does not exist';

  package mytest::central;
  use base 'npg_pipeline::pluggable';
  package main;

  my $c = mytest::central->new(qc_run => 1);
  is ($base->_script_name, $PROGRAM_NAME, 'script name');
  my $fl = join q[/], getcwd(), $config_dir, 'function_list_central_qc_run.json';
  is( $c->function_list, $fl, 'qc function list');
};

1;
