use strict;
use warnings;
use Test::More tests => 12;
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

my @tools = map { "$test_dir/$_" } qw/bamtofastq blat norm_fit/;
foreach my $tool (@tools) {
  open my $fh, '>', $tool or die 'cannot open file for writing';
  print $fh $tool or die 'cannot print';
  close $fh or warn 'failed to close file handle';
}
chmod 0755, @tools;
local $ENV{'PATH'} = join q[:], $test_dir, $ENV{'PATH'};

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $test_dir, 'logfile'),
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
  is($pluggable->_pipeline_name, '10-pluggable.t', 'pipeline name');
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
    runfolder_path => $runfolder_path,
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
  plan tests => 4;

  my @functions_in_order = qw(
    run_archival_in_progress
    upload_auto_qc_to_qc_database
    run_run_archived
    run_qc_complete
    update_warehouse_post_qc_complete
  );

  local $ENV{'PATH'} = join q[:], 't/bin', $ENV{'PATH'}; # mock LSF clients
  my $p = npg_pipeline::pluggable->new(
    function_order        => \@functions_in_order,
    runfolder_path        => $runfolder_path,
    spider                => 0,
    no_sf_resource        => 1,
    no_bsub               => 0,
    is_indexed            => 0
  );
  is($p->id_run, 1234, 'run id set correctly');
  is($p->is_indexed, 0, 'is not indexed');
  is(join(q[ ], @{$p->function_order()}), join(q[ ], @functions_in_order),
    q{function_order set on creation});
  lives_ok { $p->main() } q{no error running main};
};

subtest 'creating executor object' => sub {
  plan tests => 13;

  my $ref = {
    function_order        => [qw/run_archival_in_progress/],
    runfolder_path        => $runfolder_path,
    bam_basecall_path     => $runfolder_path,
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

  for my $etype (qw/lsf wr/) {
    $ref->{'executor_type'} = $etype;
    my $pl = npg_pipeline::pluggable->new($ref);
    is ($pl->executor_type(), $etype, "executor type is $etype as set");
    my $ex = $pl->executor();
    isa_ok ($ex, 'npg_pipeline::executor::' . $etype);
    ok (!$ex->has_analysis_path, 'analysis path is not set');
    my $path1 = join q[],$runfolder_path,'/t_10-pluggable.t_1234_',$pl->timestamp, q[-];
    my $path2 = join q[], '.commands4', uc $etype, 'jobs.', $etype eq 'lsf' ? 'json' : 'txt';
    like ($ex->commands4jobs_file_path(), qr/\A$path1(\d+)$path2\Z/,
      'file path to save commands for jobs');
  }
};

subtest 'propagating options to the lsf executor' => sub {
  plan tests => 14;

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
  my $e = $p->executor();

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

subtest 'running the pipeline (lsf executor)' => sub {
  plan tests => 6;

  my @functions_in_order = qw(
    run_archival_in_progress
    upload_auto_qc_to_qc_database
    run_run_archived
    run_qc_complete
    update_warehouse_post_qc_complete
  );

  my $ref = {
    function_order => \@functions_in_order,
    runfolder_path => $runfolder_path,
    spider         => 0,
    execute        => 0,
    no_sf_resource => 1,
    is_indexed     => 0,
  };

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

subtest 'running the pipeline (wr executor)' => sub {
  plan tests => 5;

  my @functions_in_order = qw(
    run_archival_in_progress
    upload_auto_qc_to_qc_database
    run_run_archived
    run_qc_complete
    update_warehouse_post_qc_complete
  );

  my $ref = {
    function_order => \@functions_in_order,
    runfolder_path => $runfolder_path,
    spider         => 0,
    execute        => 0,
    executor_type  => 'wr',
    is_indexed     => 0,
  };

  # soft-link wr command to /bin/false so that it fails
  my $bin = "$test_dir/bin";
  my $wr = "$bin/wr";
  symlink '/bin/false', $wr;
  local $ENV{'PATH'} = join q[:], $bin, $ENV{'PATH'};

  my $p = npg_pipeline::pluggable->new($ref);
  lives_ok { $p->main(); } q{no error running main without execution };

  $ref->{'execute'} = 1;
  throws_ok { npg_pipeline::pluggable->new($ref)->main() }
    qr/Error submitting for execution: Error submitting wr jobs/,
    q{error running main};

  $ref->{'interactive'} = 1;
  lives_ok { npg_pipeline::pluggable->new($ref)->main() }
    q{interactive mode, no error running main};

  # soft-link wr command to /bin/true so that it succeeds
  unlink $wr;
  symlink '/bin/true', $wr;
  $ref->{'interactive'} = 0;
  lives_ok { npg_pipeline::pluggable->new($ref)->main() } q{no error running main};

  $ref->{'job_name_prefix'} = 'test';
  lives_ok { npg_pipeline::pluggable->new($ref)->main() }
    q{job name prefix is set, no error running main};
};

subtest 'positions and spidering' => sub {
  plan tests => 9;

  cp 't/data/run_params/runParameters.hiseq.xml',
    join(q[/], $runfolder_path, 'runParameters.xml')
    or die 'Faile to copy run params file';

  local $ENV{'PATH'} = join q[:], 't/bin', $ENV{'PATH'}; # mock LSF clients
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_1234.csv];
  my $p = npg_pipeline::pluggable->new(
      id_run           => 1234,
      id_flowcell_lims => 2015,
      run_folder       => q{123456_IL2_1234},
      runfolder_path   => $runfolder_path,
      spider           => 0
  );
  ok(!$p->spider, 'spidering is off');
  is (join( q[ ], $p->positions), '1 2 3 4 5 6 7 8', 'positions array');

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_1234.csv];
  my $function = 'run_analysis_complete';

  $p = npg_pipeline::pluggable->new(
      id_run           => 1234,
      id_flowcell_lims => 2015,
      run_folder       => q{123456_IL2_1234},
      function_order   => [$function],
      runfolder_path   => $runfolder_path,
      lanes            => [1,2],
      spider           => 0,
      no_sf_resource   => 1,
  );
  is (join( q[ ], $p->positions), '1 2', 'positions array');
  ok(!$p->interactive, 'start job will be resumed');
  lives_ok { $p->main() } "running main for $function, non-interactively";

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_1234.csv];
  $p = npg_pipeline::pluggable->new(
      id_run           => 1234,
      id_flowcell_lims => 2015,
      run_folder       => q{123456_IL2_1234},
      function_order   => [$function],
      runfolder_path   => $runfolder_path,
      lanes            => [1,2],
      interactive      => 1,
      spider           => 0,
      no_sf_resource   => 1,
  );
  ok($p->interactive, 'start job will not be resumed');
  lives_ok { $p->main() } "running main for $function, interactively";

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_1234.csv];
  $util->set_staging_analysis_area();
  cp 't/data/run_params/runParameters.hiseq.xml',
    join(q[/], $runfolder_path, 'runParameters.xml')
    or die 'Faile to copy run params file';

  $util->create_run_info();

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
  lives_ok { $p->main() } q{running main for three qc functions};
};

subtest 'script name, pipeline name and function list' => sub {
  plan tests => 17;

  my $base = npg_pipeline::pluggable->new();
  is ($base->_script_name, $PROGRAM_NAME, 'script name');
  is ($base->_pipeline_name, '10-pluggable.t', 'pipeline name');
  throws_ok { $base->function_list }
    qr/Bad function list name: 10-pluggable\.t/,
    'error when test name is used as function list name';

  $base = npg_pipeline::pluggable->new(function_list => 'base');
  my $path = abs_path(join q[/], getcwd(), $config_dir, 'function_list_10-pluggable.t_base.json');
  throws_ok { $base->function_list }
    qr/File $path does not exist or is not readable/,
    'error when file is not found';

  $path = abs_path(join q[/], getcwd(), $config_dir, 'function_list_central.json');
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

  $base = npg_pipeline::pluggable->new(function_list => 'some+other:');
  throws_ok { $base->function_list }
    qr/Bad function list name: some\+other:/,
    'error when function list name contains illegal characters';
};

1;
