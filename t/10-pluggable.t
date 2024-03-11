use strict;
use warnings;
use Test::More tests => 16;
use Test::Exception;
use Cwd qw(getcwd abs_path);
use List::Util qw(any);
use Log::Log4perl qw(:levels);
use File::Temp qw(tempdir);
use English qw(-no_match_vars);
use File::Copy::Recursive qw(dircopy fmove fcopy);

use_ok('npg_pipeline::pluggable');

my $test_dir = tempdir(CLEANUP => 1);

my $test_bin = join q[/], $test_dir, q[bin];
mkdir $test_bin;
my @tools = map { "$test_bin/$_" } qw/bamtofastq blat norm_fit/;
foreach my $tool (@tools) {
  open my $fh, '>', $tool or die 'cannot open file for writing';
  print $fh $tool or die 'cannot print';
  close $fh or warn 'failed to close file handle';
}
chmod 0755, @tools;
local $ENV{'PATH'} = join q[:], $test_bin, $ENV{'PATH'};

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $test_dir, 'logfile'),
                          utf8   => 1});

my $product_config = q[t/data/release/config/archive_on/product_release.yml];
my $config_dir = 'data/config_files';

my $rf_name = q[210415_A00971_0162_AHNNTMDSXY];
my $test_rf = q[t/data/novaseq/] . $rf_name;
my $analysis_dir = join q[/], $test_dir,
  q[esa-sv-20201215-02/IL_seq_data/analysis];
my $runfolder_path = join q[/], $analysis_dir, $rf_name;
dircopy($test_rf, $runfolder_path);
my $bbcals_relative = q[Data/Intensities/BAM_basecalls_20210417-080715];
my $nocall_relative = $bbcals_relative . q[/no_cal];
my $nocall_path = join q[/], $runfolder_path, $nocall_relative;
mkdir $nocall_path;
symlink $nocall_path, "$runfolder_path/Latest_Summary";

my $id_run = 37416;
for my $file (qw(RunInfo.xml RunParameters.xml)) {
  my $source = join q[/], $runfolder_path, "${id_run}_${file}";
  my $target = join q[/], $runfolder_path, $file;
  fmove($source, $target);
}

my $samplesheet_path = join q[/], $runfolder_path, $bbcals_relative,
  q[metadata_cache_37416], q[samplesheet_37416.csv];

subtest 'object with no function order set - simple methods' => sub {
  plan tests => 7;

  my $pluggable = npg_pipeline::pluggable->new(
    id_run              => 1234,
    runfolder_path      => $test_dir,
    npg_tracking_schema => undef
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
  plan tests => 2;

  my $obj = npg_pipeline::pluggable->new(
    id_run              => 1234,
    runfolder_path      => $test_dir,
    function_list       => "$config_dir/function_list_central.json",
    npg_tracking_schema => undef
  );
  lives_ok {$obj->function_graph()}
   'no error creating a graph for default analysis';

  $obj = npg_pipeline::pluggable->new(
    id_run              => 1234,
    runfolder_path      => $test_dir,
    function_list       => "$config_dir/function_list_post_qc_review.json",
    npg_tracking_schema => undef
  );
  lives_ok {  $obj->function_graph() }
    'no error creating a graph for default archival';
};

subtest 'graph creation from explicitly given function list' => sub {
  plan tests => 18;

  my $obj = npg_pipeline::pluggable->new(
    id_run         => 1234,
    runfolder_path => $runfolder_path,
    function_order => [qw/run_analysis_in_progress lane_analysis_in_progress/],
    function_list => "$config_dir/function_list_central.json",
    npg_tracking_schema => undef
  );
  ok($obj->has_function_order(), 'function order is set');
  is(join(q[ ], @{$obj->function_order}), 'run_analysis_in_progress lane_analysis_in_progress',
   'function order as set');
  lives_ok {  $obj->function_graph() }
    'no error creating a graph for a preset function order list';

  my $g = $obj->function_graph();
  is($g->vertices(), 4, 'four graph nodes');

  my @p = $g->predecessors('run_analysis_in_progress');
  is (scalar @p, 1, 'one predecessor');
  is ($p[0], 'pipeline_start', 'pipeline_start is before run_analysis_in_progress');
  ok ($g->is_source_vertex('pipeline_start'), 'pipeline_start is source vertex');
  my @s = $g->successors('run_analysis_in_progress');
  is (scalar @s, 1, 'one successor');
  is ($s[0], 'lane_analysis_in_progress', 'lane_analysis_in_progress is after run_analysis_in_progress');

  @p = $g->predecessors('lane_analysis_in_progress');
  is (scalar @p, 1, 'one predecessor');
  is ($p[0], 'run_analysis_in_progress', 'run_analysis_in_progress is before lane_analysis_in_progress');
  @s = $g->successors('lane_analysis_in_progress');
  is (scalar @s, 1, 'one successor');
  is ($s[0], 'pipeline_end', 'lane_analysis_in_progress is before pipeline_end');

  ok ($g->is_sink_vertex('pipeline_end'), 'pipeline_end is sink vertex');
  @p = $g->predecessors('pipeline_end');
  is (scalar @p, 1, 'one predecessor');

  $obj = npg_pipeline::pluggable->new(
    id_run              => 1234,
    function_order      => [qw/pipeline_end/],
    runfolder_path      => $test_dir,
    function_list       => "$config_dir/function_list_central.json",
    npg_tracking_schema => undef
  );
  throws_ok { $obj->function_graph() }
    qr/Graph is not DAG/,
    'pipeline_end cannot be specified in function order';

  $obj = npg_pipeline::pluggable->new(
    function_order      => [qw/pipeline_start/],
    runfolder_path      => $test_dir,
    no_bsub             => 1,
    function_list       => "$config_dir/function_list_central.json",
    npg_tracking_schema => undef
  );
  throws_ok { $obj->function_graph() }
    qr/Graph is not DAG/,
    'pipeline_start cannot be specified in function order';

  $obj = npg_pipeline::pluggable->new(
    function_order      => ['invalid_function'],
    runfolder_path      => $test_dir,
    function_list       => "$config_dir/function_list_central.json",
    npg_tracking_schema => undef
  );
  throws_ok {$obj->function_graph()}
    qr/Function invalid_function cannot be found in the graph/;
};

subtest 'switching off functions' => sub {
  plan tests => 7;

  my $p = npg_pipeline::pluggable->new(
    runfolder_path      => $runfolder_path,
    no_irods_archival   => 1,
    no_warehouse_update => 1,
    function_list       => "$config_dir/function_list_central.json",
    npg_tracking_schema => undef
  );

  lives_ok { $p->function_graph } 'A graph!';

  # Both function name and ID are the same in most cases
  my $fn_name_id = 'archive_to_irods_samplesheet';
  my $fn_ml_name_id = 'archive_to_irods_ml_warehouse';

  ok(
    ($p->_run_function($fn_name_id, $fn_name_id)->[0]->excluded
      && $p->_run_function($fn_ml_name_id, $fn_ml_name_id)->[0]->excluded),
    'archival to irods switched off');
  ok($p->_run_function('update_ml_warehouse', 'update_ml_warehouse')->[0]->excluded,
    'update to warehouse switched off');

  $p = npg_pipeline::pluggable->new(
    runfolder_path      => $runfolder_path,
    local               => 1,
    function_list       => "$config_dir/function_list_central.json",
    npg_tracking_schema => undef
  );
  ok(($p->_run_function($fn_name_id, $fn_name_id)->[0]->excluded &&
      $p->_run_function($fn_ml_name_id, $fn_ml_name_id)->[0]->excluded),
    'archival to irods switched off');
  ok($p->no_summary_link, 'summary_link switched off');

  $p = npg_pipeline::pluggable->new(
    runfolder_path      => $runfolder_path,
    local               => 1,
    no_warehouse_update => 0,
    function_list       => "$config_dir/function_list_central.json",
    npg_tracking_schema => undef
  );
  ok(($p->_run_function($fn_name_id, $fn_name_id)->[0]->excluded &&
      $p->_run_function($fn_ml_name_id, $fn_ml_name_id)->[0]->excluded),
    'archival to irods switched off');
  ok($p->no_summary_link, 'summary_link switched off');
};

subtest 'specifying functions via function_order' => sub {
  plan tests => 4;

  my @functions_in_order = qw(
    run_archival_in_progress
    upload_auto_qc_to_qc_database
    run_run_archived
    run_qc_complete
  );

  local $ENV{'PATH'} = join q[:], 't/bin', $ENV{'PATH'}; # mock LSF clients
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = $samplesheet_path;
  my $p = npg_pipeline::pluggable->new(
    function_order         => \@functions_in_order,
    runfolder_path         => $runfolder_path,
    spider                 => 0,
    no_sf_resource         => 1,
    no_bsub                => 0,
    is_indexed             => 0,
    product_conf_file_path => $product_config,
    function_list => "$config_dir/function_list_post_qc_review.json",
    npg_tracking_schema    => undef
  );
  is($p->id_run, $id_run, 'run id set correctly');
  is($p->is_indexed, 0, 'is not indexed');
  is(join(q[ ], @{$p->function_order()}), join(q[ ], @functions_in_order),
    q{function_order set on creation});
  lives_ok { $p->main() } q{no error running main};
};

subtest 'creating executor object' => sub {
  plan tests => 13;

  my $ref = {
    function_order         => [qw/run_archival_in_progress/],
    runfolder_path         => $runfolder_path,
    bam_basecall_path      => $runfolder_path,
    spider                 => 0,
    product_conf_file_path => $product_config,
    function_list => "$config_dir/function_list_post_qc_review.json",
    npg_tracking_schema    => undef
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
    my $path1 = join q[],$runfolder_path, "/t_10-pluggable.t_${id_run}_",
      $pl->timestamp, q[-];
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
  );

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = $samplesheet_path;

  my $ref = {
    function_order        => \@functions_in_order,
    runfolder_path        => $runfolder_path,
    spider                => 0,
    function_list => "$config_dir/function_list_post_qc_review.json"
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
  );

  my $ref = {
    function_order => \@functions_in_order,
    runfolder_path => $runfolder_path,
    spider         => 0,
    execute        => 0,
    no_sf_resource => 1,
    is_indexed     => 0,
    product_conf_file_path => $product_config,
    function_list => "$config_dir/function_list_post_qc_review.json"
  };

  my $p = npg_pipeline::pluggable->new($ref);
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = $samplesheet_path;
  lives_ok { $p->main(); } q{no error running main without execution };

  $ref->{'execute'} = 1;
  $ref->{'no_bsub'} = 1;
  $p = npg_pipeline::pluggable->new($ref);
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = $samplesheet_path;
  lives_ok { $p->main(); } q{no error running main in no_bsub mode};

  $ref->{'no_bsub'} = 0;
  local $ENV{'PATH'} = join q[:], 't/bin', $ENV{'PATH'}; # mock LSF clients
  $p = npg_pipeline::pluggable->new($ref);
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = $samplesheet_path;
  lives_ok { $p->main(); } q{no error running main with mock LSF client};

  # soft-link bresume command to /bin/false so that it fails
  my $bin = "$test_dir/bin";
  mkdir $bin;
  symlink '/bin/false', "$bin/bresume";
  local $ENV{'PATH'} = join q[:], $bin, $ENV{'PATH'};
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = $samplesheet_path;
  throws_ok { npg_pipeline::pluggable->new($ref)->main() }
    qr/Failed to submit command to LSF/, q{error running main};

  $ref->{'interactive'} = 1;
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = $samplesheet_path;
  lives_ok { npg_pipeline::pluggable->new($ref)->main() }
    'no failure in interactive mode';

  $ref->{'interactive'} = 0;
  # soft-link bkill command to /bin/false so that it fails
  symlink '/bin/false', "$bin/bkill";
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = $samplesheet_path;
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
  );

  my $ref = {
    function_order => \@functions_in_order,
    runfolder_path => $runfolder_path,
    spider         => 0,
    execute        => 0,
    executor_type  => 'wr',
    is_indexed     => 0,
    product_conf_file_path => $product_config,
    function_list => "$config_dir/function_list_post_qc_review.json"
  };

  # soft-link wr command to /bin/false so that it fails
  my $bin = "$test_dir/bin";
  my $wr = "$bin/wr";
  symlink '/bin/false', $wr;
  local $ENV{'PATH'} = join q[:], $bin, $ENV{'PATH'};

  my $p = npg_pipeline::pluggable->new($ref);
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = $samplesheet_path;
  lives_ok { $p->main(); } q{no error running main without execution };

  $ref->{'execute'} = 1;
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = $samplesheet_path;
  throws_ok { npg_pipeline::pluggable->new($ref)->main() }
    qr/Error submitting for execution: Error submitting wr jobs/,
    q{error running main};

  $ref->{'interactive'} = 1;
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = $samplesheet_path;
  lives_ok { npg_pipeline::pluggable->new($ref)->main() }
    q{interactive mode, no error running main};

  # soft-link wr command to /bin/true so that it succeeds
  unlink $wr;
  symlink '/bin/true', $wr;
  $ref->{'interactive'} = 0;
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = $samplesheet_path;
  lives_ok { npg_pipeline::pluggable->new($ref)->main() } q{no error running main};

  $ref->{'job_name_prefix'} = 'test';
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = $samplesheet_path;
  lives_ok { npg_pipeline::pluggable->new($ref)->main() }
    q{job name prefix is set, no error running main};
};

subtest 'positions and spidering' => sub {
  plan tests => 9;

  local $ENV{'PATH'} = join q[:], 't/bin', $ENV{'PATH'}; # mock LSF clients
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = $samplesheet_path;
  my $p = npg_pipeline::pluggable->new(
    id_flowcell_lims => 2015,
    runfolder_path   => $runfolder_path,
    spider           => 0,
    function_list    => "$config_dir/function_list_central.json"
  );
  ok(!$p->spider, 'spidering is off');
  is (join( q[ ], $p->positions), '1 2 3 4', 'positions array');

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = $samplesheet_path;
  my $function = 'run_analysis_complete';

  $p = npg_pipeline::pluggable->new(
    id_flowcell_lims       => 2015,
    function_order         => [$function],
    runfolder_path         => $runfolder_path,
    lanes                  => [1,2],
    spider                 => 0,
    no_sf_resource         => 1,
    product_conf_file_path => $product_config,
    function_list          => "$config_dir/function_list_central.json"
  );
  is (join( q[ ], $p->positions), '1 2', 'positions array');
  ok(!$p->interactive, 'start job will be resumed');
  lives_ok { $p->main() } "running main for $function, non-interactively";

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = $samplesheet_path;
  $p = npg_pipeline::pluggable->new(
    id_flowcell_lims       => 2015,
    function_order         => [$function],
    runfolder_path         => $runfolder_path,
    lanes                  => [1,2],
    interactive            => 1,
    spider                 => 0,
    no_sf_resource         => 1,
    product_conf_file_path => $product_config,
    function_list          => "$config_dir/function_list_central.json"
  );
  ok($p->interactive, 'start job will not be resumed');
  lives_ok { $p->main() } "running main for $function, interactively";

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = $samplesheet_path;
  $p = npg_pipeline::pluggable->new(
    function_order   => [qw{qc_qX_yield qc_adapter qc_insert_size}],
    lanes            => [4],
    runfolder_path   => $runfolder_path,
    no_bsub          => 1,
    repository       => q{t/data/sequence},
    id_flowcell_lims => 2015,
    spider           => 0,
    no_sf_resource   => 1,
    product_conf_file_path => $product_config,
    function_list => "$config_dir/function_list_central.json"
  );
  mkdir $p->archive_path;
  is (join( q[ ], $p->positions), '4', 'positions array');
  lives_ok { $p->main() } q{running main for three qc functions};
};

subtest 'script name, pipeline name and function list' => sub {
  plan tests => 17;

  my $base = npg_pipeline::pluggable->new(
    function_list => "$config_dir/function_list_central.json"
  );
  is ($base->_script_name, $PROGRAM_NAME, 'script name');
  is ($base->_pipeline_name, '10-pluggable.t', 'pipeline name');
  throws_ok { npg_pipeline::pluggable->new()->function_list }
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

  fcopy($path, $test_dir);
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

subtest 'log file name, directory and path' => sub {
  plan tests => 18;

  my $log_name_re = qr/t_10-pluggable\.t_${id_run}_02122020-\d+\.log/;

  my $p = npg_pipeline::pluggable->new(
    runfolder_path   => $runfolder_path,
    timestamp        => '02122020',
  );
  like ($p->log_file_name, $log_name_re, 'log file name is built correctly');
  is ($p->log_file_dir, $runfolder_path, 'default for the log directory');
  is ($p->log_file_path, join(q[/], $p->log_file_dir, $p->log_file_name),
    'default log file path');

  $p = npg_pipeline::pluggable->new(
    runfolder_path   => $runfolder_path,
    timestamp        => '02122020',
    log_file_name    => 'custom.log',
  );
  is ($p->log_file_name, 'custom.log', 'log file name as set');
  is ($p->log_file_dir, $runfolder_path, 'default for the log directory');
  is ($p->log_file_path, join(q[/], $p->log_file_dir, $p->log_file_name),
    'custom log file path');

  $p = npg_pipeline::pluggable->new(
    runfolder_path   => $runfolder_path,
    timestamp        => '02122020',
    log_file_dir     => "$runfolder_path/custom",
  );
  like ($p->log_file_name, $log_name_re, 'default log file name');
  is ($p->log_file_dir, "$runfolder_path/custom", 'log directory as set');
  is ($p->log_file_path, join(q[/], "$runfolder_path/custom", $p->log_file_name),
    'custom log file path');

  $p = npg_pipeline::pluggable->new(
    runfolder_path   => $runfolder_path,
    timestamp        => '02122020',
    log_file_dir     => "$runfolder_path/custom",
    log_file_name    => 'custom.log',
  );
  is ($p->log_file_name, 'custom.log', 'log file name as set');
  is ($p->log_file_dir, "$runfolder_path/custom" , 'log directory as set');
  is ($p->log_file_path, "$runfolder_path/custom/custom.log",
    'custom log file path');

  # setting all three does not make sense, but is not prohibited either
  $p = npg_pipeline::pluggable->new(
    runfolder_path   => $runfolder_path,
    timestamp        => '02122020',
    log_file_dir     => "$runfolder_path/my_log",
    log_file_name    => 'custom.log',
    log_file_path    => "$runfolder_path/custom/my.log",
  );
  is ($p->log_file_name, 'custom.log', 'log file name as set');
  is ($p->log_file_dir, "$runfolder_path/my_log", 'log directory as set');
  is ($p->log_file_path, "$runfolder_path/custom/my.log",
    'custom log file path as directly set');

  $p = npg_pipeline::pluggable->new(
    runfolder_path   => $runfolder_path,
    timestamp        => '02122020',
    log_file_path    => "$runfolder_path/custom/my.log"
  );
  is ($p->log_file_name, 'my.log', 'log file name is derived from path');
  is ($p->log_file_dir, "$runfolder_path/custom",
    'log directory is derived from path');
  is ($p->log_file_path, "$runfolder_path/custom/my.log",
    'custom log file path as directly set');
};

subtest 'Copy log file and product_release config' => sub {
  plan tests => 7;

  my $p = npg_pipeline::pluggable->new(
    runfolder_path   => $runfolder_path,
    timestamp        => '02122020',
    log_file_dir     => $test_dir,
    log_file_name    => 'logfile',
    product_conf_file_path => $product_config
  );
  $p->_copy_log_to_analysis_dir();
  my $analysis_path = $p->analysis_path;
  my $default_log_copy = $analysis_path.'/logfile';
  ok(-f -s $default_log_copy, "Log file found in analysis path at $analysis_path");
  my $copy = $p->_save_product_conf_to_analysis_dir();
  ok(-f -s $analysis_path.'/'.$copy, 'Copy of product config is present');

  # Set log file path to something false to show error behaviour is fine
  $p = npg_pipeline::pluggable->new(
    runfolder_path   => '/nope',
    timestamp        => '02122020',
    log_file_dir     => $test_dir,
    log_file_name    => 'logfile',
    product_conf_file_path => $product_config
  );
  lives_ok {$p->_copy_log_to_analysis_dir()} 'Log copy to nonexistant runfolder does not die';

  $p = npg_pipeline::pluggable->new(
    runfolder_path   => $runfolder_path,
    timestamp        => '02122020',
    log_file_dir     => '/nuthin',
    log_file_name    => 'logfile',
    product_conf_file_path => $product_config
  );
  lives_ok {$p->_copy_log_to_analysis_dir()} 'Log copy of nonexistant file does not die';

  # Test what happens when no log_file_name is provided
  unlink $default_log_copy;
  ok(! -e $default_log_copy, 'Any log copy removed');
  lives_ok {
    $p->_tolerant_persist_file_to_analysis_dir($test_dir.'/logfile', undef)
  } 'missing argument defaults to using the original file name';
  note 'Checking for logfile copy in '.$p->analysis_path.' copied from '.$test_dir;
  ok (-f $default_log_copy, 'Log named with default when no other given');
};

subtest 'Check resource population from graph' => sub {
  plan tests => 2;

  my $p = npg_pipeline::pluggable->new(
    id_run => $id_run,
    function_order => ['run_analysis_complete'],
    function_list => "$config_dir/function_list_central.json"
  );
  my $graph = $p->function_graph;
  cmp_ok($graph->vertices, '==', 3, 'Expected number of vertices');
  my @attr_names = $graph->get_vertex_attribute_names('run_analysis_complete');
  ok ( any { $_ eq 'resources' } @attr_names, 'Resources loaded');
};

subtest 'Checking resources are assigned correctly from graph' => sub {
  plan tests => 4;
  # Check resources for functions are correctly merged with pipeline-wide settings
  my $p = npg_pipeline::pluggable->new(
    id_run => $id_run,
    function_list => "$config_dir/function_list_central.json"
  );
  my $resources = $p->_function_resource_requirements('update_ml_warehouse_1', 'update_ml_warehouse');
  is_deeply(
    $resources,
    {
      default => {
        minimum_cpu => 0,
        memory => 2,
        array_cpu_limit => 64, # this will be removed when create_definition() is called
        queue => 'lowload',
        db => [
          'mlwh'
        ]
      }
    }
  );

  $p = npg_pipeline::pluggable->new(
    id_run => $id_run,
    function_list => "$config_dir/function_list_post_qc_review.json"
  );
  $resources = $p->_function_resource_requirements('run_run_archived', 'run_run_archived');
  is_deeply(
    $resources,
    {
      default => {
        minimum_cpu => 0,
        queue => 'small',
        memory => 2,
        array_cpu_limit => 64
      }
    }
  );

  throws_ok {
    $p->_run_function('run_run_archived', undef)
  }
  qr{Function run requires both label/name and id},
  'Missing function ID causes resource failure';

  throws_ok {
    $p->_run_function(undef, 'run_run_archived')
  }
  qr{Function run requires both label/name and id},
  'Missing function name causes resource failure';
};
