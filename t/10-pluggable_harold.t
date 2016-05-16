use strict;
use warnings;
use Test::More tests => 57;
use Test::Deep;
use Test::Exception;
use t::util;
use t::dbic_util;

local $ENV{PATH} = join q[:], q[t/bin], q[t/bin/software/solexa/bin], $ENV{PATH};
local $ENV{http_proxy} = 'http://wibble';
local $ENV{no_proxy}   = q[];

my $util = t::util->new();

$ENV{TEST_DIR} = $util->temp_directory();

use_ok('npg_pipeline::pluggable::harold');

my $analysis_runfolder_path = $util->analysis_runfolder_path();
my $schema = t::dbic_util->new->test_schema();

{
  my $harold;
  my @functions_in_order = qw(
      create_archive_directory
      create_fastq
      create_fastqcheck
      create_gcfreq
      create_md5
    );
  lives_ok {
    $harold = npg_pipeline::pluggable::harold->new(
      id_run => 1234,
      function_order => \@functions_in_order,
      run_folder => q{123456_IL2_1234},
      runfolder_path => $analysis_runfolder_path,
    );
  } q{no croak on creation};
  isa_ok($harold, q{npg_pipeline::pluggable::harold}, q{$harold});

  ok($harold->spider, 'spidering is on');

  push @functions_in_order, 'lsf_end';
  unshift @functions_in_order, 'lsf_start';
  is(join(q[ ], @{$harold->function_order()}), join(q[ ],@functions_in_order), q{function order set on creation and wrapped correctly});
  is($harold->pipeline_name, 'harold', 'pipeline name');
  lives_ok { $harold->parallelise(); } q{no croak obtaining parallelise_hash};

  no warnings 'once';
  foreach my $function (@npg_pipeline::pluggable::harold::SAVE2FILE_STATUS_FUNCTIONS) {
    ok ($harold->can($function), qq{method $function is defined});
  }
  foreach my $function (@npg_pipeline::pluggable::harold::AUTOQC_FUNCTIONS) {
    ok ($harold->can($function), qq{method $function is defined});
  }
}

$util->set_staging_analysis_area({with_latest_summary => 1});
{
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];
  my $harold = npg_pipeline::pluggable::harold->new(
      id_run => 1234,
      run_folder => q{123456_IL2_1234},
      function_order => [],
      runfolder_path => $analysis_runfolder_path,
      spider         => 0,
    );
  ok(!$harold->interactive, 'start job will be resumed');
  lives_ok { $harold->main() } 'main method with no functions defined and no spider plugged in runs ok';

  is(scalar @{$harold->dispatch_tree->functions}, 2, 'two functions');
  is($harold->dispatch_tree->functions->[0]->{function}, 'lsf_start', 'first function is start');
  is($harold->dispatch_tree->functions->[1]->{function}, 'lsf_end', 'second function is end');

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];
  $harold = npg_pipeline::pluggable::harold->new(
      id_run => 1234,
      run_folder => q{123456_IL2_1234},
      runfolder_path => $analysis_runfolder_path,
      function_order => [],
      interactive => 1,
      spider      => 0,
    );
  ok($harold->interactive, 'start job will not be resumed');
  is (join( q[ ], $harold->positions), '1 2 3 4 5 6 7 8', 'positions array');
  is (join( q[ ], $harold->all_positions), '1 2 3 4 5 6 7 8', 'all positions array');
  lives_ok { $harold->main() } 'main method with no functions defined and no resume runs ok';
  is(scalar @{$harold->dispatch_tree->functions}, 2, 'two functions');

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];
  my $function = 'run_analysis_complete';
  $harold = npg_pipeline::pluggable::harold->new(
      id_run => 1234,
      run_folder => q{123456_IL2_1234},
      function_order => [$function],
      runfolder_path => $analysis_runfolder_path,
      lanes => [1,2],
      no_bsub => 1,
      spider  => 0,
    );
  is(scalar @{$harold->dispatch_tree->functions}, 0, 'no functions');
  is (join( q[ ], $harold->positions), '1 2', 'positions array');
  is (join( q[ ], $harold->all_positions), '1 2 3 4 5 6 7 8', 'all positions array');
  $harold->main;
  is(scalar @{$harold->dispatch_tree->functions}, 3, 'three functions');
  is($harold->dispatch_tree->functions->[0]->{function}, 'lsf_start', 'first function is start');
  is($harold->dispatch_tree->functions->[1]->{function}, $function, qq{first function is $function});
  is($harold->dispatch_tree->functions->[2]->{function}, 'lsf_end', 'third function is end');
  is($harold->dispatch_tree->functions->[2]->{job_dependencies}, q{-w'done(50)'}, 'end dependencies');

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];
  $harold = npg_pipeline::pluggable::harold->new(
      id_run => 1234,
      run_folder => q{123456_IL2_1234},
      function_order => [$function, 'dodo'],
      runfolder_path => $analysis_runfolder_path,
      resume_start_job => 0,
      lanes => [1,2],
      no_bsub => 1,
      spider  => 0,
  );
  throws_ok { $harold->main} qr/Error submitting jobs: Can't locate object method "dodo" via package "npg_pipeline::pluggable::harold"/, 'error when non-existing function is in the function order';
}

{
  my $harold;
  lives_ok {
    $harold = npg_pipeline::pluggable::harold->new(
      id_run => 6588,
      lanes => [1..8],
      run_folder => q{123456_IL2_1234},
      runfolder_path => $analysis_runfolder_path,
      npg_tracking_schema => $schema,
      no_bsub => 1,
    );
  } q{no croak on creation};

  lives_ok {
    $harold->lane_analysis_in_progress();
  } q{run update_run_lane_analysis_in_progress ok - no dependencies};
  lives_ok {
    $harold->lane_analysis_complete({
      required_job_completion => q{-w'done(123) && done(321)'},
    });
  } q{run update_run_lane_analysis_complete ok - dependencies};
}

{
  my $qc;
  lives_ok {
    $qc = npg_pipeline::pluggable::harold->new(
      id_run => 1234,
      function_order => [qw(qc_qX_yield qc_insert_size)],
      run_folder => q{123456_IL2_1234},
      runfolder_path => $analysis_runfolder_path,
    );
  } q{no croak on creation};
  $util->set_staging_analysis_area({with_latest_summary => 1});
  isa_ok($qc, q{npg_pipeline::pluggable::harold}, '$qc');
  is(join(q[ ], @{$qc->function_order()}), 'lsf_start qc_qX_yield qc_insert_size lsf_end', 'function_order set on creation');
}

{
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];
  my $qc;
  $util->set_staging_analysis_area();
  lives_ok {
    $qc = npg_pipeline::pluggable::harold->new(
      id_run => 1234,
      run_folder => q{123456_IL2_1234},
      function_order => [qw{qc_qX_yield qc_adapter qc_insert_size}],
      lanes => [4],
      runfolder_path => $analysis_runfolder_path,
      no_bsub => 1,
      repository => q{t/data/sequence},
      id_flowcell_lims => 2015,
      spider           => 0,
    );
  } q{no croak on new creation};
  mkdir $qc->archive_path;
  mkdir $qc->qc_path;
  is (join( q[ ], $qc->positions), '4', 'positions array');
  is (join( q[ ], $qc->all_positions), '1 2 3 4 5 6 7 8', 'all positions array');
  lives_ok { $qc->main() } q{no croak running qc->main()};
}

1;
