use strict;
use warnings;
use Test::More tests => 41;
use Test::Exception;
use Log::Log4perl qw(:levels);
use t::util;
use t::dbic_util;

local $ENV{PATH} = join q[:], q[t/bin], q[t/bin/software/solexa/bin], $ENV{PATH};
local $ENV{http_proxy} = 'http://wibble';
local $ENV{no_proxy}   = q[];

my $util = t::util->new();
my $tdir = $util->temp_directory();
Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $tdir, 'logfile'),
                          utf8   => 1});

$ENV{TEST_DIR} = $tdir;

use_ok('npg_pipeline::pluggable::harold');

my $analysis_runfolder_path = $util->analysis_runfolder_path();
my $schema = t::dbic_util->new->test_schema();

$util->set_staging_analysis_area({with_latest_summary => 1});
{
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];
  my $harold;
  lives_ok {
    $harold = npg_pipeline::pluggable::harold->new(
      id_run         => 1234,
      run_folder     => q{123456_IL2_1234},
      runfolder_path => $analysis_runfolder_path,
      no_bsub        => 1,
      spider         => 0,
    );
  } q{no croak on creation};
  isa_ok($harold, q{npg_pipeline::pluggable::harold});

  is($harold->pipeline_name, 'harold', 'pipeline name');

  ok(!$harold->spider, 'spidering is off');

  is (join( q[ ], $harold->positions), '1 2 3 4 5 6 7 8', 'positions array');
  is (join( q[ ], $harold->all_positions), '1 2 3 4 5 6 7 8', 'all positions array');

  lives_ok {
    $harold->lane_analysis_in_progress();
  } q{run update_run_lane_analysis_in_progress ok};
  lives_ok {
    $harold->run_analysis_complete();
  } q{run run_analysis_complete ok};

  no warnings 'once';
  foreach my $function (@npg_pipeline::pluggable::harold::SAVE2FILE_STATUS_FUNCTIONS) {
    ok ($harold->can($function), qq{method $function is defined});
  }
  foreach my $function (@npg_pipeline::pluggable::harold::AUTOQC_FUNCTIONS) {
    ok ($harold->can($function), qq{method $function is defined});
  }
}

{
  my $harold;

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];
  my $function = 'run_analysis_complete';

  $harold = npg_pipeline::pluggable::harold->new(
      id_run         => 1234,
      run_folder     => q{123456_IL2_1234},
      function_order => [$function],
      runfolder_path => $analysis_runfolder_path,
      lanes          => [1,2],
      no_bsub        => 1,
      spider         => 0,
    );
  is (join( q[ ], $harold->positions), '1 2', 'positions array');
  is (join( q[ ], $harold->all_positions), '1 2 3 4 5 6 7 8', 'all positions array');
  ok(!$harold->interactive, 'start job will be resumed');
  lives_ok { $harold->main() } "running main for $function, non-interactively";

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];
  $harold = npg_pipeline::pluggable::harold->new(
      id_run         => 1234,
      run_folder     => q{123456_IL2_1234},
      function_order => [$function],
      runfolder_path => $analysis_runfolder_path,
      lanes          => [1,2],
      interactive    => 1,
      no_bsub        => 1,
      spider         => 0,
  );
  ok($harold->interactive, 'start job will not be resumed');
  lives_ok { $harold->main() } "running main for $function, interactively";

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];
  $util->set_staging_analysis_area();
  lives_ok {
    $harold = npg_pipeline::pluggable::harold->new(
      id_run           => 1234,
      run_folder       => q{123456_IL2_1234},
      function_order   => [qw{qc_qX_yield qc_adapter qc_insert_size}],
      lanes            => [4],
      runfolder_path   => $analysis_runfolder_path,
      no_bsub          => 1,
      repository       => q{t/data/sequence},
      id_flowcell_lims => 2015,
      spider           => 0,
    );
  } q{no croak on new creation};
  mkdir $harold->archive_path;
  mkdir $harold->qc_path;
  is (join( q[ ], $harold->positions), '4', 'positions array');
  is (join( q[ ], $harold->all_positions), '1 2 3 4 5 6 7 8', 'all positions array');
  lives_ok { $harold->main() } q{running main for three qc functions};
}

1;
