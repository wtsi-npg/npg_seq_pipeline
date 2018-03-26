use strict;
use warnings;
use Test::More tests => 8;
use Test::Exception;
use File::Path qw/make_path/;
use t::util;

use_ok('npg_pipeline::function::autoqc');

my $util = t::util->new();
my $tmp = $util->temp_directory();

my @tools = map { "$tmp/$_" } qw/bamtofastq blat norm_fit/;
foreach my $tool (@tools) {
  open my $fh, '>', $tool or die 'cannot open file for writing';
  print $fh $tool or die 'cannot print';
  close $fh or warn 'failed to close file handle';
}
chmod 0755, @tools;
local $ENV{'PATH'} = join q[:], $tmp, $ENV{'PATH'};

my $recalibrated = $util->analysis_runfolder_path() .
                  q{/Data/Intensities/Bustard1.3.4_09-07-2009_auto/PB_cal};
my $pbcal = $recalibrated;

subtest 'errors' => sub {
  plan tests => 2;

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[];
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/qc/samplesheet_14353.csv';

  throws_ok {
    npg_pipeline::function::autoqc->new(
      runfolder_path    => $util->analysis_runfolder_path(),
      recalibrated_path => $recalibrated,
    )
  } qr/Attribute \(qc_to_run\) is required/,
  q{error creating object as no qc_to_run provided};

  throws_ok { npg_pipeline::function::autoqc->new(
      id_run     => 14353,
      qc_to_run  => 'some_check',
      is_indexed => 1)->create();
  } qr/Can\'t locate npg_qc\/autoqc\/checks\/some_check\.pm/,
    'non-existing check name - error';
};

subtest 'adapter' => sub {
  plan tests => 32;

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];
  $util->create_analysis({qc_dir => 1});
  my $aqc;
  lives_ok {
    $aqc = npg_pipeline::function::autoqc->new(
      runfolder_path    => $util->analysis_runfolder_path(),
      recalibrated_path => $recalibrated,
      qc_to_run         => q{adapter},
      timestamp         => q{20090709-123456},
      is_indexed        => 0,
    );
  } q{no croak on new, as required params provided};

  my $da = $aqc->create();
  ok ($da && (@{$da} == 8), 'eight definitions returned');
  
  my $d = $da->[0];
  isa_ok ($d, 'npg_pipeline::function::definition');
  is ($d->created_by, 'npg_pipeline::function::autoqc', 'created by');
  is ($d->created_on, q{20090709-123456}, 'created on');
  is ($d->identifier, 1234, 'identifier');
  ok (!$d->excluded, 'step is not excluded');
  ok (!$d->immediate_mode, 'not immediate mode');
  is ($d->queue, 'default', 'default queue');
  is ($d->job_name, 'qc_adapter_1234_20090709-123456', 'job name');
  is ($d->fs_slots_num, 1, 'one sf slots');
  is ($d->num_hosts, 1, 'one host');
  ok ($d->apply_array_cpu_limit, 'array_cpu_limit should be applied');
  ok (!$d->has_array_cpu_limit, 'array_cpu_limit not set');
  is_deeply ($d->num_cpus, [2], 'num cpus as an array');
  is ($d->log_file_dir, "$pbcal/archive/qc/log", 'log dir');
  is ($d->memory, 1500, 'memory');
  is ($d->command_preexec, 'npg_pipeline_preexec_references', 'preexec command');
  ok ($d->has_composition, 'composition object is set');
  my $composition = $d->composition;
  isa_ok ($composition, 'npg_tracking::glossary::composition');
  is ($composition->num_components, 1, 'one component');
  my $component = $composition->get_component(0);
  is ($component->id_run, 1234, 'run id correct');
  is ($component->position, 1, 'position correct');
  ok (!defined $component->tag_index, 'tag index undefined');

  foreach my $de (@{$da}) {
    my $p = $de->composition->get_component(0)->position;
    is ($de->command,
    "qc --check=adapter --id_run=1234 --position=$p --file_type=bam --qc_in=$pbcal --qc_out=$pbcal/archive/qc",
    "adapter check command for lane $p");
  }
};

subtest 'qX_yield' => sub {
  plan tests => 27;

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];
  $util->create_analysis({'qc_dir' => 1});

  my $aqc = npg_pipeline::function::autoqc->new(
    runfolder_path    => $util->analysis_runfolder_path(),
    recalibrated_path => $recalibrated,
    qc_to_run         => q{qX_yield},
    timestamp         => q{20090709-123456},
    is_indexed        => 0,
  );
  my $da = $aqc->create();
  ok ($da && (@{$da} == 8), 'eight definitions returned');
  my $d = $da->[0];
  is ($d->queue, 'default', 'default queue');
  is ($d->job_name, 'qc_qX_yield_1234_20090709-123456', 'job name');
  ok (!$d->has_memory, 'memory is not set');
  ok ($d->apply_array_cpu_limit, 'array_cpu_limit should be applied');
  ok (!$d->has_array_cpu_limit, 'array_cpu_limit not set');
  is ($d->fs_slots_num, 1, 'one sf slots');
  ok (!$d->has_num_cpus, 'num cpus is not set');
  ok (!$d->has_num_hosts, 'num hosts is not set');
  ok (!$d->has_command_preexec, 'preexec command is not set');
  is ($d->log_file_dir, "$pbcal/archive/qc/log", 'log dir');

  foreach my $de (@{$da}) {
    my $p = $de->composition->get_component(0)->position;
    is ($de->command,
    "qc --check=qX_yield --id_run=1234 --position=$p --qc_in=$pbcal/archive --qc_out=$pbcal/archive/qc",
    "qX_yield check command for lane $p");
  }

  $aqc = npg_pipeline::function::autoqc->new(
      runfolder_path    => $util->analysis_runfolder_path(),
      recalibrated_path => $recalibrated,
      qc_to_run         => q{qX_yield},
      lanes             => [4],
      timestamp         => q{20090709-123456},
      is_indexed        => 0,
  );
  $da = $aqc->create();
  ok ($da && (@{$da} == 1), 'one definition returned');
  is ($da->[0]->command,
      "qc --check=qX_yield --id_run=1234 --position=4 --qc_in=$pbcal/archive --qc_out=$pbcal/archive/qc",
      "qX_yield check command for lane 4");
  $util->create_multiplex_analysis({'qc_dir' => [7,8]});
  my $runfolder_path = $util->analysis_runfolder_path();

  $aqc = npg_pipeline::function::autoqc->new(
    runfolder_path    => $runfolder_path,
    recalibrated_path => $recalibrated,
    lanes             => [7],
    qc_to_run         => q{qX_yield},
    timestamp         => q{20090709-123456},
  );
  is ($aqc->is_indexed, 1, 'run is indexed');

  $da = $aqc->create();
  ok ($da && (@{$da} == 1), 'one definition returned - lane is not a pool');
 
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[];
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/qc/1234_samplesheet_amended.csv';
  $aqc = npg_pipeline::function::autoqc->new(
    runfolder_path    => $runfolder_path,
    recalibrated_path => $recalibrated,
    lanes             => [8],
    qc_to_run         => q{qX_yield},
    timestamp         => q{20090709-123456},
  );

  $da = $aqc->create();
  ok ($da && (@{$da} == 3), '3 definitions returned - lane is a pool of one');
  my @plexes = grep { defined $_->composition->get_component(0)->tag_index} @{$da};
  is (@plexes, 2, 'two definitions for plexes');
  foreach my $d (@plexes) {
    my $t = $d->composition->get_component(0)->tag_index;
    is ($d->command,
    "qc --check=qX_yield --id_run=1234 --position=8 --tag_index=$t --qc_in=$pbcal/archive/lane8 --qc_out=$pbcal/archive/lane8/qc",
    "qX_yield command for lane 8 tag $t");
  }
};
  
subtest 'ref_match' => sub {
  plan tests => 16;

  $util->create_multiplex_analysis({'qc_dir' => [7,8]});
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[];
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/qc/1234_samplesheet_amended.csv';
  my $runfolder_path = $util->analysis_runfolder_path();

  my $aqc = npg_pipeline::function::autoqc->new(
    runfolder_path    => $runfolder_path,
    recalibrated_path => $recalibrated,
    lanes             => [8],
    qc_to_run         => q{ref_match},
    timestamp         => q{20090709-123456},
    repository        => 't/data/sequence',
  );
  my $da = $aqc->create();
  ok ($da && (@{$da} == 3), 'three definitions returned - lane is a pool');

  my $d = $da->[0];
  is ($d->queue, 'default', 'default queue');
  is ($d->job_name, 'qc_ref_match_1234_20090709-123456', 'job name');
  is ($d->fs_slots_num, 1, 'one sf slots');
  ok (!$d->has_num_hosts, 'number of hosts not set');
  ok ($d->apply_array_cpu_limit, 'apply array_cpu_limit');
  is ($d->array_cpu_limit, 8, '8 - array_cpu_limit');
  ok (!$d->has_num_cpus, 'num cpus is not set');
  is ($d->log_file_dir, "$pbcal/archive/qc/log", 'log dir');
  is ($d->memory, 6000, 'memory');
  is ($d->command_preexec,
    'npg_pipeline_preexec_references --repository t/data/sequence',
    'preexec command');
  ok (!$d->has_num_cpus, 'num cpus is not set');
  ok (!$d->has_num_hosts, 'num hosts is not set');

  my @plexes = grep { defined $_->composition->get_component(0)->tag_index} @{$da};
  is (@plexes, 2, 'two definitions for a plexes');
  foreach my $d (@plexes) {
    my $t = $d->composition->get_component(0)->tag_index;
    is ($d->command,
    "qc --check=ref_match --id_run=1234 --position=8 --tag_index=$t --qc_in=$pbcal/archive/lane8 --qc_out=$pbcal/archive/lane8/qc",
    "ref_match command for lane 8 tag $t");
  }
};

subtest 'insert_size' => sub {
  plan tests => 3;

  $util->create_multiplex_analysis({qc_dir => [7],});
  my $runfolder_path = $util->analysis_runfolder_path();

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];

  my $aqc = npg_pipeline::function::autoqc->new(
    runfolder_path    => $runfolder_path,
    recalibrated_path => $recalibrated,
    lanes             => [7],
    qc_to_run         => q{insert_size},
    timestamp         => q{20090709-123456},
    repository        => 't/data/sequence',
  );
  is ($aqc->is_indexed, 1, 'run is indexed');
  my $da = $aqc->create();
  ok ($da && (@{$da} == 1), 'one definition returned - lane is a not pool');

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[];
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/qc/samplesheet_14353.csv';

  $aqc = npg_pipeline::function::autoqc->new(
    id_run            => 14353,
    runfolder_path    => $util->analysis_runfolder_path(),
    recalibrated_path => $recalibrated,
    lanes             => [1],
    qc_to_run         => q{sequence_error},
    timestamp         => q{20090709-123456},
    is_indexed        => 0,
    repository        => 't/data/sequence',
  );
  $da = $aqc->create();
  ok ($da && (@{$da} == 1), 'one definition returned');
};

subtest 'tag_metrics' => sub {
  plan tests => 9;

  $util->create_multiplex_analysis({qc_dir => [1],});
  my $runfolder_path = $util->analysis_runfolder_path();

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[];
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/qc/samplesheet_14353.csv';

  my $qc = npg_pipeline::function::autoqc->new(
    qc_to_run         => 'tag_metrics',
    is_indexed        => 1,
    id_run            => 14353,
    runfolder_path    => $runfolder_path,
    recalibrated_path => $recalibrated,
    bam_basecall_path => $runfolder_path,
    timestamp         => q{20090709-123456},
  );

  ok( $qc->_should_run({id_run => 14353, position => 1}, 1),
    q{lane is multiplexed - run tag metrics on a lane} );
  ok( !$qc->_should_run({id_run => 14353, position => 1}, 0),
    q{lane is not multiplexed - do not run tag metrics on a lane} );
  ok( !$qc->_should_run({id_run => 14353, position => 1, tag_index => 1}, 1),
    q{do not run tag metrics on a plex} );
  ok( !$qc->_should_run({id_run => 14353, position => 1, tag_index => 1}, 0),
    q{do not run tag metrics on a plex} );

  my $da = $qc->create();
  ok ($da && (@{$da} == 1), 'one definition returned');
  my $d = $da->[0];
  ok (!$d->excluded, 'step is not excluded');
  is ($d->command,
      "qc --check=tag_metrics --id_run=14353 --position=1 --qc_in=$runfolder_path --qc_out=$pbcal/archive/qc",
      'tag metrics command for lane 1');

  $qc = npg_pipeline::function::autoqc->new(
    id_run     => 14353,
    qc_to_run  => 'tag_metrics',
    is_indexed => 0
  );
  $da = $qc->create();
  ok ($da && (@{$da} == 1), 'one definition returned');
  ok ($da->[0]->excluded, 'step is excluded');
};

subtest 'genotype and gc_fraction' => sub {
  plan tests => 9;

  my $rf_name = '140915_HS34_14043_A_C3R77ACXX';
  my $rf_path = join q[/], $tmp, $rf_name;
  mkdir $rf_path;
  my $analysis_dir = join q[/], $rf_path, 'Data', 'Intencities', 'BAM_basecalls_20141013-161026';
  my $archive_dir = join q[/], $analysis_dir, 'no_cal', 'archive';
  my $qc_dir = join q[/], $archive_dir, 'qc';
  my $lane6_dir = join q[/], $archive_dir, 'lane6';
  my $lane6_qc_dir = join q[/], $lane6_dir, 'qc';
  
  make_path($qc_dir);
  make_path($lane6_qc_dir);

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[];
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/qc/samplesheet_14043.csv';

  my $init = {
    id_run            => 14043,
    runfolder_path    => $rf_path,
    bam_basecall_path => $analysis_dir,
    archive_path      => $archive_dir,
    is_indexed        => 1,
    repository        => 't',
    qc_to_run         => q[genotype],
  };

  my $qc = npg_pipeline::function::autoqc->new($init);
  ok ($qc->_should_run({id_run => 14043, position => 1}, 0),
    'genotype check can run for a non-indexed lane');
  ok (!$qc->_should_run({id_run => 14043, position => 6}, 1),
    'genotype check cannot run for an indexed lane');
  ok ($qc->_should_run({id_run => 14043, position => 6, tag_index => 0}, 1),
    'genotype check can run for tag 0 (the only plex is a human sample)');
  ok ($qc->_should_run({id_run => 14043, position => 6, tag_index => 1}, 1),
    'genotype check can run for tag 1 (human sample)');
  ok (!$qc->_should_run({id_run => 14043, position => 6, tag_index => 168}, 1),
    'genotype check cannot run for a spiked phix tag');

  $init->{'qc_to_run'} = q[gc_fraction];

  $qc = npg_pipeline::function::autoqc->new($init);
  ok ($qc->_should_run({id_run => 14043, position => 6}, 1), 'gc_fraction check can run');
  ok ($qc->_should_run({id_run => 14043, position => 6}, 0), 'gc_fraction check can run');
  ok ($qc->_should_run({id_run => 14043, position => 6, tag_index => 0}, 1),
    'gc_fraction check can run');
  ok ($qc->_should_run({id_run => 14043, position => 6, tag_index => 0}, 1),
   'gc_fraction check can run');
};

1;
