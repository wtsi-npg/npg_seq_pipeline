use strict;
use warnings;
use Test::More tests => 12;
use Test::Exception;
use File::Path qw/make_path/;
use File::Copy::Recursive qw/fcopy dircopy/;
use File::Slurp;

use t::util;

use_ok('npg_pipeline::function::autoqc');
use_ok('st::api::lims');
use_ok('npg_tracking::glossary::composition');
use_ok('npg_tracking::glossary::rpt');

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

my $hiseq_rf = $util->create_runfolder($tmp,
    {runfolder_name => 'function_adapter',
     analysis_path  => 'BAM_basecalls_20180802'});

my $archive_dir = $hiseq_rf->{'archive_path'};
my $rf_path     = $hiseq_rf->{'runfolder_path'};
fcopy('t/data/run_params/runParameters.hiseq.xml', "$rf_path/runParameters.xml")
  or die 'Fail to copy run param file';

subtest 'errors' => sub {
  plan tests => 2;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/qc/samplesheet_14353.csv';

  throws_ok {
    npg_pipeline::function::autoqc->new(id_run => 14353)
  } qr/Attribute \(qc_to_run\) is required/,
  q{error creating object as no qc_to_run provided};

  throws_ok { npg_pipeline::function::autoqc->new(
      id_run     => 14353,
      qc_to_run  => 'some_check')->create();
  } qr/Can\'t locate npg_qc\/autoqc\/checks\/some_check\.pm/,
    'non-existing check name - error';
};

subtest 'adapter' => sub {
  plan tests => 32;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/samplesheet_1234.csv';
 
  my $aqc;
  lives_ok {
    $aqc = npg_pipeline::function::autoqc->new(
      id_run            => 1234,
      runfolder_path    => $rf_path,
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
  is ($d->queue, 'default', 'default queue');
  is ($d->job_name, 'qc_adapter_1234_20090709-123456', 'job name');
  is ($d->fs_slots_num, 1, 'one sf slots');
  is ($d->num_hosts, 1, 'one host');
  ok ($d->apply_array_cpu_limit, 'array_cpu_limit should be applied');
  ok (!$d->has_array_cpu_limit, 'array_cpu_limit not set');
  is_deeply ($d->num_cpus, [2], 'num cpus as an array');
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
    is ($de->command, sprintf(
    'qc --check=adapter --rpt_list=%s --filename_root=%s --qc_out=%s --input_files=%s',
    qq["1234:${p}"], "1234_${p}", "$archive_dir/lane${p}/qc", "$archive_dir/lane${p}/1234_${p}.bam"),
    "adapter check command for lane $p");
  }

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/samplesheet_8747.csv';
  $aqc = npg_pipeline::function::autoqc->new(
    runfolder_path    => $rf_path,
    id_run            => 8747,
    qc_to_run         => q{adapter},
    lanes             => [1],
    timestamp         => q{20090709-123456},
    is_indexed        => 1,
  );
  $da = $aqc->create();
  ok ($da && (@{$da} == 4), 'five definitions returned - plexes only');
  is (scalar(grep { /--rpt_list=\"\d+:\d+:\d+\"/smx} map {$_->command} @{$da}), 4,
    'all commands are plex-level');
};

subtest 'spatial_filter' => sub {
  plan tests => 17;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/samplesheet_1234.csv';

  my $aqc = npg_pipeline::function::autoqc->new(
    runfolder_path    => $rf_path,
    id_run            => 1234,
    qc_to_run         => q{spatial_filter},
    timestamp         => q{20090709-123456},
    is_indexed        => 0,
  );

  my $da = $aqc->create();
  ok ($da && (@{$da} == 8), 'eight definitions returned');
  my $d = $da->[0];
  isa_ok ($d, 'npg_pipeline::function::definition');

  foreach my $de (@{$da}) {
    my $p = $de->composition->get_component(0)->position;
    is ($de->command, sprintf(
    'qc --check=spatial_filter --rpt_list=%s --filename_root=%s --qc_out=%s --input_files=%s',
    qq["1234:${p}"], "1234_${p}", "$archive_dir/lane${p}/qc", "$archive_dir/lane${p}/1234_${p}.spatial_filter.stats"),
    "spatial filter check command for lane $p, lane not indexed");
  }

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/samplesheet_8747.csv';
  $aqc = npg_pipeline::function::autoqc->new(
    runfolder_path    => $rf_path,
    id_run            => 8747,
    qc_to_run         => q{spatial_filter},
    lanes             => [(1 .. 6)],
    timestamp         => q{20090709-123456},
    is_indexed        => 1,
  );

  my %expected_tags = (
    1 => [(1..3, 168,0)],
    2 => [(4..6, 168,0)],
    3 => [(7..9, 168,0)],
    4 => [(1..6, 168,0)],
    5 => [(1..6, 168,0)],
    6 => [(1..6, 168,0)],
  );

  $da = $aqc->create();
  ok ($da && (@{$da} == 6), 'six definitions returned');
  foreach my $de (@{$da}) {
    my $p = $de->composition->get_component(0)->position;
    my @t = (map { $_->tag_index } ($de->composition->components_list()));
    is ($de->command, sprintf('qc --check=spatial_filter --rpt_list=%s --filename_root=%s --qc_out=%s %s',
                                 qq["8747:${p}"], "8747_${p}", "$archive_dir/lane${p}/qc", join q{ }, map { "--input_files=$archive_dir/lane${p}/plex$_/8747_${p}#$_.spatial_filter.stats" } @{$expected_tags{${p}}},  ),
    "spatial filter check command for lane $p, lane is indexed");
  }   
};

subtest 'qX_yield' => sub {
  plan tests => 25;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/samplesheet_1234.csv';

  my $aqc = npg_pipeline::function::autoqc->new(
    runfolder_path    => $rf_path,
    id_run            => 1234,
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

  foreach my $de (@{$da}) {
    my $p = $de->composition->get_component(0)->position;
    is ($de->command, sprintf(
    'qc --check=qX_yield --rpt_list=%s --filename_root=%s --qc_out=%s --platform_is_hiseq --input_files=%s --input_files=%s',
     qq["1234:${p}"], "1234_${p}", "$archive_dir/lane${p}/qc", "$archive_dir/lane${p}/1234_${p}_1.fastqcheck", "$archive_dir/lane${p}/1234_${p}_2.fastqcheck"),
    "qX_yield check command for lane $p");
  }

  $aqc = npg_pipeline::function::autoqc->new(
    runfolder_path    => $rf_path,
    id_run            => 1234,
    qc_to_run         => q{qX_yield},
    lanes             => [4],
    timestamp         => q{20090709-123456},
    is_indexed        => 0,
  );
  $da = $aqc->create();
  ok ($da && (@{$da} == 1), 'one definition returned');
  is ($da->[0]->command, sprintf(
    'qc --check=qX_yield --rpt_list=%s --filename_root=%s --qc_out=%s --platform_is_hiseq --input_files=%s --input_files=%s',
    qq["1234:4"], "1234_4", "$archive_dir/lane4/qc", "$archive_dir/lane4/1234_4_1.fastqcheck", "$archive_dir/lane4/1234_4_2.fastqcheck"),
    "qX_yield check command for lane 4");

  $aqc = npg_pipeline::function::autoqc->new(
    runfolder_path    => $rf_path,
    id_run            => 1234,
    lanes             => [7],
    qc_to_run         => q{qX_yield},
    timestamp         => q{20090709-123456},
    is_indexed        => 1,
  );
  $da = $aqc->create();
  ok ($da && (@{$da} == 1), 'one definition returned - lane is not a pool');
 
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/qc/1234_samplesheet_amended.csv';

  fcopy('t/data/run_params/runParameters.miseq.xml', "$rf_path/runParameters.xml")
    or die 'Fail to copy run param file';

  $aqc = npg_pipeline::function::autoqc->new(
    runfolder_path    => $rf_path,
    id_run            => 1234,
    lanes             => [8],
    qc_to_run         => q{qX_yield},
    timestamp         => q{20090709-123456},
    is_indexed        => 1,
  );

  $da = $aqc->create();
  ok ($da && (@{$da} == 3), '3 definitions returned - lane is a pool of one');
  my @plexes = grep { defined $_->composition->get_component(0)->tag_index} @{$da};
  is (@plexes, 2, 'two definitions for plexes');
  foreach my $d (@plexes) {
    my $t = $d->composition->get_component(0)->tag_index;
    is ($d->command, sprintf(
    'qc --check=qX_yield --rpt_list=%s --filename_root=%s --qc_out=%s --input_files=%s --input_files=%s',
    qq["1234:8:${t}"], "1234_8#${t}", "$archive_dir/lane8/plex${t}/qc", "$archive_dir/lane8/plex${t}/1234_8#${t}_1.fastqcheck", "$archive_dir/lane8/plex${t}/1234_8#${t}_2.fastqcheck"),
    "qX_yield command for lane 8 tag $t");
  }

  fcopy('t/data/run_params/runParameters.hiseq.xml', "$rf_path/runParameters.xml")
    or die 'Fail to copy run param file';
};
  
subtest 'ref_match' => sub {
  plan tests => 15;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/qc/1234_samplesheet_amended.csv';

  my $aqc = npg_pipeline::function::autoqc->new(
    runfolder_path    => $rf_path,
    id_run            => 1234,
    lanes             => [8],
    qc_to_run         => q{ref_match},
    timestamp         => q{20090709-123456},
    repository        => 't/data/sequence',
    is_indexed        => 1,
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
    is ($d->command, sprintf(
    'qc --check=ref_match --rpt_list=%s --filename_root=%s --qc_out=%s --input_files=%s --input_files=%s',
    qq["1234:8:${t}"], "1234_8#${t}", "$archive_dir/lane8/plex${t}/qc", "$archive_dir/lane8/plex${t}/.npg_cache_10000/1234_8#${t}_1.fastq",  "$archive_dir/lane8/plex${t}/.npg_cache_10000/1234_8#${t}_2.fastq"),
    "ref_match command for lane 8 tag $t");
  }
};

subtest 'insert_size and sequence error' => sub {
  plan tests => 5;

  fcopy('t/data/hiseq/16756_RunInfo.xml', "$rf_path/RunInfo.xml")
    or die 'Fail to copy run info file';

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/samplesheet_1234.csv';

  my $aqc = npg_pipeline::function::autoqc->new(
    runfolder_path    => $rf_path,
    id_run            => 1234,
    lanes             => [7],
    qc_to_run         => q{insert_size},
    timestamp         => q{20090709-123456},
    repository        => 't/data/sequence',
    is_indexed        => 1,
  );

  my $da = $aqc->create();
  ok ($da && (@{$da} == 1), 'one definition returned - lane is a not pool');

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/qc/samplesheet_14353.csv';

  $aqc = npg_pipeline::function::autoqc->new(
    id_run            => 14353,
    runfolder_path    => $rf_path,
    lanes             => [1],
    qc_to_run         => q{sequence_error},
    timestamp         => q{20090709-123456},
    is_indexed        => 0,
    repository        => 't/data/sequence',
  );
  $da = $aqc->create();
  ok ($da && (@{$da} == 1), 'one definition returned');
  is($da->[0]->memory, 8000, 'memory');

  $aqc = npg_pipeline::function::autoqc->new(
    id_run            => 14353,
    runfolder_path    => $rf_path,
    lanes             => [1],
    qc_to_run         => q{insert_size},
    timestamp         => q{20090709-123456},
    is_indexed        => 0,
    repository        => 't/data/sequence',
  );
  $da = $aqc->create();
  ok ($da && (@{$da} == 1), 'one definition returned');
  is($da->[0]->memory, 8000, 'memory');
};

subtest 'tag_metrics' => sub {
  plan tests => 11;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/samplesheet_8747.csv';

  my $qc;
  lives_ok {
    $qc = npg_pipeline::function::autoqc->new(
      qc_to_run         => 'tag_metrics',
      is_indexed        => 1,
      id_run            => 8747,
      lanes             => [1],
      runfolder_path    => $rf_path,
      timestamp         => q{20090709-123456},
    );
  } q{no croak on new, as required params provided};

# create products with the characteristics being tested
  my $plexed_lane_lims = st::api::lims->new(id_run => 8747, position => 1);
  my $library_lane_lims = st::api::lims->new(id_run => 8747, position => 7);
  my $plex_lims = st::api::lims->new(id_run => 8747, position => 1, tag_index => 1);
  my $plexed_lane_product = npg_pipeline::product->new(rpt_list => npg_tracking::glossary::rpt->deflate_rpt($plexed_lane_lims),lims => $plexed_lane_lims);
  my $library_lane_product = npg_pipeline::product->new(rpt_list => npg_tracking::glossary::rpt->deflate_rpt($library_lane_lims),lims => $library_lane_lims);
  my $plex_product = npg_pipeline::product->new(rpt_list => npg_tracking::glossary::rpt->deflate_rpt($plex_lims),lims => $plex_lims);

  ok( $qc->_should_run(0, $plexed_lane_product),
    q{lane is multiplexed - run tag metrics on a lane} );
  ok( !$qc->_should_run(0, $library_lane_product),
    q{lane is not multiplexed - do not run tag metrics on a lane} );
  ok( !$qc->_should_run(0, $plex_product),
    q{do not run tag metrics on a plex (hmm)} );
  ok( !$qc->_should_run(1, $plex_product),
    q{do not run tag metrics on a plex} );

  my $da = $qc->create();
  ok ($da && (@{$da} == 1), 'one definition returned');
  my $d = $da->[0];
  ok (!$d->excluded, 'step is not excluded');
  is ($d->command,
      qq[qc --check=tag_metrics --rpt_list="8747:1" --filename_root=8747_1 --qc_out=$archive_dir/lane1/qc],
      'tag metrics command for lane 1');

  lives_ok {
    $qc = npg_pipeline::function::autoqc->new(
      id_run         => 8747,
      lanes          => [8],
      runfolder_path => $rf_path,
      qc_to_run      => 'tag_metrics',
      is_indexed     => 0
    );
  } q{no croak on new, as required params provided};

  $da = $qc->create();
  ok ($da && (@{$da} == 1), 'one definition returned');
  ok ($da->[0]->excluded, 'step is excluded');
};

subtest 'genotype and gc_fraction' => sub {
  plan tests => 11;

  my $destination = "$tmp/references";
  dircopy('t/data/qc/references', $destination);
  make_path("$tmp/genotypes");
  my $new_dir = $destination . '/Homo_sapiens/CGP_GRCh37.NCBI.allchr_MT/all/fasta';
  make_path($new_dir);
  write_file("$new_dir/Homo_sapiens.GRCh37.NCBI.allchr_MT.fa", qw/some ref/);

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/qc/samplesheet_14043.csv';

  my $init = {
    id_run            => 14043,
    runfolder_path    => $rf_path,
    is_indexed        => 1,
    repository        => 't',
    qc_to_run         => q[genotype],
  };

  my $qc = npg_pipeline::function::autoqc->new($init);

# create products with the characteristics being tested
  my $plexed_lane_lims = st::api::lims->new(id_run => 14043, position => 6);
  my $library_lane_lims = st::api::lims->new(id_run => 14043, position => 1);
  my $plex0_lims = st::api::lims->new(id_run => 8747, position => 6, tag_index => 0);
  my $plex_lims = st::api::lims->new(id_run => 8747, position => 6, tag_index => 1);
  my $plex_lims_alt = st::api::lims->new(id_run => 8747, position => 8, tag_index => 22);
  my $plex168_lims = st::api::lims->new(id_run => 8747, position => 6, tag_index => 168);
  my $plexed_lane_product = npg_pipeline::product->new(rpt_list => npg_tracking::glossary::rpt->deflate_rpt($plexed_lane_lims),lims => $plexed_lane_lims);
  my $library_lane_product = npg_pipeline::product->new(rpt_list => npg_tracking::glossary::rpt->deflate_rpt($library_lane_lims),lims => $library_lane_lims);
  my $plex0_product = npg_pipeline::product->new(rpt_list => npg_tracking::glossary::rpt->deflate_rpt($plex0_lims),lims => $plex0_lims);
  my $plex_product = npg_pipeline::product->new(rpt_list => npg_tracking::glossary::rpt->deflate_rpt($plex_lims),lims => $plex_lims);
  my $plex_product_alt = npg_pipeline::product->new(rpt_list => npg_tracking::glossary::rpt->deflate_rpt($plex_lims_alt),lims => $plex_lims_alt);
  my $plex168_product = npg_pipeline::product->new(rpt_list => npg_tracking::glossary::rpt->deflate_rpt($plex168_lims),lims => $plex168_lims);

  throws_ok { $qc->_should_run(0, $library_lane_product) }
    qr/Attribute \(ref_repository\) does not pass the type constraint/,
    'ref repository does not exists - error';

  $init->{'repository'} = $tmp;

  $qc = npg_pipeline::function::autoqc->new($init);

  ok ($qc->_should_run(0, $library_lane_product),
    'genotype check can run for a non-indexed lane');
  ok (!$qc->_should_run(0, $plexed_lane_product),
    'genotype check cannot run for an indexed lane');
  ok (!$qc->_should_run(1, $plex0_product),
    'genotype check cannot run for tag 0 (no alignment)');
  ok ($qc->_should_run(1, $plex_product),
    'genotype check can run for tag 1 (human sample)');
  ok (!$qc->_should_run(1, $plex168_product),
    'genotype check cannot run for a spiked phix tag');

  $init->{'qc_to_run'} = q[gc_fraction];

  lives_ok {
    $qc = npg_pipeline::function::autoqc->new($init);
  } q{no croak on new, as required params provided};

  ok ($qc->_should_run(0, $plexed_lane_product), 'gc_fraction check can run');
  ok ($qc->_should_run(1, $plexed_lane_product), 'gc_fraction check can run (hmm)');
  ok ($qc->_should_run(1, $plex0_product),
    'gc_fraction check can run');
  ok ($qc->_should_run(1, $plex_product_alt),
   'gc_fraction check can run');
};

1;
