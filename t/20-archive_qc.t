use strict;
use warnings;
use Test::More tests => 42;
use Test::Exception;
use Test::Differences;
use Test::Warn;
use File::Path qw/make_path/;
use File::Copy::Recursive qw/dircopy/;
use File::Slurp;
use Cwd;
use t::util;

local $ENV{PATH} = join q[:], q[t/bin], q[t/bin/software/solexa/bin], $ENV{PATH};

use_ok('npg_pipeline::archive::file::qc');

my $util = t::util->new();
my $tmp = $util->temp_directory();
$ENV{TEST_DIR} = $tmp;
$ENV{TEST_FS_RESOURCE} = q{nfs_12};
local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];
local $ENV{PATH} = join q[:], q[t/bin], q[t/bin/software/solexa/bin], $ENV{PATH};

my $run_folder = $util->default_runfolder();
my $pbcal = q{/nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/Bustard1.3.4_09-07-2009_auto/PB_cal};
my $recalibrated = $util->analysis_runfolder_path() . q{/Data/Intensities/Bustard1.3.4_09-07-2009_auto/PB_cal};

my $arg_refs = {};
my $job_dep = q{-w'done(123) && done(321)'};
$arg_refs->{'required_job_completion'}  = $job_dep;;

{
   throws_ok {
    npg_pipeline::archive::file::qc->new(
      run_folder => $run_folder,
      runfolder_path => $util->analysis_runfolder_path(),
      recalibrated_path => $recalibrated,
    )
  } qr/Attribute[ ][(]qc_to_run[)][ ]is[ ]required/, q{croak on new as no qc_to_run provided};
}

{
  my $aqc;
  lives_ok {
    $aqc = npg_pipeline::archive::file::qc->new(
      run_folder => $run_folder,
      runfolder_path => $util->analysis_runfolder_path(),
      recalibrated_path => $recalibrated,
      qc_to_run => q{adapter},
      timestamp => q{20090709-123456},
      is_indexed => 0,
    );
  } q{no croak on new, as required params provided};

  $util->create_analysis({qc_dir => 1});

  my @jids;
  lives_ok { @jids = $aqc->run_qc($arg_refs); } q{no croak $aqc->run_qc()};
  is(scalar@jids, 1, q{1 job id returned});

  my $bsub_command = $util->drop_temp_part_from_paths( $aqc->_generate_bsub_command($job_dep) );
  my $expected_command = q{bsub -q srpipeline -R 'select[mem>1500] rusage[mem=1500,nfs_12=1]' -M1500 -R 'span[hosts=1]'  -n2 -w'done(123) && done(321)' -J 'qc_adapter_1234_20090709-123456[1-8]%64' -o } . $pbcal . q{/archive/qc/log/qc_adapter_1234_20090709-123456.%I.%J.out -E 'npg_pipeline_preexec_references' 'qc --check=adapter --id_run=1234 --file_type=bam --position=`echo $LSB_JOBINDEX` } . qq{--qc_in=$pbcal --qc_out=$pbcal/archive/qc'};

  is( $bsub_command, $expected_command, q{generated bsub command is correct});
}

{
  my $aqc;
  lives_ok {
    $aqc = npg_pipeline::archive::file::qc->new(
      run_folder => $run_folder,
      runfolder_path => $util->analysis_runfolder_path(),
      recalibrated_path => $recalibrated,
      qc_to_run => q{qX_yield},
      timestamp => q{20090709-123456},
      is_indexed => 0,
    );
  } q{no croak on new, as required params provided};

  $util->create_analysis({qc_dir => 1});

  my @jids;
  lives_ok { @jids = $aqc->run_qc($arg_refs); } q{no croak $aqc->run_qc()};
  is(scalar@jids, 1, q{1 job id returned});

  my $bsub_command = $util->drop_temp_part_from_paths( $aqc->_generate_bsub_command() );
  my $expected_command = q{bsub -q srpipeline -R 'rusage[nfs_12=1]'  -J 'qc_qX_yield_1234_20090709-123456[1-8]%64' -o } . $pbcal . q{/archive/qc/log/qc_qX_yield_1234_20090709-123456.%I.%J.out 'qc --check=qX_yield --id_run=1234 --position=`echo $LSB_JOBINDEX` } . qq{--qc_in=$pbcal/archive --qc_out=$pbcal/archive/qc'};
  is( $bsub_command, $expected_command, q{generated bsub command is correct});
}

{
  my $aqc = npg_pipeline::archive::file::qc->new(
      run_folder => $run_folder,
      runfolder_path => $util->analysis_runfolder_path(),
      recalibrated_path => $recalibrated,
      qc_to_run => q{qX_yield},
      lanes  => [4],
      timestamp => q{20090709-123456},
      is_indexed => 0,
  );

  $util->create_analysis({qc_dir => 1});

  my @jids;
  lives_ok { @jids = $aqc->run_qc($arg_refs) } q{no croak $aqc->run_qc()};
  is(scalar @jids, 1, q{1 job id returned});

  my $bsub_command = $util->drop_temp_part_from_paths( $aqc->_generate_bsub_command($job_dep) );
  my $expected_command = q{bsub -q srpipeline -R 'rusage[nfs_12=1]' -w'done(123) && done(321)' -J 'qc_qX_yield_1234_20090709-123456[4]%64' -o } . $pbcal . q{/archive/qc/log/qc_qX_yield_1234_20090709-123456.%I.%J.out 'qc --check=qX_yield --id_run=1234 --position=`echo $LSB_JOBINDEX` } . qq{--qc_in=$pbcal/archive --qc_out=$pbcal/archive/qc'};
  is( $bsub_command, $expected_command, q{generated bsub command is correct});
}

{
  my $args = {};
  $args->{qc_dir} = [7,8];
  $util->create_multiplex_analysis($args);
  my $runfolder_path = $util->analysis_runfolder_path();

  my $aqc = npg_pipeline::archive::file::qc->new(
      run_folder => $run_folder,
      runfolder_path => $runfolder_path,
      recalibrated_path => $recalibrated,
      lanes     => [7],
      qc_to_run => q{qX_yield},
      timestamp => q{20090709-123456},
  );
  is ($aqc->is_indexed, 1, 'run is indexed');
  my @jids;
  lives_ok { @jids = $aqc->run_qc($arg_refs); } q{no croak $aqc->run_qc()};
  is(scalar@jids, 1, q{1 job id returned}); # but the lane is not a pool

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[];
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/qc/1234_samplesheet_amended.csv';
  $aqc = npg_pipeline::archive::file::qc->new(
      run_folder => $run_folder,
      runfolder_path => $runfolder_path,
      recalibrated_path => $recalibrated,
      lanes     => [8],
      qc_to_run => q{qX_yield},
      timestamp => q{20090709-123456},
  );

  @jids = ();
  lives_ok { @jids = $aqc->run_qc($arg_refs); } q{no croak $aqc->run_qc()};
  is(scalar@jids, 2, q{2 job ids returned}); # the lane is a pool

  $aqc = npg_pipeline::archive::file::qc->new(
      run_folder => $run_folder,
      runfolder_path => $runfolder_path,
      recalibrated_path => $recalibrated,
      lanes     => [8],
      qc_to_run => q{ref_match},
      timestamp => q{20090709-123456},
      repository => 't/data/sequence',
  );
  my $indexed = 1;
  my $bsub_command = $util->drop_temp_part_from_paths( $aqc->_generate_bsub_command($job_dep, $indexed) );
  my $expected_command = q{bsub -q srpipeline -R 'select[mem>6000] rusage[mem=6000,nfs_12=1]' -M6000 -w'done(123) && done(321)' -J 'qc_ref_match_1234_20090709-123456[80000,80154]%8' -o } . $pbcal . q{/archive/qc/log/qc_ref_match_1234_20090709-123456.%I.%J.out -E 'npg_pipeline_preexec_references --repository t/data/sequence' 'qc --check=ref_match --id_run=1234 --position=`echo $LSB_JOBINDEX/10000 | bc` --tag_index=`echo $LSB_JOBINDEX%10000 | bc` --qc_in=} . $pbcal . q{/archive/lane`echo $LSB_JOBINDEX/10000 | bc` --qc_out=} . $pbcal . q{/archive/lane`echo $LSB_JOBINDEX/10000 | bc`/qc'};
  is( $bsub_command, $expected_command, q{generated bsub command is correct});

  $util->remove_staging;
}

{
  $util->create_multiplex_analysis({qc_dir => [7],});
  my $runfolder_path = $util->analysis_runfolder_path();

  my $aqc = npg_pipeline::archive::file::qc->new(
      run_folder => $run_folder,
      runfolder_path => $runfolder_path,
      recalibrated_path => $recalibrated,
      lanes     => [7],
      qc_to_run => q{insert_size},
      timestamp => q{20090709-123456},
      repository => 't/data/sequence',
  );
  is ($aqc->is_indexed, 1, 'run is indexed');
  my @jids;
  lives_ok { @jids = $aqc->run_qc($arg_refs); } q{no croak $aqc->run_qc()};
  is(scalar @jids, 1, q{1 job ids returned}); # but the lane is not a pool

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[];
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/qc/samplesheet_14353.csv';

  $aqc = npg_pipeline::archive::file::qc->new(
      id_run => 14353,
      run_folder => $run_folder,
      runfolder_path => $util->analysis_runfolder_path(),
      recalibrated_path => $recalibrated,
      lanes     => [1],
      qc_to_run => q{sequence_error},
      timestamp => q{20090709-123456},
      is_indexed => 0,
      repository => 't/data/sequence',
  );

  @jids = undef;
  lives_ok { @jids = $aqc->run_qc($arg_refs); } q{no croak $aqc->run_qc()};
  is(scalar @jids, 1, q{1 job id returned});
  
  $util->remove_staging;
}

{
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[];
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/qc/samplesheet_14353.csv';
  my $id_run = 14353;

  throws_ok { npg_pipeline::archive::file::qc->new(
      id_run => $id_run,
      qc_to_run => 'some_check',
      is_indexed => 1)
  } qr/Can\'t locate npg_qc\/autoqc\/checks\/some_check\.pm/,
    'non-existing check name - error';

  my $qc = npg_pipeline::archive::file::qc->new(
      id_run => $id_run,
      qc_to_run => 'tag_metrics',
      is_indexed => 1
  );

  ok( $qc->_should_run(1), q{lane is indexed - run tag metrics on a lane} );
  ok( !$qc->_should_run(1,1), q{lane is indexed - do not run tag metrics on a plex} );

  $qc = npg_pipeline::archive::file::qc->new(
      id_run => $id_run,
      qc_to_run => 'tag_metrics',
      is_indexed => 0
  );
  ok( !$qc->_should_run(1),   q{run is not indexed - do not run tag metrics on a lane} );
  ok( !$qc->_should_run(1,1), q{run is not indexed - do not run tag metrics on a plex} );

  mkdir join q[/], $tmp, 'lane1';
  mkdir join q[/], $tmp, 'lane1', 'qc';
  mkdir join q[/], $tmp, 'qc';

  SKIP: {
    skip 'no legacy gc_bias window_depth tool available', 4 if not `which window_depth`;
    $qc = npg_pipeline::archive::file::qc->new(
        id_run       => $id_run,
        qc_to_run    => 'gc_bias',
        repository   => 't',
        is_indexed   => 1,
        archive_path => $tmp,
    );
    ok( !$qc->_should_run(1),  q{lane is indexed - do not run gcbias on a lane} );
    ok( $qc->_should_run(1,1), q{lane is indexed - run gcbias on a plex} );

    $qc = npg_pipeline::archive::file::qc->new(
        id_run => $id_run,
        qc_to_run => 'gc_bias',
        repository => 't',
        is_indexed => 0,
        archive_path => $tmp,
    );
    ok( $qc->_should_run(1),   q{run is not indexed - run gcbias on a lane} );
    ok( !$qc->_should_run(1,1), q{run is not indexed - do not run gcbias on a plex} );
  }
}

{
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

  my $destination = "$tmp/references";
  dircopy('t/data/qc/references', $destination);
  make_path("$tmp/genotypes");
  my $new_dir = $destination . '/Homo_sapiens/CGP_GRCh37.NCBI.allchr_MT/all/fasta';
  make_path($new_dir);
  write_file("$new_dir/Homo_sapiens.GRCh37.NCBI.allchr_MT.fa", qw/some ref/);

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[];
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/qc/samplesheet_14043.csv';

  my $init = {
      id_run => 14043,
      run_folder => $rf_name,
      runfolder_path => $rf_path,
      bam_basecall_path => $analysis_dir,
      archive_path => $archive_dir,
      is_indexed => 1,
      repository => 't',
      qc_to_run => q[genotype],
  };

  my $qc = npg_pipeline::archive::file::qc->new($init);
  throws_ok { $qc->_should_run(1) }
    qr/Attribute \(ref_repository\) does not pass the type constraint/,
    'ref repository does not exists - error';

  $init->{'repository'} = $tmp;
  $qc = npg_pipeline::archive::file::qc->new($init);
  ok ($qc->_should_run(1), 'genotype check can run for a non-indexed lane');
  ok (!$qc->_should_run(6), 'genotype check cannot run for an indexed lane');
  ok ($qc->_should_run(6,0),
    'genotype check can run for tag 0 (the only plex is a human sample)');
  ok ($qc->_should_run(6,1), 'genotype check can run for tag 1 (human sample)');
  ok (!$qc->_should_run(6,168), 'genotype check cannot run for a spiked phix tag');

  $init->{'qc_to_run'} = 'gc_fraction';
  $qc = npg_pipeline::archive::file::qc->new($init);
  ok ($qc->_should_run(6), 'gc_fraction check can run');
  ok ($qc->_should_run(6,0), 'gc_fraction check can run');
  ok ($qc->_should_run(6,1) , 'gc_fraction check can run');
}

1;
