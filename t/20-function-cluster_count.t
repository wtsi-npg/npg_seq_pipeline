use strict;
use warnings;
use English qw(-no_match_vars);
use Test::More tests => 23;
use Test::Exception;
use Log::Log4perl qw(:levels);
use File::Copy qw(cp);
use File::Temp qw(tempdir);
use File::Path qw(make_path);

use t::util;

use_ok( q{npg_pipeline::function::cluster_count} );

my $util = t::util->new();
my $dir = $util->temp_directory();

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $dir, 'logfile'),
                          utf8   => 1});

my $test_data_dir_47995 = 't/data/novaseqx/20231017_LH00210_0012_B22FCNFLT3';

sub _setup_runfolder_47995 {

  my $tmp_dir = tempdir(CLEANUP => 1);
  my @dirs = split q[/], $test_data_dir_47995;
  my $rf_name = pop @dirs;
  my $rf_info = $util->create_runfolder($tmp_dir,
    {'runfolder_name' => $rf_name, 'analysis_path' => 'BAM_basecalls_20240508-204057'});
  my $rf = $rf_info->{'runfolder_path'};
  for my $file (qw(RunInfo.xml RunParameters.xml)) {
    if (cp("$test_data_dir_47995/$file", "$rf/$file") == 0) {
      die "Failed to copy $file";
    }
  }
  $rf_info->{'bam_basecall_path'} = $rf_info->{'analysis_path'};

  my $archive_path = $rf_info->{'archive_path'};
  my @paths = map { "$archive_path/lane$_/qc" } (1 .. 8);
  make_path(@paths);

  my $nocall_path = $rf_info->{'nocal_path'};
  `touch $nocall_path/47995_bfs_fofn.txt`;
  `touch $nocall_path/47995_sf_fofn.txt`;

  return $rf_info;
}

my $default = {
  default => {
    minimum_cpu => 1,
    memory => 2
  }
};

{
  my $rf_info = _setup_runfolder_47995();
  my $bam_basecall_path = $rf_info->{'bam_basecall_path'};
  my $runfolder_path = $rf_info->{'runfolder_path'};
  my $archive_path = $rf_info->{'archive_path'};
  my $id_run = 47995;
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = "$test_data_dir_47995/samplesheet_47995.csv";

  my $object;
  lives_ok {
    $object = npg_pipeline::function::cluster_count->new(
      runfolder_path    => $rf_info->{'runfolder_path'},
      bam_basecall_path => $rf_info->{'bam_basecall_path'},
      id_run            => $id_run,
      is_indexed        => 0,
      bfs_fofp_name => q{},
      sf_fofp_name => q{},
      resource => $default,
    );
  } q{obtain object ok};
  isa_ok( $object, q{npg_pipeline::function::cluster_count});

  my $da = $object->create();
  ok ($da && @{$da} == 1,
    'an array with one definition is returned for eight lanes (run-level check)');
  my $d = $da->[0];
    is ($d->created_by, 'npg_pipeline::function::cluster_count',
    'created_by is correct');
  is ($d->created_on, $object->timestamp, 'created_on is correct');
  is ($d->identifier, $id_run, 'identifier is set correctly');
  ok (!$d->excluded, 'step not excluded');
  ok (!$d->has_composition, 'composition is not set');
  lives_ok {$d->freeze()} 'definition can be serialized to JSON';
  like ($d->job_name, qr/\Anpg_pipeline_check_cluster_count_$id_run/,
    'the job is named correctly');
  is ($d->queue, 'default', 'the queue is set to default for the definition');

  my $bfs_paths = join q{ }, (map {qq[--bfs_paths=$archive_path/lane$_/qc]} (1..8));
  my $sf_paths  = join q{ }, (map {qq[--sf_paths=$archive_path/lane$_/qc]} (1..8));
  my $command = sprintf q[npg_pipeline_check_cluster_count --id_run=%i ] .
    q[--lanes=1 --lanes=2 --lanes=3 --lanes=4 --lanes=5 --lanes=6 --lanes=7 --lanes=8 ] .
    q[--bam_basecall_path=%s --runfolder_path=%s %s %s],
    $id_run, $bam_basecall_path, $runfolder_path, $bfs_paths, $sf_paths;
  is ($da->[0]->command, $command, 'correct command');
}

{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_8747.csv];
  my $runfolder_path = 't/data/example_runfolder/121103_HS29_08747_B_C1BV5ACXX';
  my $bam_basecall_path = "$runfolder_path/Data/Intensities/BAM_basecalls_20130122-085552";
  my $archive_path = "$bam_basecall_path/no_cal/archive";

  my $object = npg_pipeline::function::cluster_count->new(
    id_run => 8747,
    lanes => [1],
    runfolder_path => $runfolder_path,
    bam_basecall_path => $bam_basecall_path,
    archive_path => $archive_path,
    bfs_paths    => [ qq[$archive_path/lane1/qc] ],
    bfs_fofp_name => q{},
    sf_fofp_name => q{},
    resource => $default,
  );
  lives_ok {
    $object->run_cluster_count_check();
  } q{check runs ok};
}

{
  my $rf_info = _setup_runfolder_47995();
  my $bam_basecall_path = $rf_info->{'bam_basecall_path'};
  my $runfolder_path = $rf_info->{'runfolder_path'};
  my $archive_path = $rf_info->{'archive_path'};
  my $id_run = 47995;
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = "$test_data_dir_47995/samplesheet_47995.csv";

  my $object = npg_pipeline::function::cluster_count->new(
    id_run => 47995,
    runfolder_path => $runfolder_path,
    bam_basecall_path => $bam_basecall_path,
    archive_path => $archive_path,
    bfs_paths    => [ qq{$archive_path/lane3/qc} ],
    bfs_fofp_name => q{},
    sf_fofp_name => q{},
    resource => $default
  );
  ok( !$object->_bam_cluster_count_total({}), 'no bam cluster count total returned');

  my $is_indexed = 1;

  cp("t/data/bam_flagstats/${id_run}_3_bam_flagstats.json",
    "$archive_path/lane3/qc/${id_run}_3#0_bam_flagstats.json");
  cp("t/data/bam_flagstats/${id_run}_3_bam_flagstats.json",
    "$archive_path/lane3/qc/${id_run}_3#1_bam_flagstats.json");
  is( $object->_bam_cluster_count_total( {plex=>$is_indexed} ), 32,
    'correct bam cluster count total for plexes');

  cp("t/data/bam_flagstats/${id_run}_3_phix_bam_flagstats.json",
    "$archive_path/lane3/qc/${id_run}_3#0_phix_bam_flagstats.json");
  is( $object->_bam_cluster_count_total( {plex=>$is_indexed} ), 46,
    'correct bam cluster count total for plexes');
}

{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_8747.csv];
  my $runfolder_path = 't/data/example_runfolder/121103_HS29_08747_B_C1BV5ACXX';
  my $bam_basecall_path = "$runfolder_path/Data/Intensities/BAM_basecalls_20130122-085552";
  my $archive_path = "$bam_basecall_path/no_cal/archive";

  my $object = npg_pipeline::function::cluster_count->new(
    id_run => 8747,
    lanes => [1],
    runfolder_path => $runfolder_path,
    bam_basecall_path => $bam_basecall_path,
    archive_path => $archive_path,
    bfs_paths    => [ qq{$archive_path/lane1/qc} ],
    sf_paths     => [ qq{$archive_path/lane1/qc} ],
    bfs_fofp_name => q{},
    sf_fofp_name => q{},
    resource => $default
  );

  is( $object->_bam_cluster_count_total({plex=>1}), 301389338,
    'correct bam cluster count total');
  rename "$archive_path/lane1/qc/8747_1#0_bam_flagstats.json",
    "$archive_path/lane1/qc/8747_1#0_bam_flagstats.json.RENAMED";
  throws_ok {$object->run_cluster_count_check()} 
    qr{Cluster count in bam files not as expected},
    'Cluster count in bam files not as expected';
  rename "$archive_path/lane1/qc/8747_1#0_bam_flagstats.json.RENAMED",
    "$archive_path/lane1/qc/8747_1#0_bam_flagstats.json";

  ok($object->run_cluster_count_check(), 'Cluster count in bam files as expected');
}

{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_8747.csv];
  my $runfolder_path = 't/data/example_runfolder/121103_HS29_08747_B_C1BV5ACXX';
  my $bam_basecall_path = "$runfolder_path/Data/Intensities/BAM_basecalls_20130122-085552";
  my $archive_path = "$bam_basecall_path/no_cal/archive";
  my $recalibrated_path = "$bam_basecall_path/no_cal";

  my $common_command = sub {
    my $p = shift;
    return sprintf q{$EXECUTABLE_NAME bin/npg_pipeline_check_cluster_count } . 
    q{--bfs_fofp_name %s/lane%d/8747_bfs_fofn.txt } .
    q{--sf_fofp_name %s/lane%d/8747_sf_fofn.txt --id_run 8747 } .
    q{--bam_basecall_path %s --lanes %d --runfolder_path %s},
    $archive_path, $p, $archive_path, $p, $bam_basecall_path, $p,
    $runfolder_path;
  };

  my $c;
  my %ms=(
    1 => q{script runs ok when no spatial filter json},
    4 => q{script runs ok when spatial filter has failed reads},
    6 => q{script runs ok when no spatial filter has no PF reads},
  );
  for my $p (keys %ms) {
    $c=$common_command->($p);
    note `$c 2>&1`;
    ok( ! $CHILD_ERROR, $ms{$p} );
  }
}

1;
