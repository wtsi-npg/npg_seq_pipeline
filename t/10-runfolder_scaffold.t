use strict;
use warnings;
use Test::More tests => 6;
use Test::Exception;
use Moose::Meta::Class;
use File::Copy::Recursive qw(fcopy dircopy);
use File::Path qw(make_path);

use File::Slurp;

use t::util;

use_ok('npg_pipeline::runfolder_scaffold');

subtest 'tests for class methods' => sub {
  plan tests => 5;

  throws_ok {npg_pipeline::runfolder_scaffold->path_in_outgoing()}
    qr/Path required/,
    'error if argument path is not supplied';
  throws_ok {npg_pipeline::runfolder_scaffold->path_in_outgoing(q[])}
    qr/Path required/,
    'error if argument path is empty';
  my $path = '/tmp/analysis/folder';
  my $opath = '/tmp/outgoing/folder';
  is (npg_pipeline::runfolder_scaffold->path_in_outgoing($path),
    $opath, 'path changed to outgoing');
  is (npg_pipeline::runfolder_scaffold->path_in_outgoing($opath),
    $opath, 'path remains in outgoing');
  $path = '/tmp/incoming/folder';
  is (npg_pipeline::runfolder_scaffold->path_in_outgoing($path),
    $path, 'path is not changed');
};

subtest 'top level scaffold' => sub {
  plan tests => 10;

  my $util = t::util->new();
  my $rfh = $util->create_runfolder();

  my $rfs = Moose::Meta::Class->create_anon_class(
    superclasses => ['npg_pipeline::base'],
    roles        => [qw/npg_pipeline::runfolder_scaffold/],
  )->new_object(
      runfolder_path => $rfh->{'runfolder_path'},
      timestamp      => '2018',
      id_run         => 999
               );

  my $ip = $rfh->{'intensity_path'};
  $rfs->create_top_level();
  my $bbc_path = join q[/], $ip, 'BAM_basecalls_2018';
  ok (-e $bbc_path, 'bam basecalls directory created');
  ok (-e "$bbc_path/no_archive", 'no_archive directory created');
  ok (-e "$bbc_path/pp_archive", 'npp_archive directory created');
  my $dir = "$bbc_path/no_cal";
  ok (-e $dir, 'no_cal directory created');
  $dir = "$dir/archive";
  ok (-e $dir, 'archive directory created');
  ok (-e "$dir/tileviz", 'tileviz index directory created');
  ok (-e "$bbc_path/status", 'status directory created');
  ok (-e "$bbc_path/metadata_cache_999", 'metadata cache directory created');
  ok (-e "$bbc_path/irods_publisher_restart_files",
    'directory for iRODS publisher restart files created');
  ok (-e "$bbc_path/irods_locations_files",
    'directory for iRODS location json files created')
};

subtest 'product level scaffold, NovaSeq all lanes' => sub {
  plan tests => 101;

  my $util = t::util->new();
  my $rfh = $util->create_runfolder();
  my $rf_path = $rfh->{'runfolder_path'};
  fcopy 't/data/run_params/runParameters.novaseq.xml', "$rf_path/runParameters.xml";
  fcopy 't/data/novaseq/210111_A00513_0447_AHJ55JDSXY/RunInfo.xml', "$rf_path/RunInfo.xml";
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/products/samplesheet_novaseq4lanes.csv';
  
  my $rfs = Moose::Meta::Class->create_anon_class(
    superclasses => ['npg_pipeline::base'],
    roles        => [qw/npg_pipeline::runfolder_scaffold/],
  )->new_object(
      runfolder_path => $rf_path,
      timestamp      => '2018',
      id_run         => 999,
      lanes          => [1, 2, 3, 4]
               );

  my $ip = $rfh->{'intensity_path'};
  $rfs->create_top_level();
  my $apath = join q[/], $ip, 'BAM_basecalls_2018', 'no_cal', 'archive';
  $rfs->create_product_level();

  my @original = qw/lane1 lane2 lane3 lane4/;
  my @dirs = @original;
  push @dirs, (map {join q[/], $_, 'qc'} @original);
  push @dirs, (map {join q[/], $_, 'tileviz_'.$_} @original);
  push @dirs, (map {join q[/], $_, '.npg_cache_10000'} @original);
  push @dirs, (map {join q[/], $_, 'plex0/qc'} @original);
  push @dirs, (map {join q[/], $_, 'plex888/qc'} @original);
  map { ok (-d $_, "$_ created") } map {join q[/], $apath, $_} @dirs;

  for my $lane (@original) {
    my $file = join q[/], $apath, $lane, 'tileviz_' . $lane . '.html';
    ok (-f $file, "tileviz lane index file $file exists");
    my $content = read_file $file;
    my ($p) = $lane =~ /(\d)\Z/;
    like ($content, qr/Run 999 Lane $p Tileviz Report/, 'title exists');
    like ($content, qr/No tileviz data available for this lane/, 'info exists');
  }
  
  @original = map {'plex' . $_} (1 .. 21);
  @dirs = @original;
  push @dirs, (map {join q[/], $_, 'qc'} @original);
  push @dirs, (map {join q[/], $_, '.npg_cache_10000'} @original);
  map { ok (-d $_, "$_ created") } map {join q[/], $apath, $_} @dirs;

  my $tileviz_index = join q[/], $apath, 'tileviz', 'index.html';
  ok (-e $tileviz_index, 'tileviz index created');
  my @lines = read_file($tileviz_index);
  is (scalar @lines, 7, 'tileviz index contains seven lines');
};

subtest 'product level scaffold, NovaSeq selected lanes' => sub {
  plan tests => 165;

  my $util = t::util->new();
  my $rfh = $util->create_runfolder();
  my $rf_path = $rfh->{'runfolder_path'};
  fcopy 't/data/run_params/runParameters.novaseq.xml', "$rf_path/runParameters.xml";
  fcopy 't/data/novaseq/210111_A00513_0447_AHJ55JDSXY/RunInfo.xml', "$rf_path/RunInfo.xml";
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/products/samplesheet_novaseq4lanes.csv';
  
  my $rfs = Moose::Meta::Class->create_anon_class(
    superclasses => ['npg_pipeline::base'],
    roles        => [qw/npg_pipeline::runfolder_scaffold/],
  )->new_object(
      runfolder_path => $rf_path,
      timestamp      => '2018',
      id_run         => 999,
      lanes          => [2, 3]
               );

  my $ip = $rfh->{'intensity_path'};
  $rfs->create_top_level();
  my $apath = join q[/], $ip, 'BAM_basecalls_2018', 'no_cal', 'archive';
  my $napath = join q[/], $ip, 'BAM_basecalls_2018', 'no_archive';
  my $ppapath = join q[/], $ip, 'BAM_basecalls_2018', 'pp_archive';
  $rfs->create_product_level();

  my @original = qw/lane2 lane3/;
  my @dirs = @original;
  push @dirs, (map {join q[/], $_, 'qc'} @original);
  push @dirs, (map {join q[/], $_, 'tileviz_'.$_} @original);
  push @dirs, (map {join q[/], $_, '.npg_cache_10000'} @original);
  push @dirs, (map {join q[/], $_, 'plex0/qc'} @original);
  push @dirs, (map {join q[/], $_, 'plex888/qc'} @original); 
  map { ok (-d $_, "$_ created") } map {join q[/], $apath, $_} @dirs;

  @dirs = qw/lane1 lane4/;
  map { ok (!-e $_, "$_ not created") } map {join q[/], $apath, $_} @dirs;

  @original = map {'lane2-3/plex' . $_} (1 .. 21);
  @dirs = @original;
  push @dirs, (map {join q[/], $_, 'qc'} @original);
  push @dirs, (map {join q[/], $_, '.npg_cache_10000'} @original);
  map { ok (-d $_, "$_ created") } map {join q[/], $apath, $_} @dirs;
  map { ok (-d $_, "$_ created") } map {join q[/], $napath, $_} @original;
  map { ok (-d $_, "$_ created") } map {join q[/], $ppapath, $_} @original;

  my $tileviz_index = join q[/], $apath, 'tileviz', 'index.html';
  ok (-e $tileviz_index, 'tileviz index created');
  my @lines = read_file($tileviz_index);
  is (scalar @lines, 5, 'tileviz index contains five lines');

  for my $l ( (2, 3) ) {
    ok ((!-l "$napath/lane${l}/stage1/999_${l}.cram"),
      "link for lane $l file is not created");
  }

  for my $t ( (1 .. 21) ) {
    my $name = "999_2-3#${t}.cram";
    my $file = "$napath/lane2-3/plex${t}/stage1/$name";
    ok ((-l $file), "link for plex $t is created");
    is (readlink $file, "../../../../no_cal/$name", 'relative path is used');
  }
};

subtest 'product level scaffold, library merge for NovaSeqX' => sub {
  plan tests => 369;

  my $tdir = t::util->new()->temp_directory();
  my $rf_name = q[20231017_LH00210_0012_B22FCNFLT3];
  my $test_path = qq[t/data/novaseqx/${rf_name}];
  my $rf_path = join q[/], $tdir, $rf_name;
  dircopy($test_path, $rf_path);
  my $in_path = join q[/], $rf_path, 'Data', 'Intensities';
  make_path($in_path);
  my $id_run = 47995;
  my $date = '2023';
  my $apath = join q[/], $in_path, "BAM_basecalls_$date", 'no_cal', 'archive';

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = qq[$rf_path/samplesheet_${id_run}.csv];
  
  my $rfs = Moose::Meta::Class->create_anon_class(
    superclasses => ['npg_pipeline::base'],
    roles        => [qw/npg_pipeline::runfolder_scaffold/],
  )->new_object(
    runfolder_path => $rf_path,
    timestamp      => $date,
    id_run         => $id_run,
  );
  $rfs->create_top_level();
  $rfs->create_product_level();

  my @lane_dirs = map { 'lane' . $_ } (1 .. 8);
  my @dirs = @lane_dirs;
  push @dirs, (map {join q[/], $_, 'qc'} @lane_dirs);
  push @dirs, (map {join q[/], $_, 'tileviz_'.$_} @lane_dirs);
  push @dirs, (map {join q[/], $_, '.npg_cache_10000'} @lane_dirs);
  map { ok (-d $_, "$_ created") } map {join q[/], $apath, $_} @dirs;
  
  # All lanes are spiked.
  for my $lane (@lane_dirs) {
    for my $t ( (0, 888) ) {
      my $plex_dir = join q[/], $apath, $lane, 'plex' . $t;
      ok ((-d $plex_dir), "plex directory $plex_dir exists");
      ok ((-d "$plex_dir/qc"), "qc directory for $plex_dir exists");
      ok ((-d "$plex_dir/.npg_cache_10000"), "cache directory for $plex_dir exists");
    }
    # Test just a few directories.
    for my $t ( (1, 10) ) {
      my $plex_dir = join q[/], $apath, $lane, 'plex' . $t;
      ok (!(-d $plex_dir), "plex directory $plex_dir does not exist");
    }
  }

  my %num_samples_per_merge = (
    '1-2' => 17,
    '3-4' => 10,
    '5-6' => 22,
    '7-8' => 38,
  );
  foreach my $merge (keys %num_samples_per_merge) {
    my $merge_dir = join q[/], $apath, 'lane' . $merge;
    ok ((-d $merge_dir), "$merge_dir exists");
    ok (!(-d "$merge_dir/qc"),
      "qc directory for $merge_dir does not exist");
    ok (!(-d "$merge_dir/.npg_cache_10000"),
      "cache directory for $merge_dir does not exist");
    foreach my $t ((1 .. $num_samples_per_merge{$merge})) {
      my $plex_dir = join q[/], $merge_dir, 'plex' . $t;
      ok ((-d $plex_dir), "$plex_dir exists");
      ok ((-d "$plex_dir/qc"), "qc directory for $plex_dir exists");
      ok ((-d "$plex_dir/.npg_cache_10000"),
        "cache directory for $plex_dir does not exist");
    }
  }
};

1;
