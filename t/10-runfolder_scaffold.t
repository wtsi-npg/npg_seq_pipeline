use strict;
use warnings;
use Test::More tests => 5;
use Test::Exception;
use Moose::Meta::Class;
use File::Copy qw(cp);
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
  plan tests => 6;

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
  my $dir = "$bbc_path/no_cal";
  ok (-e $dir, 'no_cal directory created');
  $dir = "$dir/archive";
  ok (-e $dir, 'archive directory created');
  ok (-e "$dir/tileviz", 'tileviz index directory created');
  ok (-e "$bbc_path/status", 'status directory created');
  ok (-e "$bbc_path/metadata_cache_999", 'metadata cache directory created');
};

subtest 'product level scaffold, NovaSeq all lanes' => sub {
  plan tests => 99;

  my $util = t::util->new();
  my $rfh = $util->create_runfolder();
  my $rf_path = $rfh->{'runfolder_path'};
  cp 't/data/run_params/runParameters.novaseq.xml',  "$rf_path/runParameters.xml";
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
  push @dirs, (map {join q[/], $_, 'tileviz'} @original);
  push @dirs, (map {join q[/], $_, '.npg_cache_10000'} @original);
  map { ok (-d $_, "$_ created") } map {join q[/], $apath, $_} @dirs;

  for my $lane (@original) {
    my $file = join q[/], $apath, $lane, 'tileviz.html';
    ok (-f $file, "tileviz lane index file $file exists");
    my $content = read_file $file;
    my ($p) = $lane =~ /(\d)\Z/;
    like ($content, qr/Run 999 Lane $p Tileviz Report/, 'title exists');
    like ($content, qr/No tileviz data available for this lane/, 'info exists');
  }
  
  @original = map {'plex' . $_} (0 .. 21, 888);
  @dirs = @original;
  push @dirs, (map {join q[/], $_, 'qc'} @original);
  push @dirs, (map {join q[/], $_, '.npg_cache_10000'} @original);
  map { ok (-d $_, "$_ created") } map {join q[/], $apath, $_} @dirs;

  my $tileviz_index = join q[/], $apath, 'tileviz', 'index.html';
  ok (-e $tileviz_index, 'tileviz index created');
  my @lines = read_file($tileviz_index);
  is (scalar @lines, 7, 'tileviz index contains sevel lines');
};

subtest 'product level scaffold, NovaSeq selected lanes' => sub {
  plan tests => 81;

  my $util = t::util->new();
  my $rfh = $util->create_runfolder();
  my $rf_path = $rfh->{'runfolder_path'};
  cp 't/data/run_params/runParameters.novaseq.xml',  "$rf_path/runParameters.xml";
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
  $rfs->create_product_level();

  my @original = qw/lane2 lane3/;
  my @dirs = @original;
  push @dirs, (map {join q[/], $_, 'qc'} @original);
  push @dirs, (map {join q[/], $_, 'tileviz'} @original);
  push @dirs, (map {join q[/], $_, '.npg_cache_10000'} @original);
  map { ok (-d $_, "$_ created") } map {join q[/], $apath, $_} @dirs;

  @dirs = qw/lane1 lane4/;
  map { ok (!-e $_, "$_ not created") } map {join q[/], $apath, $_} @dirs;

  @original = map {'lane2-3/plex' . $_} (0 .. 21, 888);
  @dirs = @original;
  push @dirs, (map {join q[/], $_, 'qc'} @original);
  push @dirs, (map {join q[/], $_, '.npg_cache_10000'} @original);
  map { ok (-d $_, "$_ created") } map {join q[/], $apath, $_} @dirs;

  my $tileviz_index = join q[/], $apath, 'tileviz', 'index.html';
  ok (-e $tileviz_index, 'tileviz index created');
  my @lines = read_file($tileviz_index);
  is (scalar @lines, 5, 'tileviz index contains five lines');
};

1;
