use strict;
use warnings;
use Test::More tests => 6;
use Test::Exception;
use File::Temp qw/tempdir/;
use File::Path qw/make_path/;
use File::Copy;
use Log::Log4perl qw/:levels/;
use File::Slurp;
use Moose::Meta::Class;
use DateTime;

use npg_testing::db;

my $dir = tempdir( CLEANUP => 1);

use_ok('npg_pipeline::function::pp_archiver');

my $exec = join q[/], $dir, 'npg_upload2climb';
open my $fh1, '>', $exec or die 'failed to open file for writing';
print $fh1 'echo "npg_upload2climb mock"' or warn 'failed to print';
close $fh1 or warn 'failed to close file handle';
chmod 755, $exec;
local $ENV{PATH} = join q[:], $dir, $ENV{PATH};

my $logfile = join q[/], $dir, 'logfile';

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => $logfile,
                          utf8   => 1});

# setup runfolder
my $runfolder_path = join q[/], $dir, 'novaseq', '180709_A00538_0010_BH3FCMDRXX';
my $bbc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20180805-013153';
my $archive_path = join q[/], $bbc_path, 'no_cal/archive';
my $no_archive_path = join q[/], $bbc_path, 'no_archive';
my $pp_archive_path = join q[/], $bbc_path, 'pp_archive';

make_path $archive_path;
make_path $no_archive_path;
copy('t/data/novaseq/180709_A00538_0010_BH3FCMDRXX/RunInfo.xml', "$runfolder_path/RunInfo.xml") or die
'Copy failed';
copy('t/data/novaseq/180709_A00538_0010_BH3FCMDRXX/RunParameters.xml', "$runfolder_path/runParameters.xml")
or die 'Copy failed';

my $timestamp = q[20180701-123456];
my $repo_dir = q[t/data/portable_pipelines/ncov2019-artic-nf/cf01166c42a];
my $product_conf = qq[$repo_dir/product_release.yml];

local $ENV{NPG_MANIFEST4PP_FILE} = undef;

sub _test_manifest {
  my $mpath = shift;
  ok (-f $mpath, 'manifest file exists');
  my @lines = read_file $mpath;
  is (scalar @lines, 1, 'manifest contains one line');
  like ($lines[0], qr/\Asample_name/, 'this line contains a header');
  unlink $mpath;
}

subtest 'archiver is not configured: function skipped and an empty manifest generation' => sub {
  plan tests => 15;

  my $mpath = join q[/], $dir, 'manifest_test1';
  ok (!-e $mpath, 'prereq - manifest file does not exist');

  my $init = {
    product_conf_file_path => $product_conf,
    archive_path           => $archive_path,
    runfolder_path         => $runfolder_path,
    id_run                 => 26291,
    timestamp              => $timestamp,
    repository             => $dir,
    qc_schema              => undef,
    _manifest_path         => $mpath
  };

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_33990.csv];

  my $f = npg_pipeline::function::pp_archiver->new($init);
  my $ds = $f->create();
  is (scalar @{$ds}, 1, '1 definition is returned');
  isa_ok ($ds->[0], 'npg_pipeline::function::definition');
  is ($ds->[0]->excluded, 1, 'function is excluded');
  ok (!-e $mpath, 'manifest file does not exist');

  $f = npg_pipeline::function::pp_archiver->new($init);
  is ($f->_generate_manifest4archiver(), 0, 'an empty manifest is generated');
  _test_manifest($mpath);

  $f = npg_pipeline::function::pp_archiver->new($init);
  $ds = $f->generate_manifest();
  is (scalar @{$ds}, 1, '1 definition is returned');
  isa_ok ($ds->[0], 'npg_pipeline::function::definition');
  is ($ds->[0]->excluded, 1, 'function is excluded');
  _test_manifest($mpath);
};

subtest 'manifest path and using a pre-set path' => sub {
  plan tests => 16;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_33990.csv];

  my $f = npg_pipeline::function::pp_archiver->new(
    product_conf_file_path => $product_conf,
    archive_path           => $archive_path,
    runfolder_path         => $runfolder_path,
    id_run                 => 26291,
    timestamp              => $timestamp,
    repository             => $dir,
    qc_schema              => undef,
  );

  my $mpath = $f->_manifest_path;
  like ($mpath, qr/\A$bbc_path\/manifest4pp_upload_26291_20180701-123456-\d+.tsv\Z/,
    'generated manifest path is in the analysis directory');
  is ($ENV{NPG_MANIFEST4PP_FILE}, undef, 'env var is not set');

  $f->generate_manifest();
  _test_manifest($mpath);
  is ($ENV{NPG_MANIFEST4PP_FILE}, undef, 'env var is not set');

  $mpath = join q[/], $dir, 'manifest_test12';
  (not -e $mpath) or die "unexpectedly found existing $mpath";
  local $ENV{NPG_MANIFEST4PP_FILE} = $mpath;

  $f = npg_pipeline::function::pp_archiver->new(
    product_conf_file_path => $product_conf,
    archive_path           => $archive_path,
    runfolder_path         => $runfolder_path,
    id_run                 => 26291,
    timestamp              => $timestamp,
    repository             => $dir,
    qc_schema              => undef,
  );
  is ($f->_manifest_path, $mpath, 'manifest path as pre-set');
  is ($f->_generate_manifest4archiver(), 0, 'an empty manifest is generated');
  is ($ENV{NPG_MANIFEST4PP_FILE}, $mpath, 'env var is set');
  _test_manifest($mpath);

  my $text = 'existing manifest test';
  write_file($mpath, $text);
  
  $f = npg_pipeline::function::pp_archiver->new(
    product_conf_file_path => $product_conf,
    archive_path           => $archive_path,
    runfolder_path         => $runfolder_path,
    id_run                 => 26291,
    timestamp              => $timestamp,
    repository             => $dir,
    qc_schema              => undef,
  );
  is ($f->_manifest_path, $mpath, 'manifest path as pre-set');
  is ($f->_generate_manifest4archiver(), undef, 'manifest is not generated');
  is ($ENV{NPG_MANIFEST4PP_FILE}, $mpath, 'env var is set');
  is (read_file($mpath), $text, 'preset manifets has not changed');
};

subtest 'product config for pp archival validation' => sub {
  plan tests => 3;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_33990.csv];
  my $pc = 't/data/portable_pipelines/ncov2019-artic-nf/v.3/product_release_two_pps.yml';
  my $f = npg_pipeline::function::pp_archiver->new(
    product_conf_file_path => $pc,
    archive_path           => $archive_path,
    runfolder_path         => $runfolder_path,
    id_run                 => 26291,
    timestamp              => $timestamp,
    repository             => $dir,
    qc_schema              => undef,
  );
  throws_ok { $f->create } 
    qr/Multiple external archives are not supported/,
    'error when two versions of the pipeline are marked for archival';
  
  $pc = 't/data/portable_pipelines/ncov2019-artic-nf/v.3/product_release_no_staging_root.yml';
  $f = npg_pipeline::function::pp_archiver->new(
    product_conf_file_path => $pc,
    archive_path           => $archive_path,
    runfolder_path         => $runfolder_path,
    id_run                 => 26291,
    timestamp              => $timestamp,
    repository             => $dir,
    qc_schema              => undef,
  );
  throws_ok { $f->create } 
    qr/pp_staging_root is not defined/,
    'error when the staging root is not defined for a pp which is marked for archival';

  my $new_pc = join q[/], $dir, 'product_release.yml';
  copy $pc, $new_pc or die "Failed to copy $pc to $new_pc";
  my $staging = "$dir/staging";
  write_file($new_pc, {append => 1}, "        pp_staging_root: $staging");

  $f = npg_pipeline::function::pp_archiver->new(
    product_conf_file_path => $new_pc,
    archive_path           => $archive_path,
    runfolder_path         => $runfolder_path,
    id_run                 => 26291,
    timestamp              => $timestamp,
    repository             => $dir,
    qc_schema              => undef,
  );
  throws_ok { $f->create }
    qr/$staging does not exist or is not a directory/,
    'error when the staging root directory does not exist';
};

subtest 'definition and manifest generation' => sub {
  plan tests => 62;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_33990.csv];
  my $id_run = 26291;
  my $product_conf =  join(q[/], $dir, 'product_release.yml');

  make_path "$dir/staging"; # create staging root

  my $init = {
    product_conf_file_path => $product_conf,
    archive_path           => $archive_path,
    runfolder_path         => $runfolder_path,
    id_run                 => $id_run,
    timestamp              => $timestamp,
    repository             => $dir,
    qc_schema              => undef,
  };

  my $f = npg_pipeline::function::pp_archiver->new($init);
  throws_ok { $f->create }
    qr/qc schema argument is required/, 'db access is required';

  my $schema = Moose::Meta::Class->create_anon_class(roles => [qw/npg_testing::db/])
    ->new_object()->create_test_db(q[npg_qc::Schema], q[t/data/qc_outcomes/fixtures]);
  my $uqc_rs = $schema->resultset(q[UqcOutcomeEnt]);
  is ($uqc_rs->search({})->count, 0, 'no data in uqc_outcome table');

  $init->{qc_schema} = $schema;
  
  my %dict = map { $_->short_desc => $_->id_uqc_outcome }
             $schema->resultset(q[UqcOutcomeDict])->search({})->all();
  
  $f = npg_pipeline::function::pp_archiver->new($init);
  my $manifest_path = $f->_manifest_path;
  ok (!-e $manifest_path, 'manifest file does not exist');
  my $ds = $f->create();
  ok (!-e $manifest_path, 'manifest file does not exist');
  is (scalar @{$ds}, 1, '1 definition is returned');
  is ($ds->[0]->excluded, 1, 'no uqc outcomes, function is excluded');

  my $rows = {};
  my $cclass = q[npg_tracking::glossary::composition::component::illumina];
  my $time = DateTime->now();

  for my $tag_index ((1 .. 3)) {
    my $c = npg_tracking::glossary::composition->new(components => [
      $cclass->new(id_run => $id_run, position => 1, tag_index => $tag_index),
      $cclass->new(id_run => $id_run, position => 2, tag_index => $tag_index),
    ]);
    my $id = $uqc_rs->find_or_create_seq_composition($c)->id_seq_composition;
    $rows->{$tag_index} = $uqc_rs->create({
                             id_seq_composition => $id,
                             id_uqc_outcome     => $dict{'Accepted'},
                             username           => 'cat',
                             modified_by        => 'dog',
                             rationale          => 'test',
                             last_modified      => $time
                           });
  }

  $f = npg_pipeline::function::pp_archiver->new($init);
  $manifest_path = $f->_manifest_path;
  ok (!-e $manifest_path, 'manifest file does not exist');
  $ds = $f->create();
  ok (-e $manifest_path, 'manifest file exists');
  is (scalar @{$ds}, 1, '1 definition is returned');
  my $d = $ds->[0];
  is ($d->excluded, undef, 'function is not excluded');
  is ($d->composition, undef, 'composition is not defined');
  is ($d->job_name, "pp_archiver_$id_run", 'job name');
  is ($d->command, "$exec $manifest_path", 'correct command');

  ok ($f->merge_lanes, 'merge flag is true');
  my @data_products = @{$f->products->{'data_products'}};
  is (scalar @data_products, 5, '5 data products');
  is (scalar(grep { $f->is_release_data($_) } @data_products), 3,
    '3 products for release');

  my @lines = read_file($manifest_path);
  is (scalar @lines, 4, 'manifest contains 4 lines');
  unlink $manifest_path;

  is ((shift @lines), join(qq[\t],
    qw(sample_name files_glob staging_archive_path product_json id_product)) . qq[\n],
    'correct header line');
  my @line = (
    'A1',
    "$pp_archive_path/plex1/ncov2019_artic_nf/v.3/qc_pass_climb_upload/*/*/*{am,fa}",
    "$dir/staging/26291/BAM_basecalls_20180805-013153/180709_A00538_0010_BH3FCMDRXX",
    '{"components":[{"id_run":26291,"position":1,"tag_index":1},{"id_run":26291,"position":2,"tag_index":1}]}',
    "b65be328691835deeff44c4025fadecd9af6512c10044754dd2161d8a7c85000\n"
  );
  is ((shift @lines), join(qq[\t], @line), 'correct line for merged plex 1');

  $rows->{1}->update({id_uqc_outcome => $dict{'Rejected'}});

  $f = npg_pipeline::function::pp_archiver->new($init);
  $manifest_path = $f->_manifest_path;
  ok (!-e $manifest_path, 'manifest file does not exist');
  $ds = $f->create();
  ok (-e $manifest_path, 'manifest file exists');
  @lines = read_file($manifest_path);
  is (scalar @lines, 3, 'manifest contains 3 lines');
  unlink $manifest_path;

  $rows->{2}->update({id_uqc_outcome => $dict{'Undecided'}});

  $f = npg_pipeline::function::pp_archiver->new($init);
  $manifest_path = $f->_manifest_path;
  ok (!-e $manifest_path, 'manifest file does not exist');
  $ds = $f->create();
  ok (-e $manifest_path, 'manifest file exists');
  @lines = read_file($manifest_path);
  is (scalar @lines, 2, 'manifest contains 2 lines');
  unlink $manifest_path;

  $rows->{3}->delete();

  $f = npg_pipeline::function::pp_archiver->new($init);
  $manifest_path = $f->_manifest_path;
  ok (!-e $manifest_path, 'manifest file does not exist');
  $ds = $f->create();
  ok (!-e $manifest_path, 'manifest file does no exists');
  is (scalar @{$ds}, 1, '1 definition is returned');
  is ($ds->[0]->excluded, 1, 'no uqc outcomes, function is excluded');

  $rows = {};

  for my $p ((1, 2)) {
    for my $tag_index ((1 .. 3)) {
      my $c = npg_tracking::glossary::composition->new(components => [
        $cclass->new(id_run => $id_run, position => $p, tag_index => $tag_index),
      ]);
      my $id = $uqc_rs->find_or_create_seq_composition($c)->id_seq_composition;
      $rows->{$p}->{$tag_index} = $uqc_rs->create({
                                  id_seq_composition => $id,
                                  id_uqc_outcome     => $dict{'Accepted'},
                                  username           => 'cat',
                                  modified_by        => 'dog',
                                  rationale          => 'test',
                                  last_modified      => $time
                                });
    }
  } 
  
  $init->{merge_lanes } = 0;

  $f = npg_pipeline::function::pp_archiver->new($init);
  $manifest_path = $f->_manifest_path;
  ok (!-e $manifest_path, 'manifest file does not exist');
  $ds = $f->create();
  is (scalar @{$ds}, 1, 'one definition is generated');
  is ($ds->[0]->command, "$exec $manifest_path", 'correct command');
  ok (-e $manifest_path, 'manifest file exists');
  @lines = read_file($manifest_path);
  is (scalar @lines, 4, 'manifest contains 4 lines');
  unlink $manifest_path;
  shift @lines;
  @line = (
    'A1',
    "$pp_archive_path/lane1/plex1/ncov2019_artic_nf/v.3/qc_pass_climb_upload/*/*/*{am,fa}",
    "$dir/staging/26291/BAM_basecalls_20180805-013153/180709_A00538_0010_BH3FCMDRXX",
    '{"components":[{"id_run":26291,"position":1,"tag_index":1}]}',
    "3709acf46bbedf27819413030709fb2f196ba5e8642b4d2b4319f7bddfa8c2c9\n");
  is ((shift @lines), join(qq[\t], @line), 'correct line for unmerged plex 1');
  like ((shift @lines), qr{/lane1/plex2/}, 'correct line for unmerged plex 2');
  like ((shift @lines), qr{/lane1/plex3/}, 'correct line for unmerged plex 3');
  
  $rows->{1}->{1}->update({id_uqc_outcome => $dict{'Rejected'}});
  $rows->{1}->{2}->update({id_uqc_outcome => $dict{'Undecided'}});

  $f = npg_pipeline::function::pp_archiver->new($init);
  $manifest_path = $f->_manifest_path;
  ok (!-e $manifest_path, 'manifest file does not exist');
  $ds = $f->create();
  is (scalar @{$ds}, 1, 'one definition is generated');
  is ($ds->[0]->excluded, undef, 'function is not excluded');
  ok (-e $manifest_path, 'manifest file exists');
  @data_products = @{$f->products->{'data_products'}};
  is (scalar @data_products, 10, '10 data products');
  is (scalar(grep { $f->is_release_data($_) } @data_products), 6,
    '6 products for release');
  
  @lines = read_file($manifest_path);
  is (scalar @lines, 4, 'manifest contains 4 lines');
  unlink $manifest_path;
  shift @lines;
  like ((shift @lines), qr{/lane2/plex1/}, 'correct line for unmerged plex 1');
  like ((shift @lines), qr{/lane2/plex2/}, 'correct line for unmerged plex 2');
  like ((shift @lines), qr{/lane1/plex3/}, 'correct line for unmerged plex 3');

  $rows->{1}->{3}->delete();

  $f = npg_pipeline::function::pp_archiver->new($init);
  $manifest_path = $f->_manifest_path;
  ok (!-e $manifest_path, 'manifest file does not exist');
  $ds = $f->create();
  is (scalar @{$ds}, 1, 'one definition is generated');
  is ($ds->[0]->excluded, undef, 'function is not excluded');
  ok (-e $manifest_path, 'manifest file exists');
  @lines = read_file($manifest_path);
  is (scalar @lines, 4, 'manifest contains 4 lines');
  unlink $manifest_path;
  shift @lines;
  like ((shift @lines), qr{/lane2/plex1/}, 'correct line for unmerged plex 1');
  like ((shift @lines), qr{/lane2/plex2/}, 'correct line for unmerged plex 2');
  like ((shift @lines), qr{/lane2/plex3/}, 'correct line for unmerged plex 3');

  $rows->{2}->{3}->delete();

  $f = npg_pipeline::function::pp_archiver->new($init);
  $manifest_path = $f->_manifest_path;
  ok (!-e $manifest_path, 'manifest file does not exist');
  $ds = $f->create();
  is (scalar @{$ds}, 1, 'one definition is generated');
  is ($ds->[0]->excluded, undef, 'function is not excluded');
  ok (-e $manifest_path, 'manifest file exists');
  @lines = read_file($manifest_path);
  is (scalar @lines, 3, 'manifest contains 3 lines');
  unlink $manifest_path;
  shift @lines;
  like ((shift @lines), qr{/lane2/plex1/}, 'correct line for unmerged plex 1');
  like ((shift @lines), qr{/lane2/plex2/}, 'correct line for unmerged plex 2'); 
};

subtest 'skip unknown pipeline' => sub {
  plan tests => 2;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_33990.csv];
  my $f = npg_pipeline::function::pp_archiver->new(
    product_conf_file_path => qq[$repo_dir/product_release_unknown_pp.yml],
    archive_path           => $archive_path,
    runfolder_path         => $runfolder_path,
    id_run                 => 26291,
    timestamp              => $timestamp,
    repository             => $dir,
    qc_schema              => undef,
  );

  my $ds = $f->create();
  is (scalar @{$ds}, 1, '1 definition is returned');
  is ($ds->[0]->excluded, 1, 'function is excluded');
    'unknown pipeline archivable pipeline skipped';
};

1;
