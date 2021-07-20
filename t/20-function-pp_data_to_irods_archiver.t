use strict;
use warnings;
use Cwd;
use Math::Random::Secure qw(srand);
use JSON;
use File::Slurp;
use Test::More tests => 4;
use Test::Exception;

my $runfolder_path = 't/data/novaseq/200709_A00948_0157_AHM2J2DRXX';
my $bbc_path = join q[/], getcwd(), $runfolder_path,
               'Data/Intensities/BAM_basecalls_20200710-105415';

local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = join q[/], $bbc_path,
        'metadata_cache_34576/samplesheet_34576.csv';

my $pkg = 'npg_pipeline::function::pp_data_to_irods_archiver';
use_ok($pkg);

my %init = (
  conf_path => 't/data/release/config/pp_archival',
  id_run => 34576,
  runfolder_path => $runfolder_path,
  resource => {
    default => {
      minimum_cpu => 1,
      memory => 2,
      queue => 'lowload',
      fs_slots_num => 1,
      reserve_irods_slots => 1
    }
  }
);


subtest 'local flag' => sub {
  plan tests => 3;

  my $archiver = $pkg->new(
    %init,
    local => 1,
  );

  my $ds = $archiver->create;
  is(scalar @{$ds}, 1, 'one definition is returned');
  isa_ok($ds->[0], 'npg_pipeline::function::definition');
  is($ds->[0]->excluded, 1, 'function is excluded');
};

subtest 'no_irods_archival flag' => sub {
  plan tests => 3;

  my $archiver = $pkg->new(
    %init,
    no_irods_archival => 1
  );
  my $ds = $archiver->create;
  is(scalar @{$ds}, 1, 'one definition is returned');
  isa_ok($ds->[0], 'npg_pipeline::function::definition');
  is($ds->[0]->excluded, 1, 'function is excluded');
};

subtest 'create job definition' => sub {
  plan tests => 32;

  # To predict the name of the file with metadata,
  # seed the random number generator.
  srand('1x4y5z8k');

  my $archiver = $pkg->new(
    %init,
    timestamp => '20200806-130730',
  );
  my $ds = $archiver->create;

  my $num_expected = 407;
  is(scalar @{$ds}, $num_expected, "expected $num_expected definitions");
  my $d = $ds->[0];
  isa_ok($d, 'npg_pipeline::function::definition');
  ok (!$d->excluded, 'function is not excluded');
  is ($d->queue, 'lowload', 'queue is lowload');
  ok ($d->reserve_irods_slots, 'reserve_irods_slots flag is true');
  is ($d->fs_slots_num, 1, 'number of fs slots is 1');
  ok ($d->composition, 'composition attribute is defined');
  is ($d->composition->freeze2rpt, '34576:1:1',
    'composition is for lane 1 plex 1');
  ok ($d->command_preexec, 'command preexec is defined');
  is ($d->identifier, 34576, 'job identifier is run id');
  is ($d->created_by, $pkg, "definition created by $pkg");
  is ($d->created_on, '20200806-130730', 'correct timestamp');
  is ($d->job_name, 'pp_data_to_irods_archiver_34576_20200806-130730',
    'job_name is correct');

  my $meta_file = $bbc_path . q(/irods_publisher_restart_files/) .
    q(pp_data_to_irods_archiver_34576_20200806-130730-2065184135_) .
    q(d28ec931b99c952007283973d380111784f69ed3215cffb2783a9fb878961798.metadata.json);

  is ($d->command, 'npg_publish_tree.pl' .
    q( --collection /seq/illumina/pp/runs/34/34576/lane1/plex1) .
    q( --source ) . $bbc_path . q(/pp_archive/lane1/plex1) .
    q( --group 'ss_6187#seq') .
    q( --metadata ) . $meta_file .
    q( --include 'ncov2019_artic_nf/v0.(7|8)\\b\\S+trim\\S+/\\S+bam') .
    q( --include 'ncov2019_artic_nf/v0.(11)\\b\\S+trim\\S+/\\S+cram') .
    q( --include 'ncov2019_artic_nf/v0.\\d+\\b\\S+make\\S+/\\S+consensus.fa') .
    q( --include 'ncov2019_artic_nf/v0.\\d+\\b\\S+call\\S+/\\S+variants.tsv') .
    q( --exclude 'test_file_pollution'),
    'correct command');

  ok (-e $meta_file, 'metadata file (with sample supplier name) is created');
  my $meta = from_json(read_file($meta_file));
  is (scalar @{$meta}, 4);
  my $h = $meta->[0];
  is ($h->{attribute}, 'composition');
  is ($h->{value}, '{"components":[{"id_run":34576,"position":1,"tag_index":1}]}');
  $h = $meta->[1];
  is ($h->{attribute}, 'id_product');
  is ($h->{value}, 'd28ec931b99c952007283973d380111784f69ed3215cffb2783a9fb878961798');
  $h = $meta->[2];
  is ($h->{attribute}, 'sample_supplier_name');
  is ($h->{value}, 'BRIS-1852F16');
  $h = $meta->[3];
  is ($h->{attribute}, 'target');
  is ($h->{value}, 'pp');

  # missing Sample supplier name
  $meta_file = $bbc_path . q(/irods_publisher_restart_files/) .
    q(pp_data_to_irods_archiver_34576_20200806-130730-) .
    q(2769995162_696911af0b6c1ecae6cd7ed3f3c5c9961a1d7d9f6075a5efbaa080e0c6410a33) .
    q(.metadata.json);
  ok (-e $meta_file, 'metadata (without sample supplier name) file is created');
  $meta = from_json(read_file($meta_file));
  is (scalar @{$meta}, 3);
  $h = $meta->[0];
  is ($h->{attribute}, 'composition');
  is ($h->{value}, '{"components":[{"id_run":34576,"position":1,"tag_index":2}]}');
  $h = $meta->[1];
  is ($h->{attribute}, 'id_product');
  is ($h->{value}, '696911af0b6c1ecae6cd7ed3f3c5c9961a1d7d9f6075a5efbaa080e0c6410a33');
  $h = $meta->[2];
  is ($h->{attribute}, 'target');
  is ($h->{value}, 'pp');

};

