use strict;
use warnings;
use Test::More tests => 4;
use Test::Exception;
use Test::Deep;
use Test::Warn;
use File::Temp qw/tempdir/;
use File::Path qw/make_path/;
use Cwd qw/cwd abs_path/;
use Perl6::Slurp;
use File::Copy;
use Log::Log4perl qw/:levels/;
use JSON;
use Cwd;
use List::Util qw/first/;

use Moose::Util qw(apply_all_roles);

use st::api::lims;

use_ok('npg_pipeline::function::merge_recompress');

my $dir     = tempdir( CLEANUP => 1);

# Setup TestDB
{
package TestDB;
use Moose;

with 'npg_testing::db';
}

# See README in fixtures for a description of the test data.
my $qc = TestDB->new
(sqlite_utf8_enabled => 1,
verbose             => 0)->create_test_db('npg_qc::Schema',
't/data/qc_outcomes/fixtures');
# setup runfolder
my $runfolder      = '180709_A00538_0010_BH3FCMDRXX';
my $runfolder_path = join q[/], $dir, 'novaseq', $runfolder;
my $timestamp      = '20180701-123456';

local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/novaseq/180709_A00538_0010_BH3FCMDRXX/Data/Intensities/BAM_basecalls_20180805-013153/metadata_cache_26291/samplesheet_26291.csv];
my $bc_path = join q[/], $runfolder_path,
'Data/Intensities/BAM_basecalls_20180805-013153/no_cal';
for ((4, 5)) {
`mkdir -p $bc_path/lane$_`;
}

copy('t/data/novaseq/180709_A00538_0010_BH3FCMDRXX/RunInfo.xml', "$runfolder_path/RunInfo.xml") or die
'Copy failed';
copy('t/data/novaseq/180709_A00538_0010_BH3FCMDRXX/RunParameters.xml', "$runfolder_path/runParameters.xml")
or die 'Copy failed';

subtest 'no_haplotype_caller flag' => sub {
  plan tests => 4;

  my $hc = npg_pipeline::function::merge_recompress->new
    (conf_path          => "t/data/release/config/haplotype_caller_on",
    runfolder_path      => $runfolder_path,
    id_run              => 26291,
    timestamp           => $timestamp,
    qc_schema           => $qc,
    no_haplotype_caller => 1);
  ok($hc->no_haplotype_caller, 'no_haplotype_caller flag is set to true');
  my $ds = $hc->create;
  is(scalar @{$ds}, 1, 'one definition is returned');
  isa_ok($ds->[0], 'npg_pipeline::function::definition');
  is($ds->[0]->excluded, 1, 'function is excluded');
};

subtest 'no_haplotype_caller flag unset' => sub {
  plan tests => 4;

  my $hc = npg_pipeline::function::merge_recompress->new
    (conf_path          => "t/data/release/config/haplotype_caller_on",
    runfolder_path      => $runfolder_path,
    id_run              => 26291,
    timestamp           => $timestamp,
    qc_schema           => $qc,
    repository          => $dir);
  ok($hc->no_haplotype_caller == 0, 'no_haplotype_caller flag is set to false');
  my $ds = $hc->create;
  is(scalar @{$ds}, 12, '12 definitions are returned');
  isa_ok($ds->[0], 'npg_pipeline::function::definition');
  is($ds->[0]->excluded, undef, 'function is not excluded');
};

#######
# In an array of definitions find and return a definition
# that corresponds to a position and, optionally, tag index.
# Use composition property of the definition object.
#
sub _find {
  my ($a, $p, $t) = @_;
  my $d= first { my $c = $_->composition->get_component(0);
                 $c->position == $p &&
                 (defined $t ? $c->tag_index == $t : !defined $c->tag_index) }
         @{$a};
  if (!$d) {
    die "failed to find definition for position $p, tag " . defined $t ? $t : 'none';
  }
  return $d;
}

subtest 'run merge_recompress' => sub {
  plan tests => 20;

  my $rna_gen;
  lives_ok {
  $rna_gen = npg_pipeline::function::merge_recompress->new(
    conf_path         => 't/data/release/config/haplotype_caller_on',
    run_folder        => $runfolder,
    runfolder_path    => $runfolder_path,
    id_run            => 26291,
    timestamp         => $timestamp,
    verbose           => 0,
    qc_schema         => $qc,
    repository        => $dir
  )
  } 'no error creating an object';

  is ($rna_gen->id_run, 26291, 'id_run inferred correctly');

  my $qc_in  = $dir . q[/novaseq/180709_A00538_0010_BH3FCMDRXX/Data/Intensities/BAM_basecalls_20180805-013153/no_cal/archive/lane4/plex3];
  my $qc_out = join q[/], $qc_in, q[qc];
  make_path "$bc_path/archive/tileviz";
  apply_all_roles($rna_gen, 'npg_pipeline::runfolder_scaffold');
  $rna_gen->create_product_level();

  my $da = $rna_gen->create();

  ok ($da && @{$da} == 12, sprintf("array of 12 definitions is returned, got %d", scalar@{$da}));

  my @input_files = map { sprintf "$dir/novaseq/180709_A00538_0010_BH3FCMDRXX/Data/Intensities/BAM_basecalls_20180805-013153/no_cal/archive/plex4/chunk/26291#4.%s.g.vcf.gz", $_ } (1..24);

  my $input_files_str = join q{ }, @input_files;

  my $command = qq{bcftools concat -O z -o $dir/novaseq/180709_A00538_0010_BH3FCMDRXX/Data/Intensities/BAM_basecalls_20180805-013153/no_cal/archive/plex4/26291#4.g.vcf.gz }.$input_files_str;

  my $mem = 2000;
  my $d = _find($da, 1, 4);
  isa_ok ($d, 'npg_pipeline::function::definition');
  is ($d->created_by, 'npg_pipeline::function::merge_recompress', 'created by correct');
  is ($d->created_on, $timestamp, 'timestamp');
  is ($d->identifier, 26291, 'identifier is set correctly');
  is ($d->job_name, 'merge_recompress_26291', 'job name');
  ok (!$d->excluded, 'step not excluded');
  ok ($d->has_composition, 'composition is set');
  isa_ok ($d->composition, 'npg_tracking::glossary::composition',
  'composition object present');
  is ($d->composition->num_components, 2, 'two components in the composition');
  is ($d->command, $command, 'correct command for position 2, tag 4');
  is ($d->memory, $mem, "memory $mem");
  is ($d->command_preexec, undef);
  is ($d->queue, 'default', 'default queue');
  is_deeply ($d->num_cpus, [1], 'range of cpu numbers');
  is ($d->num_hosts, 1, 'one host');
  is ($d->fs_slots_num, 2, 'two sf slots');
  lives_ok {$d->freeze()} 'definition can be serialized to JSON';
};
