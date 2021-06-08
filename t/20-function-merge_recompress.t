use strict;
use warnings;
use Test::More tests => 4;
use Test::Exception;
use File::Temp qw/tempdir/;
use File::Path qw/make_path/;
use File::Copy;
use Log::Log4perl qw/:levels/;


my $dir     = tempdir( CLEANUP => 1);
local $ENV{NPG_REPOSITORY_ROOT} = $dir;

use_ok('npg_pipeline::function::merge_recompress');

+my $bcftools_exec = join q[/], $dir, 'bcftools';
+open my $fh, '>', $bcftools_exec or die 'failed to open file for writing';
+print $fh 'echo "bcftools executable  mock"' or warn 'failed to print';
+close $fh or warn 'failed to close file handle';
+chmod 755, $bcftools_exec;
+local $ENV{PATH} = join q[:], $dir, $ENV{PATH};

# setup runfolder
my $runfolder_path = join q[/], $dir, '180709_A00538_0010_BH3FCMDRXX';
my $archive_path   = join q[/], $runfolder_path,
  'Data/Intensities/BAM_basecalls_20180805-013153/no_cal/archive';
my $no_archive_path   = join q[/], $runfolder_path,
'Data/Intensities/BAM_basecalls_20180805-013153/no_archive';
make_path $archive_path;
make_path $no_archive_path;
my $timestamp = '20180701-123456';

local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/novaseq/180709_A00538_0010_BH3FCMDRXX/Data/Intensities/BAM_basecalls_20180805-013153/metadata_cache_26291/samplesheet_26291.csv];

copy('t/data/novaseq/180709_A00538_0010_BH3FCMDRXX/RunInfo.xml', "$runfolder_path/RunInfo.xml") or die
'Copy failed';
copy('t/data/novaseq/180709_A00538_0010_BH3FCMDRXX/RunParameters.xml', "$runfolder_path/runParameters.xml")
or die 'Copy failed';

my $conf_path = 't/data/release/config/haplotype_caller_on_study_specific';

my %common_args = (
  conf_path           => $conf_path,
  archive_path        => $archive_path,
  runfolder_path      => $runfolder_path,
  id_run              => 26291,
  resource            => {
    default => {
      minimum_cpu => 1,
      memory => 2,
      fs_slots_num => 2
    }
  }
);

subtest 'no_haplotype_caller flag' => sub {
  plan tests => 4;

  my $mr = npg_pipeline::function::merge_recompress->new(
    %common_args,
    no_haplotype_caller => 1,
  );
  ok($mr->no_haplotype_caller, 'no_haplotype_caller flag is set to true');
  my $ds = $mr->create;
  is(scalar @{$ds}, 1, 'one definition is returned');
  isa_ok($ds->[0], 'npg_pipeline::function::definition');
  is($ds->[0]->excluded, 1, 'function is excluded');
};

subtest 'no_haplotype_caller flag unset' => sub {
  plan tests => 4;

  my $mr = npg_pipeline::function::merge_recompress->new(
    %common_args,
    timestamp => $timestamp,
    repository => $dir
  );
  ok($mr->no_haplotype_caller == 0, 'no_haplotype_caller flag is set to false');
  my $ds = $mr->create;
  is(scalar @{$ds}, 12, '12 definitions are returned');
  isa_ok($ds->[0], 'npg_pipeline::function::definition');
  is($ds->[0]->excluded, undef, 'function is not excluded');
};

subtest 'run merge_recompress' => sub {
  plan tests => 17;

  my $mr = npg_pipeline::function::merge_recompress->new(
    %common_args,
    timestamp => $timestamp,
  );

  my $da = $mr->create();
  ok (($da && @{$da} == 12), sprintf("array of 12 definitions is returned, got %d", scalar@{$da}));

  my $plex4_archive = "$archive_path/plex4";
  my $plex4_no_archive = "$no_archive_path/plex4";
  my @input_files = map { sprintf "$plex4_no_archive/chunk/26291#4.%s.g.vcf.gz", $_ } (1..24);
  my $input_files_str = join q{ }, @input_files;

  my $command = qq{$bcftools_exec concat -O z -o $plex4_archive/26291#4.g.vcf.gz }.$input_files_str.qq{ && $bcftools_exec tabix -p vcf $plex4_archive/26291#4.g.vcf.gz};

  my $mem = 2000;
  my $d = $da->[3];
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
  is ($d->command, $command, 'correct command for tag 4');
  is ($d->memory, $mem, "memory $mem");
  is ($d->command_preexec, undef);
  is ($d->queue, 'default', 'default queue');
  is_deeply ($d->num_cpus, [1], 'range of cpu numbers');
  is ($d->fs_slots_num, 2, 'two sf slots');
  lives_ok {$d->freeze()} 'definition can be serialized to JSON';
};

1;

