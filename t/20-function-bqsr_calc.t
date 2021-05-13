use strict;
use warnings;
use Test::More tests => 5;
use Test::Exception;
use File::Temp qw/tempdir/;
use File::Path qw/make_path/;
use File::Copy;
use Log::Log4perl qw/:levels/;

my $dir = tempdir( CLEANUP => 1);
# set this variable before the ref cache singleton is loaded
local $ENV{NPG_REPOSITORY_ROOT} = $dir;

use_ok('npg_pipeline::function::bqsr_calc');

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $INFO,
                          file   => join(q[/], $dir, 'logfile'),
                          utf8   => 1});

my $ref_dir = join q[/],$dir,'references';
my $res_dir = join q[/],$dir,'resources';

my $fasta_dir = join q[/], $ref_dir, 'Homo_sapiens/GRCh38_15_plus_hs38d1/all/fasta';
make_path $fasta_dir;
my $ref_fasta = "$fasta_dir/GRCh38_15_plus_hs38d1.fa";
open my $fh, '>', $ref_fasta or die 'failed to open file for writing';
print $fh 'test reference' or warn 'failed to print';
close $fh or warn 'failed to close file handle';
my $annot_dir = "$res_dir/Homo_sapiens/GRCh38_15_plus_hs38d1";
make_path $annot_dir;

`touch $annot_dir/dbsnp_138.hg38.vcf.gz`;
`touch $annot_dir/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz`;

my $gatk_exec = join q[/], $dir, 'gatk';
open my $fh1, '>', $gatk_exec or die 'failed to open file for writing';
print $fh1 'echo "GATK mock"' or warn 'failed to print';
close $fh1 or warn 'failed to close file handle';
chmod 755, $gatk_exec;
local $ENV{PATH} = join q[:], $dir, $ENV{PATH};

# setup runfolder
my $runfolder_path = join q[/], $dir, '180709_A00538_0010_BH3FCMDRXX';
my $archive_path = join q[/], $runfolder_path,
  'Data/Intensities/BAM_basecalls_20180805-013153/no_cal/archive';
make_path $archive_path;
my $timestamp      = '20180701-123456';

my $command = qq{$gatk_exec BaseRecalibrator -O $archive_path/plex4/26291#4.bqsr_table -I $archive_path/plex4/26291#4.cram -R $fasta_dir/GRCh38_15_plus_hs38d1.fa --known-sites $annot_dir/dbsnp_138.hg38.vcf.gz --known-sites $annot_dir/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz};

local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/novaseq/180709_A00538_0010_BH3FCMDRXX/Data/Intensities/BAM_basecalls_20180805-013153/metadata_cache_26291/samplesheet_26291.csv];

copy('t/data/novaseq/180709_A00538_0010_BH3FCMDRXX/RunInfo.xml', "$runfolder_path/RunInfo.xml") or die
'Copy failed';
copy('t/data/novaseq/180709_A00538_0010_BH3FCMDRXX/RunParameters.xml', "$runfolder_path/runParameters.xml")
or die 'Copy failed';

my $default = {
  default => {
    fs_slots_num => 2,
    memory => 6,
    minimum_cpu => 1
  }
};

subtest 'no config' => sub {
  plan tests => 3;

  my $hc = npg_pipeline::function::bqsr_calc->new(
    conf_path           => 't/data/release/config/bqsr_off',
    archive_path        => $archive_path,
    runfolder_path      => $runfolder_path,
    id_run              => 26291,
    timestamp           => $timestamp,
    resource            => $default
  );
  my $ds = $hc->create;
  is(scalar @{$ds}, 1, 'one definition is returned');
  isa_ok($ds->[0], 'npg_pipeline::function::definition');
  is($ds->[0]->excluded, 1, 'function is excluded');
};

subtest 'bqsr study specific defaulted on' => sub {
  plan tests => 3;

  my $hc = npg_pipeline::function::bqsr_calc->new(
    conf_path           => "t/data/release/config/bqsr_on_study_specific",
    runfolder_path      => $runfolder_path,
    archive_path        => $archive_path,
    id_run              => 26291,
    timestamp           => $timestamp,
    repository          => $dir,
    resource            => $default
  );
  my $ds = $hc->create;
  is(scalar @{$ds}, 12, '12 definitions are returned');
  isa_ok($ds->[0], 'npg_pipeline::function::definition');
  is($ds->[0]->excluded, undef, 'function is not excluded');
};

subtest 'create function definitions' => sub {
  plan tests => 19;

  my $bqsr_gen;
  lives_ok {
  $bqsr_gen = npg_pipeline::function::bqsr_calc->new(
    conf_path         => 't/data/release/config/bqsr_on_study_specific',
    archive_path      => $archive_path,
    runfolder_path    => $runfolder_path,
    id_run            => 26291,
    timestamp         => $timestamp,
    verbose           => 0,
    repository        => $dir,
    resource            => $default
  )
  } 'no error creating an object';

  is ($bqsr_gen->id_run, 26291, 'id_run inferred correctly');
  my $da = $bqsr_gen->create();
  ok ($da && @{$da} == 12, sprintf("array of 12 definitions is returned, got %d", scalar@{$da}));

  my $mem = 6000;
  my $d = $da->[3];
  isa_ok ($d, 'npg_pipeline::function::definition');
  is ($d->created_by, 'npg_pipeline::function::bqsr_calc', 'created by correct');
  is ($d->created_on, $timestamp, 'timestamp');
  is ($d->identifier, 26291, 'identifier is set correctly');
  is ($d->job_name, 'bqsr_calc_26291', 'job name');
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
  is ($d->fs_slots_num, 2, 'two sf slots');
  lives_ok {$d->freeze()} 'definition can be serialized to JSON';
};

subtest 'rep repos root from env' => sub {
  plan tests => 1;

  my $bqsr_gen = npg_pipeline::function::bqsr_calc->new(
    conf_path         => 't/data/release/config/bqsr_on_study_specific',
    archive_path      => $archive_path,
    runfolder_path    => $runfolder_path,
    id_run            => 26291,
    timestamp         => $timestamp,
    verbose           => 0,
    resource          => $default
  );
  my $da = $bqsr_gen->create();
  is ($da->[3]->command, $command, 'correct command for tag 4');
};

1
