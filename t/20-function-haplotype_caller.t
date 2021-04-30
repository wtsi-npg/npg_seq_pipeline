use strict;
use warnings;
use Test::More tests => 9;
use Test::Exception;
use File::Temp qw/tempdir/;
use File::Path qw/make_path/;
use File::Copy;
use Log::Log4perl qw/:levels/;

my $dir = tempdir( CLEANUP => 1);
# set this variable before the ref cache singleton is loaded
local $ENV{NPG_REPOSITORY_ROOT} = $dir;

use_ok('npg_pipeline::function::haplotype_caller');

my $repos   = join q[/], $dir, 'references';
my $fasta_dir = join q[/], $repos, 'Homo_sapiens/GRCh38_15_plus_hs38d1/all/fasta';
make_path $fasta_dir;
my $ref_fasta = "$fasta_dir/GRCh38_15_plus_hs38d1.fa";
open my $fh, '>', $ref_fasta or die 'failed to open file for writing';
print $fh 'test reference' or warn 'failed to print';
close $fh or warn 'failed to close file handle';

my $gatk_exec = join q[/], $dir, 'gatk';
open my $fh1, '>', $gatk_exec or die 'failed to open file for writing';
print $fh1 'echo "GATK mock"' or warn 'failed to print';
close $fh1 or warn 'failed to close file handle';
chmod 755, $gatk_exec;
local $ENV{PATH} = join q[:], $dir, $ENV{PATH};

my $logfile = join q[/], $dir, 'logfile';

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $dir, 'logfile'),
                          utf8   => 1});

# setup runfolder
my $runfolder_path = join q[/], $dir, 'novaseq', '180709_A00538_0010_BH3FCMDRXX';
my $archive_path = join q[/], $runfolder_path,
  'Data/Intensities/BAM_basecalls_20180805-013153/no_cal/archive';
my $no_archive_path = join q[/], $runfolder_path,
'Data/Intensities/BAM_basecalls_20180805-013153/no_archive';

make_path $archive_path;
make_path $no_archive_path;
my $timestamp      = '20180701-123456';

my $command = qq{$gatk_exec HaplotypeCaller --emit-ref-confidence GVCF -R $ref_fasta --pcr-indel-model NONE -I $archive_path/plex4/26291#4.cram -O $no_archive_path/plex4/chunk/26291#4.1.g.vcf.gz -L $dir/calling_intervals/Homo_sapiens/GRCh38_15_plus_hs38d1/hs38primary/hs38primary.1.interval_list};

local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/novaseq/180709_A00538_0010_BH3FCMDRXX/Data/Intensities/BAM_basecalls_20180805-013153/metadata_cache_26291/samplesheet_26291.csv];

copy('t/data/novaseq/180709_A00538_0010_BH3FCMDRXX/RunInfo.xml', "$runfolder_path/RunInfo.xml") or die
'Copy failed';
copy('t/data/novaseq/180709_A00538_0010_BH3FCMDRXX/RunParameters.xml', "$runfolder_path/runParameters.xml")
or die 'Copy failed';


subtest 'no_haplotype_caller flag' => sub {
  plan tests => 4;

  my $hc = npg_pipeline::function::haplotype_caller->new(
    conf_path           => 't/data/release/config/haplotype_caller_on_study_specific',
    archive_path        => $archive_path,
    runfolder_path      => $runfolder_path,
    id_run              => 26291,
    timestamp           => $timestamp,
    no_haplotype_caller => 1);
  ok($hc->no_haplotype_caller, 'no_haplotype_caller flag is set to true');
  my $ds = $hc->create;
  is(scalar @{$ds}, 1, 'one definition is returned');
  isa_ok($ds->[0], 'npg_pipeline::function::definition');
  is($ds->[0]->excluded, 1, 'function is excluded');
};

subtest 'no_haplotype_caller flag unset' => sub {
  plan tests => 4;

  my $hc = npg_pipeline::function::haplotype_caller->new(
    conf_path          => 't/data/release/config/haplotype_caller_on_study_specific',
    archive_path        => $archive_path,
    runfolder_path      => $runfolder_path,
    id_run              => 26291,
    timestamp           => $timestamp,
    repository          => $dir);
  ok($hc->no_haplotype_caller == 0, 'no_haplotype_caller flag is set to false');
  my $ds = $hc->create;
  is(scalar @{$ds}, 288, '288 definitions are returned');
  isa_ok($ds->[0], 'npg_pipeline::function::definition');
  is($ds->[0]->excluded, undef, 'function is not excluded');
};

subtest 'no_haplotype_caller flag unset and no study settings' => sub {
  plan tests => 4;

  my $hc = npg_pipeline::function::haplotype_caller->new(
    conf_path          => 't/data/release/config/haplotype_caller_on',
    archive_path        => $archive_path,
    runfolder_path      => $runfolder_path,
    id_run              => 26291,
    timestamp           => $timestamp,
    repository          => $dir);
  ok($hc->no_haplotype_caller == 0, 'no_haplotype_caller flag is set to false');
  my $ds = $hc->create;
  is(scalar @{$ds}, 1, '1 definitions are returned');
  isa_ok($ds->[0], 'npg_pipeline::function::definition');
  is($ds->[0]->excluded, 1, 'function is excluded');
};

subtest 'no_haplotype_caller flag unset and study reference settings' => sub {
  plan tests => 4;

  my $hc = npg_pipeline::function::haplotype_caller->new(
    conf_path          => 't/data/release/config/haplotype_caller_on_study_specific_reference',
    archive_path        => $archive_path,
    runfolder_path      => $runfolder_path,
    id_run              => 26291,
    timestamp           => $timestamp,
    repository          => $dir);
  ok($hc->no_haplotype_caller == 0, 'no_haplotype_caller flag is set to false');
  my $ds = $hc->create;
  is(scalar @{$ds}, 288, '288 definitions are returned');
  isa_ok($ds->[0], 'npg_pipeline::function::definition');
  is($ds->[0]->excluded, undef, 'function is not excluded');
};

subtest 'no_haplotype_caller flag unset and study wrong reference settings' => sub {
  plan tests => 4;

  my $hc = npg_pipeline::function::haplotype_caller->new(
    conf_path          => 't/data/release/config/haplotype_caller_off_study_specific_reference',
    archive_path        => $archive_path,
    runfolder_path      => $runfolder_path,
    id_run              => 26291,
    timestamp           => $timestamp,
    repository          => $dir);
  ok($hc->no_haplotype_caller == 0, 'no_haplotype_caller flag is set to false');
  my $ds = $hc->create;
  is(scalar @{$ds}, 1, '1 definitions are returned');
  isa_ok($ds->[0], 'npg_pipeline::function::definition');
  is($ds->[0]->excluded, 1, 'function is excluded as study on but wrong reference');
};

subtest 'run hc' => sub {
  plan tests => 20;

  my $hc_gen;
  lives_ok {
    $hc_gen = npg_pipeline::function::haplotype_caller->new(
      conf_path         => 't/data/release/config/haplotype_caller_on_study_specific',
      archive_path      => $archive_path,
      runfolder_path    => $runfolder_path,
      id_run            => 26291,
      timestamp         => $timestamp,
      repository        => $dir
    )
  } 'no error creating an object';

  my $da = $hc_gen->create();

  ok ($da && @{$da} == 288, sprintf("array of 288 definitions is returned, got %d", scalar@{$da}));

  ok (-d "$no_archive_path/plex4/chunk", 'output directory created');

  my $mem = 8000;
  my $d = $da->[72]; # 73rd array member
  isa_ok ($d, 'npg_pipeline::function::definition');
  is ($d->created_by, 'npg_pipeline::function::haplotype_caller', 'created by correct');
  is ($d->created_on, $timestamp, 'timestamp');
  is ($d->identifier, 26291, 'identifier is set correctly');
  is ($d->job_name, 'haplotype_caller_26291', 'job name');
  ok (!$d->excluded, 'step not excluded');
  ok ($d->has_composition, 'composition is set');
  isa_ok ($d->composition, 'npg_tracking::glossary::composition',
    'composition object present');
  is ($d->composition->num_components, 2, 'two components in the composition');
  is ($d->command, $command, 'correct command for tag 4');
  is ($d->memory, $mem, "memory $mem");
  is ($d->command_preexec, undef);
  is ($d->queue, 'default', 'default queue');
  is_deeply ($d->num_cpus, [4], 'range of cpu numbers');
  is ($d->num_hosts, 1, 'one host');
  is ($d->fs_slots_num, 2, 'four sf slots');
  lives_ok {$d->freeze()} 'definition can be serialized to JSON';
};

subtest 'run hc with bqsr' => sub {
  plan tests => 20;

  my $hc_gen;
  lives_ok {
    $hc_gen = npg_pipeline::function::haplotype_caller->new(
      conf_path         => 't/data/release/config/haplotype_caller_bqsr_on_study_specific',
      archive_path      => $archive_path,
      runfolder_path    => $runfolder_path,
      id_run            => 26291,
      timestamp         => $timestamp,
      repository        => $dir
    )
  } 'no error creating an object';

  my $da = $hc_gen->create();

  ok ($da && @{$da} == 288, sprintf("array of 288 definitions is returned, got %d", scalar@{$da}));

  my $command =
    join q{ && },
    (q{TMPDIR=`mktemp -d -t bqsr-XXXXXXXXXX`},
    q{trap "(rm -r $TMPDIR || :)" EXIT},
    q{echo "BQSR tempdir: $TMPDIR"},
    qq{$gatk_exec ApplyBQSR -R $ref_fasta --preserve-qscores-less-than 6 --static-quantized-quals 10 --static-quantized-quals 20 --static-quantized-quals 30 --bqsr-recal-file $archive_path/plex4/26291#4.bqsr_table -I $archive_path/plex4/26291#4.cram -O \$TMPDIR/26291#4.1_bqsr.cram -L $dir/calling_intervals/Homo_sapiens/GRCh38_15_plus_hs38d1/hs38primary/hs38primary.1.interval_list},
    qq{$gatk_exec HaplotypeCaller --emit-ref-confidence GVCF -R $ref_fasta --pcr-indel-model NONE -I \$TMPDIR/26291#4.1_bqsr.cram -O $no_archive_path/plex4/chunk/26291#4.1.g.vcf.gz -L $dir/calling_intervals/Homo_sapiens/GRCh38_15_plus_hs38d1/hs38primary/hs38primary.1.interval_list});

  ok (-d "$no_archive_path/plex4/chunk", 'output directory created');

  my $mem = 8000;
  my $d = $da->[72]; # 73rd array member
  isa_ok ($d, 'npg_pipeline::function::definition');
  is ($d->created_by, 'npg_pipeline::function::haplotype_caller', 'created by correct');
  is ($d->created_on, $timestamp, 'timestamp');
  is ($d->identifier, 26291, 'identifier is set correctly');
  is ($d->job_name, 'haplotype_caller_26291', 'job name');
  ok (!$d->excluded, 'step not excluded');
  ok ($d->has_composition, 'composition is set');
  isa_ok ($d->composition, 'npg_tracking::glossary::composition',
    'composition object present');
  is ($d->composition->num_components, 2, 'two components in the composition');
  is ($d->command, $command, 'correct command for tag 4');
  is ($d->memory, $mem, "memory $mem");
  is ($d->command_preexec, undef);
  is ($d->queue, 'default', 'default queue');
  is_deeply ($d->num_cpus, [4], 'range of cpu numbers');
  is ($d->num_hosts, 1, 'one host');
  is ($d->fs_slots_num, 2, 'four sf slots');
  lives_ok {$d->freeze()} 'definition can be serialized to JSON';
};

subtest 'rep repos root from env' => sub {
  plan tests => 1;

  my $hc_gen = npg_pipeline::function::haplotype_caller->new(
    conf_path         => 't/data/release/config/haplotype_caller_on_study_specific',
    archive_path      => $archive_path,
    runfolder_path    => $runfolder_path,
    id_run            => 26291,
    timestamp         => $timestamp
  );
  my $da = $hc_gen->create();
  is ($da->[72]->command, $command, 'correct command for tag 4');
};

1;
