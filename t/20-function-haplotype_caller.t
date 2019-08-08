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

use_ok('npg_pipeline::function::haplotype_caller');

my $odir    = abs_path cwd;
my $dir     = tempdir( CLEANUP => 1);

my $logfile = join q[/], $dir, 'logfile';

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $dir, 'logfile'),
                          utf8   => 1});

# Create test reference repository
my %builds = ();
$builds{'Homo_sapiens'} = ['1000Genomes_hs37d5','GRCh38_15','GRCh38_full_analysis_set_plus_decoy_hla','GRCh38X','GRCh38_15_plus_hs38d1'];
$builds{'Mus_musculus'} = ['GRCm38','NCBIm37'];
$builds{'PhiX'} = ['Illumina'];
$builds{'Strongyloides_ratti'} = ['20100601'];
$builds{'Plasmodium_falciparum'} = ['3D7_Oct11v3'];
my %cbuilds = ();
$cbuilds{'Homo_sapiens'} = ['GRCh38_full_analysis_set_plus_decoy_hla'];

my $ref_dir = join q[/],$dir,'references';
my $tra_dir = join q[/],$dir,'transcriptomes';
my $cal_dir = join q[/],$dir,'calling_intervals';
foreach my $org (keys %builds){
    foreach my $rel (@{ $builds{$org} }){
        my $rel_dir     = join q[/],$ref_dir,$org,$rel,'all';
        my $bowtie2_dir = join q[/],$rel_dir,'bowtie2';
        my $bwa0_6_dir  = join q[/],$rel_dir,'bwa0_6';
        my $fasta_dir   = join q[/],$rel_dir,'fasta';
        my $picard_dir  = join q[/],$rel_dir,'picard';
        my $star_dir    = join q[/],$rel_dir,'star';
        my $hisat2_dir  = join q[/],$rel_dir,'hisat2';
        my $target_dir  = join q[/],$rel_dir,'target';
        my $targeta_dir = join q[/],$rel_dir,'target_autosome';
        make_path($bowtie2_dir, $bwa0_6_dir, $picard_dir, $star_dir,
                  $fasta_dir, $hisat2_dir, {verbose => 0});
        if($rel eq 'GRCh38_full_analysis_set_plus_decoy_hla'){
          make_path($target_dir, $targeta_dir, {verbose => 0});
        } 
    }
    symlink_default($ref_dir,$org,$builds{$org}->[0]);
}
foreach my $org (keys %cbuilds){
    foreach my $rel (@{ $cbuilds{$org} }){
        my $rel_dir     = join q[/],$cal_dir,$org,$rel,'hs38primary';
        make_path($rel_dir);
        foreach my $i (1..22) {
            `touch $cal_dir/$org/$rel/hs38primary/hs38primary.$i.intervals_list`;
        }
    }
}

my $gbs_dir    = join q[/],$dir,'gbs_plex','Hs_MajorQC','default','all';
foreach my $gtype_dir (qw/fasta bwa0_6 picard/) {
    my $gdir = join q[/],$gbs_dir,$gtype_dir;
    make_path($gdir, {verbose => 0});
    `touch $gdir/Hs_MajorQC.fa`;
}

# make default symlink
sub symlink_default {
    my($ref_dir,$org,$rel) = @_;
    my $orig_dir = getcwd();
    my $rel_dir = join q[/],$ref_dir,$org;
    chdir qq[$rel_dir];
    eval { symlink($rel,"default") };
    print "symlink error $@" if $@;
    chdir $orig_dir;
    return;
}

`touch $ref_dir/PhiX/default/all/bwa0_6/phix.fa`;
`touch $ref_dir/PhiX/Illumina/all/fasta/phix-illumina.fa`;
`touch $ref_dir/PhiX/default/all/picard/phix.fa.dict`;
`touch $ref_dir/Homo_sapiens/1000Genomes_hs37d5/all/fasta/hs37d5.fa`;
`touch $ref_dir/Homo_sapiens/1000Genomes_hs37d5/all/picard/hs37d5.fa`;
`touch $ref_dir/Homo_sapiens/1000Genomes_hs37d5/all/bwa0_6/hs37d5.fa`;
`touch $ref_dir/Homo_sapiens/1000Genomes_hs37d5/all/bowtie2/hs37d5.fa`;
`touch $ref_dir/Homo_sapiens/1000Genomes_hs37d5/all/bowtie2/Homo_sapiens.GRCh37.NCBI.allchr_MT.fa`;
`touch $ref_dir/Homo_sapiens/GRCh38_15/all/bwa0_6/Homo_sapiens.GRCh38_15.fa`;
`touch $ref_dir/Homo_sapiens/GRCh38_15/all/fasta/Homo_sapiens.GRCh38_15.fa`;
`touch $ref_dir/Homo_sapiens/GRCh38_15/all/picard/Homo_sapiens.GRCh38_15.fa.dict`;
`touch $ref_dir/Homo_sapiens/GRCh38_15/all/hisat2/Homo_sapiens.GRCh38_15.1.ht2`;
`touch $ref_dir/Homo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/all/fasta/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa`;
`touch $ref_dir/Homo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/all/picard/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa.dict`;
`touch $ref_dir/Homo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/all/bwa0_6/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa.alt`;
`touch $ref_dir/Homo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/all/target/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa.interval_list`;
`touch $ref_dir/Homo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/all/target_autosome/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa.interval_list`;
`touch $ref_dir/Homo_sapiens/GRCh38X/all/fasta/Homo_sapiens.GRCh38X.fa`;
`touch $ref_dir/Homo_sapiens/GRCh38_15_plus_hs38d1/all/fasta/GRCh38_15_plus_hs38d1.fa`;
`touch $ref_dir/Strongyloides_ratti/20100601/all/fasta/rat.fa`;
`touch $ref_dir/Strongyloides_ratti/20100601/all/picard/rat.fa`;
`touch $ref_dir/Strongyloides_ratti/20100601/all/bwa0_6/rat.fa`;
`touch $ref_dir/Mus_musculus/GRCm38/all/fasta/Mus_musculus.GRCm38.68.dna.toplevel.fa`;
`touch $ref_dir/Mus_musculus/GRCm38/all/star`;
`touch $ref_dir/Mus_musculus/NCBIm37/all/fasta/mm_ref_NCBI37_1.fasta`;
`touch $ref_dir/Mus_musculus/NCBIm37/all/bowtie2/mm_ref_NCBI37_1.fasta`;
`touch $ref_dir/Mus_musculus/NCBIm37/all/picard/mm_ref_NCBI37_1.fasta`;
`touch $ref_dir/Mus_musculus/NCBIm37/all/bwa0_6/mm_ref_NCBI37_1.fasta`;
`touch $ref_dir/Plasmodium_falciparum/3D7_Oct11v3/all/fasta/Pf3D7_v3.fasta`;
`touch $ref_dir/Plasmodium_falciparum/3D7_Oct11v3/all/picard/Pf3D7_v3.fasta.dict`;


###12597_1    study: genomic sequencing, library type: No PCR
###12597_8#7  npg/run/12597.xml st/studies/2775.xml  batches/26550.xml samples/1886325.xml  <- Epigenetics, library type: qPCR only
###12597_4#3  npg/run/12597.xml st/studies/2893.xml  batches/26550.xml samples/1886357.xml  <- transcriptomics, library type: RNA-seq dUTP
###   1886357.xml edited to change reference to Mus_musculus (NCBIm37 + ensembl_67_transcriptome)
### batches/26550.xml edited to have the following plex composition: # plex TTAGGC 3 lane 4 and plex CAGATC 7 lane 8

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

  my $hc = npg_pipeline::function::haplotype_caller->new
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

  my $hc = npg_pipeline::function::haplotype_caller->new
    (conf_path          => "t/data/release/config/haplotype_caller_on",
    runfolder_path      => $runfolder_path,
    id_run              => 26291,
    timestamp           => $timestamp,
    qc_schema           => $qc,
    repository          => $dir);
  ok($hc->no_haplotype_caller == 0, 'no_haplotype_caller flag is set to false');
  my $ds = $hc->create;
  is(scalar @{$ds}, 288, '288 definitions are returned');
  isa_ok($ds->[0], 'npg_pipeline::function::definition');
  is($ds->[0]->excluded, undef, 'function is not excluded');
};


subtest 'run hc' => sub {
  plan tests => 21;

  my $hc_gen;
  lives_ok {
    $hc_gen = npg_pipeline::function::haplotype_caller->new(
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

  is ($hc_gen->id_run, 26291, 'id_run inferred correctly');

  my $qc_in  = $dir . q[/novaseq/180709_A00538_0010_BH3FCMDRXX/Data/Intensities/BAM_basecalls_20180805-013153/no_cal/archive/lane4/plex3];
  my $qc_out = join q[/], $qc_in, q[qc];
  make_path "$bc_path/archive/tileviz";
  apply_all_roles($hc_gen, 'npg_pipeline::runfolder_scaffold');
  $hc_gen->create_product_level();

  my $da = $hc_gen->create();

  ok ($da && @{$da} == 288, sprintf("array of 288 definitions is returned, got %d", scalar@{$da}));

  my $command = qq{gatk HaplotypeCaller --emit-ref-confidence GVCF -R $dir/references/Homo_sapiens/GRCh38_15_plus_hs38d1/all/fasta/GRCh38_15_plus_hs38d1.fa --pcr-indel-model CONSERVATIVE -I $dir/novaseq/180709_A00538_0010_BH3FCMDRXX/Data/Intensities/BAM_basecalls_20180805-013153/no_cal/archive/plex4/26291#4.cram -O $dir/novaseq/180709_A00538_0010_BH3FCMDRXX/Data/Intensities/BAM_basecalls_20180805-013153/no_cal/archive/plex4/chunk/26291#4.1.g.vcf.gz -L $dir/calling_intervals/Homo_sapiens/GRCh38_15_plus_hs38d1/hs38primary/hs38primary.1.interval_list};

  ok (-d "$dir/novaseq/180709_A00538_0010_BH3FCMDRXX/Data/Intensities/BAM_basecalls_20180805-013153/no_cal/archive/plex4/chunk",
    'output directory created');

  my $mem = 3600;
  my $d = _find($da, 1, 4);
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
  is ($d->command, $command, 'correct command for position 2, tag 4');
  is ($d->memory, $mem, "memory $mem");
  is ($d->command_preexec, undef);
  is ($d->queue, 'default', 'default queue');
  is_deeply ($d->num_cpus, [4], 'range of cpu numbers');
  is ($d->num_hosts, 1, 'one host');
  is ($d->fs_slots_num, 2, 'four sf slots');
  lives_ok {$d->freeze()} 'definition can be serialized to JSON';
};
