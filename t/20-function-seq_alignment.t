use strict;
use warnings;
use Test::More tests => 16;
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

use_ok('npg_pipeline::function::seq_alignment');

my $odir    = abs_path cwd;
my $dir     = tempdir( CLEANUP => 1);

my $logfile = join q[/], $dir, 'logfile';

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $dir, 'logfile'),
                          utf8   => 1});

# Create test reference repository
my %builds = ();
$builds{'Homo_sapiens'} = ['1000Genomes_hs37d5','GRCh38_15','GRCh38_full_analysis_set_plus_decoy_hla','GRCh38X'];
$builds{'Mus_musculus'} = ['GRCm38','NCBIm37'];
$builds{'PhiX'} = ['Illumina'];
$builds{'Strongyloides_ratti'} = ['20100601'];
$builds{'Plasmodium_falciparum'} = ['3D7_Oct11v3'];
my %tbuilds = ();
$tbuilds{'1000Genomes_hs37d5'} = ['ensembl_75_transcriptome'];
$tbuilds{'GRCh38_15'} = ['ensembl_76_transcriptome'];
$tbuilds{'NCBIm37'} = ['ensembl_67_transcriptome'];
$tbuilds{'GRCm38'} = ['ensembl_75_transcriptome','ensembl_84_transcriptome'];
$tbuilds{'3D7_Oct11v3'} = ['genedb_161015_transcriptome'];

my $ref_dir = join q[/],$dir,'references';
my $tra_dir = join q[/],$dir,'transcriptomes';
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
        if ($tbuilds{$rel}) {
            foreach my $tra_ver (@{ $tbuilds{$rel} }){
                my $tra_ver_dir = join q[/],$tra_dir,$org,$tra_ver,$rel;
                my $gtf_dir    = join q[/],$tra_ver_dir,'gtf';
                my $rnaseq_dir = join q[/],$tra_ver_dir,'RNA-SeQC';
                my $tophat_dir = join q[/],$tra_ver_dir,'tophat2';
                my $salmon_dir = join q[/],$tra_ver_dir,'salmon';
                my $fasta_dir  = join q[/],$tra_ver_dir,'fasta';
                make_path($gtf_dir, $rnaseq_dir, $tophat_dir, $salmon_dir, $fasta_dir, {verbose => 0});
            }
        }
    }
    symlink_default($ref_dir,$org,$builds{$org}->[0]);
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
`touch $tra_dir/Homo_sapiens/ensembl_75_transcriptome/1000Genomes_hs37d5/gtf/ensembl_75_transcriptome-1000Genomes_hs37d5.gtf`;
`touch $tra_dir/Homo_sapiens/ensembl_75_transcriptome/1000Genomes_hs37d5/tophat2/1000Genomes_hs37d5.known.2.bt2`;
`touch $tra_dir/Homo_sapiens/ensembl_75_transcriptome/1000Genomes_hs37d5/fasta/1000Genomes_hs37d5.fa`;
`touch $tra_dir/Mus_musculus/ensembl_67_transcriptome/NCBIm37/gtf/ensembl_67_transcriptome-NCBIm37.gtf`;
`touch $tra_dir/Mus_musculus/ensembl_67_transcriptome/NCBIm37/tophat2/NCBIm37.known.1.bt2`;
`touch $tra_dir/Mus_musculus/ensembl_84_transcriptome/GRCm38/gtf/ensembl_84_transcriptome-GRCm38.gtf`;
`touch $tra_dir/Mus_musculus/ensembl_84_transcriptome/GRCm38/fasta/GRCm38.fa`;

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

subtest 'basic functionality' => sub {
  plan tests => 33;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/rna_seq/samplesheet_12597.csv];
  my $runfolder = q{140409_HS34_12597_A_C333TACXX};
  my $runfolder_path = join q[/], $dir, $runfolder;
  my $bc_path = join q[/], $runfolder_path,
                'Data/Intensities/BAM_basecalls_20140515-073611/no_cal';
  for ((4, 5)) {
    `mkdir -p $bc_path/lane$_`;
  }

  copy('t/data/rna_seq/12597_RunInfo.xml', "$runfolder_path/RunInfo.xml") or die
    'Copy failed';
  copy('t/data/run_params/runParameters.hiseq.xml', "$runfolder_path/runParameters.xml")
    or die 'Copy failed';

  my $rna_gen;
  lives_ok {
    $rna_gen = npg_pipeline::function::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2014},
      verbose           => 0,
      repository        => $dir,
      force_phix_split  => 0,
      conf_path         => 't/data/release/config/seq_alignment',
    )
  } 'no error creating an object';

  is ($rna_gen->id_run, 12597, 'id_run inferred correctly');

  my $qc_in  = $dir . q[/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/plex3];
  my $qc_out = join q[/], $qc_in, q[qc];
  make_path "$bc_path/archive/tileviz";
  apply_all_roles($rna_gen, 'npg_pipeline::runfolder_scaffold');
  $rna_gen->create_product_level();

  my $da = $rna_gen->generate();

  ok ($da && @{$da} == 8, 'array of 8 definitions is returned');

  my $unique_string = $rna_gen->_job_id();
  my $tmp_dir = qq{$bc_path/archive/tmp_} . $unique_string;
  my $plex_temp_dir = $tmp_dir . q{/12597_4#3};
  my $command = qq{bash -c ' mkdir -p $plex_temp_dir ; cd $plex_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/12597_4#3_p4s2_pv_in.json -export_param_vals 12597_4#3_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_12597_4#3.json && viv.pl -s -x -v 3 -o viv_12597_4#3.log run_12597_4#3.json } .
    qq{ && qc --check bam_flagstats --filename_root 12597_4#3 --qc_in $qc_in --qc_out $qc_out --rpt_list "12597:4:3" --input_files $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/plex3/12597_4#3.cram} .
    qq{ && qc --check bam_flagstats --filename_root 12597_4#3_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "12597:4:3" --subset phix --input_files $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/plex3/12597_4#3.cram} .
     q{ && qc --check alignment_filter_metrics --filename_root 12597_4#3 --qc_in $PWD --qc_out }.$qc_out.q{ --rpt_list "12597:4:3" --input_files 12597_4#3_bam_alignment_filter_metrics.json} .
    qq{ && qc --check rna_seqc --filename_root 12597_4#3 --qc_in $qc_in --qc_out } . $qc_out . qq{ --rpt_list "12597:4:3" --input_files $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/plex3/12597_4#3.bam}.
     q{ '};

  my $mem = 32000;
  my $d = _find($da, 4, 3);
  isa_ok ($d, 'npg_pipeline::function::definition');
  is ($d->created_by, 'npg_pipeline::function::seq_alignment', 'created by correct');
  is ($d->created_on, '2014', 'timestamp');
  is ($d->identifier, 12597, 'identifier is set correctly');
  is ($d->job_name, 'seq_alignment_12597_2014', 'job name');
  ok (!$d->excluded, 'step not excluded');
  ok ($d->has_composition, 'composition is set');
  isa_ok ($d->composition, 'npg_tracking::glossary::composition',
    'composition object present');
  is ($d->composition->num_components, 1, 'one component in the composition');
  is ($d->command, $command, 'correct command for position 4, tag 3');
  is ($d->memory, $mem, "memory $mem");
  is ($d->command_preexec, "npg_pipeline_preexec_references --repository $dir", 'preexec');
  is ($d->queue, 'default', 'default queue');
  is_deeply ($d->num_cpus, [12,16], 'range of cpu numbers');
  is ($d->num_hosts, 1, 'one host');
  is ($d->fs_slots_num, 4, 'four sf slots');
  lives_ok {$d->freeze()} 'definition can be serialized to JSON';

  $qc_in  = $dir . q[/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/plex0];
  $qc_out = join q[/], $qc_in, q[qc];
  $plex_temp_dir = $tmp_dir . q{/12597_4#0};
  $command = qq{bash -c ' mkdir -p $plex_temp_dir ; cd $plex_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/12597_4#0_p4s2_pv_in.json -export_param_vals 12597_4#0_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_12597_4#0.json && viv.pl -s -x -v 3 -o viv_12597_4#0.log run_12597_4#0.json } .
    qq{ && qc --check bam_flagstats --filename_root 12597_4#0 --qc_in $qc_in --qc_out $qc_out --rpt_list "12597:4:0" --input_files $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/plex0/12597_4#0.cram --skip_markdups_metrics} .
    qq{ && qc --check bam_flagstats --filename_root 12597_4#0_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "12597:4:0" --subset phix --input_files $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/plex0/12597_4#0.cram} .
     q{ && qc --check alignment_filter_metrics --filename_root 12597_4#0 --qc_in $PWD --qc_out } .$qc_out.q{ --rpt_list "12597:4:0" --input_files 12597_4#0_bam_alignment_filter_metrics.json}.
     q{ '};

  $d = _find($da, 4, 0);
  is ($d->job_name, 'seq_alignment_12597_2014', 'job name');
  is ($d->command, $command, 'correct command for position 4, tag 0');
  is ($d->memory, $mem, "memory $mem");
  is ($d->command_preexec, "npg_pipeline_preexec_references --repository $dir", 'preexec');
  is_deeply ($d->num_cpus, [12,16], 'range of cpu numbers');
  is ($d->queue, 'default', 'default queue');
  is ($d->num_hosts, 1, 'one host');
  is ($d->fs_slots_num, 4, 'four sf slots');

  #### force on phix_split
  lives_ok {
    $rna_gen = npg_pipeline::function::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2014},
      verbose           => 0,
      repository        => $dir,
      force_phix_split  => 1,
      conf_path         => 't/data/release/config/seq_alignment',
    )
  } 'no error creating an object (forcing on phix split)';

  $da = $rna_gen->generate();

  my $base = "$bc_path/archive/lane5";
  ok (-f "$base/plex168/12597_5#168.composition.json",
    'composition JSON file exists for spiked phix tag');
  ok (!-e "$base/plex168/12597_5#168_phix.composition.json",
    'composition JSON file does not exist for split out phix');

  #####  phiX control libraries
  $qc_in =~ s{lane4/plex0}{lane5/plex168}smg;
  $qc_out =~ s{lane4/plex0}{lane5/plex168}smg;
  $unique_string = $rna_gen->_job_id();
  $tmp_dir = qq{$bc_path/archive/tmp_} . $unique_string;
  $plex_temp_dir = $tmp_dir . q{/12597_5#168};
  $command = qq{bash -c ' mkdir -p $plex_temp_dir ; cd $plex_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/12597_5#168_p4s2_pv_in.json -export_param_vals 12597_5#168_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_12597_5#168.json && viv.pl -s -x -v 3 -o viv_12597_5#168.log run_12597_5#168.json } .
    qq{ && qc --check bam_flagstats --filename_root 12597_5#168 --qc_in $qc_in --qc_out $qc_out --rpt_list "12597:5:168" --input_files $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane5/plex168/12597_5#168.cram} .
    q{ --skip_markdups_metrics '};

  $d = _find($da, 5, 168);
  is ($d->command, $command, 'correct command for position 5, tag 168 (spiked in phix)');

  #### monoplex (non-RNA Seq)
  $qc_in  =~ s{/lane\d+/plex\d+}{/lane1};
  $qc_out =~ s{/lane\d+/plex\d+}{/lane1};
  my $lane_temp_dir = $tmp_dir . q{/12597_1};
  $command = qq{bash -c ' mkdir -p $lane_temp_dir ; cd $lane_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/12597_1_p4s2_pv_in.json -export_param_vals 12597_1_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_12597_1.json && viv.pl -s -x -v 3 -o viv_12597_1.log run_12597_1.json } .
    qq{ && qc --check bam_flagstats --filename_root 12597_1 --qc_in $qc_in --qc_out $qc_out --rpt_list "12597:1" --input_files $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane1/12597_1.cram} .
    qq{ && qc --check bam_flagstats --filename_root 12597_1_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "12597:1" --subset phix --input_files $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane1/12597_1.cram} .
     q{ && qc --check alignment_filter_metrics --filename_root 12597_1 --qc_in $PWD --qc_out } . $qc_out . q{ --rpt_list "12597:1" --input_files 12597_1_bam_alignment_filter_metrics.json '};

  $d = _find($da, 1);
  is ($d->command, $command, 'correct non-multiplex lane args generated');
};

subtest 'RNASeq analysis' => sub {
  plan tests => 22;

  ##RNASeq library  13066_8  library_type = Illumina cDNA protocol

  my $runfolder = q{140529_HS18_13066_A_C3C3KACXX};
  my $runfolder_path = join q[/], $dir, $runfolder;
  my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20140606-133530/no_cal';
  `mkdir -p $bc_path`;
  my $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20140606-133530/metadata_cache_13066';
  `mkdir -p $cache_dir`;
  copy('t/data/rna_seq/13066_RunInfo.xml', "$runfolder_path/RunInfo.xml") or die 'Copy failed';
  copy('t/data/run_params/runParameters.hiseq.xml', "$runfolder_path/runParameters.xml")
    or die 'Copy failed';

  # Edited to add 1000Genomes_hs37d5 + ensembl_75_transcriptome to lane 8
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/rna_seq/samplesheet_13066.csv];

  my $qc_in  = qq{$bc_path/archive/lane8};
  my $qc_out = join q[/], $qc_in, q[qc];

  my $rna_gen;
  lives_ok {
    $rna_gen = npg_pipeline::function::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2014},
      repository        => $dir,
      conf_path         => 't/data/release/config/seq_alignment',
    )
  } 'no error creating an object';
  is ($rna_gen->id_run, 13066, 'id_run inferred correctly');
  make_path "$bc_path/archive/tileviz";
  apply_all_roles($rna_gen, 'npg_pipeline::runfolder_scaffold');
  $rna_gen->create_product_level();

  my $da = $rna_gen->generate('analysis_pipeline');
  ok ($da && @{$da} == 2, 'array of two definitions is returned');
  my $d = _find($da, 8);

  my $unique_string = $rna_gen->_job_id();
  my $tmp_dir = qq{$bc_path/archive/tmp_} . $unique_string;
  my $lane_temp_dir = $tmp_dir . q{/13066_8};
  my $command = qq{bash -c ' mkdir -p $lane_temp_dir ; cd $lane_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/13066_8_p4s2_pv_in.json -export_param_vals 13066_8_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_13066_8.json && viv.pl -s -x -v 3 -o viv_13066_8.log run_13066_8.json } .
    qq{ && qc --check bam_flagstats --filename_root 13066_8 --qc_in $qc_in --qc_out $qc_out --rpt_list "13066:8" --input_files $dir/140529_HS18_13066_A_C3C3KACXX/Data/Intensities/BAM_basecalls_20140606-133530/no_cal/archive/lane8/13066_8.cram} .
    qq{ && qc --check bam_flagstats --filename_root 13066_8_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "13066:8" --subset phix --input_files $dir/140529_HS18_13066_A_C3C3KACXX/Data/Intensities/BAM_basecalls_20140606-133530/no_cal/archive/lane8/13066_8.cram} .
     q{ && qc --check alignment_filter_metrics --filename_root 13066_8 --qc_in $PWD --qc_out } . $qc_out . qq{ --rpt_list "13066:8" --input_files 13066_8_bam_alignment_filter_metrics.json} .
    qq{ && qc --check rna_seqc --filename_root 13066_8 --qc_in $qc_in --qc_out } . $qc_out . qq{ --rpt_list "13066:8" --input_files $dir/140529_HS18_13066_A_C3C3KACXX/Data/Intensities/BAM_basecalls_20140606-133530/no_cal/archive/lane8/13066_8.bam '};

  is ($d->command, $command, 'correct command for lane 8');
  is ($d->memory, 32000, 'memory');

  my $l = st::api::lims->new(id_run => 13066, position => 8);
  my $dp = npg_pipeline::product->new(lims => $l, rpt_list => q[13066:8],);
  is ($rna_gen->_do_rna_analysis($dp), 1, 'do RNA analysis on pair end RNA library with transcriptome index');

  # lane 7 to be aligned with STAR and thus requires more memory
  $d = _find($da, 7);
  is ($d->memory, 38000, 'more memory');


  ##HiSeq, run 17550, multiple organisms RNA libraries suitable for RNA analysis
  $runfolder = q{150910_HS40_17550_A_C75BCANXX};
  $runfolder_path = join q[/], $dir, $runfolder;
  $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20170629-170201/no_cal';
  $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20170629-170201/metadata_cache_17550';
  `mkdir -p $bc_path`;
  `mkdir -p $cache_dir`;
  copy('t/data/rna_seq/17550_RunInfo.xml', "$runfolder_path/RunInfo.xml") or die 'Copy failed';
  copy('t/data/run_params/runParameters.hiseq.xml', "$runfolder_path/runParameters.xml")
    or die 'Copy failed';

  for ((3,4,6,8)) {
    `mkdir -p $bc_path/lane$_`;
  }

  local $ENV{'NPG_CACHED_SAMPLESHEET_FILE'} = q[t/data/rna_seq/samplesheet_17550.csv];
  my @SUPPORTED_RNA_ANALYSES = qw{tophat2 star hisat2};
  my $DEFAULT_RNA_ANALYSIS = q{tophat2};

  lives_ok {
    $rna_gen = npg_pipeline::function::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2017},
      repository        => $dir,
      conf_path         => 't/data/release/config/seq_alignment',
    )
  } 'no error creating an object';
  is ($rna_gen->id_run, 17550, 'id_run inferred correctly');
  make_path "$bc_path/archive/tileviz";
  apply_all_roles($rna_gen, 'npg_pipeline::runfolder_scaffold');
  $rna_gen->create_product_level();

  $da = $rna_gen->generate('analysis_pipeline');
  $d = _find($da, 3, 1);
  is ($d->memory, 38000, 'more memory');
  $d = _find($da, 3, 3);
  is ($d->memory, 38000, 'more memory');

  #test: reference genome selected has an unsupported 'analysis' defined
  $l = st::api::lims->new(id_run => 17550, position => 4, tag_index => 1);
  my $rpt_list = q[17550:4:1];
  my $product = npg_pipeline::product->new(rpt_list => $rpt_list,lims => $l);
  lives_ok { $rna_gen->_alignment_command($product, {}) }
    'executes _alignment_command method successfully';
  $dp = npg_pipeline::product->new(lims => $l, rpt_list => $rpt_list,);
  is ($rna_gen->_do_rna_analysis($dp), 1,
      'transcriptome version in reference, so RNA analysis');
  my $rna_analysis = $rna_gen->_analysis($l->reference_genome, $rpt_list);
  is ((List::Util::none {$_ eq $rna_analysis} @SUPPORTED_RNA_ANALYSES), 1,
      qq['$rna_analysis' is not a supported RNA Analysis, using default instead]);

  # Library type is RNA, but no transcriptome version has been defined in
  # reference: Homo_sapiens (GRCh38_15)
  $l = st::api::lims->new(id_run => 17550, position => 6, tag_index => 2);
  $dp = npg_pipeline::product->new(lims => $l, rpt_list => q[17550:6:2],);
  is ($rna_gen->_do_rna_analysis($dp), 0, 'no transcriptome version in reference, so no RNA analysis');

  #Library type is not RNA: ChIP-Seq Auto
  $l = st::api::lims->new(id_run => 17550, position => 8, tag_index => 1);
  $dp = npg_pipeline::product->new(lims => $l, rpt_list => q[17550:8:1],);
  is ($rna_gen->_do_rna_analysis($dp), 0, 'not an RNA library, so no RNA analysis');

  #Library type is not RNA: ChIP-Seq Auto but RNA aligner defined: supported RNA analysis: star
  $l = st::api::lims->new(id_run => 17550, position => 8, tag_index => 2);
  $rpt_list = q[17550:8:2];
  $dp = npg_pipeline::product->new(lims => $l, rpt_list => $rpt_list,);
  is ($rna_gen->_do_rna_analysis($dp), 1, 'not an RNA library but RNA aligner, so RNA analysis');
  $rna_analysis = $rna_gen->_analysis($l->reference_genome, $rpt_list);
  is ((List::Util::none {$_ eq $rna_analysis} @SUPPORTED_RNA_ANALYSES), q[],
      qq['$rna_analysis' is a supported RNA Analysis]);

  #Library type is RNA: supported RNA analyses: default (tophat2) and hisat2
  $l = st::api::lims->new(id_run => 17550, position => 3, tag_index => 2);
  $rpt_list = q[17550:3:2];
  $dp = npg_pipeline::product->new(lims => $l, rpt_list => $rpt_list,);
  $rna_analysis = $rna_gen->_analysis($l->reference_genome, $rpt_list) // $DEFAULT_RNA_ANALYSIS;
  is ($rna_analysis, $DEFAULT_RNA_ANALYSIS, qq['$rna_analysis' is default supported RNA Analysis]);
  $l = st::api::lims->new(id_run => 17550, position => 4, tag_index => 3);
  $rpt_list = q[17550:4:3];
  $dp = npg_pipeline::product->new(lims => $l, rpt_list => $rpt_list,);
  $rna_analysis = $rna_gen->_analysis($l->reference_genome, $rpt_list);
  is ((List::Util::none {$_ eq $rna_analysis} @SUPPORTED_RNA_ANALYSES), q[],
      qq['$rna_analysis' is a supported RNA Analysis]);

  ##HiSeq, run 25269, single end RNA libraries suitable for RNA analysis
  $runfolder = q{180228_HS35_25269_B_H7WJ3BCX2};
  $runfolder_path = join q[/], $dir, $runfolder;
  $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20180301-014343/no_cal';
  $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20180301-014343/metadata_cache_25269';
  `mkdir -p $bc_path`;
  `mkdir -p $cache_dir`;
  copy('t/data/rna_seq/25269_RunInfo.xml', "$runfolder_path/RunInfo.xml") or die 'Copy failed';
  copy('t/data/run_params/runParameters.hiseq.xml', "$runfolder_path/runParameters.xml")
    or die 'Copy failed';

  for ((1,2)) {
    `mkdir -p $bc_path/lane$_`;
  }

  local $ENV{'NPG_CACHED_SAMPLESHEET_FILE'} = q[t/data/rna_seq/samplesheet_25269.csv];

  lives_ok {
    $rna_gen = npg_pipeline::function::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2018},
      repository        => $dir,
      conf_path         => 't/data/release/config/seq_alignment',
    )
  } 'no error creating an object';

  # Single end RNA library: suitable for RNA analysis
  $l = st::api::lims->new(id_run => 25269, position => 1, tag_index => 1);
  $dp = npg_pipeline::product->new(lims => $l, rpt_list => q[25269:1:1],);
  is ($rna_gen->_do_rna_analysis($dp), 1, 'do RNA analysis on single end RNA library');
};

subtest 'test 3' => sub {
  plan tests => 3;
  ## single ended v. short , old flowcell, CRIPSR

  my $runfolder = q{151215_HS38_18472_A_H55HVADXX};
  my $runfolder_path = join q[/], q(t/data/example_runfolder), $runfolder;
  `cp -r $runfolder_path $dir`;
  $runfolder_path = join q[/], $dir, $runfolder;
  my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20151215-215034';
  my $cache_dir = join q[/], $bc_path, 'metadata_cache_18472';
  `mkdir -p $bc_path/no_cal/lane2`;
  copy('t/data/run_params/runParameters.hiseq.xml', "$runfolder_path/runParameters.xml")
    or die 'Copy failed';

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = join q[/], $cache_dir, q[samplesheet_18472.csv];

  my $se_gen;
  lives_ok {
    $se_gen = npg_pipeline::function::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => "$bc_path/no_cal",
      timestamp         => q{2015},
      repository        => $dir,
      conf_path         => 't/data/release/config/seq_alignment',
    )
  } 'no error creating an object';
  is ($se_gen->id_run, 18472, 'id_run inferred correctly');

  my $qc_in = qq[$bc_path/no_cal/archive/lane2/plex1];
  my $qc_out = join q[/], $qc_in, q[qc];
  my $unique_string = $se_gen->_job_id();
  my $tmp_dir = qq{$bc_path/no_cal/archive/tmp_} . $unique_string;
  my $plex_temp_dir = $tmp_dir . q{/18472_2#1};
  my $command = qq{bash -c ' mkdir -p $plex_temp_dir ; cd $plex_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/no_cal/18472_2#1_p4s2_pv_in.json -export_param_vals 18472_2#1_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_18472_2#1.json && viv.pl -s -x -v 3 -o viv_18472_2#1.log run_18472_2#1.json } .
    qq{ && qc --check bam_flagstats --filename_root 18472_2#1 --qc_in $qc_in --qc_out $qc_out --rpt_list "18472:2:1" --input_files $dir/151215_HS38_18472_A_H55HVADXX/Data/Intensities/BAM_basecalls_20151215-215034/no_cal/archive/lane2/plex1/18472_2#1.cram} .
    qq{ && qc --check bam_flagstats --filename_root 18472_2#1_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "18472:2:1" --subset phix --input_files $dir/151215_HS38_18472_A_H55HVADXX/Data/Intensities/BAM_basecalls_20151215-215034/no_cal/archive/lane2/plex1/18472_2#1.cram} .
     q{ && qc --check alignment_filter_metrics --filename_root 18472_2#1 --qc_in $PWD --qc_out } .$qc_out.q{ --rpt_list "18472:2:1" --input_files 18472_2#1_bam_alignment_filter_metrics.json}.
     q{ '};
  make_path "$bc_path/no_cal/archive/tileviz";
  apply_all_roles($se_gen, 'npg_pipeline::runfolder_scaffold');
  $se_gen->create_product_level();

  my $da = $se_gen->generate('analysis_pipeline');
  my $d = _find($da, 2, 1);
  is ($d->command, $command, 'correct command for lane 2 plex 1');
};

subtest 'test 4' => sub {
  plan tests => 10;
  ##HiSeqX, run 16839_7

  my $runfolder = q{150709_HX4_16839_A_H7MHWCCXX};
  my $runfolder_path = join q[/], $dir, $runfolder;
  my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150712-121006/no_cal';
  for ((1, 7)) {
    `mkdir -p $bc_path/lane$_`;
  }

  my $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150712-121006/metadata_cache_16839';
  `mkdir -p $cache_dir`;
  copy('t/data/hiseqx/16839_RunInfo.xml', "$runfolder_path/RunInfo.xml") or die 'Copy failed';
  copy('t/data/run_params/runParameters.hiseqx.upgraded.xml', "$runfolder_path/runParameters.xml")
    or die 'Copy failed';

  my $fasta_ref = "$ref_dir/Homo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/all/fasta/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa";
  my $target_file = "$ref_dir/Homo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/all/target/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa";

  local $ENV{'NPG_CACHED_SAMPLESHEET_FILE'} = q[t/data/hiseqx/samplesheet_16839.csv];
  local $ENV{'NPG_PIPELINE_JS_SCRIPTS_DIR'} = q[t/bin/];

  my $hsx_gen;
  lives_ok {
    $hsx_gen = npg_pipeline::function::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2015},
      repository        => $dir,
      conf_path         => 't/data/release/config/seq_alignment',
    )
  } 'no error creating an object';
  is ($hsx_gen->id_run, 16839, 'id_run inferred correctly');

  my $l = st::api::lims->new(id_run => 16839, position => 1, tag_index => 0);
  my $dp = npg_pipeline::product->new(lims => $l, rpt_list => q[16839:1:0],);
  is ($hsx_gen->_ref($dp, 'fasta'), $fasta_ref, 'reference for tag zero');
  is ($hsx_gen->_ref($dp, 'target'), $target_file, 'target for tag zero');
  my $k = st::api::lims->new(id_run => 16839, position => 1, tag_index => 1);
  my $kdp = npg_pipeline::product->new(lims => $k, rpt_list => q[16839:1:1],);
  is ($hsx_gen->_ref($kdp, 'target'), $target_file, 'target file for tag 1');

  my $old_ss = $ENV{'NPG_CACHED_SAMPLESHEET_FILE'};
  my $ss = slurp $old_ss;
  $ss =~ s/GRCh38_full_analysis_set_plus_decoy_hla/GRCh38X/;
  my $new_ss = "$dir/multiref_samplesheet_16839.csv";
  open my $fhss, '>', $new_ss or die "Cannot open $new_ss for writing";
  print $fhss $ss or die "Cannot write to $new_ss";
  close $fhss or warn "Failed to close $new_ss";

  # new samplesheet has miltiple references in lane 1
  local $ENV{'NPG_CACHED_SAMPLESHEET_FILE'} = $new_ss;
  $l = st::api::lims->new(id_run => 16839, position => 1, tag_index => 0);
  $dp = npg_pipeline::product->new(lims => $l, rpt_list => q[16839:1:0],);

  my $other_ref;
  warnings_exist { $other_ref = $hsx_gen->_ref($dp, 'fasta') }
    qr/Multiple references for 16839:1:0/,
    'warning about multiple references';
  is ($other_ref, undef, 'multiple references in a lane - no reference for tag zero returned');

  # restore old samplesheet
  local $ENV{'NPG_CACHED_SAMPLESHEET_FILE'} = $old_ss;
  my $qc_in  = qq{$dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane7/plex7};
  my $qc_out = qq{$qc_in/qc};

  my $unique_string = $hsx_gen->_job_id();
  my $tmp_dir = qq{$bc_path/archive/tmp_} . $unique_string;
  my $plex_temp_dir = $tmp_dir . q{/16839_7#7};
  my $command = qq{bash -c ' mkdir -p $plex_temp_dir ; cd $plex_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/16839_7#7_p4s2_pv_in.json -export_param_vals 16839_7#7_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16839_7#7.json && viv.pl -s -x -v 3 -o viv_16839_7#7.log run_16839_7#7.json } .
    qq{ && qc --check bam_flagstats --filename_root 16839_7#7 --qc_in $qc_in --qc_out $qc_out --rpt_list "16839:7:7" --input_files $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane7/plex7/16839_7#7.cram} .
    qq{ && qc --check bam_flagstats --filename_root 16839_7#7_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "16839:7:7" --subset phix --input_files $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane7/plex7/16839_7#7.cram} .
     q{ && qc --check alignment_filter_metrics --filename_root 16839_7#7 --qc_in $PWD --qc_out } .$qc_out.q{ --rpt_list "16839:7:7" --input_files 16839_7#7_bam_alignment_filter_metrics.json}.
     q{ '};
  make_path "$bc_path/archive/tileviz";
  apply_all_roles($hsx_gen, 'npg_pipeline::runfolder_scaffold');
  $hsx_gen->create_product_level();

  my $da = $hsx_gen->generate('analysis_pipeline');
  my $d = _find($da, 7, 7);
  is ($d->command, $command, 'command for HiSeqX run 16839 lane 7 tag 7');

  $qc_in =~ s{/plex7}{/plex15};
  $qc_out = qq{$qc_in/qc};
  $plex_temp_dir = $tmp_dir . q{/16839_7#15};
  $command = qq{bash -c ' mkdir -p $plex_temp_dir ; cd $plex_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/16839_7#15_p4s2_pv_in.json -export_param_vals 16839_7#15_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16839_7#15.json && viv.pl -s -x -v 3 -o viv_16839_7#15.log run_16839_7#15.json } .
    qq{ && qc --check bam_flagstats --filename_root 16839_7#15 --qc_in $qc_in --qc_out $qc_out --rpt_list "16839:7:15" --input_files $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane7/plex15/16839_7#15.cram} .
    qq{ && qc --check bam_flagstats --filename_root 16839_7#15_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "16839:7:15" --subset phix --input_files $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane7/plex15/16839_7#15.cram} .
     q{ && qc --check alignment_filter_metrics --filename_root 16839_7#15 --qc_in $PWD --qc_out } .$qc_out.q{ --rpt_list "16839:7:15" --input_files 16839_7#15_bam_alignment_filter_metrics.json}.
     q{ '};

  $d = _find($da, 7, 15);
  is ($d->command, $command, 'command for HiSeqX run 16839 lane 7 tag 15');

  $qc_in =~ s{/plex15}{/plex0};
  $qc_out = qq{$qc_in/qc};
  $plex_temp_dir = $tmp_dir . q{/16839_7#0};
  $command = qq{bash -c ' mkdir -p $plex_temp_dir ; cd $plex_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/16839_7#0_p4s2_pv_in.json -export_param_vals 16839_7#0_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16839_7#0.json && viv.pl -s -x -v 3 -o viv_16839_7#0.log run_16839_7#0.json } .
    qq{ && qc --check bam_flagstats --filename_root 16839_7#0 --qc_in $qc_in --qc_out $qc_out --rpt_list "16839:7:0" --input_files $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane7/plex0/16839_7#0.cram --skip_markdups_metrics} .
    qq{ && qc --check bam_flagstats --filename_root 16839_7#0_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "16839:7:0" --subset phix --input_files $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane7/plex0/16839_7#0.cram} .
     q{ && qc --check alignment_filter_metrics --filename_root 16839_7#0 --qc_in $PWD --qc_out } .$qc_out.q{ --rpt_list "16839:7:0" --input_files 16839_7#0_bam_alignment_filter_metrics.json}.
     q{ '};

  $d = _find($da, 7, 0);
  is ($d->command, $command, 'command for HiSeqX run 16839 lane 7 tag 0');
};

subtest 'Newer flowcell' => sub {
  plan tests => 5;
  ##HiSeq, run 16807_6 (newer flowcell)

  my $runfolder = q{150707_HS38_16807_A_C7U2YANXX};
  my $runfolder_path = join q[/], $dir, $runfolder;
  my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150707-232614/no_cal';
  `mkdir -p $bc_path/lane6`;

  my $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150707-232614/metadata_cache_16807';
  `mkdir -p $cache_dir`;
  copy('t/data/hiseq/16807_RunInfo.xml', "$runfolder_path/RunInfo.xml") or die 'Copy failed';
  copy('t/data/run_params/runParameters.hiseqx.upgraded.xml', "$runfolder_path/runParameters.xml")
   or die 'Copy failed';

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/hiseq/samplesheet_16807.csv];

  my $hs_gen;
  lives_ok {
    $hs_gen = npg_pipeline::function::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2015},
      repository        => $dir,
      conf_path         => 't/data/release/config/seq_alignment',
    )
  } 'no error creating an object';
  is ($hs_gen->id_run, 16807, 'id_run inferred correctly');
  make_path "$bc_path/archive/tileviz";
  apply_all_roles($hs_gen, 'npg_pipeline::runfolder_scaffold');
  $hs_gen->create_product_level();

  my $da = $hs_gen->generate('analysis_pipeline');

  my $qc_in  = qq{$bc_path/archive/lane6/plex1};
  my $qc_out = qq{$qc_in/qc};
  my $unique_string = $hs_gen->_job_id();
  my $tmp_dir = qq{$bc_path/archive/tmp_} . $unique_string;
  my $plex_temp_dir = $tmp_dir . q{/16807_6#1};

  my $command = qq{bash -c ' mkdir -p $plex_temp_dir ; cd $plex_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/16807_6#1_p4s2_pv_in.json -export_param_vals 16807_6#1_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16807_6#1.json && viv.pl -s -x -v 3 -o viv_16807_6#1.log run_16807_6#1.json } .
    qq{ && qc --check bam_flagstats --filename_root 16807_6#1 --qc_in $qc_in --qc_out $qc_out --rpt_list "16807:6:1" --input_files $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/lane6/plex1/16807_6#1.cram} .
    qq{ && qc --check bam_flagstats --filename_root 16807_6#1_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "16807:6:1" --subset phix --input_files $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/lane6/plex1/16807_6#1.cram} .
     q{ && qc --check alignment_filter_metrics --filename_root 16807_6#1 --qc_in $PWD --qc_out } .$qc_out.q{ --rpt_list "16807:6:1" --input_files 16807_6#1_bam_alignment_filter_metrics.json}.
     q{ '};

  my $d = _find($da, 6, 1);
  is ($d->command, $command, 'command for HiSeq run 16807 lane 6 tag 1');

  $qc_in  = qq{$bc_path/archive/lane6/plex2};
  $qc_out = qq{$qc_in/qc};
  $plex_temp_dir = $tmp_dir . q{/16807_6#2};
  $command = qq{bash -c ' mkdir -p $plex_temp_dir ; cd $plex_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/16807_6#2_p4s2_pv_in.json -export_param_vals 16807_6#2_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16807_6#2.json && viv.pl -s -x -v 3 -o viv_16807_6#2.log run_16807_6#2.json } .
    qq{ && qc --check bam_flagstats --filename_root 16807_6#2 --qc_in $qc_in --qc_out $qc_out --rpt_list "16807:6:2" --input_files $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/lane6/plex2/16807_6#2.cram} .
    qq{ && qc --check bam_flagstats --filename_root 16807_6#2_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "16807:6:2" --subset phix --input_files $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/lane6/plex2/16807_6#2.cram} .
     q{ && qc --check alignment_filter_metrics --filename_root 16807_6#2 --qc_in $PWD --qc_out } .$qc_out.q{ --rpt_list "16807:6:2" --input_files 16807_6#2_bam_alignment_filter_metrics.json}.
     q{ '};

  $d = _find($da, 6, 2);
  is ($d->command, $command, 'command for HiSeq run 16807 lane 6 tag 2');

  $qc_in  = qq{$bc_path/archive/lane6/plex0};
  $qc_out = qq{$qc_in/qc};
  $plex_temp_dir = $tmp_dir . q{/16807_6#0};
  $command = qq{bash -c ' mkdir -p $plex_temp_dir ; cd $plex_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/16807_6#0_p4s2_pv_in.json -export_param_vals 16807_6#0_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16807_6#0.json && viv.pl -s -x -v 3 -o viv_16807_6#0.log run_16807_6#0.json } .
    qq{ && qc --check bam_flagstats --filename_root 16807_6#0 --qc_in $qc_in --qc_out $qc_out --rpt_list "16807:6:0" --input_files $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/lane6/plex0/16807_6#0.cram --skip_markdups_metrics} .
    qq{ && qc --check bam_flagstats --filename_root 16807_6#0_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "16807:6:0" --subset phix --input_files $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/lane6/plex0/16807_6#0.cram} .
     q{ && qc --check alignment_filter_metrics --filename_root 16807_6#0 --qc_in $PWD --qc_out } .$qc_out.q{ --rpt_list "16807:6:0" --input_files 16807_6#0_bam_alignment_filter_metrics.json}.
     q{ '};

  $d = _find($da, 6, 0);
  is ($d->command, $command, 'command for HiSeq run 16807 lane 6 tag 0');
};

subtest 'MiSeq WES baits' => sub {
  plan tests => 7;
  ##MiSeq, run 20268_1 (newer flowcell) - WITH bait added to samplesheet for lane 1

  my $bait_dir = join q[/],$dir,'baits','Human_all_exon_V5','1000Genomes_hs37d5';
  `mkdir -p $bait_dir`;
  `touch $bait_dir/S04380110-PTR.interval_list`;
  `touch $bait_dir/S04380110-CTR.interval_list`;

  my $runfolder = q{160704_MS3_20268_A_MS4000667-300V2};
  my $runfolder_path = join q[/], $dir, $runfolder;
  my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20160712-154117/no_cal';
  `mkdir -p $bc_path/lane1`;
  my $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20160712-154117/metadata_cache_20268';
  `mkdir -p $cache_dir`;

  copy('t/data/hiseq/20268_RunInfo.xml', "$runfolder_path/RunInfo.xml") or die 'Copy failed';
  copy('t/data/run_params/runParameters.miseq.xml', "$runfolder_path/runParameters.xml")
    or die 'Copy failed';

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/hiseq/samplesheet_20268.csv];

  my $bait_gen;
  lives_ok {
    $bait_gen = npg_pipeline::function::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2016},
      repository        => $dir,
      verbose           => 1,
      conf_path         => 't/data/release/config/seq_alignment',
    )
  } 'no error creating an object';

  is ($bait_gen->id_run, 20268, 'id_run inferred correctly');
  make_path "$bc_path/archive/tileviz";
  apply_all_roles($bait_gen, 'npg_pipeline::runfolder_scaffold');
  $bait_gen->create_product_level();

  my $da = $bait_gen->generate('analysis_pipeline');

  my $qc_in  = qq{$bc_path/archive/lane1/plex1};
  my $qc_out = qq{$qc_in/qc};
  my $unique_string = $bait_gen->_job_id();
  my $tmp_dir = qq{$bc_path/archive/tmp_} . $unique_string;
  my $plex_temp_dir = $tmp_dir . q{/20268_1#1};
  my $command = qq{bash -c ' mkdir -p $plex_temp_dir ; cd $plex_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/20268_1#1_p4s2_pv_in.json -export_param_vals 20268_1#1_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_20268_1#1.json && viv.pl -s -x -v 3 -o viv_20268_1#1.log run_20268_1#1.json } .
    qq{ && qc --check bam_flagstats --filename_root 20268_1#1 --qc_in $qc_in --qc_out $qc_out --rpt_list "20268:1:1" --input_files $dir/160704_MS3_20268_A_MS4000667-300V2/Data/Intensities/BAM_basecalls_20160712-154117/no_cal/archive/lane1/plex1/20268_1#1.cram} .
    qq{ && qc --check bam_flagstats --filename_root 20268_1#1_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "20268:1:1" --subset phix --input_files $dir/160704_MS3_20268_A_MS4000667-300V2/Data/Intensities/BAM_basecalls_20160712-154117/no_cal/archive/lane1/plex1/20268_1#1.cram} .
     q{ && qc --check alignment_filter_metrics --filename_root 20268_1#1 --qc_in $PWD --qc_out } .$qc_out.q{ --rpt_list "20268:1:1" --input_files 20268_1#1_bam_alignment_filter_metrics.json}.
     q{ '};

  my $d = _find($da, 1, 1);
  is ($d->command, $command, 'command for run 20268 lane 1 tag 1');

  $qc_in = qq{$bc_path/archive/lane1/plex2};
  $qc_out = qq{$qc_in/qc};
  $plex_temp_dir = $tmp_dir . q{/20268_1#2};
  $command = qq{bash -c ' mkdir -p $plex_temp_dir ; cd $plex_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/20268_1#2_p4s2_pv_in.json -export_param_vals 20268_1#2_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_20268_1#2.json && viv.pl -s -x -v 3 -o viv_20268_1#2.log run_20268_1#2.json } .
    qq{ && qc --check bam_flagstats --filename_root 20268_1#2 --qc_in $qc_in --qc_out $qc_out --rpt_list "20268:1:2" --input_files $dir/160704_MS3_20268_A_MS4000667-300V2/Data/Intensities/BAM_basecalls_20160712-154117/no_cal/archive/lane1/plex2/20268_1#2.cram} .
    qq{ && qc --check bam_flagstats --filename_root 20268_1#2_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "20268:1:2" --subset phix --input_files $dir/160704_MS3_20268_A_MS4000667-300V2/Data/Intensities/BAM_basecalls_20160712-154117/no_cal/archive/lane1/plex2/20268_1#2.cram} .
     q{ && qc --check alignment_filter_metrics --filename_root 20268_1#2 --qc_in $PWD --qc_out } .$qc_out.q{ --rpt_list "20268:1:2" --input_files 20268_1#2_bam_alignment_filter_metrics.json}.
     q{ '};

  $d = _find($da, 1, 2);
  is ($d->command, $command, 'command for run 20268 lane 1 tag 2');

  $qc_in = qq{$bc_path/archive/lane1/plex0};
  $qc_out = qq{$qc_in/qc};
  $plex_temp_dir = $tmp_dir . q{/20268_1#0};
  $command = qq{bash -c ' mkdir -p $plex_temp_dir ; cd $plex_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/20268_1#0_p4s2_pv_in.json -export_param_vals 20268_1#0_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_20268_1#0.json && viv.pl -s -x -v 3 -o viv_20268_1#0.log run_20268_1#0.json } .
    qq{ && qc --check bam_flagstats --filename_root 20268_1#0 --qc_in $qc_in --qc_out $qc_out --rpt_list "20268:1:0" --input_files $dir/160704_MS3_20268_A_MS4000667-300V2/Data/Intensities/BAM_basecalls_20160712-154117/no_cal/archive/lane1/plex0/20268_1#0.cram --skip_markdups_metrics} .
    qq{ && qc --check bam_flagstats --filename_root 20268_1#0_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "20268:1:0" --subset phix --input_files $dir/160704_MS3_20268_A_MS4000667-300V2/Data/Intensities/BAM_basecalls_20160712-154117/no_cal/archive/lane1/plex0/20268_1#0.cram} .
     q{ && qc --check alignment_filter_metrics --filename_root 20268_1#0 --qc_in $PWD --qc_out } .$qc_out.q{ --rpt_list "20268:1:0" --input_files 20268_1#0_bam_alignment_filter_metrics.json}.
     q{ '};

  $d = _find($da, 1, 0);
  is ($d->command, $command, 'command for run 20268 lane 1 tag 0');

  ## check json file for tag 1
  my $json_file = qq{$bc_path/20268_1#1_p4s2_pv_in.json};
  ok (-e $json_file, 'json params file exists for run 20268 lane 1 tag 1');
  my $h = from_json(slurp($json_file));

  my $expected = {
     'assign_local'=> {},
     'assign' => [
         {"subsetsubpath"=>  '.npg_cache_10000/',
          "samtools_executable"=> 'samtools',
          "reference_genome_fasta" =>$dir . '/references/Homo_sapiens/1000Genomes_hs37d5/all/fasta/hs37d5.fa',
          "rpt"=> "20268_1#1",
          "alignment_method"=> "bwa_mem",
          "incrams"=> [
              $dir ."/160704_MS3_20268_A_MS4000667-300V2/Data/Intensities/BAM_basecalls_20160712-154117/no_cal/20268_1#1.cram"
              ],
              "tag_metrics_files"=> $dir .'/160704_MS3_20268_A_MS4000667-300V2/Data/Intensities/BAM_basecalls_20160712-154117/no_cal/archive/lane1/qc/20268_1.tag_metrics.json',
              "run_lane_ss_fq2" => $dir ."/160704_MS3_20268_A_MS4000667-300V2/Data/Intensities/BAM_basecalls_20160712-154117/no_cal/archive/lane1/plex1/.npg_cache_10000/20268_1#1_2.fastq",
              "outdatadir"=> $dir .'/160704_MS3_20268_A_MS4000667-300V2/Data/Intensities/BAM_basecalls_20160712-154117/no_cal/archive/lane1/plex1',
              "s2_input_format"=> 'cram',
              "s2_position"=> 'POSITION',
              "target_regions_file" => $dir .'/baits/Human_all_exon_V5/1000Genomes_hs37d5/S04380110-PTR.interval_list',
              "s2_filter_files"=> $dir .'/160704_MS3_20268_A_MS4000667-300V2/Data/Intensities/BAM_basecalls_20160712-154117/no_cal/20268_1.spatial_filter',
              "s2_id_run"=> 20268,
              "alignment_reference_genome"=> $dir .'/references/Homo_sapiens/1000Genomes_hs37d5/all/bwa0_6/hs37d5.fa',
              "spatial_filter_file"=> 'DUMMY',
              "s2_se_pe"=> 'pe',
              "s2_tag_index"=> 1,
              "reference_dict"=> $dir .'/references/Homo_sapiens/1000Genomes_hs37d5/all/picard/hs37d5.fa.dict',
              "spatial_filter_rg_value"=> "20268_1#1",
              "seqchksum_orig_file"=> $dir ."/160704_MS3_20268_A_MS4000667-300V2/Data/Intensities/BAM_basecalls_20160712-154117/no_cal/archive/lane1/plex1/20268_1#1.orig.seqchksum",
              "bwa_executable"=> "bwa0_6",
              "phix_reference_genome_fasta"=> $dir .'/references/PhiX/Illumina/all/fasta/phix-illumina.fa',
              "af_metrics"=> "20268_1#1_bam_alignment_filter_metrics.json",
              "run_lane_ss_fq1"=> $dir."/160704_MS3_20268_A_MS4000667-300V2/Data/Intensities/BAM_basecalls_20160712-154117/no_cal/archive/lane1/plex1/.npg_cache_10000/20268_1#1_1.fastq",
              "bait_regions_file"=> $dir .'/baits/Human_all_exon_V5/1000Genomes_hs37d5/S04380110-CTR.interval_list',
              "recal_dir"=> $dir .'/160704_MS3_20268_A_MS4000667-300V2/Data/Intensities/BAM_basecalls_20160712-154117/no_cal',
              "markdup_method" => "biobambam",
              "markdup_optical_distance_value" => 100,
         },],
     'ops' => {  
         'prune' => ['fop.*_bmd_multiway:calibration_pu-',
                      'foptgt.*samtools_stats_F0.*_target_autosome.*-',
                      'fop(phx|hs)_samtools_stats_F0.*_target.*-',
                      'fop(phx|hs)_samtools_stats_F0.*00_bait.*-'],
      'splice' => []          
     },
  };

  is_deeply($h, $expected, 'correct json file content for run 20268 lane 1 tag 1 (with bait)');

};

subtest 'cycle count over threshold' => sub {
  plan tests => 5;
  ##MiSeq, run 16850_1 (cycle count over threshold (currently >= 101))

  my $runfolder = q{150710_MS2_16850_A_MS3014507-500V2};
  my $runfolder_path = join q[/], $dir, $runfolder;
  my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150712-022206/no_cal';
  `mkdir -p $bc_path/lane1`;
  my $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150712-022206/metadata_cache_16850';
  `mkdir -p $cache_dir`;
  copy('t/data/miseq/16850_RunInfo.xml', "$runfolder_path/RunInfo.xml") or die 'Copy failed';
  copy('t/data/run_params/runParameters.miseq.xml', "$runfolder_path/runParameters.xml")
    or die 'Copy failed';

 local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/miseq/samplesheet_16850.csv];

  my $ms_gen;
  lives_ok {
    $ms_gen = npg_pipeline::function::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2015},
      repository        => $dir,
      conf_path         => 't/data/release/config/seq_alignment',
    )
  } 'no error creating an object';
  is ($ms_gen->id_run, 16850, 'id_run inferred correctly');
  make_path "$bc_path/archive/tileviz";
  apply_all_roles($ms_gen, 'npg_pipeline::runfolder_scaffold');
  $ms_gen->create_product_level();

  my $da = $ms_gen->generate('analysis_pipeline');

  my $qc_in  = qq{$bc_path/archive/lane1/plex1};
  my $qc_out = qq{$qc_in/qc};
  my $unique_string = $ms_gen->_job_id();
  my $tmp_dir = qq{$bc_path/archive/tmp_} . $unique_string;
  my $plex_temp_dir = $tmp_dir . q{/16850_1#1};
  my $command = qq{bash -c ' mkdir -p $plex_temp_dir ; cd $plex_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/16850_1#1_p4s2_pv_in.json -export_param_vals 16850_1#1_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16850_1#1.json && viv.pl -s -x -v 3 -o viv_16850_1#1.log run_16850_1#1.json } .
    qq{ && qc --check bam_flagstats --filename_root 16850_1#1 --qc_in $qc_in --qc_out $qc_out --rpt_list "16850:1:1" --input_files $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/lane1/plex1/16850_1#1.cram} .
    qq{ && qc --check bam_flagstats --filename_root 16850_1#1_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "16850:1:1" --subset phix --input_files $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/lane1/plex1/16850_1#1.cram} .
     q{ && qc --check alignment_filter_metrics --filename_root 16850_1#1 --qc_in $PWD --qc_out } .$qc_out.q{ --rpt_list "16850:1:1" --input_files 16850_1#1_bam_alignment_filter_metrics.json}.
     q{ '};

  my $d = _find($da, 1, 1);
  is ($d->command, $command, 'command for HiSeq run 16850 lane 1 tag 1');

  $qc_in  = qq{$bc_path/archive/lane1/plex2};
  $qc_out = qq{$qc_in/qc};
  $plex_temp_dir = $tmp_dir . q{/16850_1#2};
  $command = qq{bash -c ' mkdir -p $plex_temp_dir ; cd $plex_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/16850_1#2_p4s2_pv_in.json -export_param_vals 16850_1#2_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16850_1#2.json && viv.pl -s -x -v 3 -o viv_16850_1#2.log run_16850_1#2.json } .
    qq{ && qc --check bam_flagstats --filename_root 16850_1#2 --qc_in $qc_in --qc_out $qc_out --rpt_list "16850:1:2" --input_files $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/lane1/plex2/16850_1#2.cram} .
    qq{ && qc --check bam_flagstats --filename_root 16850_1#2_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "16850:1:2" --subset phix --input_files $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/lane1/plex2/16850_1#2.cram} .
     q{ && qc --check alignment_filter_metrics --filename_root 16850_1#2 --qc_in $PWD --qc_out } .$qc_out.q{ --rpt_list "16850:1:2" --input_files 16850_1#2_bam_alignment_filter_metrics.json}.
     q{ '};

  $d = _find($da, 1, 2);
  is ($d->command, $command, 'command for MiSeq run 16850 lane 1 tag 2');

  $qc_in  = qq{$bc_path/archive/lane1/plex0};
  $qc_out = qq{$qc_in/qc};
  $plex_temp_dir = $tmp_dir . q{/16850_1#0};
  $command = qq{bash -c ' mkdir -p $plex_temp_dir ; cd $plex_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/16850_1#0_p4s2_pv_in.json -export_param_vals 16850_1#0_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16850_1#0.json && viv.pl -s -x -v 3 -o viv_16850_1#0.log run_16850_1#0.json } .
    qq{ && qc --check bam_flagstats --filename_root 16850_1#0 --qc_in $qc_in --qc_out $qc_out --rpt_list "16850:1:0" --input_files $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/lane1/plex0/16850_1#0.cram --skip_markdups_metrics} .
    qq{ && qc --check bam_flagstats --filename_root 16850_1#0_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "16850:1:0" --subset phix --input_files $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/lane1/plex0/16850_1#0.cram} .
     q{ && qc --check alignment_filter_metrics --filename_root 16850_1#0 --qc_in $PWD --qc_out } .$qc_out.q{ --rpt_list "16850:1:0" --input_files 16850_1#0_bam_alignment_filter_metrics.json}.
     q{ '};

  $d = _find($da, 1, 0);
  is ($d->command, $command, 'command for MiSeq run 16850 lane 1 tag 0');
};

subtest 'nonconsented human split, no target alignment' => sub {
  plan tests => 5;
  ##MiSeq, run 16756_1 (nonconsented human split, no target alignment)

  my $runfolder = q{150701_HS36_16756_B_C711RANXX};
  my $runfolder_path = join q[/], $dir, $runfolder;
  my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150707-132329/no_cal';
  `mkdir -p $bc_path/lane1`;
  my $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150707-132329/metadata_cache_16756';
  `mkdir -p $cache_dir`;
  copy('t/data/hiseq/16756_RunInfo.xml', "$runfolder_path/RunInfo.xml") or die 'Copy failed';
  copy('t/data/run_params/runParameters.miseq.xml', "$runfolder_path/runParameters.xml")
    or die 'Copy failed';

  # default human reference needed for alignment for unconsented human split
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/hiseq/samplesheet_16756.csv];

  my $hs_gen;
  lives_ok {
    $hs_gen = npg_pipeline::function::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2015},
      repository        => $dir,
      conf_path         => 't/data/release/config/seq_alignment',
    )
  } 'no error creating an object';
  is ($hs_gen->id_run, 16756, 'id_run inferred correctly');
  make_path "$bc_path/archive/tileviz";
  apply_all_roles($hs_gen, 'npg_pipeline::runfolder_scaffold');
  $hs_gen->create_product_level();

  my $da = $hs_gen->generate('analysis_pipeline');

  my $qc_in  = qq{$bc_path/archive/lane1/plex1};
  my $qc_out = qq{$qc_in/qc};
  my $unique_string = $hs_gen->_job_id();
  my $tmp_dir = qq{$bc_path/archive/tmp_} . $unique_string;
  my $plex_temp_dir = $tmp_dir . q{/16756_1#1};
  my $command = qq{bash -c ' mkdir -p $plex_temp_dir ; cd $plex_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/16756_1#1_p4s2_pv_in.json -export_param_vals 16756_1#1_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_humansplit_template.json > run_16756_1#1.json && viv.pl -s -x -v 3 -o viv_16756_1#1.log run_16756_1#1.json } .
    qq{ && qc --check bam_flagstats --filename_root 16756_1#1 --qc_in $qc_in --qc_out $qc_out --rpt_list "16756:1:1" --input_files $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/plex1/16756_1#1.cram --skip_markdups_metrics} .
    qq{ && qc --check bam_flagstats --filename_root 16756_1#1_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "16756:1:1" --subset phix --input_files $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/plex1/16756_1#1.cram} .
     q{ && qc --check alignment_filter_metrics --filename_root 16756_1#1 --qc_in $PWD --qc_out } .$qc_out.q{ --rpt_list "16756:1:1" --input_files 16756_1#1_bam_alignment_filter_metrics.json}.
    qq{ && qc --check bam_flagstats --filename_root 16756_1#1_human --qc_in $qc_in --qc_out $qc_out --rpt_list "16756:1:1" --subset human --input_files $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/plex1/16756_1#1.cram} .
     q{ '};

  my $d = _find($da, 1, 1);
  is ($d->command, $command, 'command for run 16756 lane 1 tag 1');

  $qc_in  = qq{$bc_path/archive/lane1/plex2};
  $qc_out = qq{$qc_in/qc};
  $plex_temp_dir = $tmp_dir . q{/16756_1#2};
  $command = qq{bash -c ' mkdir -p $plex_temp_dir ; cd $plex_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/16756_1#2_p4s2_pv_in.json -export_param_vals 16756_1#2_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_humansplit_template.json > run_16756_1#2.json && viv.pl -s -x -v 3 -o viv_16756_1#2.log run_16756_1#2.json } .
    qq{ && qc --check bam_flagstats --filename_root 16756_1#2 --qc_in $qc_in --qc_out $qc_out --rpt_list "16756:1:2" --input_files $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/plex2/16756_1#2.cram --skip_markdups_metrics} .
    qq{ && qc --check bam_flagstats --filename_root 16756_1#2_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "16756:1:2" --subset phix --input_files $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/plex2/16756_1#2.cram} .
     q{ && qc --check alignment_filter_metrics --filename_root 16756_1#2 --qc_in $PWD --qc_out } .$qc_out.q{ --rpt_list "16756:1:2" --input_files 16756_1#2_bam_alignment_filter_metrics.json}.
    qq{ && qc --check bam_flagstats --filename_root 16756_1#2_human --qc_in $qc_in --qc_out $qc_out --rpt_list "16756:1:2" --subset human --input_files $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/plex2/16756_1#2.cram} .
     q{ '};

  $d = _find($da, 1, 2);
  is ($d->command, $command, 'command for run 16756 lane 1 tag 2');

  $qc_in  = qq{$bc_path/archive/lane1/plex0};
  $qc_out = qq{$qc_in/qc};
  $plex_temp_dir = $tmp_dir . q{/16756_1#0};
  $command = qq{bash -c ' mkdir -p $plex_temp_dir ; cd $plex_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/16756_1#0_p4s2_pv_in.json -export_param_vals 16756_1#0_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_humansplit_template.json > run_16756_1#0.json && viv.pl -s -x -v 3 -o viv_16756_1#0.log run_16756_1#0.json } .
    qq{ && qc --check bam_flagstats --filename_root 16756_1#0 --qc_in $qc_in --qc_out $qc_out --rpt_list "16756:1:0" --input_files $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/plex0/16756_1#0.cram --skip_markdups_metrics} .
    qq{ && qc --check bam_flagstats --filename_root 16756_1#0_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "16756:1:0" --subset phix --input_files $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/plex0/16756_1#0.cram} .
     q{ && qc --check alignment_filter_metrics --filename_root 16756_1#0 --qc_in $PWD --qc_out } .$qc_out.q{ --rpt_list "16756:1:0" --input_files 16756_1#0_bam_alignment_filter_metrics.json}.
    qq{ && qc --check bam_flagstats --filename_root 16756_1#0_human --qc_in $qc_in --qc_out $qc_out --rpt_list "16756:1:0" --subset human --input_files $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/plex0/16756_1#0.cram} .
     q{ '};

  $d = _find($da, 1, 0);
  is ($d->command, $command, 'command for run 16756 lane 1 tag 0');
};

subtest 'nonconsented human split, target alignment' => sub {
  plan tests => 5;
  ##MiSeq, run 16866_1 (nonconsented human split, target alignment)

  my $runfolder = q{150713_MS8_16866_A_MS3734403-300V2};
  my $runfolder_path = join q[/], $dir, $runfolder;
  my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150714-133929/no_cal';
  `mkdir -p $bc_path/lane1`;
  my $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150714-133929/metadata_cache_16866';
  `mkdir -p $cache_dir`;

  copy('t/data/miseq/16866_RunInfo.xml', "$runfolder_path/RunInfo.xml") or die 'Copy failed';
  copy('t/data/run_params/runParameters.miseq.xml', "$runfolder_path/runParameters.xml")
    or die 'Copy failed';

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/miseq/samplesheet_16866.csv];

  my $ms_gen;
  lives_ok {
    $ms_gen = npg_pipeline::function::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2015},
      repository        => $dir,
      conf_path         => 't/data/release/config/seq_alignment',
    )
  } 'no error creating an object';
  is ($ms_gen->id_run, 16866, 'id_run inferred correctly');
  make_path "$bc_path/archive/tileviz";
  apply_all_roles($ms_gen, 'npg_pipeline::runfolder_scaffold');
  $ms_gen->create_product_level();

  my $da = $ms_gen->generate('analysis_pipeline');

  my $qc_in  = qq{$bc_path/archive/lane1/plex1};
  my $qc_out = qq{$qc_in/qc};
  my $unique_string = $ms_gen->_job_id();
  my $tmp_dir = qq{$bc_path/archive/tmp_} . $unique_string;
  my $plex_temp_dir = $tmp_dir . q{/16866_1#1};
  my $command = qq{bash -c ' mkdir -p $plex_temp_dir ; cd $plex_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/16866_1#1_p4s2_pv_in.json -export_param_vals 16866_1#1_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_humansplit_template.json > run_16866_1#1.json && viv.pl -s -x -v 3 -o viv_16866_1#1.log run_16866_1#1.json } .
    qq{ && qc --check bam_flagstats --filename_root 16866_1#1 --qc_in $qc_in --qc_out $qc_out --rpt_list "16866:1:1" --input_files $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/plex1/16866_1#1.cram} .
    qq{ && qc --check bam_flagstats --filename_root 16866_1#1_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "16866:1:1" --subset phix --input_files $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/plex1/16866_1#1.cram} .
     q{ && qc --check alignment_filter_metrics --filename_root 16866_1#1 --qc_in $PWD --qc_out } .$qc_out.q{ --rpt_list "16866:1:1" --input_files 16866_1#1_bam_alignment_filter_metrics.json}.
    qq{ && qc --check bam_flagstats --filename_root 16866_1#1_human --qc_in $qc_in --qc_out $qc_out --rpt_list "16866:1:1" --subset human --input_files $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/plex1/16866_1#1.cram} .
     q{ '};

  my $d = _find($da, 1, 1);
  is ($d->command, $command, 'command for run 16866 lane 1 tag 1');

  $qc_in  = qq{$bc_path/archive/lane1/plex2};
  $qc_out = qq{$qc_in/qc};
  $plex_temp_dir = $tmp_dir . q{/16866_1#2};
  $command = qq{bash -c ' mkdir -p $plex_temp_dir ; cd $plex_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/16866_1#2_p4s2_pv_in.json -export_param_vals 16866_1#2_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_humansplit_template.json > run_16866_1#2.json && viv.pl -s -x -v 3 -o viv_16866_1#2.log run_16866_1#2.json } .
    qq{ && qc --check bam_flagstats --filename_root 16866_1#2 --qc_in $qc_in --qc_out $qc_out --rpt_list "16866:1:2" --input_files $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/plex2/16866_1#2.cram} .
    qq{ && qc --check bam_flagstats --filename_root 16866_1#2_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "16866:1:2" --subset phix --input_files $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/plex2/16866_1#2.cram} .
     q{ && qc --check alignment_filter_metrics --filename_root 16866_1#2 --qc_in $PWD --qc_out } .$qc_out.q{ --rpt_list "16866:1:2" --input_files 16866_1#2_bam_alignment_filter_metrics.json}.
    qq{ && qc --check bam_flagstats --filename_root 16866_1#2_human --qc_in $qc_in --qc_out $qc_out --rpt_list "16866:1:2" --subset human --input_files $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/plex2/16866_1#2.cram} .
     q{ '};

  $d = _find($da, 1, 2);
  is ($d->command, $command, 'command for run 16866 lane 1 tag 2');

  $qc_in  = qq{$bc_path/archive/lane1/plex0};
  $qc_out = qq{$qc_in/qc};
  $plex_temp_dir = $tmp_dir . q{/16866_1#0};
  $command = qq{bash -c ' mkdir -p $plex_temp_dir ; cd $plex_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/16866_1#0_p4s2_pv_in.json -export_param_vals 16866_1#0_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_humansplit_template.json > run_16866_1#0.json && viv.pl -s -x -v 3 -o viv_16866_1#0.log run_16866_1#0.json } .
    qq{ && qc --check bam_flagstats --filename_root 16866_1#0 --qc_in $qc_in --qc_out $qc_out --rpt_list "16866:1:0" --input_files $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/plex0/16866_1#0.cram --skip_markdups_metrics} .
    qq{ && qc --check bam_flagstats --filename_root 16866_1#0_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "16866:1:0" --subset phix --input_files $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/plex0/16866_1#0.cram} .
     q{ && qc --check alignment_filter_metrics --filename_root 16866_1#0 --qc_in $PWD --qc_out } .$qc_out.q{ --rpt_list "16866:1:0" --input_files 16866_1#0_bam_alignment_filter_metrics.json}.
    qq{ && qc --check bam_flagstats --filename_root 16866_1#0_human --qc_in $qc_in --qc_out $qc_out --rpt_list "16866:1:0" --subset human --input_files $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/plex0/16866_1#0.cram} .
     q{ '};

  $d = _find($da, 1, 0);
  is ($d->command, $command, 'command for run 16866 lane 1 tag 0');
};

subtest 'no target alignment, no human split' => sub {
  plan tests => 5;
  ##MiSeq, run 20990_1 (no target alignment, no human split)

  my $runfolder = q{161010_MS5_20990_A_MS4548606-300V2};
  my $runfolder_path = join q[/], $dir, $runfolder;
  my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20161011-102905/no_cal';
  `mkdir -p $bc_path/lane1`;
  my $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20161011-102905/metadata_cache_20990';
  `mkdir $cache_dir`;

  copy('t/data/miseq/20990_RunInfo.xml', "$runfolder_path/RunInfo.xml") or die 'Copy failed';
  copy('t/data/run_params/runParameters.miseq.xml', "$runfolder_path/runParameters.xml")
    or die 'Copy failed';

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/miseq/samplesheet_20990.csv];

  my $ms_gen;
  lives_ok {
    $ms_gen = npg_pipeline::function::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2016},
      repository        => $dir,
      conf_path         => 't/data/release/config/seq_alignment',
    )
  } 'no error creating an object';
  is ($ms_gen->id_run, 20990, 'id_run (20990) inferred correctly');
  make_path "$bc_path/archive/tileviz";
  apply_all_roles($ms_gen, 'npg_pipeline::runfolder_scaffold');
  $ms_gen->create_product_level();

  my $da = $ms_gen->generate('analysis_pipeline');

  my $qc_in  = qq{$bc_path/archive/lane1/plex1};
  my $qc_out = qq{$qc_in/qc};
  my $unique_string = $ms_gen->_job_id();
  my $tmp_dir = qq{$bc_path/archive/tmp_} . $unique_string;
  my $plex_temp_dir = $tmp_dir . q{/20990_1#1};
  my $command = qq{bash -c ' mkdir -p $plex_temp_dir ; cd $plex_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/20990_1#1_p4s2_pv_in.json -export_param_vals 20990_1#1_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_20990_1#1.json && viv.pl -s -x -v 3 -o viv_20990_1#1.log run_20990_1#1.json  && qc --check bam_flagstats --filename_root 20990_1#1 --qc_in $qc_in --qc_out $qc_out --rpt_list "20990:1:1" --input_files $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/archive/lane1/plex1/20990_1#1.cram --skip_markdups_metrics && qc --check bam_flagstats --filename_root 20990_1#1_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "20990:1:1" --subset phix --input_files $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/archive/lane1/plex1/20990_1#1.cram && qc --check alignment_filter_metrics --filename_root 20990_1#1 --qc_in \$PWD --qc_out $qc_out --rpt_list "20990:1:1" --input_files 20990_1#1_bam_alignment_filter_metrics.json '};

  my $d = _find($da, 1, 1);
  is ($d->command, $command, 'command for run 20990 lane 1 tag 1');

  $qc_in  = qq{$bc_path/archive/lane1/plex0};
  $qc_out = qq{$qc_in/qc};
  $plex_temp_dir = $tmp_dir . q{/20990_1#0};
  $command = qq{bash -c ' mkdir -p $plex_temp_dir ; cd $plex_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/20990_1#0_p4s2_pv_in.json -export_param_vals 20990_1#0_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_20990_1#0.json && viv.pl -s -x -v 3 -o viv_20990_1#0.log run_20990_1#0.json  && qc --check bam_flagstats --filename_root 20990_1#0 --qc_in $qc_in --qc_out $qc_out --rpt_list "20990:1:0" --input_files $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/archive/lane1/plex0/20990_1#0.cram --skip_markdups_metrics && qc --check bam_flagstats --filename_root 20990_1#0_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "20990:1:0" --subset phix --input_files $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/archive/lane1/plex0/20990_1#0.cram && qc --check alignment_filter_metrics --filename_root 20990_1#0 --qc_in \$PWD --qc_out $qc_out --rpt_list "20990:1:0" --input_files 20990_1#0_bam_alignment_filter_metrics.json '};

  $d = _find($da, 1, 0);
  is ($d->command, $command, 'command for run 20990 lane 1 tag 0');

  $qc_in  = qq{$bc_path/archive/lane1/plex2};
  $qc_out = qq{$qc_in/qc};
  $plex_temp_dir = $tmp_dir . q{/20990_1#2};
  $command = qq{bash -c ' mkdir -p $plex_temp_dir ; cd $plex_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/20990_1#2_p4s2_pv_in.json -export_param_vals 20990_1#2_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_20990_1#2.json && viv.pl -s -x -v 3 -o viv_20990_1#2.log run_20990_1#2.json  && qc --check bam_flagstats --filename_root 20990_1#2 --qc_in $qc_in --qc_out $qc_out --rpt_list "20990:1:2" --input_files $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/archive/lane1/plex2/20990_1#2.cram --skip_markdups_metrics && qc --check bam_flagstats --filename_root 20990_1#2_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "20990:1:2" --subset phix --input_files $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/archive/lane1/plex2/20990_1#2.cram && qc --check alignment_filter_metrics --filename_root 20990_1#2 --qc_in \$PWD --qc_out $qc_out --rpt_list "20990:1:2" --input_files 20990_1#2_bam_alignment_filter_metrics.json '};

  $d = _find($da, 1, 2);
  is ($d->command, $command, 'command for run 20990 lane 1 tag 2');
};

subtest 'chromium' => sub {
  plan tests => 5;
  ##HiSeqX, run 16839_1

  my $ref_dir = join q[/],$dir,'references','Homo_sapiens','GRCh38_full_analysis_set_plus_decoy_hla','all';
  `mkdir -p $ref_dir/fasta`;
  `mkdir -p $ref_dir/bwa0_6`;
  `mkdir -p $ref_dir/picard`;

  my $runfolder = q{150709_HX4_16839_A_H7MHWCCXX};
  my $runfolder_path = join q[/], $dir, $runfolder;
  my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150712-121006/no_cal';
  `mkdir -p $bc_path/lane1`;
  my $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150712-121006/metadata_cache_16839';
  `mkdir -p $cache_dir`;
  copy('t/data/hiseqx/16839_RunInfo.xml', "$runfolder_path/RunInfo.xml") or die 'Copy failed';
  copy('t/data/run_params/runParameters.hiseqx.upgraded.xml', "$runfolder_path/runParameters.xml")
    or die 'Copy failed';

  # Chromium libs are not aligned
  my $old_ss = q[t/data/hiseqx/samplesheet_16839.csv];
  my $ss = slurp $old_ss;
  $ss =~ s/Standard/Chromium single cell/;
  my $new_ss = "$dir/chromium_samplesheet_16839.csv";
  open my $fhss, '>', $new_ss or die "Cannot open $new_ss for writing";
  print $fhss $ss or die "Cannot write to $new_ss";
  close $fhss or warn "Failed to close $new_ss";
  # new samplesheet has one chromium sample in lane 1
  local $ENV{'NPG_CACHED_SAMPLESHEET_FILE'} = $new_ss;
  local $ENV{'NPG_PIPELINE_JS_SCRIPTS_DIR'} = q[t/bin/];

  my $chromium_gen;
  lives_ok {
    $chromium_gen = npg_pipeline::function::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2015},
      repository        => $dir,
      conf_path         => 't/data/release/config/seq_alignment',
    )
  } 'no error creating an object';
  is ($chromium_gen->id_run, 16839, 'id_run inferred correctly');
  make_path "$bc_path/archive/tileviz";
  apply_all_roles($chromium_gen, 'npg_pipeline::runfolder_scaffold');
  $chromium_gen->create_product_level();

  my $da = $chromium_gen->generate('analysis_pipeline');

  my $qc_in  = qq{$dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane1/plex1};
  my $qc_out = qq{$qc_in/qc};
  my $unique_string = $chromium_gen->_job_id();
  my $tmp_dir = qq{$bc_path/archive/tmp_} . $unique_string;
  my $plex_temp_dir = $tmp_dir . q{/16839_1#1};
  my $command = qq{bash -c ' mkdir -p $plex_temp_dir ; cd $plex_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/16839_1#1_p4s2_pv_in.json -export_param_vals 16839_1#1_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16839_1#1.json && viv.pl -s -x -v 3 -o viv_16839_1#1.log run_16839_1#1.json } .
    qq{ && qc --check bam_flagstats --filename_root 16839_1#1 --qc_in $qc_in --qc_out $qc_out --rpt_list "16839:1:1" --input_files $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane1/plex1/16839_1#1.cram --skip_markdups_metrics} .
    qq{ && qc --check bam_flagstats --filename_root 16839_1#1_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "16839:1:1" --subset phix --input_files $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane1/plex1/16839_1#1.cram} .
     q{ && qc --check alignment_filter_metrics --filename_root 16839_1#1 --qc_in $PWD --qc_out } .$qc_out.q{ --rpt_list "16839:1:1" --input_files 16839_1#1_bam_alignment_filter_metrics.json}.
     q{ '};

  my $d = _find($da, 1, 1);
  is ($d->command, $command, 'command for run 16839 lane 1 tag 1');

  $qc_in  = qq{$dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane1/plex9};
  $qc_out = qq{$qc_in/qc};
  $plex_temp_dir = $tmp_dir . q{/16839_1#9};
  $command = qq{bash -c ' mkdir -p $plex_temp_dir ; cd $plex_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/16839_1#9_p4s2_pv_in.json -export_param_vals 16839_1#9_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16839_1#9.json && viv.pl -s -x -v 3 -o viv_16839_1#9.log run_16839_1#9.json } .
    qq{ && qc --check bam_flagstats --filename_root 16839_1#9 --qc_in $qc_in --qc_out $qc_out --rpt_list "16839:1:9" --input_files $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane1/plex9/16839_1#9.cram} .
    qq{ && qc --check bam_flagstats --filename_root 16839_1#9_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "16839:1:9" --subset phix --input_files $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane1/plex9/16839_1#9.cram} .
     q{ && qc --check alignment_filter_metrics --filename_root 16839_1#9 --qc_in $PWD --qc_out } .$qc_out.q{ --rpt_list "16839:1:9" --input_files 16839_1#9_bam_alignment_filter_metrics.json}.
     q{ '};

  $d = _find($da, 1, 9);
  is ($d->command, $command, 'command for run 16839 lane 1 tag 9');

  $qc_in  = qq{$dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane1/plex0};
  $qc_out = qq{$qc_in/qc};
  $plex_temp_dir = $tmp_dir . q{/16839_1#0};
  $command = qq{bash -c ' mkdir -p $plex_temp_dir ; cd $plex_temp_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/16839_1#0_p4s2_pv_in.json -export_param_vals 16839_1#0_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16839_1#0.json && viv.pl -s -x -v 3 -o viv_16839_1#0.log run_16839_1#0.json } .
    qq{ && qc --check bam_flagstats --filename_root 16839_1#0 --qc_in $qc_in --qc_out $qc_out --rpt_list "16839:1:0" --input_files $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane1/plex0/16839_1#0.cram --skip_markdups_metrics} .
    qq{ && qc --check bam_flagstats --filename_root 16839_1#0_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "16839:1:0" --subset phix --input_files $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane1/plex0/16839_1#0.cram} .
     q{ && qc --check alignment_filter_metrics --filename_root 16839_1#0 --qc_in $PWD --qc_out } .$qc_out.q{ --rpt_list "16839:1:0" --input_files 16839_1#0_bam_alignment_filter_metrics.json}.
     q{ '};

  $d = _find($da, 1, 0);
  is ($d->command, $command, 'command for run 16839 lane 1 tag 0');
};

subtest 'miseq' => sub {
  plan tests => 11;

  my $runfolder = q{171020_MS5_24135_A_MS5476963-300V2};
  my $runfolder_path = join q[/], $dir, $runfolder;
  my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20171127-134427/no_cal';
  `mkdir -p $bc_path`;
  my $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20171127-134427/metadata_cache_24135';
  `mkdir -p $cache_dir`;
  `mkdir $bc_path/lane1`;

  copy('t/data/miseq/24135_RunInfo.xml', "$runfolder_path/RunInfo.xml") or die 'Copy failed';
  copy('t/data/run_params/runParameters.miseq.xml', "$runfolder_path/runParameters.xml")
    or die 'Copy failed';

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/miseq/samplesheet_24135.csv];

  my $ms_gen;
  lives_ok {
    $ms_gen = npg_pipeline::function::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2017},
      repository        => $dir,
      conf_path         => 't/data/release/config/seq_alignment',
    )
  } 'no error creating an object';
  is ($ms_gen->id_run, 24135, 'id_run inferred correctly');

  make_path "$bc_path/archive/tileviz";
  apply_all_roles($ms_gen, 'npg_pipeline::runfolder_scaffold');
  $ms_gen->create_product_level();

  my $da = $ms_gen->generate('analysis_pipeline');

  my $base = "$bc_path/archive/lane1";
  my @files = ('plex1/24135_1#1_phix.composition.json',
               'plex1/24135_1#1.composition.json',
               'plex2/24135_1#2_phix.composition.json',
               'plex2/24135_1#2.composition.json',
               'plex0/24135_1#0_phix.composition.json',
               'plex0/24135_1#0.composition.json');
  for my $f (@files) {
    ok (-f "$base/$f", "file $f exists");
  }

  my $unique_string = $ms_gen->_job_id();
  my $tmp_dir = qq{$bc_path/archive/tmp_} . $unique_string;

  my $qc_in  = qq{$bc_path/archive/lane1/plex1};
  my $qc_out = qq{$qc_in/qc};
  my $tmp_plex_dir = $tmp_dir . '/24135_1#1';
  my $command = qq{bash -c ' mkdir -p $tmp_plex_dir ; cd $tmp_plex_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/24135_1#1_p4s2_pv_in.json -export_param_vals 24135_1#1_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_24135_1#1.json && viv.pl -s -x -v 3 -o viv_24135_1#1.log run_24135_1#1.json  && qc --check bam_flagstats --filename_root 24135_1#1 --qc_in $qc_in --qc_out $qc_out --rpt_list "24135:1:1" --input_files $dir/171020_MS5_24135_A_MS5476963-300V2/Data/Intensities/BAM_basecalls_20171127-134427/no_cal/archive/lane1/plex1/24135_1#1.cram --skip_markdups_metrics && qc --check bam_flagstats --filename_root 24135_1#1_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "24135:1:1" --subset phix --input_files $dir/171020_MS5_24135_A_MS5476963-300V2/Data/Intensities/BAM_basecalls_20171127-134427/no_cal/archive/lane1/plex1/24135_1#1.cram && qc --check alignment_filter_metrics --filename_root 24135_1#1 --qc_in \$PWD --qc_out $qc_out --rpt_list "24135:1:1" --input_files 24135_1#1_bam_alignment_filter_metrics.json && qc --check genotype_call --filename_root 24135_1#1 --qc_in $qc_in --qc_out $qc_out} . q{ --rpt_list "24135:1:1" '};

  my $d = _find($da, 1, 1);
  is ($d->command(), $command, 'correct command for MiSeq lane 24135_1 tag index 1');

  $qc_in  = qq{$bc_path/archive/lane1/plex0};
  $qc_out = qq{$qc_in/qc};
  $tmp_plex_dir = $tmp_dir . '/24135_1#0';
  $command = qq{bash -c ' mkdir -p $tmp_plex_dir ; cd $tmp_plex_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/24135_1#0_p4s2_pv_in.json -export_param_vals 24135_1#0_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_24135_1#0.json && viv.pl -s -x -v 3 -o viv_24135_1#0.log run_24135_1#0.json  && qc --check bam_flagstats --filename_root 24135_1#0 --qc_in $qc_in --qc_out $qc_out --rpt_list "24135:1:0" --input_files $dir/171020_MS5_24135_A_MS5476963-300V2/Data/Intensities/BAM_basecalls_20171127-134427/no_cal/archive/lane1/plex0/24135_1#0.cram --skip_markdups_metrics && qc --check bam_flagstats --filename_root 24135_1#0_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "24135:1:0" --subset phix --input_files $dir/171020_MS5_24135_A_MS5476963-300V2/Data/Intensities/BAM_basecalls_20171127-134427/no_cal/archive/lane1/plex0/24135_1#0.cram && qc --check alignment_filter_metrics --filename_root 24135_1#0 --qc_in \$PWD --qc_out $qc_out --rpt_list "24135:1:0" --input_files 24135_1#0_bam_alignment_filter_metrics.json && qc --check genotype_call --filename_root 24135_1#0 --qc_in $qc_in --qc_out $qc_out} . q{ --rpt_list "24135:1:0" '};

  $d = _find($da, 1, 0);
  is ($d->command(), $command, 'correct command for MiSeq lane 24135_1 tag index 0');

  $qc_in  = qq{$bc_path/archive/lane1/plex2};
  $qc_out = qq{$qc_in/qc};
  $tmp_plex_dir = $tmp_dir . '/24135_1#2';
  $command = qq{bash -c ' mkdir -p $tmp_plex_dir ; cd $tmp_plex_dir && vtfp.pl -template_path \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib -param_vals $bc_path/24135_1#2_p4s2_pv_in.json -export_param_vals 24135_1#2_p4s2_pv_out_$unique_string.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 12` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 12 --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_24135_1#2.json && viv.pl -s -x -v 3 -o viv_24135_1#2.log run_24135_1#2.json  && qc --check bam_flagstats --filename_root 24135_1#2 --qc_in $qc_in --qc_out $qc_out --rpt_list "24135:1:2" --input_files $dir/171020_MS5_24135_A_MS5476963-300V2/Data/Intensities/BAM_basecalls_20171127-134427/no_cal/archive/lane1/plex2/24135_1#2.cram --skip_markdups_metrics && qc --check bam_flagstats --filename_root 24135_1#2_phix --qc_in $qc_in --qc_out $qc_out --rpt_list "24135:1:2" --subset phix --input_files $dir/171020_MS5_24135_A_MS5476963-300V2/Data/Intensities/BAM_basecalls_20171127-134427/no_cal/archive/lane1/plex2/24135_1#2.cram && qc --check alignment_filter_metrics --filename_root 24135_1#2 --qc_in \$PWD --qc_out $qc_out --rpt_list "24135:1:2" --input_files 24135_1#2_bam_alignment_filter_metrics.json && qc --check genotype_call --filename_root 24135_1#2 --qc_in $qc_in --qc_out $qc_out} . q{ --rpt_list "24135:1:2" '};

  $d = _find($da, 1, 2);
  is ($d->command(), $command, 'correct command for MiSeq lane 24135_1 tag index 2');
};

subtest 'generate compositions only' => sub {
  plan tests => 8;

  my $runfolder = q{171020_MS5_24135_A_MS5476963-300V2};
  my $runfolder_path = join q[/], $dir, 'compositions', $runfolder;
  my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20171127-134427/no_cal';
  make_path $bc_path;
  my $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20171127-134427/metadata_cache_24135';
  make_path $cache_dir;
  make_path "$bc_path/lane1";
  make_path "$bc_path/archive/tileviz";

  copy('t/data/miseq/24135_RunInfo.xml', "$runfolder_path/RunInfo.xml") or die 'Copy failed';
  copy('t/data/run_params/runParameters.miseq.xml', "$runfolder_path/runParameters.xml")
    or die 'Copy failed';

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/miseq/samplesheet_24135.csv];

  my $ms_gen = npg_pipeline::function::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2017},
      repository        => $dir,
      conf_path         => 't/data/release/config/seq_alignment',
  );
  apply_all_roles($ms_gen, 'npg_pipeline::runfolder_scaffold');
  $ms_gen->create_product_level();
  
  my $da = $ms_gen->generate_compositions('finishing_pipeline');
  ok (($da and (@{$da} == 1)), 'one definition returned');
  ok ($da->[0], 'function is excluded');

  my $base = "$bc_path/archive/lane1";
  my @files = ('plex1/24135_1#1_phix.composition.json',
               'plex1/24135_1#1.composition.json',
               'plex2/24135_1#2_phix.composition.json',
               'plex2/24135_1#2.composition.json',
               'plex0/24135_1#0_phix.composition.json',
               'plex0/24135_1#0.composition.json');
  for my $f (@files) {
    ok (-f "$base/$f", "file $f exists");
  }
};

subtest 'product_release_tests' => sub {
  plan tests => 92;

  my %test_runs = (
    16850 => { platform => 'miseq', runfolder_name => '150710_MS2_16850_A_MS3014507-500V2', markdup_method => 'samtools', },
    16866 => { platform => 'miseq', runfolder_name => '150713_MS8_16866_A_MS3734403-300V2', markdup_method => 'samtools', },
    20990 => { platform => 'miseq', runfolder_name => '161010_MS5_20990_A_MS4548606-300V2', markdup_method => 'picard', },
    24135 => { platform => 'miseq', runfolder_name => '171020_MS5_24135_A_MS5476963-300V2', markdup_method => 'samtools', },
    16756 => { platform => 'hiseq', runfolder_name => '150701_HS36_16756_B_C711RANXX', markdup_method => 'samtools', },
    16803 => { platform => 'hiseq', runfolder_name => '150706_HS21_16803_B_HGLFHADXX', markdup_method => 'samtools', },
    16807 => { platform => 'hiseq', runfolder_name => '150707_HS38_16807_A_C7U2YANXX', markdup_method => 'samtools', },
    20268 => { platform => 'hiseq', runfolder_name => '160704_MS3_20268_A_MS4000667-300V2', markdup_method => 'biobambam', },
    16839 => { platform => 'hiseqx', runfolder_name => '150709_HX4_16839_A_H7MHWCCXX', markdup_method => 'samtools', },
  );

  for my $run (keys %test_runs) {
    my $run_details = $test_runs{$run};
    my $runfolder_path = join q[/], $dir, $run_details->{runfolder_name};
    my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_17760704-123456/no_cal';
    `mkdir -p $bc_path`;
    my $cache_dir = join q[/], $runfolder_path, "Data/Intensities/BAM_basecalls_17760704-123456/metadata_cache_$run";
    `mkdir -p $cache_dir`;
 
    copy("t/data/$run_details->{platform}/${run}_RunInfo.xml", "$runfolder_path/RunInfo.xml") or die 'Copy failed';
    copy("t/data/run_params/runParameters.miseq.xml", "$runfolder_path/runParameters.xml")
      or die "runParameters.xml copy failed";
 
    local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = qq[t/data/$run_details->{platform}/samplesheet_${run}.csv];
 
    my $sa_gen;
    lives_ok {
      $sa_gen = npg_pipeline::function::seq_alignment->new(
        run_folder        => $run_details->{runfolder_name},
        runfolder_path    => $runfolder_path,
        recalibrated_path => $bc_path,
        timestamp         => q{1776},
        repository        => $dir,
        conf_path         => 't/data/release/config/seq_alignment',
      )
    } 'no error creating an object';
    is ($sa_gen->id_run, $run, 'id_run inferred correctly');

    my $dps;
    lives_ok { $dps = $sa_gen->products->{data_products} } "no error finding data products for run $run";
    for my $i (0..$#{$dps}) {
      my $markdup_method = $sa_gen->markdup_method($dps->[$i]);

      is ($markdup_method, $run_details->{markdup_method}, "markdup_method for entry $i for run $run should be inferred as $markdup_method");
    }
  }

subtest 'BWA MEM 2 test' => sub {
  plan tests => 4;

  my $runfolder = q{171020_MS5_24135_A_MS5476963-300V2};
  my $runfolder_path = join q[/], $dir, 'compositions', $runfolder;
  my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20171127-134427/no_cal';
  make_path $bc_path;
  my $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20171127-134427/metadata_cache_24135';
  make_path $cache_dir;
  make_path "$bc_path/lane1";
  make_path "$bc_path/archive/tileviz";

  copy('t/data/miseq/24135_RunInfo.xml', "$runfolder_path/RunInfo.xml") or die 'Copy failed';
  copy('t/data/run_params/runParameters.miseq.xml', "$runfolder_path/runParameters.xml")
    or die 'Copy failed';

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/miseq/samplesheet_24135_bwa_mem2.csv];

  my $ms_gen = npg_pipeline::function::seq_alignment->new(
    run_folder        => $runfolder,
    runfolder_path    => $runfolder_path,
    recalibrated_path => $bc_path,
    timestamp         => q{2017},
    repository        => $dir
  );
  apply_all_roles($ms_gen, 'npg_pipeline::runfolder_scaffold');
  $ms_gen->create_product_level();

  my $da = $ms_gen->generate('analysis_pipeline');
  ok (($da and (@{$da} == 3)), 'three definitions returned');
  my $d = _find($da, 1, 1);
  isa_ok ($d, 'npg_pipeline::function::definition');
  ok (!$d->excluded, 'step not excluded');

  my $l = st::api::lims->new(id_run => 24135, position => 1, tag_index => 2);
  my $analysis = $ms_gen->_analysis($l->reference_genome, '24135:1:2');
  ok ($analysis eq "bwa_mem2", 'run 24135 lane 1 tag 2 Analysis is BWA MEM 2');
};

1;
