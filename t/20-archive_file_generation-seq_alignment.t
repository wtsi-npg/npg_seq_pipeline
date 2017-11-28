use strict;
use warnings;
use Test::More tests => 12;
use Test::Exception;
use Test::Deep;
use Test::Warn;
use Test::Log::Log4perl;
use File::Temp qw/tempdir/;
use File::Path qw/make_path/;
use Cwd qw/cwd abs_path/;
use Perl6::Slurp;
use File::Copy;
use Log::Log4perl qw(:levels);
use JSON;
use Cwd;

use st::api::lims;

use_ok('npg_pipeline::archive::file::generation::seq_alignment');
local $ENV{'NPG_WEBSERVICE_CACHE_DIR'} = q[t/data/rna_seq];
local $ENV{'TEST_FS_RESOURCE'} = 'nfs-sf3';
local $ENV{PATH} = join q[:], q[t/bin], q[t/bin/software/solexa/bin], $ENV{PATH};
local $ENV{CLASSPATH} = q[t/bin/software/solexa/jars];

my $odir = abs_path cwd;
my $dir = tempdir( CLEANUP => 1);

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $dir, 'logfile'),
                          utf8   => 1});

# 90 lines of code instead of 113 to generate a reference and transcriptome reference. but you can add more!
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
        my $bwa_dir     = join q[/],$rel_dir,'bwa';
        my $bwa0_6_dir  = join q[/],$rel_dir,'bwa0_6';
        my $fasta_dir   = join q[/],$rel_dir,'fasta';
        my $picard_dir  = join q[/],$rel_dir,'picard';
        my $star_dir    = join q[/],$rel_dir,'star';
        make_path($bowtie2_dir, $bwa_dir, $bwa0_6_dir, $picard_dir, $star_dir, $fasta_dir, {verbose => 0});
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
`touch $ref_dir/Homo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/all/fasta/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa`;
`touch $ref_dir/Homo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/all/picard/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa.dict`;
`touch $ref_dir/Homo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/all/bwa0_6/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa.alt`;
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

subtest 'test 1' => sub {
  plan tests => 21;

  my $runfolder = q{140409_HS34_12597_A_C333TACXX};
  my $runfolder_path = join q[/], $dir, $runfolder;
  my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20140515-073611/no_cal';
  `mkdir -p $bc_path`;
 
  copy("t/data/rna_seq/12597_RunInfo.xml","$runfolder_path/RunInfo.xml") or die "Copy failed: $!"; #to get information that it is paired end

  my $rna_gen;
  lives_ok {
    $rna_gen = npg_pipeline::archive::file::generation::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2014},
      verbose           => 0,
      repository        => $dir,
      no_bsub           => 1,
      force_phix_split  => 0,
      ###uncomment to check V4 failure, flowcell_id=>'C333TANXX',
    )
  } 'no error creating an object';

  is ($rna_gen->id_run, 12597, 'id_run inferred correctly');
  ok ((not $rna_gen->_has_newer_flowcell), 'not HT V4 or RR V2') or diag $rna_gen->flowcell_id;

  my $qc_in  = $dir . q[/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4];
  my $qc_out = join q[/], $qc_in, q[qc];

  my $args = {};
  $args->{'40003'} = qq{bash -c '\ mkdir -p $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/tmp_\$LSB_JOBID/12597_4#3 ; cd $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/tmp_\$LSB_JOBID/12597_4#3 && vtfp.pl -param_vals $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/lane4/12597_4#3_p4s2_pv_in.json -export_param_vals 12597_4#3_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_12597_4#3.json && viv.pl -s -x -v 3 -o viv_12597_4#3.log run_12597_4#3.json } .
    qq{ && qc --check bam_flagstats --id_run 12597 --position 4 --qc_in $qc_in --qc_out $qc_out --tag_index 3} .
    qq{ && qc --check bam_flagstats --id_run 12597 --position 4 --qc_in $qc_in --qc_out $qc_out --subset phix --tag_index 3} .
     q{ && qc --check alignment_filter_metrics --id_run 12597 --position 4 --qc_in $PWD --qc_out } .$qc_out.q{ --tag_index 3}.
    qq{ && qc --check rna_seqc --id_run 12597 --position 4 --qc_in $qc_in --qc_out } . $qc_out . q{ --tag_index 3}.
     q{ '};

  $args->{'40000'} = qq{bash -c '\ mkdir -p $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/tmp_\$LSB_JOBID/12597_4#0 ; cd $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/tmp_\$LSB_JOBID/12597_4#0 && vtfp.pl -param_vals $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/lane4/12597_4#0_p4s2_pv_in.json -export_param_vals 12597_4#0_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_12597_4#0.json && viv.pl -s -x -v 3 -o viv_12597_4#0.log run_12597_4#0.json } .
    qq{ && qc --check bam_flagstats --id_run 12597 --position 4 --qc_in $qc_in --qc_out $qc_out --tag_index 0} .
    qq{ && qc --check bam_flagstats --id_run 12597 --position 4 --qc_in $qc_in --qc_out $qc_out --subset phix --tag_index 0} .
     q{ && qc --check alignment_filter_metrics --id_run 12597 --position 4 --qc_in $PWD --qc_out } .$qc_out.q{ --tag_index 0}.  
     q{ '};
  
  lives_ok {$rna_gen->_generate_command_arguments([4])}
    'no error generating command arguments';
  
  cmp_deeply ($rna_gen->_job_args, $args,
    'correct command arguments for pooled RNASeq lane');

  is ($rna_gen->_job_args->{'4000'}, $args->{'4000'}, 'correct tag 0 args generated');

  my $required_job_completion = 55;
  my $mem = 32000;
  my $expected = qq{bsub -q srpipeline -E 'npg_pipeline_preexec_references --repository $dir' -R 'select[mem>$mem] rusage[mem=$mem,nfs-sf3=4]' -M$mem -R 'span[hosts=1]'}.qq{ -n12,16 $required_job_completion -J 'seq_alignment_12597_2014[40000,40003]' -o $bc_path/archive/log/seq_alignment_12597_2014.%I.%J.out }.q('perl -Mstrict -MJSON -MFile::Slurp -Mopen='"'"':encoding(UTF8)'"'"' -e '"'"'exec from_json(read_file shift@ARGV)->{shift@ARGV} or die q(failed exec)'"'"' ) . $bc_path . q{/seq_alignment_12597_2014_$LSB_JOBID $LSB_JOBINDEX'}; 

  is($rna_gen->_command2submit($required_job_completion), $expected, 'command to submit is correct');


  my $fname;
  lives_ok{$fname = $rna_gen->_save_arguments(55)} 'writing args to a file without error';
  is ($fname, $bc_path. q{/seq_alignment_12597_2014_55}, 'file name correct');
  ok (-e $fname, 'file exists');
  my $actual_json = slurp($fname);
  my $actual_hash = decode_json $actual_json;

  my $json = qq({"40003":"bash -c ' mkdir -p $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/tmp_\$LSB_JOBID/12597_4#3 ; cd $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/tmp_\$LSB_JOBID/12597_4#3 && vtfp.pl -param_vals $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/lane4/12597_4#3_p4s2_pv_in.json -export_param_vals 12597_4#3_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_12597_4#3.json && viv.pl -s -x -v 3 -o viv_12597_4#3.log run_12597_4#3.json ) .
    qq{ && qc --check bam_flagstats --id_run 12597 --position 4 --qc_in $qc_in --qc_out $qc_out --tag_index 3} .
    qq{ && qc --check bam_flagstats --id_run 12597 --position 4 --qc_in $qc_in --qc_out $qc_out --subset phix --tag_index 3} .
     q{ && qc --check alignment_filter_metrics --id_run 12597 --position 4 --qc_in $PWD --qc_out } .$qc_out.q{ --tag_index 3}.
    qq{ && qc --check rna_seqc --id_run 12597 --position 4 --qc_in $qc_in --qc_out } . $qc_out . q{ --tag_index 3}.
  qq( '","40000":"bash -c ' mkdir -p $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/tmp_\$LSB_JOBID/12597_4#0 ; cd $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/tmp_\$LSB_JOBID/12597_4#0 && vtfp.pl -param_vals $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/lane4/12597_4#0_p4s2_pv_in.json -export_param_vals 12597_4#0_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_12597_4#0.json && viv.pl -s -x -v 3 -o viv_12597_4#0.log run_12597_4#0.json ) .
    qq{ && qc --check bam_flagstats --id_run 12597 --position 4 --qc_in $qc_in --qc_out $qc_out --tag_index 0} .
    qq{ && qc --check bam_flagstats --id_run 12597 --position 4 --qc_in $qc_in --qc_out $qc_out --subset phix --tag_index 0} .
     q{ && qc --check alignment_filter_metrics --id_run 12597 --position 4 --qc_in $PWD --qc_out } .$qc_out.q{ --tag_index 0}.
  q( '"});

  my $expected_hash = decode_json $json;
  cmp_deeply($actual_hash, $expected_hash, 'correct json file content (for dUTP library)');

  #### force on phix_split
  lives_ok {
    $rna_gen = npg_pipeline::archive::file::generation::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2014},
      verbose           => 0,
      repository        => $dir,
      no_bsub           => 1,
      force_phix_split  => 1,
    )
  } 'no error creating an object (forcing on phix split)';

  #####  phiX control libraries
  $qc_in =~ s{lane4}{lane5}smg;
  $qc_out =~ s{lane4}{lane5}smg;
  $args->{'5168'} = qq{bash -c ' mkdir -p $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/tmp_\$LSB_JOBID/12597_5#168 ; cd $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/tmp_\$LSB_JOBID/12597_5#168 && vtfp.pl -param_vals $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/lane5/12597_5#168_p4s2_pv_in.json -export_param_vals 12597_5#168_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_12597_5#168.json && viv.pl -s -x -v 3 -o viv_12597_5#168.log run_12597_5#168.json } .
    qq{ && qc --check bam_flagstats --id_run 12597 --position 5 --qc_in $qc_in --qc_out $qc_out --tag_index 168} .
  q( ');

  lives_ok {$rna_gen->_generate_command_arguments([5])}
     'no error generating command arguments for non-RNASeq lane';
  is ($rna_gen->_job_args->{'50168'}, $args->{'5168'}, 'correct non-RNASeq lane args generated');

  #### monoplex (non-RNA Seq)
  lives_ok {$rna_gen->_generate_command_arguments([1])}
     'no error generating command arguments for non-multiplex lane';
  $qc_in  =~ s{/lane.}{};
  $qc_out =~ s{/lane.}{}; 
  $args->{'1'} = qq{bash -c ' mkdir -p $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/tmp_\$LSB_JOBID/12597_1 ; cd $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/tmp_\$LSB_JOBID/12597_1 && vtfp.pl -param_vals $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/12597_1_p4s2_pv_in.json -export_param_vals 12597_1_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_12597_1.json && viv.pl -s -x -v 3 -o viv_12597_1.log run_12597_1.json } .
    qq{ && qc --check bam_flagstats --id_run 12597 --position 1 --qc_in $qc_in --qc_out $qc_out} .
    qq{ && qc --check bam_flagstats --id_run 12597 --position 1 --qc_in $qc_in --qc_out $qc_out --subset phix} .
     q{ && qc --check alignment_filter_metrics --id_run 12597 --position 1 --qc_in $PWD --qc_out } . $qc_out . q{ '};

  is ($rna_gen->_job_args->{'1'}, $args->{'1'}, 'correct non-multiplex lane args generated');
  ok((not $rna_gen->_has_newer_flowcell), 'HT V3 flowcell recognised as older flowcell');
  #### check for newer flowcells
  lives_ok {
    $rna_gen = npg_pipeline::archive::file::generation::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2014},
      verbose           => 0,
      repository        => $dir,
      no_bsub           => 1,
      force_phix_split  => 1,
      flowcell_id       => 'C333TBCXX',
    )
  } 'no error creating an object with RR V2 flowcell (forcing on phix split)';
  ok ($rna_gen->_has_newer_flowcell, 'RR V2 flowcell recognised as newer flowcell');

  lives_ok {
    $rna_gen = npg_pipeline::archive::file::generation::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2014},
      verbose           => 0,
      repository        => $dir,
      no_bsub           => 1,
      force_phix_split  => 1,
      flowcell_id       => 'C333TANXX',
    )
  } 'no error creating an object with HT V4 flowcell (forcing on phix split)';
  ok ($rna_gen->_has_newer_flowcell, 'HT V4 flowcell recognised as newer flowcell');
};

subtest 'test 2' => sub {
  plan tests => 16;

  ##RNASeq library  13066_8  library_type = Illumina cDNA protocol

  my $runfolder = q{140529_HS18_13066_A_C3C3KACXX};
  my $runfolder_path = join q[/], $dir, $runfolder;
  my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20140606-133530/no_cal';
  `mkdir -p $bc_path`;
  my $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20140606-133530/metadata_cache_13066';
  `mkdir -p $cache_dir`;
  copy("t/data/rna_seq/13066_RunInfo.xml","$runfolder_path/RunInfo.xml") or die "Copy failed: $!"; #to get information that it is paired end

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/rna_seq/samplesheet_13066.csv]; #edited to add 1000Genomes_hs37d5 + ensembl_75_transcriptome to lane 8

  my $rna_gen;
  lives_ok {
    $rna_gen = npg_pipeline::archive::file::generation::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2014},
      repository        => $dir,
      no_bsub           => 1,
    )
  } 'no error creating an object';
  is ($rna_gen->id_run, 13066, 'id_run inferred correctly');

  my $qc_in  = qq{$dir/140529_HS18_13066_A_C3C3KACXX/Data/Intensities/BAM_basecalls_20140606-133530/no_cal/archive};
  my $qc_out = join q[/], $qc_in, q[qc];
  my $args = {};
  $args->{8} = qq{bash -c ' mkdir -p $dir/140529_HS18_13066_A_C3C3KACXX/Data/Intensities/BAM_basecalls_20140606-133530/no_cal/archive/tmp_\$LSB_JOBID/13066_8 ; cd $dir/140529_HS18_13066_A_C3C3KACXX/Data/Intensities/BAM_basecalls_20140606-133530/no_cal/archive/tmp_\$LSB_JOBID/13066_8 && vtfp.pl -param_vals $dir/140529_HS18_13066_A_C3C3KACXX/Data/Intensities/BAM_basecalls_20140606-133530/no_cal/13066_8_p4s2_pv_in.json -export_param_vals 13066_8_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_13066_8.json && viv.pl -s -x -v 3 -o viv_13066_8.log run_13066_8.json } .
    qq{ && qc --check bam_flagstats --id_run 13066 --position 8 --qc_in $qc_in --qc_out $qc_out} .
    qq{ && qc --check bam_flagstats --id_run 13066 --position 8 --qc_in $qc_in --qc_out $qc_out --subset phix} .
     q{ && qc --check alignment_filter_metrics --id_run 13066 --position 8 --qc_in $PWD --qc_out } . $qc_out .
    qq{ && qc --check rna_seqc --id_run 13066 --position 8 --qc_in $qc_in --qc_out } . $qc_out . q{ '};

  lives_ok {$rna_gen->_generate_command_arguments([8])}
     'no error generating command arguments';
  cmp_deeply ($rna_gen->_job_args, $args,
    'correct command arguments for library RNASeq lane (unstranded Illumina cDNA library)');
  is ($rna_gen->_using_alt_reference, 0, 'Not using alternate reference');

  my $l = st::api::lims->new(id_run => 13066, position => 8);
  is ($rna_gen->_do_rna_analysis($l), 1, 'do RNA analysis on pair end RNA library with transcriptome index');

  ##HiSeq, run 17550, multiple organisms RNA libraries suitable for RNA analysis
  $runfolder = q{150910_HS40_17550_A_C75BCANXX};
  $runfolder_path = join q[/], $dir, $runfolder;
  $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20170629-170201/no_cal';
  $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20170629-170201/metadata_cache_17550';
  `mkdir -p $bc_path`;
  `mkdir -p $cache_dir`;
  copy("t/data/rna_seq/17550_RunInfo.xml","$runfolder_path/RunInfo.xml") or die "Copy failed: $!"; #to get information that it is paired end

  local $ENV{'NPG_CACHED_SAMPLESHEET_FILE'} = q[t/data/rna_seq/samplesheet_17550.csv];

  lives_ok {
      $rna_gen = npg_pipeline::archive::file::generation::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2017},
      repository        => $dir,
      no_bsub           => 1,)
  } 'no error creating an object';

  is ($rna_gen->id_run, 17550, 'id_run inferred correctly');
  
  my $tlogger = Test::Log::Log4perl->get_logger('npg_pipeline.archive.file.generation.seq_alignment');

  #test: library type is not RNA: ChIP-Seq Auto
  $l = st::api::lims->new(id_run => 17550, position => 8, tag_index => 1);
  Test::Log::Log4perl->start();
  is ($rna_gen->_do_rna_analysis($l), 0, 'not an RNA library, so no RNA analysis');
  $tlogger->debug(qr/not RNA library type/);
  Test::Log::Log4perl->end('logged: not rna library type');
  
  $l = st::api::lims->new(id_run => 17550, position => 4, tag_index => 1);
  lives_ok { $rna_gen->_generate_command_arguments([4]) } 'executes _generate_command_arguments method successfully for id_run 17550 lane 4 tag_index 1';
  lives_ok { $rna_gen->_job_args } 'executes _job_args method successfully';
  Test::Log::Log4perl->start();
  lives_ok { $rna_gen->_lsf_alignment_command($l, 1) } 'executes _lsf_alignment_command method successfully';
  my $re = '^Reference\ set\ for.*$';
  $tlogger->debug(qr/Do\ RNAseq\ analysis/);
# for my $i (1..6) { $tlogger->info(qr/$re/); }
  for my $i (1..5) { $tlogger->info(qr/$re/); }
  $tlogger->debug(qr/No\ bait\ set$/);
# $tlogger->info(qr/No\ bait\ set$/);
  $tlogger->info(qr/Analysis\:/);
  $tlogger->info(qr/Unsupported\ RNA\ analysis/);
  $tlogger->info(qr/$re/);
  $tlogger->info(qr/Using\ p4/);
  $tlogger->info(qr/do\_target\_alignment\ is\ true/);
  $tlogger->info(qr/spike\_tag\ is\ false/);
  $tlogger->info(qr/human\_split\ is/);
  $tlogger->info(qr/nchs\ is\ false/);
  $tlogger->info(qr/p4\ parameters\ written\ to/);
  $tlogger->info(qr/Using\ p4\ template\ alignment\_wtsi\_stage2\_template\.json/);  
  Test::Log::Log4perl->end('generated and logged: lsf command for unsupported rna analysis and used default aligner');

  #test: library type is RNA but no transcriptome version has been defined in reference: Homo_sapiens (GRCh38_15)
  $l = st::api::lims->new(id_run => 17550, position => 6, tag_index => 2);
  Test::Log::Log4perl->start();
  is ($rna_gen->_do_rna_analysis($l), 0, 'no transcriptome version in reference, so no RNA analysis');
  $tlogger->debug(qr/Reference without transcriptome/);
  Test::Log::Log4perl->end('logged: no transcriptome version in reference');

};

subtest 'test 3' => sub {
  plan tests => 5;
  ## single ended v. short , old flowcell, CRIPSR

  my $runfolder = q{151215_HS38_18472_A_H55HVADXX};
  my $runfolder_path = join q[/], q(t/data/example_runfolder), $runfolder;
  `cp -r $runfolder_path $dir`;
  $runfolder_path = join q[/], $dir, $runfolder;
  my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20151215-215034';
  my $cache_dir = join q[/], $bc_path, 'metadata_cache_18472';

  local $ENV{'NPG_WEBSERVICE_CACHE_DIR'}  = join q[/], $cache_dir;
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = join q[/], $cache_dir, q[samplesheet_18472.csv];

  my $se_gen;
  lives_ok {
    $se_gen = npg_pipeline::archive::file::generation::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => "$bc_path/no_cal",
      timestamp         => q{2015},
      repository        => $dir,
      no_bsub           => 1,
    )
  } 'no error creating an object';
  is ($se_gen->id_run, 18472, 'id_run inferred correctly');

  my $qc_in = qq[$bc_path/no_cal/archive/lane2];
  my $qc_out = join q[/], $qc_in, q[qc];
  my $args = qq{bash -c ' mkdir -p $bc_path/no_cal/archive/tmp_\$LSB_JOBID/18472_2#1 ; cd $bc_path/no_cal/archive/tmp_\$LSB_JOBID/18472_2#1 && vtfp.pl -param_vals $bc_path/no_cal/lane2/18472_2#1_p4s2_pv_in.json -export_param_vals 18472_2#1_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_18472_2#1.json && viv.pl -s -x -v 3 -o viv_18472_2#1.log run_18472_2#1.json } .
    qq{ && qc --check bam_flagstats --id_run 18472 --position 2 --qc_in $qc_in --qc_out $qc_out --tag_index 1} .
    qq{ && qc --check bam_flagstats --id_run 18472 --position 2 --qc_in $qc_in --qc_out $qc_out --subset phix --tag_index 1} .
     q{ && qc --check alignment_filter_metrics --id_run 18472 --position 2 --qc_in $PWD --qc_out } .$qc_out.q{ --tag_index 1}.    
     q{ '};

  lives_ok {$se_gen->_generate_command_arguments([2])}
     'no error generating command arguments';
  is($se_gen->_job_args->{'20001'}, $args,
    'correct command arguments for plex of short single read run');
  ok(!$se_gen->_using_alt_reference, 'Not using alternate reference');
};

subtest 'test 4' => sub {
  plan tests => 8;
  ##HiSeqX, run 16839_7

  my $runfolder = q{150709_HX4_16839_A_H7MHWCCXX};
  my $runfolder_path = join q[/], $dir, $runfolder;
  my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150712-121006/no_cal';
  `mkdir -p $bc_path`;
  my $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150712-121006/metadata_cache_16839';
  `mkdir -p $cache_dir`;
  copy("t/data/hiseqx/16839_RunInfo.xml","$runfolder_path/RunInfo.xml") or die "Copy failed: $!"; #to get information that it is paired end
  my $fasta_ref = "$ref_dir/Homo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/all/fasta/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa";

  local $ENV{'NPG_WEBSERVICE_CACHE_DIR'} = q[t/data/hiseqx];
  local $ENV{'NPG_CACHED_SAMPLESHEET_FILE'} = q[t/data/hiseqx/samplesheet_16839.csv];

  my $hsx_gen;
  lives_ok {
    $hsx_gen = npg_pipeline::archive::file::generation::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2015},
      repository        => $dir,
      no_bsub           => 1,
    )
  } 'no error creating an object';
  is ($hsx_gen->id_run, 16839, 'id_run inferred correctly');

  my $l = st::api::lims->new(id_run => 16839, position => 1, tag_index => 0);
  is ($hsx_gen->_ref($l, 'fasta'), $fasta_ref, 'reference for tag zero');
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
  my $other_ref;
  warnings_exist { $other_ref = $hsx_gen->_ref($l, 'fasta') }
    qr/Multiple references for st::api::lims object, driver - samplesheet/,
    'warning about multiple references';
  is ($other_ref, undef, 'multiple references in a lane - no reference for tag zero returned');

  # restore old samplesheet
  local $ENV{'NPG_CACHED_SAMPLESHEET_FILE'} = $old_ss;
  my $qc_in  = qq{$dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane7};
  my $qc_out = qq{$qc_in/qc};
  my $args = {};
  $args->{70007} = qq{bash -c ' mkdir -p $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/tmp_\$LSB_JOBID/16839_7#7 ; cd $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/tmp_\$LSB_JOBID/16839_7#7 && vtfp.pl -param_vals $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/lane7/16839_7#7_p4s2_pv_in.json -export_param_vals 16839_7#7_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16839_7#7.json && viv.pl -s -x -v 3 -o viv_16839_7#7.log run_16839_7#7.json } .
    qq{ && qc --check bam_flagstats --id_run 16839 --position 7 --qc_in $qc_in --qc_out $qc_out --tag_index 7} .
    qq{ && qc --check bam_flagstats --id_run 16839 --position 7 --qc_in $qc_in --qc_out $qc_out --subset phix --tag_index 7} .
     q{ && qc --check alignment_filter_metrics --id_run 16839 --position 7 --qc_in $PWD --qc_out } .$qc_out.q{ --tag_index 7}.
     q{ '};
  $args->{70015} = qq{bash -c ' mkdir -p $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/tmp_\$LSB_JOBID/16839_7#15 ; cd $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/tmp_\$LSB_JOBID/16839_7#15 && vtfp.pl -param_vals $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/lane7/16839_7#15_p4s2_pv_in.json -export_param_vals 16839_7#15_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16839_7#15.json && viv.pl -s -x -v 3 -o viv_16839_7#15.log run_16839_7#15.json } .
    qq{ && qc --check bam_flagstats --id_run 16839 --position 7 --qc_in $qc_in --qc_out $qc_out --tag_index 15} .
    qq{ && qc --check bam_flagstats --id_run 16839 --position 7 --qc_in $qc_in --qc_out $qc_out --subset phix --tag_index 15} .
     q{ && qc --check alignment_filter_metrics --id_run 16839 --position 7 --qc_in $PWD --qc_out } .$qc_out.q{ --tag_index 15}.
     q{ '};
  $args->{70000} = qq{bash -c ' mkdir -p $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/tmp_\$LSB_JOBID/16839_7#0 ; cd $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/tmp_\$LSB_JOBID/16839_7#0 && vtfp.pl -param_vals $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/lane7/16839_7#0_p4s2_pv_in.json -export_param_vals 16839_7#0_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16839_7#0.json && viv.pl -s -x -v 3 -o viv_16839_7#0.log run_16839_7#0.json } .
    qq{ && qc --check bam_flagstats --id_run 16839 --position 7 --qc_in $qc_in --qc_out $qc_out --tag_index 0} .
    qq{ && qc --check bam_flagstats --id_run 16839 --position 7 --qc_in $qc_in --qc_out $qc_out --subset phix --tag_index 0} .
     q{ && qc --check alignment_filter_metrics --id_run 16839 --position 7 --qc_in $PWD --qc_out } .$qc_out.q{ --tag_index 0}.
     q{ '};

  lives_ok {$hsx_gen->_generate_command_arguments([7])}
     'no error generating hsx command arguments';
  cmp_deeply ($hsx_gen->_job_args, $args,
    'correct command arguments for HiSeqX lane 16839_7');
  is ($hsx_gen->_using_alt_reference, 1, 'Using alternate reference');
};

subtest 'test 5' => sub {
  plan tests => 4;
  ##HiSeq, run 16807_6 (newer flowcell)

  my $runfolder = q{150707_HS38_16807_A_C7U2YANXX};
  my $runfolder_path = join q[/], $dir, $runfolder;
  my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150707-232614/no_cal';
  `mkdir -p $bc_path`;
  my $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150707-232614/metadata_cache_16807';
  `mkdir -p $cache_dir`;
  copy("t/data/hiseq/16807_RunInfo.xml","$runfolder_path/RunInfo.xml") or die "Copy failed: $!"; #to get information that it is paired end

  local $ENV{'NPG_WEBSERVICE_CACHE_DIR'} = q[t/data/hiseq];
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/hiseq/samplesheet_16807.csv];

  my $hs_gen;
  lives_ok {
    $hs_gen = npg_pipeline::archive::file::generation::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2015},
      repository        => $dir,
      no_bsub           => 1,
    )
  } 'no error creating an object';
  is ($hs_gen->id_run, 16807, 'id_run inferred correctly');

  my $qc_in  = qq{$bc_path/archive/lane6};
  my $qc_out = qq{$qc_in/qc};
  my $args = {};
  $args->{60001} = qq{bash -c ' mkdir -p $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/tmp_\$LSB_JOBID/16807_6#1 ; cd $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/tmp_\$LSB_JOBID/16807_6#1 && vtfp.pl -param_vals $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/lane6/16807_6#1_p4s2_pv_in.json -export_param_vals 16807_6#1_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16807_6#1.json && viv.pl -s -x -v 3 -o viv_16807_6#1.log run_16807_6#1.json } .
    qq{ && qc --check bam_flagstats --id_run 16807 --position 6 --qc_in $qc_in --qc_out $qc_out --tag_index 1} .
    qq{ && qc --check bam_flagstats --id_run 16807 --position 6 --qc_in $qc_in --qc_out $qc_out --subset phix --tag_index 1} .
     q{ && qc --check alignment_filter_metrics --id_run 16807 --position 6 --qc_in $PWD --qc_out } .$qc_out.q{ --tag_index 1}.
     q{ '};    
  $args->{60002} = qq{bash -c ' mkdir -p $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/tmp_\$LSB_JOBID/16807_6#2 ; cd $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/tmp_\$LSB_JOBID/16807_6#2 && vtfp.pl -param_vals $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/lane6/16807_6#2_p4s2_pv_in.json -export_param_vals 16807_6#2_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16807_6#2.json && viv.pl -s -x -v 3 -o viv_16807_6#2.log run_16807_6#2.json } .
    qq{ && qc --check bam_flagstats --id_run 16807 --position 6 --qc_in $qc_in --qc_out $qc_out --tag_index 2} .
    qq{ && qc --check bam_flagstats --id_run 16807 --position 6 --qc_in $qc_in --qc_out $qc_out --subset phix --tag_index 2} .
     q{ && qc --check alignment_filter_metrics --id_run 16807 --position 6 --qc_in $PWD --qc_out } .$qc_out.q{ --tag_index 2}.
     q{ '};  
  $args->{60000} = qq{bash -c ' mkdir -p $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/tmp_\$LSB_JOBID/16807_6#0 ; cd $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/tmp_\$LSB_JOBID/16807_6#0 && vtfp.pl -param_vals $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/lane6/16807_6#0_p4s2_pv_in.json -export_param_vals 16807_6#0_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16807_6#0.json && viv.pl -s -x -v 3 -o viv_16807_6#0.log run_16807_6#0.json } .
    qq{ && qc --check bam_flagstats --id_run 16807 --position 6 --qc_in $qc_in --qc_out $qc_out --tag_index 0} .
    qq{ && qc --check bam_flagstats --id_run 16807 --position 6 --qc_in $qc_in --qc_out $qc_out --subset phix --tag_index 0} .
     q{ && qc --check alignment_filter_metrics --id_run 16807 --position 6 --qc_in $PWD --qc_out } .$qc_out.q{ --tag_index 0}.
     q{ '};

  lives_ok {$hs_gen->_generate_command_arguments([6])}
     'no error generating hs command arguments';
  cmp_deeply ($hs_gen->_job_args, $args,
    'correct command arguments for HiSeq lane 16807_6');
};


subtest 'test 6' => sub {
  plan tests => 4;
  ##MiSeq, run 20268_1 (newer flowcell) - WITH bait added to samplesheet for lane 1 

  my $bait_dir = join q[/],$dir,'baits','Human_all_exon_V5','1000Genomes_hs37d5';
  `mkdir -p $bait_dir`;
  `touch $bait_dir/S04380110-PTR.interval_list`;
  `touch $bait_dir/S04380110-CTR.interval_list`;

  my $runfolder = q{160704_MS3_20268_A_MS4000667-300V2};
  my $runfolder_path = join q[/], $dir, $runfolder;
  my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20160712-154117/no_cal';
  `mkdir -p $bc_path`;
  my $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20160712-154117/metadata_cache_20268';
  `mkdir -p $cache_dir`;

  copy("t/data/hiseq/20268_RunInfo.xml","$runfolder_path/RunInfo.xml") or die "Copy failed: $!";

  # dummy reference should already exist
  # system(qq[ls -lR $ref_dir]);

  local $ENV{'NPG_WEBSERVICE_CACHE_DIR'} = q[t/data/hiseq];
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/hiseq/samplesheet_20268.csv];

  my $bait_gen;
  lives_ok {
    $bait_gen = npg_pipeline::archive::file::generation::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2016},
      repository        => $dir,
      no_bsub           => 1,
    )
  } 'no error creating an object';

  is ($bait_gen->id_run, 20268, 'id_run inferred correctly');

  my $qc_in  = qq{$bc_path/archive/lane1};
  my $qc_out = qq{$qc_in/qc};
  my $args = {};
  $args->{10001} = qq{bash -c ' mkdir -p $dir/160704_MS3_20268_A_MS4000667-300V2/Data/Intensities/BAM_basecalls_20160712-154117/no_cal/archive/tmp_\$LSB_JOBID/20268_1#1 ; cd $dir/160704_MS3_20268_A_MS4000667-300V2/Data/Intensities/BAM_basecalls_20160712-154117/no_cal/archive/tmp_\$LSB_JOBID/20268_1#1 && vtfp.pl -param_vals $dir/160704_MS3_20268_A_MS4000667-300V2/Data/Intensities/BAM_basecalls_20160712-154117/no_cal/lane1/20268_1#1_p4s2_pv_in.json -export_param_vals 20268_1#1_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_20268_1#1.json && viv.pl -s -x -v 3 -o viv_20268_1#1.log run_20268_1#1.json } .
    qq{ && qc --check bam_flagstats --id_run 20268 --position 1 --qc_in $qc_in --qc_out $qc_out --tag_index 1} .
    qq{ && qc --check bam_flagstats --id_run 20268 --position 1 --qc_in $qc_in --qc_out $qc_out --subset phix --tag_index 1} .
     q{ && qc --check alignment_filter_metrics --id_run 20268 --position 1 --qc_in $PWD --qc_out } .$qc_out.q{ --tag_index 1}.
     q{ '};
  $args->{10002} = qq{bash -c ' mkdir -p $dir/160704_MS3_20268_A_MS4000667-300V2/Data/Intensities/BAM_basecalls_20160712-154117/no_cal/archive/tmp_\$LSB_JOBID/20268_1#2 ; cd $dir/160704_MS3_20268_A_MS4000667-300V2/Data/Intensities/BAM_basecalls_20160712-154117/no_cal/archive/tmp_\$LSB_JOBID/20268_1#2 && vtfp.pl -param_vals $dir/160704_MS3_20268_A_MS4000667-300V2/Data/Intensities/BAM_basecalls_20160712-154117/no_cal/lane1/20268_1#2_p4s2_pv_in.json -export_param_vals 20268_1#2_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_20268_1#2.json && viv.pl -s -x -v 3 -o viv_20268_1#2.log run_20268_1#2.json } .
    qq{ && qc --check bam_flagstats --id_run 20268 --position 1 --qc_in $qc_in --qc_out $qc_out --tag_index 2} .
    qq{ && qc --check bam_flagstats --id_run 20268 --position 1 --qc_in $qc_in --qc_out $qc_out --subset phix --tag_index 2} .
     q{ && qc --check alignment_filter_metrics --id_run 20268 --position 1 --qc_in $PWD --qc_out } .$qc_out.q{ --tag_index 2}.
     q{ '};
  $args->{10000} = qq{bash -c ' mkdir -p $dir/160704_MS3_20268_A_MS4000667-300V2/Data/Intensities/BAM_basecalls_20160712-154117/no_cal/archive/tmp_\$LSB_JOBID/20268_1#0 ; cd $dir/160704_MS3_20268_A_MS4000667-300V2/Data/Intensities/BAM_basecalls_20160712-154117/no_cal/archive/tmp_\$LSB_JOBID/20268_1#0 && vtfp.pl -param_vals $dir/160704_MS3_20268_A_MS4000667-300V2/Data/Intensities/BAM_basecalls_20160712-154117/no_cal/lane1/20268_1#0_p4s2_pv_in.json -export_param_vals 20268_1#0_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_20268_1#0.json && viv.pl -s -x -v 3 -o viv_20268_1#0.log run_20268_1#0.json } .
    qq{ && qc --check bam_flagstats --id_run 20268 --position 1 --qc_in $qc_in --qc_out $qc_out --tag_index 0} .
    qq{ && qc --check bam_flagstats --id_run 20268 --position 1 --qc_in $qc_in --qc_out $qc_out --subset phix --tag_index 0} .
     q{ && qc --check alignment_filter_metrics --id_run 20268 --position 1 --qc_in $PWD --qc_out } .$qc_out.q{ --tag_index 0}.
     q{ '};

  lives_ok {$bait_gen->_generate_command_arguments([1])}
     'no error generating bait command arguments';

  my @a1 = (split / /, $args->{10002});
  my @a2 = (split / /, $bait_gen->_job_args->{10002});
  for my $i (0..$#a1) {
	if($a1[$i] ne $a2[$i]) {
		print "DIFF AT ELEM $i:\n$a1[$i]\n$a2[$i]\n";
	}
  }

  cmp_deeply ($bait_gen->_job_args, $args,
      'correct command arguments for HiSeq lane 20268_1');
};


subtest 'test 7' => sub {
  plan tests => 4;
  ##MiSeq, run 16850_1 (cycle count over threshold (currently >= 101))

  my $runfolder = q{150710_MS2_16850_A_MS3014507-500V2};
  my $runfolder_path = join q[/], $dir, $runfolder;
  my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150712-022206/no_cal';
  `mkdir -p $bc_path`;
  my $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150712-022206/metadata_cache_16850';
  `mkdir -p $cache_dir`;
  copy("t/data/miseq/16850_RunInfo.xml","$runfolder_path/RunInfo.xml") or die "Copy failed: $!"; #to get information that it is paired end

  local $ENV{'NPG_WEBSERVICE_CACHE_DIR'} = q[t/data/miseq];
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/miseq/samplesheet_16850.csv];

  my $ms_gen;
  lives_ok {
    $ms_gen = npg_pipeline::archive::file::generation::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2015},
      repository        => $dir,
      no_bsub           => 1,
    )
  } 'no error creating an object';
  is ($ms_gen->id_run, 16850, 'id_run inferred correctly');

  my $qc_in  = qq{$bc_path/archive/lane1};
  my $qc_out = qq{$qc_in/qc};
  my $args = {};
  $args->{10001} = qq{bash -c ' mkdir -p $bc_path/archive/tmp_\$LSB_JOBID/16850_1#1 ; cd $bc_path/archive/tmp_\$LSB_JOBID/16850_1#1 && vtfp.pl -param_vals $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/lane1/16850_1#1_p4s2_pv_in.json -export_param_vals 16850_1#1_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16850_1#1.json && viv.pl -s -x -v 3 -o viv_16850_1#1.log run_16850_1#1.json } .
    qq{ && qc --check bam_flagstats --id_run 16850 --position 1 --qc_in $qc_in --qc_out $qc_out --tag_index 1} .
    qq{ && qc --check bam_flagstats --id_run 16850 --position 1 --qc_in $qc_in --qc_out $qc_out --subset phix --tag_index 1} .
     q{ && qc --check alignment_filter_metrics --id_run 16850 --position 1 --qc_in $PWD --qc_out } .$qc_out.q{ --tag_index 1}.
     q{ '};
  $args->{10002} = qq{bash -c ' mkdir -p $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/tmp_\$LSB_JOBID/16850_1#2 ; cd $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/tmp_\$LSB_JOBID/16850_1#2 && vtfp.pl -param_vals $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/lane1/16850_1#2_p4s2_pv_in.json -export_param_vals 16850_1#2_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16850_1#2.json && viv.pl -s -x -v 3 -o viv_16850_1#2.log run_16850_1#2.json } .
    qq{ && qc --check bam_flagstats --id_run 16850 --position 1 --qc_in $qc_in --qc_out $qc_out --tag_index 2} .
    qq{ && qc --check bam_flagstats --id_run 16850 --position 1 --qc_in $qc_in --qc_out $qc_out --subset phix --tag_index 2} .
     q{ && qc --check alignment_filter_metrics --id_run 16850 --position 1 --qc_in $PWD --qc_out } .$qc_out.q{ --tag_index 2}.
     q{ '};
  $args->{10000} = qq{bash -c ' mkdir -p $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/tmp_\$LSB_JOBID/16850_1#0 ; cd $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/tmp_\$LSB_JOBID/16850_1#0 && vtfp.pl -param_vals $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/lane1/16850_1#0_p4s2_pv_in.json -export_param_vals 16850_1#0_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16850_1#0.json && viv.pl -s -x -v 3 -o viv_16850_1#0.log run_16850_1#0.json } .
    qq{ && qc --check bam_flagstats --id_run 16850 --position 1 --qc_in $qc_in --qc_out $qc_out --tag_index 0} .
    qq{ && qc --check bam_flagstats --id_run 16850 --position 1 --qc_in $qc_in --qc_out $qc_out --subset phix --tag_index 0} .
     q{ && qc --check alignment_filter_metrics --id_run 16850 --position 1 --qc_in $PWD --qc_out } .$qc_out.q{ --tag_index 0}.
     q{ '};

  lives_ok {$ms_gen->_generate_command_arguments([1])}
     'no error generating ms command arguments';
  cmp_deeply ($ms_gen->_job_args, $args,
    'correct command arguments for MiSeq lane 16850_1');
};

subtest 'test 8' => sub {
  plan tests => 4;
  ##MiSeq, run 16756_1 (nonconsented human split, no target alignment)

  my $runfolder = q{150701_HS36_16756_B_C711RANXX};
  my $runfolder_path = join q[/], $dir, $runfolder;
  my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150707-132329/no_cal';
  `mkdir -p $bc_path`;
  my $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150707-132329/metadata_cache_16756';
  `mkdir -p $cache_dir`;
  copy("t/data/hiseq/16756_RunInfo.xml","$runfolder_path/RunInfo.xml") or die "Copy failed: $!"; #to get information that it is paired end

  # default human reference needed for alignment for unconsented human split
  local $ENV{'NPG_WEBSERVICE_CACHE_DIR'} = q[t/data/hiseq];
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/hiseq/samplesheet_16756.csv];

  my $hs_gen;
  lives_ok {
    $hs_gen = npg_pipeline::archive::file::generation::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2015},
      repository        => $dir,
      no_bsub           => 1,
    )
  } 'no error creating an object';
  is ($hs_gen->id_run, 16756, 'id_run inferred correctly');

  my $qc_in  = qq{$bc_path/archive/lane1};
  my $qc_out = qq{$qc_in/qc};
  my $args = {};
  $args->{10001} = qq{bash -c ' mkdir -p $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/tmp_\$LSB_JOBID/16756_1#1 ; cd $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/tmp_\$LSB_JOBID/16756_1#1 && vtfp.pl -param_vals $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/lane1/16756_1#1_p4s2_pv_in.json -export_param_vals 16756_1#1_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_humansplit_notargetalign_template.json > run_16756_1#1.json && viv.pl -s -x -v 3 -o viv_16756_1#1.log run_16756_1#1.json } .
    qq{ && qc --check bam_flagstats --id_run 16756 --position 1 --qc_in $qc_in --qc_out $qc_out --tag_index 1 --skip_markdups_metrics} .
    qq{ && qc --check bam_flagstats --id_run 16756 --position 1 --qc_in $qc_in --qc_out $qc_out --subset phix --tag_index 1} .
     q{ && qc --check alignment_filter_metrics --id_run 16756 --position 1 --qc_in $PWD --qc_out } .$qc_out.q{ --tag_index 1}.
    qq{ && qc --check bam_flagstats --id_run 16756 --position 1 --qc_in $qc_in --qc_out $qc_out --subset human --tag_index 1} .
     q{ '};
  $args->{10002} = qq{bash -c ' mkdir -p $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/tmp_\$LSB_JOBID/16756_1#2 ; cd $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/tmp_\$LSB_JOBID/16756_1#2 && vtfp.pl -param_vals $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/lane1/16756_1#2_p4s2_pv_in.json -export_param_vals 16756_1#2_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_humansplit_notargetalign_template.json > run_16756_1#2.json && viv.pl -s -x -v 3 -o viv_16756_1#2.log run_16756_1#2.json } .
    qq{ && qc --check bam_flagstats --id_run 16756 --position 1 --qc_in $qc_in --qc_out $qc_out --tag_index 2 --skip_markdups_metrics} .
    qq{ && qc --check bam_flagstats --id_run 16756 --position 1 --qc_in $qc_in --qc_out $qc_out --subset phix --tag_index 2} .
     q{ && qc --check alignment_filter_metrics --id_run 16756 --position 1 --qc_in $PWD --qc_out } .$qc_out.q{ --tag_index 2}.
    qq{ && qc --check bam_flagstats --id_run 16756 --position 1 --qc_in $qc_in --qc_out $qc_out --subset human --tag_index 2} .
     q{ '};
  $args->{10000} = qq{bash -c ' mkdir -p $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/tmp_\$LSB_JOBID/16756_1#0 ; cd $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/tmp_\$LSB_JOBID/16756_1#0 && vtfp.pl -param_vals $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/lane1/16756_1#0_p4s2_pv_in.json -export_param_vals 16756_1#0_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_humansplit_notargetalign_template.json > run_16756_1#0.json && viv.pl -s -x -v 3 -o viv_16756_1#0.log run_16756_1#0.json } .
    qq{ && qc --check bam_flagstats --id_run 16756 --position 1 --qc_in $qc_in --qc_out $qc_out --tag_index 0 --skip_markdups_metrics} .
    qq{ && qc --check bam_flagstats --id_run 16756 --position 1 --qc_in $qc_in --qc_out $qc_out --subset phix --tag_index 0} .
     q{ && qc --check alignment_filter_metrics --id_run 16756 --position 1 --qc_in $PWD --qc_out } .$qc_out.q{ --tag_index 0}.
    qq{ && qc --check bam_flagstats --id_run 16756 --position 1 --qc_in $qc_in --qc_out $qc_out --subset human --tag_index 0} .    
     q{ '};

  lives_ok {$hs_gen->_generate_command_arguments([1])}
     'no error generating hs command arguments';
  cmp_deeply ($hs_gen->_job_args, $args,
    'correct command arguments for HiSeq lane 16756_1');
};

subtest 'test 9' => sub {
  plan tests => 4;
  ##MiSeq, run 16866_1 (nonconsented human split, target alignment)

  my $runfolder = q{150713_MS8_16866_A_MS3734403-300V2};
  my $runfolder_path = join q[/], $dir, $runfolder;
  my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150714-133929/no_cal';
  `mkdir -p $bc_path`;
  my $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150714-133929/metadata_cache_16866';
  `mkdir -p $cache_dir`;

  copy("t/data/miseq/16866_RunInfo.xml","$runfolder_path/RunInfo.xml") or die "Copy failed: $!"; #to get information that it is paired end
  local $ENV{'NPG_WEBSERVICE_CACHE_DIR'} = q[t/data/miseq];
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/miseq/samplesheet_16866.csv];

  my $ms_gen;
  lives_ok {
    $ms_gen = npg_pipeline::archive::file::generation::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2015},
      repository        => $dir,
      no_bsub           => 1,
    )
  } 'no error creating an object';
  is ($ms_gen->id_run, 16866, 'id_run inferred correctly');

  my $qc_in  = qq{$bc_path/archive/lane1};
  my $qc_out = qq{$qc_in/qc};
  my $args = {};

  $args->{10001} = qq{bash -c ' mkdir -p $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/tmp_\$LSB_JOBID/16866_1#1 ; cd $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/tmp_\$LSB_JOBID/16866_1#1 && vtfp.pl -param_vals $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/lane1/16866_1#1_p4s2_pv_in.json -export_param_vals 16866_1#1_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_humansplit_template.json > run_16866_1#1.json && viv.pl -s -x -v 3 -o viv_16866_1#1.log run_16866_1#1.json } .
    qq{ && qc --check bam_flagstats --id_run 16866 --position 1 --qc_in $qc_in --qc_out $qc_out --tag_index 1} .
    qq{ && qc --check bam_flagstats --id_run 16866 --position 1 --qc_in $qc_in --qc_out $qc_out --subset phix --tag_index 1} .
     q{ && qc --check alignment_filter_metrics --id_run 16866 --position 1 --qc_in $PWD --qc_out } .$qc_out.q{ --tag_index 1}.
    qq{ && qc --check bam_flagstats --id_run 16866 --position 1 --qc_in $qc_in --qc_out $qc_out --subset human --tag_index 1} .
     q{ '};  
  $args->{10002} = qq{bash -c ' mkdir -p $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/tmp_\$LSB_JOBID/16866_1#2 ; cd $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/tmp_\$LSB_JOBID/16866_1#2 && vtfp.pl -param_vals $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/lane1/16866_1#2_p4s2_pv_in.json -export_param_vals 16866_1#2_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_humansplit_template.json > run_16866_1#2.json && viv.pl -s -x -v 3 -o viv_16866_1#2.log run_16866_1#2.json } .
    qq{ && qc --check bam_flagstats --id_run 16866 --position 1 --qc_in $qc_in --qc_out $qc_out --tag_index 2} .
    qq{ && qc --check bam_flagstats --id_run 16866 --position 1 --qc_in $qc_in --qc_out $qc_out --subset phix --tag_index 2} .
     q{ && qc --check alignment_filter_metrics --id_run 16866 --position 1 --qc_in $PWD --qc_out } .$qc_out.q{ --tag_index 2}.
    qq{ && qc --check bam_flagstats --id_run 16866 --position 1 --qc_in $qc_in --qc_out $qc_out --subset human --tag_index 2} .
     q{ '};
  $args->{10000} = qq{bash -c ' mkdir -p $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/tmp_\$LSB_JOBID/16866_1#0 ; cd $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/tmp_\$LSB_JOBID/16866_1#0 && vtfp.pl -param_vals $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/lane1/16866_1#0_p4s2_pv_in.json -export_param_vals 16866_1#0_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_humansplit_template.json > run_16866_1#0.json && viv.pl -s -x -v 3 -o viv_16866_1#0.log run_16866_1#0.json } .
    qq{ && qc --check bam_flagstats --id_run 16866 --position 1 --qc_in $qc_in --qc_out $qc_out --tag_index 0} .
    qq{ && qc --check bam_flagstats --id_run 16866 --position 1 --qc_in $qc_in --qc_out $qc_out --subset phix --tag_index 0} .
     q{ && qc --check alignment_filter_metrics --id_run 16866 --position 1 --qc_in $PWD --qc_out } .$qc_out.q{ --tag_index 0}.
    qq{ && qc --check bam_flagstats --id_run 16866 --position 1 --qc_in $qc_in --qc_out $qc_out --subset human --tag_index 0} .
     q{ '};

  lives_ok {$ms_gen->_generate_command_arguments([1])}
     'no error generating ms command arguments';
  cmp_deeply ($ms_gen->_job_args, $args,
    'correct command arguments for HiSeq lane 16866_1');
};

subtest 'test 10' => sub {
  plan tests => 4;
  ##MiSeq, run 20990_1 (no target alignment, no human split)

  my $runfolder = q{161010_MS5_20990_A_MS4548606-300V2};
  my $runfolder_path = join q[/], $dir, $runfolder;
  my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20161011-102905/no_cal';
  `mkdir -p $bc_path`;
  my $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20161011-102905/metadata_cache_20990';
  `mkdir $cache_dir`;

  copy("t/data/miseq/20990_RunInfo.xml","$runfolder_path/RunInfo.xml") or die "Copy failed: $!"; #to get information that it is paired end
  local $ENV{'NPG_WEBSERVICE_CACHE_DIR'} = q[t/data/miseq];
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/miseq/samplesheet_20990.csv];

  my $ms_gen;
  lives_ok {
    $ms_gen = npg_pipeline::archive::file::generation::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2016},
      repository        => $dir,
      no_bsub           => 1,
    )
  } 'no error creating an object';
  is ($ms_gen->id_run, 20990, 'id_run (20990) inferred correctly');

  my $qc_in  = qq{$bc_path/archive/lane1};
  my $qc_out = qq{$qc_in/qc};
  my $args = {};

  $args->{10001} = qq{bash -c ' mkdir -p $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/archive/tmp_\$LSB_JOBID/20990_1#1 ; cd $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/archive/tmp_\$LSB_JOBID/20990_1#1 && vtfp.pl -param_vals $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/lane1/20990_1#1_p4s2_pv_in.json -export_param_vals 20990_1#1_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_20990_1#1.json && viv.pl -s -x -v 3 -o viv_20990_1#1.log run_20990_1#1.json  && qc --check bam_flagstats --id_run 20990 --position 1 --qc_in $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/archive/lane1 --qc_out $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/archive/lane1/qc --tag_index 1 --skip_markdups_metrics && qc --check bam_flagstats --id_run 20990 --position 1 --qc_in $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/archive/lane1 --qc_out $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/archive/lane1/qc --subset phix --tag_index 1 && qc --check alignment_filter_metrics --id_run 20990 --position 1 --qc_in \$PWD --qc_out $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/archive/lane1/qc --tag_index 1 '};
  $args->{10000} = qq{bash -c ' mkdir -p $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/archive/tmp_\$LSB_JOBID/20990_1#0 ; cd $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/archive/tmp_\$LSB_JOBID/20990_1#0 && vtfp.pl -param_vals $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/lane1/20990_1#0_p4s2_pv_in.json -export_param_vals 20990_1#0_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_20990_1#0.json && viv.pl -s -x -v 3 -o viv_20990_1#0.log run_20990_1#0.json  && qc --check bam_flagstats --id_run 20990 --position 1 --qc_in $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/archive/lane1 --qc_out $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/archive/lane1/qc --tag_index 0 --skip_markdups_metrics && qc --check bam_flagstats --id_run 20990 --position 1 --qc_in $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/archive/lane1 --qc_out $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/archive/lane1/qc --subset phix --tag_index 0 && qc --check alignment_filter_metrics --id_run 20990 --position 1 --qc_in \$PWD --qc_out $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/archive/lane1/qc --tag_index 0 '};
  $args->{10002} = qq{bash -c ' mkdir -p $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/archive/tmp_\$LSB_JOBID/20990_1#2 ; cd $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/archive/tmp_\$LSB_JOBID/20990_1#2 && vtfp.pl -param_vals $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/lane1/20990_1#2_p4s2_pv_in.json -export_param_vals 20990_1#2_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_20990_1#2.json && viv.pl -s -x -v 3 -o viv_20990_1#2.log run_20990_1#2.json  && qc --check bam_flagstats --id_run 20990 --position 1 --qc_in $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/archive/lane1 --qc_out $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/archive/lane1/qc --tag_index 2 --skip_markdups_metrics && qc --check bam_flagstats --id_run 20990 --position 1 --qc_in $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/archive/lane1 --qc_out $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/archive/lane1/qc --subset phix --tag_index 2 && qc --check alignment_filter_metrics --id_run 20990 --position 1 --qc_in \$PWD --qc_out $dir/161010_MS5_20990_A_MS4548606-300V2/Data/Intensities/BAM_basecalls_20161011-102905/no_cal/archive/lane1/qc --tag_index 2 '};

  lives_ok {$ms_gen->_generate_command_arguments([1])}
     'no error generating ms command arguments';

  cmp_deeply ($ms_gen->_job_args, $args,
    'correct command arguments for MiSeq lane 20990_1');
};

subtest 'test 11' => sub {
  plan tests => 4;
  ##HiSeqX, run 16839_1

  my $ref_dir = join q[/],$dir,'references','Homo_sapiens','GRCh38_full_analysis_set_plus_decoy_hla','all';
  `mkdir -p $ref_dir/fasta`;
  `mkdir -p $ref_dir/bwa0_6`;
  `mkdir -p $ref_dir/picard`;

  my $runfolder = q{150709_HX4_16839_A_H7MHWCCXX};
  my $runfolder_path = join q[/], $dir, $runfolder;
  my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150712-121006/no_cal';
  `mkdir -p $bc_path`;
  my $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150712-121006/metadata_cache_16839';
  `mkdir -p $cache_dir`;
  copy("t/data/hiseqx/16839_RunInfo.xml","$runfolder_path/RunInfo.xml") or die "Copy failed: $!"; #to get information that it is paired end

  local $ENV{'NPG_WEBSERVICE_CACHE_DIR'} = q[t/data/hiseqx];
  local $ENV{'NPG_CACHED_SAMPLESHEET_FILE'} = q[t/data/hiseqx/samplesheet_16839.csv];

  my $chromium_gen;
  lives_ok {
    $chromium_gen = npg_pipeline::archive::file::generation::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2015},
      repository        => $dir,
      no_bsub           => 1,
    )
  } 'no error creating an object';
  is ($chromium_gen->id_run, 16839, 'id_run inferred correctly');

  my $old_ss = $ENV{'NPG_CACHED_SAMPLESHEET_FILE'};
  my $ss = slurp $old_ss;
  $ss =~ s/Standard/Chromium single cell/;
  my $new_ss = "$dir/chromium_samplesheet_16839.csv";
  open my $fhss, '>', $new_ss or die "Cannot open $new_ss for writing";
  print $fhss $ss or die "Cannot write to $new_ss";
  close $fhss or warn "Failed to close $new_ss";
  # new samplesheet has one chromium sample in lane 1
  local $ENV{'NPG_CACHED_SAMPLESHEET_FILE'} = $new_ss;

  my $qc_in  = qq{$dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane1};
  my $qc_out = qq{$qc_in/qc};
  my $args = {};
  $args->{10001} = qq{bash -c ' mkdir -p $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/tmp_\$LSB_JOBID/16839_1#1 ; cd $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/tmp_\$LSB_JOBID/16839_1#1 && vtfp.pl -param_vals $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/lane1/16839_1#1_p4s2_pv_in.json -export_param_vals 16839_1#1_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16839_1#1.json && viv.pl -s -x -v 3 -o viv_16839_1#1.log run_16839_1#1.json } .
    qq{ && qc --check bam_flagstats --id_run 16839 --position 1 --qc_in $qc_in --qc_out $qc_out --tag_index 1 --skip_markdups_metrics} .
    qq{ && qc --check bam_flagstats --id_run 16839 --position 1 --qc_in $qc_in --qc_out $qc_out --subset phix --tag_index 1} .
     q{ && qc --check alignment_filter_metrics --id_run 16839 --position 1 --qc_in $PWD --qc_out } .$qc_out.q{ --tag_index 1}.
     q{ '};
  $args->{10009} = qq{bash -c ' mkdir -p $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/tmp_\$LSB_JOBID/16839_1#9 ; cd $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/tmp_\$LSB_JOBID/16839_1#9 && vtfp.pl -param_vals $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/lane1/16839_1#9_p4s2_pv_in.json -export_param_vals 16839_1#9_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16839_1#9.json && viv.pl -s -x -v 3 -o viv_16839_1#9.log run_16839_1#9.json } .
    qq{ && qc --check bam_flagstats --id_run 16839 --position 1 --qc_in $qc_in --qc_out $qc_out --tag_index 9} .
    qq{ && qc --check bam_flagstats --id_run 16839 --position 1 --qc_in $qc_in --qc_out $qc_out --subset phix --tag_index 9} .
     q{ && qc --check alignment_filter_metrics --id_run 16839 --position 1 --qc_in $PWD --qc_out } .$qc_out.q{ --tag_index 9}.
     q{ '};
  $args->{10000} = qq{bash -c ' mkdir -p $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/tmp_\$LSB_JOBID/16839_1#0 ; cd $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/tmp_\$LSB_JOBID/16839_1#0 && vtfp.pl -param_vals $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/lane1/16839_1#0_p4s2_pv_in.json -export_param_vals 16839_1#0_p4s2_pv_out_\${LSB_JOBID}.json -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16839_1#0.json && viv.pl -s -x -v 3 -o viv_16839_1#0.log run_16839_1#0.json } .
    qq{ && qc --check bam_flagstats --id_run 16839 --position 1 --qc_in $qc_in --qc_out $qc_out --tag_index 0} .
    qq{ && qc --check bam_flagstats --id_run 16839 --position 1 --qc_in $qc_in --qc_out $qc_out --subset phix --tag_index 0} .
     q{ && qc --check alignment_filter_metrics --id_run 16839 --position 1 --qc_in $PWD --qc_out } .$qc_out.q{ --tag_index 0}.
     q{ '};

  lives_ok {$chromium_gen->_generate_command_arguments([1])}
     'no error generating command arguments for a lane with one chromium sample';
  cmp_deeply ($chromium_gen->_job_args, $args,
    'correct command arguments for lane 16839_1 with one chromium sample');
};

1;
