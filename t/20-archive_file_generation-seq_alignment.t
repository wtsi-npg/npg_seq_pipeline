use strict;
use warnings;
use Test::More tests => 20;
use Test::Exception;
use Test::Deep;
use File::Temp qw/tempdir/;
use Cwd qw/cwd abs_path/;
use  Perl6::Slurp;
use File::Copy;

use_ok('npg_pipeline::archive::file::generation::seq_alignment');
local $ENV{'NPG_WEBSERVICE_CACHE_DIR'} = q[t/data/rna_seq];
local $ENV{'TEST_FS_RESOURCE'} = 'nfs-sf3';
local $ENV{PATH} = join q[:], q[t/bin], q[t/bin/software/solexa/bin], $ENV{PATH};
local $ENV{CLASSPATH} = q[t/bin/software/solexa/bin/aligners/illumina2bam/current];

my $odir = abs_path cwd;
my $dir = tempdir( CLEANUP => 1);

###12597_1    study: genomic sequencing, library type: No PCR
###12597_8#7  npg/run/12597.xml st/studies/2775.xml  batches/26550.xml samples/1886325.xml  <- Epigenetics, library type: qPCR only
###12597_4#3  npg/run/12597.xml st/studies/2893.xml  batches/26550.xml samples/1886357.xml  <- transcriptomics, library type: RNA-seq dUTP
###   1886357.xml edited to change reference to Mus_musculus (NCBIm37 + ensembl_67_transcriptome)
### batches/26550.xml edited to have the following plex composition: # plex TTAGGC 3 lane 4 and plex CAGATC 7 lane 8 

my $phix_ref_dir = "$dir/references/PhiX/Illumina/all/fasta";
`mkdir -p $phix_ref_dir`;
`ln -s Illumina $dir/references/PhiX/default`;
my $phix_ref = "$phix_ref_dir/phix-illumina.fa";
`touch $phix_ref`;

{
my $ref_dir = join q[/],$dir,'references','Mus_musculus','NCBIm37','all';
`mkdir -p $ref_dir/fasta`;
`mkdir $ref_dir/bowtie2`;
`mkdir $ref_dir/picard`;
`touch $ref_dir/fasta/mm_ref_NCBI37_1.fasta`;
`touch $ref_dir/bowtie2/mm_ref_NCBI37_1.fasta`;
`touch $ref_dir/picard/mm_ref_NCBI37_1.fasta`;

my $ref = qq[$ref_dir/bowtie2/mm_ref_NCBI37_1.fasta] ; 
my $runfolder = q{140409_HS34_12597_A_C333TACXX};
my $runfolder_path = join q[/], $dir, $runfolder;
my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20140515-073611/no_cal';
`mkdir -p $bc_path`;
 
#RunInfo.xml 
copy("t/data/rna_seq/12597_RunInfo.xml","$runfolder_path/RunInfo.xml") or die "Copy failed: $!"; #to get information that it is paired end


my $transcriptome_dir = join q[/],$dir,'transcriptomes','Mus_musculus','ensembl_67_transcriptome','NCBIm37';
`mkdir -p $transcriptome_dir/gtf`;
`mkdir -p $transcriptome_dir/tophat2`;
`touch $transcriptome_dir/gtf/ensembl_67_transcriptome-NCBIm37.gtf`;
`touch $transcriptome_dir/tophat2/NCBIm37.known.1.bt2`;

my $rna_gen;
  lives_ok {
    $rna_gen = npg_pipeline::archive::file::generation::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2014},
      verbose           => 1,
      repository        => $dir,
      no_bsub           => 1,
      ###uncomment to check V4 failure, flowcell_id=>'C333TANXX',
    )
  } 'no error creating an object';

  is ($rna_gen->id_run, 12597, 'id_run inferred correctly');
  ok ((not $rna_gen->_is_v4_run), 'not V4') or diag $rna_gen->flowcell_id;

  my $args = {};
  $args->{'4003'} = qq{bash -c '\ mkdir -p $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/tmp_\$LSB_JOBID/12597_4#3 ; cd $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/tmp_\$LSB_JOBID/12597_4#3 && vtfp.pl -s -keys samtools_executable -vals samtools1_1 -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `echo \$LSB_MCPU_HOSTS | cut -d " " -f2` -keys indatadir -vals $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/lane4 -keys outdatadir -vals $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4 -keys af_metrics -vals 12597_4#3.bam_alignment_filter_metrics.json -keys rpt -vals 12597_4#3 -keys reference_dict -vals $dir/references/Mus_musculus/NCBIm37/all/picard/mm_ref_NCBI37_1.fasta.dict -keys reference_genome_fasta -vals $dir/references/Mus_musculus/NCBIm37/all/fasta/mm_ref_NCBI37_1.fasta -keys phix_reference_genome_fasta -vals $phix_ref -keys alignment_filter_jar -vals $odir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/AlignmentFilter.jar -keys alignment_reference_genome -vals $dir/references/Mus_musculus/NCBIm37/all/bowtie2/mm_ref_NCBI37_1.fasta -keys library_type -vals fr-firststrand -keys transcriptome_val -vals $dir/transcriptomes/Mus_musculus/ensembl_67_transcriptome/NCBIm37/tophat2/NCBIm37.known -keys alignment_method -vals tophat2 \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_12597_4#3.json && viv.pl -s -x -v 3 -o viv_12597_4#3.log run_12597_4#3.json  } .
    q{&& perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>$ARGV[2], position=>$ARGV[3], tag_index=>$ARGV[4]); $o->parsing_metrics_file($ARGV[0]); open my$fh,q(<),$ARGV[1]; $o->parsing_flagstats($fh); close$fh; $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/12597_4#3.markdups_metrics.txt $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/12597_4#3.flagstat 12597 4 3 $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/qc && } .
    q{perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(human_split=>q(phix), id_run=>$ARGV[2], position=>$ARGV[3], tag_index=>$ARGV[4]); $o->parsing_metrics_file($ARGV[0]); open my$fh,q(<),$ARGV[1]; $o->parsing_flagstats($fh); close$fh; $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/12597_4#3_phix.markdups_metrics.txt $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/12597_4#3_phix.flagstat 12597 4 3 $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/qc } .
    q{&& qc --check alignment_filter_metrics --qc_in $PWD --id_run 12597 --position 4 } .
    qq{--qc_out $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/qc --tag_index 3 }.
    q{'};

   $args->{'4000'} = qq{bam_alignment.pl --id_run 12597 --position 4 --tag_index 0 --input $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/lane4/12597_4#0.bam --output_prefix $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/12597_4#0 --do_markduplicates  --is_paired_read};
     
  
 lives_ok {$rna_gen->_generate_command_arguments([4])}
     'no error generating command arguments';
  

  cmp_deeply ($rna_gen->_job_args, $args,
    'correct command arguments for pooled RNASeq lane');


  is ($rna_gen->_job_args->{'4000'},$args->{'4000'},'correct tag 0 args generated');


  my $required_job_completion = 55;
  my $mem = 32000;
  my $expected = qq{bsub -q srpipeline -E 'npg_pipeline_preexec_references --repository $dir' -R 'select[mem>$mem] rusage[mem=$mem,nfs-sf3=4]' -M$mem -R 'span[hosts=1]'}.qq{ -n12,16 $required_job_completion -J 'seq_alignment_12597_2014[4000,4003]' -o $bc_path/archive/log/seq_alignment_12597_2014.%I.%J.out }.q('perl -Mstrict -MJSON -MFile::Slurp -e '"'"'exec from_json(read_file shift@ARGV)->{shift@ARGV} or die q(failed exec)'"'"' ) . $bc_path . q{/seq_alignment_12597_2014_$LSB_JOBID $LSB_JOBINDEX'}; 

  is($rna_gen->_command2submit($required_job_completion), $expected, 'command to submit is correct');


my $fname;
lives_ok{$fname = $rna_gen->_save_arguments(55)} 'writing args to a file without error';
is ($fname, $bc_path. q{/seq_alignment_12597_2014_55}, 'file name correct');
ok (-e $fname, 'file exists');

my @lines = slurp($fname);

my $json = qq({"4003":"bash -c ' mkdir -p $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/tmp_\$LSB_JOBID/12597_4#3 ; cd $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/tmp_\$LSB_JOBID/12597_4#3 && vtfp.pl -s -keys samtools_executable -vals samtools1_1 -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `echo \$LSB_MCPU_HOSTS | cut -d \\" \\" -f2` -keys indatadir -vals $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/lane4 -keys outdatadir -vals $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4 -keys af_metrics -vals 12597_4#3.bam_alignment_filter_metrics.json -keys rpt -vals 12597_4#3 -keys reference_dict -vals $dir/references/Mus_musculus/NCBIm37/all/picard/mm_ref_NCBI37_1.fasta.dict -keys reference_genome_fasta -vals $dir/references/Mus_musculus/NCBIm37/all/fasta/mm_ref_NCBI37_1.fasta -keys phix_reference_genome_fasta -vals $phix_ref -keys alignment_filter_jar -vals $odir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/AlignmentFilter.jar -keys alignment_reference_genome -vals $dir/references/Mus_musculus/NCBIm37/all/bowtie2/mm_ref_NCBI37_1.fasta -keys library_type -vals fr-firststrand -keys transcriptome_val -vals $dir/transcriptomes/Mus_musculus/ensembl_67_transcriptome/NCBIm37/tophat2/NCBIm37.known -keys alignment_method -vals tophat2 \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_12597_4#3.json && viv.pl -s -x -v 3 -o viv_12597_4#3.log run_12597_4#3.json  ) .
  q(&& perl -e '\"'\"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>$ARGV[2], position=>$ARGV[3], tag_index=>$ARGV[4]); $o->parsing_metrics_file($ARGV[0]); open my$fh,q(<),$ARGV[1]; $o->parsing_flagstats($fh); close$fh; $o->store($ARGV[-1]) '\"'\"' ) .
  qq($dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/12597_4#3.markdups_metrics.txt $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/12597_4#3.flagstat 12597 4 3 $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/qc && ) .
  q(perl -e '\"'\"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(human_split=>q(phix), id_run=>$ARGV[2], position=>$ARGV[3], tag_index=>$ARGV[4]); $o->parsing_metrics_file($ARGV[0]); open my$fh,q(<),$ARGV[1]; $o->parsing_flagstats($fh); close$fh; $o->store($ARGV[-1]) '\"'\"' ) .
  qq($dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/12597_4#3_phix.markdups_metrics.txt $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/12597_4#3_phix.flagstat 12597 4 3 $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/qc ) .
    q{&& qc --check alignment_filter_metrics --qc_in $PWD --id_run 12597 --position 4 } .
    qq{--qc_out $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/qc --tag_index 3 }.
  qq('","4000":"bam_alignment.pl --id_run 12597 --position 4 --tag_index 0 --input $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/lane4/12597_4#0.bam --output_prefix $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/12597_4#0 --do_markduplicates  --is_paired_read"});

cmp_deeply(\@lines, [$json ], 'correct json file content (for dUTP library)');

#####  non-RNASeq libraries (i.e. not Illumina cDNA protocol (unstranded) and RNA-seq dUTP (stranded))  pattern match looks for /(?:cD|R)NA/sxm

  $args->{'5040'} = qq{bam_alignment.pl --id_run 12597 --position 5 --tag_index 40 --input $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/lane5/12597_5#40.bam --output_prefix $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane5/12597_5#40 --do_markduplicates  --is_paired_read};
 
 lives_ok {$rna_gen->_generate_command_arguments([5])}
     'no error generating command arguments for non-RNASeq lane';

 is ($rna_gen->_job_args->{'5040'},$args->{'5040'},'correct non-RNASeq lane args generated');

#### monoplex (non-RNA Seq)

lives_ok {$rna_gen->_generate_command_arguments([1])}
     'no error generating command arguments for non-multiplex lane';

 $args->{'1'} = qq{bam_alignment.pl --id_run 12597 --position 1 --input $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/12597_1.bam --output_prefix $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/12597_1 --do_markduplicates  --is_paired_read};

 is ($rna_gen->_job_args->{'1'},$args->{'1'},'correct non-multiplex lane args generated');

}

{  ##RNASeq library  13066_8  library_type = Illumina cDNA protocol 

my $ref_dir = join q[/],$dir,'references','Homo_sapiens','1000Genomes_hs37d5','all';
`mkdir -p $ref_dir/fasta`;
`mkdir $ref_dir/bowtie2`;
`mkdir $ref_dir/picard`;
`touch $ref_dir/fasta/hs37d5.fa`;
`touch $ref_dir/bowtie2/hs37d5.fa`;
`touch $ref_dir/picard/hs37d5.fa`;

my $ref = qq[$ref_dir/bowtie2/Homo_sapiens.GRCh37.NCBI.allchr_MT.fa];
my $runfolder = q{140529_HS18_13066_A_C3C3KACXX};
my $runfolder_path = join q[/], $dir, $runfolder;
my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20140606-133530/no_cal';
`mkdir -p $bc_path`;
my $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20140606-133530/metadata_cache_13066';
`mkdir $cache_dir`;

copy("t/data/rna_seq/13066_RunInfo.xml","$runfolder_path/RunInfo.xml") or die "Copy failed: $!"; #to get information that it is paired end


my $transcriptome_dir = join q[/],$dir,'transcriptomes','Homo_sapiens','ensembl_75_transcriptome','1000Genomes_hs37d5';
`mkdir -p $transcriptome_dir/gtf`;
`mkdir -p $transcriptome_dir/tophat2`;
`touch $transcriptome_dir/gtf/ensembl_75_transcriptome-1000Genomes_hs37d5.gtf`;
`touch $transcriptome_dir/tophat2/1000Genomes_hs37d5.known.2.bt2`;

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

  my $args = {};

  $args->{8} = qq{bash -c ' mkdir -p $dir/140529_HS18_13066_A_C3C3KACXX/Data/Intensities/BAM_basecalls_20140606-133530/no_cal/archive/tmp_\$LSB_JOBID/13066_8 ; cd $dir/140529_HS18_13066_A_C3C3KACXX/Data/Intensities/BAM_basecalls_20140606-133530/no_cal/archive/tmp_\$LSB_JOBID/13066_8 && vtfp.pl -s -keys samtools_executable -vals samtools1_1 -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `echo \$LSB_MCPU_HOSTS | cut -d " " -f2` -keys indatadir -vals $dir/140529_HS18_13066_A_C3C3KACXX/Data/Intensities/BAM_basecalls_20140606-133530/no_cal -keys outdatadir -vals $dir/140529_HS18_13066_A_C3C3KACXX/Data/Intensities/BAM_basecalls_20140606-133530/no_cal/archive -keys af_metrics -vals 13066_8.bam_alignment_filter_metrics.json -keys rpt -vals 13066_8 -keys reference_dict -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/picard/hs37d5.fa.dict -keys reference_genome_fasta -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/fasta/hs37d5.fa -keys phix_reference_genome_fasta -vals $phix_ref -keys alignment_filter_jar -vals $odir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/AlignmentFilter.jar -keys alignment_reference_genome -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/bowtie2/hs37d5.fa -keys library_type -vals fr-unstranded -keys transcriptome_val -vals $dir/transcriptomes/Homo_sapiens/ensembl_75_transcriptome/1000Genomes_hs37d5/tophat2/1000Genomes_hs37d5.known -keys alignment_method -vals tophat2 \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_13066_8.json && viv.pl -s -x -v 3 -o viv_13066_8.log run_13066_8.json  } .
    q{&& perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>$ARGV[2], position=>$ARGV[3]); $o->parsing_metrics_file($ARGV[0]); open my$fh,q(<),$ARGV[1]; $o->parsing_flagstats($fh); close$fh; $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/140529_HS18_13066_A_C3C3KACXX/Data/Intensities/BAM_basecalls_20140606-133530/no_cal/archive/13066_8.markdups_metrics.txt $dir/140529_HS18_13066_A_C3C3KACXX/Data/Intensities/BAM_basecalls_20140606-133530/no_cal/archive/13066_8.flagstat 13066 8 $dir/140529_HS18_13066_A_C3C3KACXX/Data/Intensities/BAM_basecalls_20140606-133530/no_cal/archive/qc && } .
    q{perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(human_split=>q(phix), id_run=>$ARGV[2], position=>$ARGV[3]); $o->parsing_metrics_file($ARGV[0]); open my$fh,q(<),$ARGV[1]; $o->parsing_flagstats($fh); close$fh; $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/140529_HS18_13066_A_C3C3KACXX/Data/Intensities/BAM_basecalls_20140606-133530/no_cal/archive/13066_8_phix.markdups_metrics.txt $dir/140529_HS18_13066_A_C3C3KACXX/Data/Intensities/BAM_basecalls_20140606-133530/no_cal/archive/13066_8_phix.flagstat 13066 8 $dir/140529_HS18_13066_A_C3C3KACXX/Data/Intensities/BAM_basecalls_20140606-133530/no_cal/archive/qc } .
    q{&& qc --check alignment_filter_metrics --qc_in $PWD --id_run 13066 --position 8 } .
    qq{--qc_out $dir/140529_HS18_13066_A_C3C3KACXX/Data/Intensities/BAM_basecalls_20140606-133530/no_cal/archive/qc }.
    q{'};

  lives_ok {$rna_gen->_generate_command_arguments([8])}
     'no error generating command arguments';

  cmp_deeply ($rna_gen->_job_args, $args,
    'correct command arguments for library RNASeq lane (unstranded Illumina cDNA library)');
}

1;
