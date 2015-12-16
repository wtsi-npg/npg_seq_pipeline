use strict;
use warnings;
use Test::More tests => 53;
use Test::Exception;
use Test::Deep;
use File::Temp qw/tempdir/;
use Cwd qw/cwd abs_path/;
use Perl6::Slurp;
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
`mkdir -p $dir/references/PhiX/default/all/bwa0_6`;
`touch $dir/references/PhiX/default/all/bwa0_6/phix.fa`;

`mkdir -p $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/fasta`;
`touch $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/fasta/hs37d5.fa`;
`mkdir -p $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/picard`;
`touch $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/picard/hs37d5.fa`;
`mkdir -p $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/bwa0_6`;
`touch $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/bwa0_6/hs37d5.fa`;
`ln -s 1000Genomes_hs37d5 $dir/references/Homo_sapiens/default`;

my $ref_dir = join q[/],$dir,'references','Mus_musculus','NCBIm37','all';
`mkdir -p $ref_dir/fasta`;
`mkdir -p $dir/references/Strongyloides_ratti/20100601/all/fasta`;
`touch $dir/references/Strongyloides_ratti/20100601/all/fasta/rat.fa`;
`mkdir -p $dir/references/Strongyloides_ratti/20100601/all/picard`;
`touch $dir/references/Strongyloides_ratti/20100601/all/picard/rat.fa`;
`mkdir -p $dir/references/Strongyloides_ratti/20100601/all/bwa0_6`;
`touch $dir/references/Strongyloides_ratti/20100601/all/bwa0_6/rat.fa`;
`mkdir $ref_dir/bowtie2`;
`mkdir $ref_dir/picard`;
`mkdir $ref_dir/bwa0_6`;
`touch $ref_dir/fasta/mm_ref_NCBI37_1.fasta`;
`touch $ref_dir/bowtie2/mm_ref_NCBI37_1.fasta`;
`touch $ref_dir/picard/mm_ref_NCBI37_1.fasta`;
`touch $ref_dir/bwa0_6/mm_ref_NCBI37_1.fasta`;

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
      force_phix_split  => 0,
      ###uncomment to check V4 failure, flowcell_id=>'C333TANXX',
    )
  } 'no error creating an object';

  is ($rna_gen->id_run, 12597, 'id_run inferred correctly');
  ok ((not $rna_gen->_has_newer_flowcell), 'not HT V4 or RR V2') or diag $rna_gen->flowcell_id;

  my $args = {};
  $args->{'4003'} = qq{bash -c '\ mkdir -p $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/tmp_\$LSB_JOBID/12597_4#3 ; cd $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/tmp_\$LSB_JOBID/12597_4#3 && vtfp.pl -s -keys samtools_executable -vals samtools1 -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` -keys indatadir -vals $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/lane4 -keys outdatadir -vals $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4 -keys af_metrics -vals 12597_4#3.bam_alignment_filter_metrics.json -keys rpt -vals 12597_4#3 -keys reference_dict -vals $dir/references/Mus_musculus/NCBIm37/all/picard/mm_ref_NCBI37_1.fasta.dict -keys reference_genome_fasta -vals $dir/references/Mus_musculus/NCBIm37/all/fasta/mm_ref_NCBI37_1.fasta -keys phix_reference_genome_fasta -vals $phix_ref -keys alignment_filter_jar -vals $odir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/AlignmentFilter.jar -keys alignment_reference_genome -vals $dir/references/Mus_musculus/NCBIm37/all/bowtie2/mm_ref_NCBI37_1.fasta -keys library_type -vals fr-firststrand -keys transcriptome_val -vals $dir/transcriptomes/Mus_musculus/ensembl_67_transcriptome/NCBIm37/tophat2/NCBIm37.known -keys alignment_method -vals tophat2 \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_12597_4#3.json && viv.pl -s -x -v 3 -o viv_12597_4#3.log run_12597_4#3.json  } .
    q{&& perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>12597,position=>4,sequence_file=>$ARGV[0],tag_index=>3); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/12597_4#3.cram $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/qc && } .
    q{perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>12597,position=>4,sequence_file=>$ARGV[0],subset=>q(phix),tag_index=>3); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/12597_4#3_phix.cram $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/qc } .
    q{&&   qc --check alignment_filter_metrics --qc_in $PWD --id_run 12597 --position 4 } .
    qq{--qc_out $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/qc --tag_index 3 }.
    q{'};

  $args->{'4000'} = qq{bash -c '\ mkdir -p $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/tmp_\$LSB_JOBID/12597_4#0 ; cd $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/tmp_\$LSB_JOBID/12597_4#0 && vtfp.pl -s -keys samtools_executable -vals samtools1 -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` -keys indatadir -vals $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/lane4 -keys outdatadir -vals $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4 -keys af_metrics -vals 12597_4#0.bam_alignment_filter_metrics.json -keys rpt -vals 12597_4#0 -keys reference_dict -vals $dir/references/Mus_musculus/NCBIm37/all/picard/mm_ref_NCBI37_1.fasta.dict -keys reference_genome_fasta -vals $dir/references/Mus_musculus/NCBIm37/all/fasta/mm_ref_NCBI37_1.fasta -keys phix_reference_genome_fasta -vals $phix_ref -keys alignment_filter_jar -vals $odir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/AlignmentFilter.jar -keys alignment_reference_genome -vals $dir/references/Mus_musculus/NCBIm37/all/bwa0_6/mm_ref_NCBI37_1.fasta -keys bwa_executable -vals bwa0_6 -keys alignment_method -vals bwa_aln \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_12597_4#0.json && viv.pl -s -x -v 3 -o viv_12597_4#0.log run_12597_4#0.json  } .
  q{&& perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>12597,position=>4,sequence_file=>$ARGV[0],tag_index=>0); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
  qq{$dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/12597_4#0.cram $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/qc && } .
  q{perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>12597,position=>4,sequence_file=>$ARGV[0],subset=>q(phix),tag_index=>0); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
  qq{$dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/12597_4#0_phix.cram $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/qc &&   } .
  q{qc --check alignment_filter_metrics --qc_in $PWD --id_run 12597 --position 4 } .
  qq{--qc_out $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/qc --tag_index 0 }.
    q{'};
  
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

my $xjson = qq({"4003":"bash -c ' mkdir -p $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/tmp_\$LSB_JOBID/12597_4#3 ; cd $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/tmp_\$LSB_JOBID/12597_4#3 && vtfp.pl -s -keys samtools_executable -vals samtools1 -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` -keys indatadir -vals $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/lane4 -keys outdatadir -vals $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4 -keys af_metrics -vals 12597_4#3.bam_alignment_filter_metrics.json -keys rpt -vals 12597_4#3 -keys reference_dict -vals $dir/references/Mus_musculus/NCBIm37/all/picard/mm_ref_NCBI37_1.fasta.dict -keys reference_genome_fasta -vals $dir/references/Mus_musculus/NCBIm37/all/fasta/mm_ref_NCBI37_1.fasta -keys phix_reference_genome_fasta -vals $phix_ref -keys alignment_filter_jar -vals $odir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/AlignmentFilter.jar -keys alignment_reference_genome -vals $dir/references/Mus_musculus/NCBIm37/all/bowtie2/mm_ref_NCBI37_1.fasta -keys library_type -vals fr-firststrand -keys transcriptome_val -vals $dir/transcriptomes/Mus_musculus/ensembl_67_transcriptome/NCBIm37/tophat2/NCBIm37.known -keys alignment_method -vals tophat2 \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_12597_4#3.json && viv.pl -s -x -v 3 -o viv_12597_4#3.log run_12597_4#3.json  ) .
  q(&& perl -e '\"'\"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>12597,position=>4,sequence_file=>$ARGV[0],tag_index=>3); $o->execute(); $o->store($ARGV[-1]) '\"'\"' ) .
  qq($dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/12597_4#3.cram $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/qc && ) .
  q(perl -e '\"'\"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>12597,position=>4,sequence_file=>$ARGV[0],subset=>q(phix),tag_index=>3); $o->execute(); $o->store($ARGV[-1]) '\"'\"' ) .
  qq($dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/12597_4#3_phix.cram $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/qc ) .
  q{&&   qc --check alignment_filter_metrics --qc_in $PWD --id_run 12597 --position 4 } .
  qq{--qc_out $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/qc --tag_index 3 }.
  qq('","4000":"bam_alignment.pl --id_run 12597 --position 4 --tag_index 0 --input $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/lane4/12597_4#0.bam --output_prefix $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/12597_4#0 --is_paired_read"});

my $json = qq({"4003":"bash -c ' mkdir -p $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/tmp_\$LSB_JOBID/12597_4#3 ; cd $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/tmp_\$LSB_JOBID/12597_4#3 && vtfp.pl -s -keys samtools_executable -vals samtools1 -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` -keys indatadir -vals $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/lane4 -keys outdatadir -vals $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4 -keys af_metrics -vals 12597_4#3.bam_alignment_filter_metrics.json -keys rpt -vals 12597_4#3 -keys reference_dict -vals $dir/references/Mus_musculus/NCBIm37/all/picard/mm_ref_NCBI37_1.fasta.dict -keys reference_genome_fasta -vals $dir/references/Mus_musculus/NCBIm37/all/fasta/mm_ref_NCBI37_1.fasta -keys phix_reference_genome_fasta -vals $phix_ref -keys alignment_filter_jar -vals $odir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/AlignmentFilter.jar -keys alignment_reference_genome -vals $dir/references/Mus_musculus/NCBIm37/all/bowtie2/mm_ref_NCBI37_1.fasta -keys library_type -vals fr-firststrand -keys transcriptome_val -vals $dir/transcriptomes/Mus_musculus/ensembl_67_transcriptome/NCBIm37/tophat2/NCBIm37.known -keys alignment_method -vals tophat2 \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_12597_4#3.json && viv.pl -s -x -v 3 -o viv_12597_4#3.log run_12597_4#3.json  ) .
  q(&& perl -e '\"'\"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>12597,position=>4,sequence_file=>$ARGV[0],tag_index=>3); $o->execute(); $o->store($ARGV[-1]) '\"'\"' ) .
  qq($dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/12597_4#3.cram $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/qc && ) .
  q(perl -e '\"'\"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>12597,position=>4,sequence_file=>$ARGV[0],subset=>q(phix),tag_index=>3); $o->execute(); $o->store($ARGV[-1]) '\"'\"' ) .
  qq($dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/12597_4#3_phix.cram $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/qc ) .
  q(&&   qc --check alignment_filter_metrics --qc_in $PWD --id_run 12597 --position 4 ) .
  qq(--qc_out $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/qc --tag_index 3 ) .
  qq('","4000":"bash -c ' mkdir -p $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/tmp_\$LSB_JOBID/12597_4#0 ; cd $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/tmp_\$LSB_JOBID/12597_4#0 && vtfp.pl -s -keys samtools_executable -vals samtools1 -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` -keys indatadir -vals $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/lane4 -keys outdatadir -vals $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4 -keys af_metrics -vals 12597_4#0.bam_alignment_filter_metrics.json -keys rpt -vals 12597_4#0 -keys reference_dict -vals $dir/references/Mus_musculus/NCBIm37/all/picard/mm_ref_NCBI37_1.fasta.dict -keys reference_genome_fasta -vals $dir/references/Mus_musculus/NCBIm37/all/fasta/mm_ref_NCBI37_1.fasta -keys phix_reference_genome_fasta -vals $phix_ref -keys alignment_filter_jar -vals $odir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/AlignmentFilter.jar -keys alignment_reference_genome -vals $dir/references/Mus_musculus/NCBIm37/all/bwa0_6/mm_ref_NCBI37_1.fasta -keys bwa_executable -vals bwa0_6 -keys alignment_method -vals bwa_aln \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_12597_4#0.json && viv.pl -s -x -v 3 -o viv_12597_4#0.log run_12597_4#0.json  ) .
  q(&& perl -e '\"'\"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>12597,position=>4,sequence_file=>$ARGV[0],tag_index=>0); $o->execute(); $o->store($ARGV[-1]) '\"'\"' ) .
  qq($dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/12597_4#0.cram $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/qc ) .
  q(&& perl -e '\"'\"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>12597,position=>4,sequence_file=>$ARGV[0],subset=>q(phix),tag_index=>0); $o->execute(); $o->store($ARGV[-1]) '\"'\"' ) .
  qq($dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/12597_4#0_phix.cram $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/qc &&   qc --check alignment_filter_metrics --qc_in \$PWD --id_run 12597 --position 4 --qc_out $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane4/qc --tag_index 0 '"});

cmp_deeply(\@lines, [$json ], 'correct json file content (for dUTP library)');

#### force on phix_split
  lives_ok {
    $rna_gen = npg_pipeline::archive::file::generation::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2014},
      verbose           => 1,
      repository        => $dir,
      no_bsub           => 1,
      force_phix_split  => 1,
    )
  } 'no error creating an object (forcing on phix split)';

#####  phiX control libraries

  $args->{'5168'} = qq{bam_alignment.pl --id_run 12597 --position 5 --tag_index 168 --input $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/lane5/12597_5#168.bam --output_prefix $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/lane5/12597_5#168 --is_paired_read};
 
  lives_ok {$rna_gen->_generate_command_arguments([5])}
     'no error generating command arguments for non-RNASeq lane';

  is ($rna_gen->_job_args->{'5168'},$args->{'5168'},'correct non-RNASeq lane args generated');

#### monoplex (non-RNA Seq)

  lives_ok {$rna_gen->_generate_command_arguments([1])}
     'no error generating command arguments for non-multiplex lane';

  $args->{'1'} = qq{bam_alignment.pl --spiked_phix_split --id_run 12597 --position 1 --input $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/12597_1.bam --output_prefix $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/12597_1 --is_paired_read};

  $args->{'1'} = qq{bash -c ' mkdir -p $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/tmp_\$LSB_JOBID/12597_1 ; cd $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/tmp_\$LSB_JOBID/12597_1 && vtfp.pl -s -keys samtools_executable -vals samtools1 -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` -keys indatadir -vals $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal -keys outdatadir -vals $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive -keys af_metrics -vals 12597_1.bam_alignment_filter_metrics.json -keys rpt -vals 12597_1 -keys reference_dict -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/picard/hs37d5.fa.dict -keys reference_genome_fasta -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/fasta/hs37d5.fa -keys phix_reference_genome_fasta -vals $dir/references/PhiX/Illumina/all/fasta/phix-illumina.fa -keys alignment_filter_jar -vals $odir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/AlignmentFilter.jar -keys alignment_reference_genome -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/bwa0_6/hs37d5.fa -keys bwa_executable -vals bwa0_6 -keys alignment_method -vals bwa_aln \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_12597_1.json && viv.pl -s -x -v 3 -o viv_12597_1.log run_12597_1.json  && perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my\$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>12597,position=>1,sequence_file=>\$ARGV[0]); \$o->execute(); \$o->store(\$ARGV[-1]) '"'"' $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/12597_1.cram $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/qc && perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my\$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>12597,position=>1,sequence_file=>\$ARGV[0],subset=>q(phix)); \$o->execute(); \$o->store(\$ARGV[-1]) '"'"' $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/12597_1_phix.cram $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/qc &&   qc --check alignment_filter_metrics --qc_in \$PWD --id_run 12597 --position 1 --qc_out $dir/140409_HS34_12597_A_C333TACXX/Data/Intensities/BAM_basecalls_20140515-073611/no_cal/archive/qc '};


  is ($rna_gen->_job_args->{'1'},$args->{'1'},'correct non-multiplex lane args generated');

  ok((not $rna_gen->_has_newer_flowcell), 'HT V3 flowcell recognised as older flowcell');
#### check for newer flowcells
  lives_ok {
    $rna_gen = npg_pipeline::archive::file::generation::seq_alignment->new(
      run_folder        => $runfolder,
      runfolder_path    => $runfolder_path,
      recalibrated_path => $bc_path,
      timestamp         => q{2014},
      verbose           => 1,
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
      verbose           => 1,
      repository        => $dir,
      no_bsub           => 1,
      force_phix_split  => 1,
      flowcell_id       => 'C333TANXX',
    )
  } 'no error creating an object with HT V4 flowcell (forcing on phix split)';
  ok ($rna_gen->_has_newer_flowcell, 'HT V4 flowcell recognised as newer flowcell');

}

{  ##RNASeq library  13066_8  library_type = Illumina cDNA protocol 

my $ref_dir = join q[/],$dir,'references','Homo_sapiens','1000Genomes_hs37d5','all';
`mkdir -p $ref_dir/fasta`;
`mkdir $ref_dir/bowtie2`;
`mkdir $ref_dir/picard`;
`mkdir $ref_dir/bwa0_6`;
`touch $ref_dir/fasta/hs37d5.fa`;
`touch $ref_dir/bowtie2/hs37d5.fa`;
`touch $ref_dir/picard/hs37d5.fa`;
`touch $ref_dir/bwa0_6/hs37d5.fa`;

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

  $args->{8} = qq{bash -c ' mkdir -p $dir/140529_HS18_13066_A_C3C3KACXX/Data/Intensities/BAM_basecalls_20140606-133530/no_cal/archive/tmp_\$LSB_JOBID/13066_8 ; cd $dir/140529_HS18_13066_A_C3C3KACXX/Data/Intensities/BAM_basecalls_20140606-133530/no_cal/archive/tmp_\$LSB_JOBID/13066_8 && vtfp.pl -s -keys samtools_executable -vals samtools1 -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` -keys indatadir -vals $dir/140529_HS18_13066_A_C3C3KACXX/Data/Intensities/BAM_basecalls_20140606-133530/no_cal -keys outdatadir -vals $dir/140529_HS18_13066_A_C3C3KACXX/Data/Intensities/BAM_basecalls_20140606-133530/no_cal/archive -keys af_metrics -vals 13066_8.bam_alignment_filter_metrics.json -keys rpt -vals 13066_8 -keys reference_dict -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/picard/hs37d5.fa.dict -keys reference_genome_fasta -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/fasta/hs37d5.fa -keys phix_reference_genome_fasta -vals $phix_ref -keys alignment_filter_jar -vals $odir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/AlignmentFilter.jar -keys alignment_reference_genome -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/bowtie2/hs37d5.fa -keys library_type -vals fr-unstranded -keys transcriptome_val -vals $dir/transcriptomes/Homo_sapiens/ensembl_75_transcriptome/1000Genomes_hs37d5/tophat2/1000Genomes_hs37d5.known -keys alignment_method -vals tophat2 \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_13066_8.json && viv.pl -s -x -v 3 -o viv_13066_8.log run_13066_8.json  } .
    q{&& perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>13066,position=>8,sequence_file=>$ARGV[0]); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/140529_HS18_13066_A_C3C3KACXX/Data/Intensities/BAM_basecalls_20140606-133530/no_cal/archive/13066_8.cram $dir/140529_HS18_13066_A_C3C3KACXX/Data/Intensities/BAM_basecalls_20140606-133530/no_cal/archive/qc && } .
    q{perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>13066,position=>8,sequence_file=>$ARGV[0],subset=>q(phix)); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/140529_HS18_13066_A_C3C3KACXX/Data/Intensities/BAM_basecalls_20140606-133530/no_cal/archive/13066_8_phix.cram $dir/140529_HS18_13066_A_C3C3KACXX/Data/Intensities/BAM_basecalls_20140606-133530/no_cal/archive/qc } .
    q{&&   qc --check alignment_filter_metrics --qc_in $PWD --id_run 13066 --position 8 } .
    qq{--qc_out $dir/140529_HS18_13066_A_C3C3KACXX/Data/Intensities/BAM_basecalls_20140606-133530/no_cal/archive/qc }.
    q{'};

  lives_ok {$rna_gen->_generate_command_arguments([8])}
     'no error generating command arguments';

  cmp_deeply ($rna_gen->_job_args, $args,
    'correct command arguments for library RNASeq lane (unstranded Illumina cDNA library)');

  is ($rna_gen->_using_alt_reference, 0, 'Not using alternate reference');
}

{  ## single ended v. short , old flowcell, CRIPSR

my $runfolder = q{151215_HS38_18472_A_H55HVADXX};
my $runfolder_path = join q[/], q(t/data/example_runfolder), $runfolder;
`cp -r $runfolder_path $dir`;
$runfolder_path = join q[/], $dir, $runfolder;
my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20151215-215034';
my $cache_dir = join q[/], $bc_path, 'metadata_cache_18472';
`mkdir -p $dir/references/Homo_sapiens/GRCh38_15/all/{bwa0_6,fasta,picard}/`;
`touch $dir/references/Homo_sapiens/GRCh38_15/all/{bwa0_6,fasta}/Homo_sapiens.GRCh38_15.fa`;
`touch $dir/references/Homo_sapiens/GRCh38_15/all/picard/Homo_sapiens.GRCh38_15.fa.dict`;

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

  my $args = qq{bash -c ' mkdir -p $bc_path/no_cal/archive/tmp_\$LSB_JOBID/18472_2#1 ; cd $bc_path/no_cal/archive/tmp_\$LSB_JOBID/18472_2#1 && vtfp.pl -s -keys samtools_executable -vals samtools1 -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` -keys indatadir -vals $bc_path/no_cal/lane2 -keys outdatadir -vals $bc_path/no_cal/archive/lane2 -keys af_metrics -vals 18472_2#1.bam_alignment_filter_metrics.json -keys rpt -vals 18472_2#1 -keys reference_dict -vals $dir/references/Homo_sapiens/GRCh38_15/all/picard/Homo_sapiens.GRCh38_15.fa.dict -keys reference_genome_fasta -vals $dir/references/Homo_sapiens/GRCh38_15/all/fasta/Homo_sapiens.GRCh38_15.fa -keys phix_reference_genome_fasta -vals $phix_ref -keys alignment_filter_jar -vals $odir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/AlignmentFilter.jar -keys alignment_reference_genome -vals $dir/references/Homo_sapiens/GRCh38_15/all/bwa0_6/Homo_sapiens.GRCh38_15.fa -keys bwa_executable -vals bwa0_6 -keys alignment_method -vals bwa_aln_se -nullkeys bwa_mem_p_flag \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_18472_2#1.json && viv.pl -s -x -v 3 -o viv_18472_2#1.log run_18472_2#1.json  } .
    q{&& perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>18472,position=>2,sequence_file=>$ARGV[0],tag_index=>1); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$bc_path/no_cal/archive/lane2/18472_2#1.cram $bc_path/no_cal/archive/lane2/qc && } .
    q{perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>18472,position=>2,sequence_file=>$ARGV[0],subset=>q(phix),tag_index=>1); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$bc_path/no_cal/archive/lane2/18472_2#1_phix.cram $bc_path/no_cal/archive/lane2/qc } .
    q{&&   qc --check alignment_filter_metrics --qc_in $PWD --id_run 18472 --position 2 } .
    qq{--qc_out $bc_path/no_cal/archive/lane2/qc --tag_index 1 }.
    q{'};

  lives_ok {$se_gen->_generate_command_arguments([2])}
     'no error generating command arguments';

  is($se_gen->_job_args->{'2001'}, $args,
    'correct command arguments for plex of short single read run');

  ok(!$se_gen->_using_alt_reference, 'Not using alternate reference');
}

{  ##HiSeqX, run 16839_7

my $ref_dir = join q[/],$dir,'references','Homo_sapiens','GRCh38_full_analysis_set_plus_decoy_hla','all';
`mkdir -p $ref_dir/fasta`;
`mkdir $ref_dir/bwa0_6`;
`mkdir $ref_dir/picard`;

my $runfolder = q{150709_HX4_16839_A_H7MHWCCXX};
my $runfolder_path = join q[/], $dir, $runfolder;
my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150712-121006/no_cal';
`mkdir -p $bc_path`;
my $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150712-121006/metadata_cache_16839';
`mkdir $cache_dir`;

copy("t/data/hiseqx/16839_RunInfo.xml","$runfolder_path/RunInfo.xml") or die "Copy failed: $!"; #to get information that it is paired end

`touch $ref_dir/fasta/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa`;
`touch $ref_dir/picard/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa.dict`;
`touch $ref_dir/bwa0_6/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa.alt`;
`touch $ref_dir/bwa0_6/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa.amb`;
`touch $ref_dir/bwa0_6/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa.ann`;
`touch $ref_dir/bwa0_6/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa.bwt`;
`touch $ref_dir/bwa0_6/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa.pac`;
`touch $ref_dir/bwa0_6/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa.sa`;

# system(qq[ls -lR $ref_dir_bwa0_6]);

local $ENV{'NPG_WEBSERVICE_CACHE_DIR'} = q[t/data/hiseqx];
local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/hiseqx/samplesheet_16839.csv];

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

  my $args = {};

  $args->{7007} = qq{bash -c ' mkdir -p $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/tmp_\$LSB_JOBID/16839_7#7 ; cd $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/tmp_\$LSB_JOBID/16839_7#7 && vtfp.pl -s -keys samtools_executable -vals samtools1 -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` -keys indatadir -vals $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/lane7 -keys outdatadir -vals $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane7 -keys af_metrics -vals 16839_7#7.bam_alignment_filter_metrics.json -keys rpt -vals 16839_7#7 -keys reference_dict -vals $dir/references/Homo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/all/picard/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa.dict -keys reference_genome_fasta -vals $dir/references/Homo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/all/fasta/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa -keys phix_reference_genome_fasta -vals $phix_ref -keys alignment_filter_jar -vals $odir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/AlignmentFilter.jar -keys alignment_reference_genome -vals $dir/references/Homo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/all/bwa0_6/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa -keys bwa_executable -vals bwa0_6 -keys alignment_method -vals bwa_mem \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16839_7#7.json && viv.pl -s -x -v 3 -o viv_16839_7#7.log run_16839_7#7.json  } .
    q{&& perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16839,position=>7,sequence_file=>$ARGV[0],tag_index=>7); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane7/16839_7#7.cram $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane7/qc && } .
    q{perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16839,position=>7,sequence_file=>$ARGV[0],subset=>q(phix),tag_index=>7); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane7/16839_7#7_phix.cram $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane7/qc } .
    q{&&   qc --check alignment_filter_metrics --qc_in $PWD --id_run 16839 --position 7 } .
    qq{--qc_out $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane7/qc --tag_index 7 }.
    q{'};
  $args->{7015} = qq{bash -c ' mkdir -p $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/tmp_\$LSB_JOBID/16839_7#15 ; cd $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/tmp_\$LSB_JOBID/16839_7#15 && vtfp.pl -s -keys samtools_executable -vals samtools1 -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` -keys indatadir -vals $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/lane7 -keys outdatadir -vals $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane7 -keys af_metrics -vals 16839_7#15.bam_alignment_filter_metrics.json -keys rpt -vals 16839_7#15 -keys reference_dict -vals $dir/references/Homo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/all/picard/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa.dict -keys reference_genome_fasta -vals $dir/references/Homo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/all/fasta/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa -keys phix_reference_genome_fasta -vals $phix_ref -keys alignment_filter_jar -vals $odir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/AlignmentFilter.jar -keys alignment_reference_genome -vals $dir/references/Homo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/all/bwa0_6/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa -keys bwa_executable -vals bwa0_6 -keys alignment_method -vals bwa_mem \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16839_7#15.json && viv.pl -s -x -v 3 -o viv_16839_7#15.log run_16839_7#15.json  } .
    q{&& perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16839,position=>7,sequence_file=>$ARGV[0],tag_index=>15); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane7/16839_7#15.cram $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane7/qc && } .
    q{perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16839,position=>7,sequence_file=>$ARGV[0],subset=>q(phix),tag_index=>15); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane7/16839_7#15_phix.cram $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane7/qc } .
    q{&&   qc --check alignment_filter_metrics --qc_in $PWD --id_run 16839 --position 7 } .
    qq{--qc_out $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane7/qc --tag_index 15 }.
    q{'};
  $args->{7000} = qq{bash -c ' mkdir -p $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/tmp_\$LSB_JOBID/16839_7#0 ; cd $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/tmp_\$LSB_JOBID/16839_7#0 && vtfp.pl -s -keys samtools_executable -vals samtools1 -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` -keys indatadir -vals $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/lane7 -keys outdatadir -vals $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane7 -keys af_metrics -vals 16839_7#0.bam_alignment_filter_metrics.json -keys rpt -vals 16839_7#0 -keys reference_dict -vals $dir/references/Homo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/all/picard/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa.dict -keys reference_genome_fasta -vals $dir/references/Homo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/all/fasta/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa -keys phix_reference_genome_fasta -vals $phix_ref -keys alignment_filter_jar -vals $odir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/AlignmentFilter.jar -keys alignment_reference_genome -vals $dir/references/Homo_sapiens/GRCh38_full_analysis_set_plus_decoy_hla/all/bwa0_6/Homo_sapiens.GRCh38_full_analysis_set_plus_decoy_hla.fa -keys bwa_executable -vals bwa0_6 -keys alignment_method -vals bwa_mem \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16839_7#0.json && viv.pl -s -x -v 3 -o viv_16839_7#0.log run_16839_7#0.json  } .
    q{&& perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16839,position=>7,sequence_file=>$ARGV[0],tag_index=>0); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane7/16839_7#0.cram $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane7/qc && } .
    q{perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16839,position=>7,sequence_file=>$ARGV[0],subset=>q(phix),tag_index=>0); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane7/16839_7#0_phix.cram $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane7/qc } .
    q{&&   qc --check alignment_filter_metrics --qc_in $PWD --id_run 16839 --position 7 } .
    qq{--qc_out $dir/150709_HX4_16839_A_H7MHWCCXX/Data/Intensities/BAM_basecalls_20150712-121006/no_cal/archive/lane7/qc --tag_index 0 }.
    q{'};

  lives_ok {$hsx_gen->_generate_command_arguments([7])}
     'no error generating hsx command arguments';

  cmp_deeply ($hsx_gen->_job_args, $args,
    'correct command arguments for HiSeqX lane 16839_7');

  is ($hsx_gen->_using_alt_reference, 1, 'Using alternate reference');
}

{  ##HiSeq, run 16807_6 (newer flowcell)

my $ref_dir = join q[/],$dir,'references','Homo_sapiens','1000Genomes_hs37d5','all';
`mkdir -p $ref_dir/fasta`;
`mkdir $ref_dir/bwa0_6`;
`mkdir $ref_dir/picard`;

my $runfolder = q{150707_HS38_16807_A_C7U2YANXX};
my $runfolder_path = join q[/], $dir, $runfolder;
my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150707-232614/no_cal';
`mkdir -p $bc_path`;
my $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150707-232614/metadata_cache_16807';
`mkdir $cache_dir`;

copy("t/data/hiseq/16807_RunInfo.xml","$runfolder_path/RunInfo.xml") or die "Copy failed: $!"; #to get information that it is paired end

# dummy reference should already exist

# system(qq[ls -lR $ref_dir]);

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

  my $args = {};

  $args->{6001} = qq{bash -c ' mkdir -p $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/tmp_\$LSB_JOBID/16807_6#1 ; cd $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/tmp_\$LSB_JOBID/16807_6#1 && vtfp.pl -s -keys samtools_executable -vals samtools1 -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` -keys indatadir -vals $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/lane6 -keys outdatadir -vals $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/lane6 -keys af_metrics -vals 16807_6#1.bam_alignment_filter_metrics.json -keys rpt -vals 16807_6#1 -keys reference_dict -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/picard/hs37d5.fa.dict -keys reference_genome_fasta -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/fasta/hs37d5.fa -keys phix_reference_genome_fasta -vals $dir/references/PhiX/Illumina/all/fasta/phix-illumina.fa -keys alignment_filter_jar -vals $odir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/AlignmentFilter.jar -keys alignment_reference_genome -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/bwa0_6/hs37d5.fa -keys bwa_executable -vals bwa0_6 -keys alignment_method -vals bwa_mem -nullkeys bwa_mem_p_flag \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16807_6#1.json && viv.pl -s -x -v 3 -o viv_16807_6#1.log run_16807_6#1.json } .
    q{ && perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16807,position=>6,sequence_file=>$ARGV[0],tag_index=>1); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/lane6/16807_6#1.cram $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/lane6/qc && } .
    q{perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16807,position=>6,sequence_file=>$ARGV[0],subset=>q(phix),tag_index=>1); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/lane6/16807_6#1_phix.cram $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/lane6/qc } .
    q{&&   qc --check alignment_filter_metrics --qc_in $PWD --id_run 16807 --position 6 } .
    qq{--qc_out $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/lane6/qc --tag_index 1 }.
    q{'};
  $args->{6002} = qq{bash -c ' mkdir -p $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/tmp_\$LSB_JOBID/16807_6#2 ; cd $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/tmp_\$LSB_JOBID/16807_6#2 && vtfp.pl -s -keys samtools_executable -vals samtools1 -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` -keys indatadir -vals $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/lane6 -keys outdatadir -vals $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/lane6 -keys af_metrics -vals 16807_6#2.bam_alignment_filter_metrics.json -keys rpt -vals 16807_6#2 -keys reference_dict -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/picard/hs37d5.fa.dict -keys reference_genome_fasta -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/fasta/hs37d5.fa -keys phix_reference_genome_fasta -vals $dir/references/PhiX/Illumina/all/fasta/phix-illumina.fa -keys alignment_filter_jar -vals $odir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/AlignmentFilter.jar -keys alignment_reference_genome -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/bwa0_6/hs37d5.fa -keys bwa_executable -vals bwa0_6 -keys alignment_method -vals bwa_mem -nullkeys bwa_mem_p_flag \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16807_6#2.json && viv.pl -s -x -v 3 -o viv_16807_6#2.log run_16807_6#2.json } .
    q{ && perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16807,position=>6,sequence_file=>$ARGV[0],tag_index=>2); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/lane6/16807_6#2.cram $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/lane6/qc && } .
    q{perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16807,position=>6,sequence_file=>$ARGV[0],subset=>q(phix),tag_index=>2); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/lane6/16807_6#2_phix.cram $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/lane6/qc } .
    q{&&   qc --check alignment_filter_metrics --qc_in $PWD --id_run 16807 --position 6 } .
    qq{--qc_out $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/lane6/qc --tag_index 2 }.
    q{'};
  $args->{6000} = qq{bash -c ' mkdir -p $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/tmp_\$LSB_JOBID/16807_6#0 ; cd $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/tmp_\$LSB_JOBID/16807_6#0 && vtfp.pl -s -keys samtools_executable -vals samtools1 -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` -keys indatadir -vals $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/lane6 -keys outdatadir -vals $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/lane6 -keys af_metrics -vals 16807_6#0.bam_alignment_filter_metrics.json -keys rpt -vals 16807_6#0 -keys reference_dict -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/picard/hs37d5.fa.dict -keys reference_genome_fasta -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/fasta/hs37d5.fa -keys phix_reference_genome_fasta -vals $dir/references/PhiX/Illumina/all/fasta/phix-illumina.fa -keys alignment_filter_jar -vals $odir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/AlignmentFilter.jar -keys alignment_reference_genome -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/bwa0_6/hs37d5.fa -keys bwa_executable -vals bwa0_6 -keys alignment_method -vals bwa_mem -nullkeys bwa_mem_p_flag \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16807_6#0.json && viv.pl -s -x -v 3 -o viv_16807_6#0.log run_16807_6#0.json } .
    q{ && perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16807,position=>6,sequence_file=>$ARGV[0],tag_index=>0); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/lane6/16807_6#0.cram $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/lane6/qc && } .
    q{perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16807,position=>6,sequence_file=>$ARGV[0],subset=>q(phix),tag_index=>0); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/lane6/16807_6#0_phix.cram $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/lane6/qc } .
    q{&&   qc --check alignment_filter_metrics --qc_in $PWD --id_run 16807 --position 6 } .
    qq{--qc_out $dir/150707_HS38_16807_A_C7U2YANXX/Data/Intensities/BAM_basecalls_20150707-232614/no_cal/archive/lane6/qc --tag_index 0 }.
    q{'};

  lives_ok {$hs_gen->_generate_command_arguments([6])}
     'no error generating hs command arguments';

my @a1 = (split / /, $args->{6001});
my @a2 = (split / /, $hs_gen->_job_args->{6001});
for my $i (0..$#a1) {
	if($a1[$i] ne $a2[$i]) {
		print "DIFF AT ELEM $i:\n$a1[$i]\n$a2[$i]\n";
	}
}

  cmp_deeply ($hs_gen->_job_args, $args,
    'correct command arguments for HiSeq lane 16807_6');
}

{  ##MiSeq, run 16850_1 (cycle count over threshold (currently >= 101))

my $ref_dir = join q[/],$dir,'references','Plasmodium_falciparum','3D7_Oct11v3','all';
`mkdir -p $ref_dir/fasta`;
`mkdir $ref_dir/bwa0_6`;
`mkdir $ref_dir/picard`;

my $runfolder = q{150710_MS2_16850_A_MS3014507-500V2};
my $runfolder_path = join q[/], $dir, $runfolder;
my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150712-022206/no_cal';
`mkdir -p $bc_path`;
my $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150712-022206/metadata_cache_16850';
`mkdir $cache_dir`;

copy("t/data/miseq/16850_RunInfo.xml","$runfolder_path/RunInfo.xml") or die "Copy failed: $!"; #to get information that it is paired end

`touch $ref_dir/fasta/Pf3D7_v3.fasta`;
`touch $ref_dir/picard/Pf3D7_v3.fasta.dict`;
`touch $ref_dir/bwa0_6/Pf3D7_v3.fasta.amb`;
`touch $ref_dir/bwa0_6/Pf3D7_v3.fasta.ann`;
`touch $ref_dir/bwa0_6/Pf3D7_v3.fasta.bwt`;
`touch $ref_dir/bwa0_6/Pf3D7_v3.fasta.pac`;
`touch $ref_dir/bwa0_6/Pf3D7_v3.fasta.sa`;

system(qq[ls -lR $ref_dir]);

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

  my $args = {};

  $args->{1001} = qq{bash -c ' mkdir -p $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/tmp_\$LSB_JOBID/16850_1#1 ; cd $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/tmp_\$LSB_JOBID/16850_1#1 && vtfp.pl -s -keys samtools_executable -vals samtools1 -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` -keys indatadir -vals $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/lane1 -keys outdatadir -vals $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/lane1 -keys af_metrics -vals 16850_1#1.bam_alignment_filter_metrics.json -keys rpt -vals 16850_1#1 -keys reference_dict -vals $dir/references/Plasmodium_falciparum/3D7_Oct11v3/all/picard/Pf3D7_v3.fasta.dict -keys reference_genome_fasta -vals $dir/references/Plasmodium_falciparum/3D7_Oct11v3/all/fasta/Pf3D7_v3.fasta -keys phix_reference_genome_fasta -vals $dir/references/PhiX/Illumina/all/fasta/phix-illumina.fa -keys alignment_filter_jar -vals $odir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/AlignmentFilter.jar -keys alignment_reference_genome -vals $dir/references/Plasmodium_falciparum/3D7_Oct11v3/all/bwa0_6/Pf3D7_v3.fasta -keys bwa_executable -vals bwa0_6 -keys alignment_method -vals bwa_mem \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16850_1#1.json && viv.pl -s -x -v 3 -o viv_16850_1#1.log run_16850_1#1.json } .
    q{ && perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16850,position=>1,sequence_file=>$ARGV[0],tag_index=>1); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/lane1/16850_1#1.cram $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/lane1/qc && } .
    q{perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16850,position=>1,sequence_file=>$ARGV[0],subset=>q(phix),tag_index=>1); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/lane1/16850_1#1_phix.cram $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/lane1/qc } .
    q{&&   qc --check alignment_filter_metrics --qc_in $PWD --id_run 16850 --position 1 } .
    qq{--qc_out $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/lane1/qc --tag_index 1 } .
    q{'};

  $args->{1002} = qq{bash -c ' mkdir -p $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/tmp_\$LSB_JOBID/16850_1#2 ; cd $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/tmp_\$LSB_JOBID/16850_1#2 && vtfp.pl -s -keys samtools_executable -vals samtools1 -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` -keys indatadir -vals $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/lane1 -keys outdatadir -vals $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/lane1 -keys af_metrics -vals 16850_1#2.bam_alignment_filter_metrics.json -keys rpt -vals 16850_1#2 -keys reference_dict -vals $dir/references/Plasmodium_falciparum/3D7_Oct11v3/all/picard/Pf3D7_v3.fasta.dict -keys reference_genome_fasta -vals $dir/references/Plasmodium_falciparum/3D7_Oct11v3/all/fasta/Pf3D7_v3.fasta -keys phix_reference_genome_fasta -vals $dir/references/PhiX/Illumina/all/fasta/phix-illumina.fa -keys alignment_filter_jar -vals $odir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/AlignmentFilter.jar -keys alignment_reference_genome -vals $dir/references/Plasmodium_falciparum/3D7_Oct11v3/all/bwa0_6/Pf3D7_v3.fasta -keys bwa_executable -vals bwa0_6 -keys alignment_method -vals bwa_mem \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16850_1#2.json && viv.pl -s -x -v 3 -o viv_16850_1#2.log run_16850_1#2.json } .
    q{ && perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16850,position=>1,sequence_file=>$ARGV[0],tag_index=>2); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/lane1/16850_1#2.cram $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/lane1/qc && } .
    q{perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16850,position=>1,sequence_file=>$ARGV[0],subset=>q(phix),tag_index=>2); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/lane1/16850_1#2_phix.cram $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/lane1/qc } .
    q{&&   qc --check alignment_filter_metrics --qc_in $PWD --id_run 16850 --position 1 } .
    qq{--qc_out $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/lane1/qc --tag_index 2 } .
    q{'};

  $args->{1000} = qq{bash -c ' mkdir -p $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/tmp_\$LSB_JOBID/16850_1#0 ; cd $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/tmp_\$LSB_JOBID/16850_1#0 && vtfp.pl -s -keys samtools_executable -vals samtools1 -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` -keys indatadir -vals $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/lane1 -keys outdatadir -vals $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/lane1 -keys af_metrics -vals 16850_1#0.bam_alignment_filter_metrics.json -keys rpt -vals 16850_1#0 -keys reference_dict -vals $dir/references/Plasmodium_falciparum/3D7_Oct11v3/all/picard/Pf3D7_v3.fasta.dict -keys reference_genome_fasta -vals $dir/references/Plasmodium_falciparum/3D7_Oct11v3/all/fasta/Pf3D7_v3.fasta -keys phix_reference_genome_fasta -vals $dir/references/PhiX/Illumina/all/fasta/phix-illumina.fa -keys alignment_filter_jar -vals $odir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/AlignmentFilter.jar -keys alignment_reference_genome -vals $dir/references/Plasmodium_falciparum/3D7_Oct11v3/all/bwa0_6/Pf3D7_v3.fasta -keys bwa_executable -vals bwa0_6 -keys alignment_method -vals bwa_mem \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json > run_16850_1#0.json && viv.pl -s -x -v 3 -o viv_16850_1#0.log run_16850_1#0.json } .
    q{ && perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16850,position=>1,sequence_file=>$ARGV[0],tag_index=>0); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/lane1/16850_1#0.cram $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/lane1/qc && } .
    q{perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16850,position=>1,sequence_file=>$ARGV[0],subset=>q(phix),tag_index=>0); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/lane1/16850_1#0_phix.cram $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/lane1/qc } .
    q{&&   qc --check alignment_filter_metrics --qc_in $PWD --id_run 16850 --position 1 } .
    qq{--qc_out $dir/150710_MS2_16850_A_MS3014507-500V2/Data/Intensities/BAM_basecalls_20150712-022206/no_cal/archive/lane1/qc --tag_index 0 } .
    q{'};

  lives_ok {$ms_gen->_generate_command_arguments([1])}
     'no error generating ms command arguments';

  cmp_deeply ($ms_gen->_job_args, $args,
    'correct command arguments for MiSeq lane 16850_1');
}

{  ##MiSeq, run 16756_1 (nonconsented human split, no target alignment)

my $ref_dir = join q[/],$dir,'references','Plasmodium_falciparum','3D7_Oct11v3','all';
`mkdir -p $ref_dir/fasta`;
`mkdir $ref_dir/bwa0_6`;
`mkdir $ref_dir/picard`;

my $runfolder = q{150701_HS36_16756_B_C711RANXX};
my $runfolder_path = join q[/], $dir, $runfolder;
my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150707-132329/no_cal';
`mkdir -p $bc_path`;
my $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150707-132329/metadata_cache_16756';
`mkdir $cache_dir`;

copy("t/data/hiseq/16756_RunInfo.xml","$runfolder_path/RunInfo.xml") or die "Copy failed: $!"; #to get information that it is paired end

# default human reference needed for alignment for unconsented human split
my $default_source = join q[/],$dir,'references','Homo_sapiens','1000Genomes_hs37d5';
my $default_target = join q[/],$dir,'references','Homo_sapiens','default';
`ln -s $default_source $default_target`;

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

  my $args = {};

  $args->{1001} = qq{bash -c ' mkdir -p $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/tmp_\$LSB_JOBID/16756_1#1 ; cd $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/tmp_\$LSB_JOBID/16756_1#1 && vtfp.pl -s -keys samtools_executable -vals samtools1 -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` -keys indatadir -vals $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/lane1 -keys outdatadir -vals $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1 -keys af_metrics -vals 16756_1#1.bam_alignment_filter_metrics.json -keys rpt -vals 16756_1#1 -keys reference_dict_hs -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/picard/hs37d5.fa.dict -keys hs_reference_genome_fasta -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/fasta/hs37d5.fa -keys phix_reference_genome_fasta -vals $dir/references/PhiX/Illumina/all/fasta/phix-illumina.fa -keys alignment_filter_jar -vals $odir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/AlignmentFilter.jar -keys hs_alignment_reference_genome -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/bwa0_6/hs37d5.fa -keys bwa_executable -vals bwa0_6 -keys alignment_method -vals bwa_mem -keys alignment_hs_method -vals bwa_aln \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_humansplit_notargetalign_template.json > run_16756_1#1.json && viv.pl -s -x -v 3 -o viv_16756_1#1.log run_16756_1#1.json } .
    q{ && perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16756,position=>1,sequence_file=>$ARGV[0],tag_index=>1); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/16756_1#1.cram $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/qc && } .
    q{perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16756,position=>1,sequence_file=>$ARGV[0],subset=>q(phix),tag_index=>1); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/16756_1#1_phix.cram $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/qc && } .
    q{ perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16756,position=>1,sequence_file=>$ARGV[0],subset=>q(human),tag_index=>1); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/16756_1#1_human.cram $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/qc } .
    q{&& qc --check alignment_filter_metrics --qc_in $PWD --id_run 16756 --position 1 } .
    qq{--qc_out $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/qc --tag_index 1 } .
    q{'};
  $args->{1002} = qq{bash -c ' mkdir -p $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/tmp_\$LSB_JOBID/16756_1#2 ; cd $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/tmp_\$LSB_JOBID/16756_1#2 && vtfp.pl -s -keys samtools_executable -vals samtools1 -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` -keys indatadir -vals $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/lane1 -keys outdatadir -vals $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1 -keys af_metrics -vals 16756_1#2.bam_alignment_filter_metrics.json -keys rpt -vals 16756_1#2 -keys reference_dict_hs -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/picard/hs37d5.fa.dict -keys hs_reference_genome_fasta -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/fasta/hs37d5.fa -keys phix_reference_genome_fasta -vals $dir/references/PhiX/Illumina/all/fasta/phix-illumina.fa -keys alignment_filter_jar -vals $odir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/AlignmentFilter.jar -keys hs_alignment_reference_genome -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/bwa0_6/hs37d5.fa -keys bwa_executable -vals bwa0_6 -keys alignment_method -vals bwa_mem -keys alignment_hs_method -vals bwa_aln \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_humansplit_notargetalign_template.json > run_16756_1#2.json && viv.pl -s -x -v 3 -o viv_16756_1#2.log run_16756_1#2.json } .
    q{ && perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16756,position=>1,sequence_file=>$ARGV[0],tag_index=>2); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/16756_1#2.cram $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/qc && } .
    q{perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16756,position=>1,sequence_file=>$ARGV[0],subset=>q(phix),tag_index=>2); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/16756_1#2_phix.cram $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/qc && } .
    q{ perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16756,position=>1,sequence_file=>$ARGV[0],subset=>q(human),tag_index=>2); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/16756_1#2_human.cram $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/qc } .
    q{&& qc --check alignment_filter_metrics --qc_in $PWD --id_run 16756 --position 1 } .
    qq{--qc_out $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/qc --tag_index 2 } .
    q{'};
  $args->{1000} = qq{bash -c ' mkdir -p $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/tmp_\$LSB_JOBID/16756_1#0 ; cd $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/tmp_\$LSB_JOBID/16756_1#0 && vtfp.pl -s -keys samtools_executable -vals samtools1 -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` -keys indatadir -vals $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/lane1 -keys outdatadir -vals $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1 -keys af_metrics -vals 16756_1#0.bam_alignment_filter_metrics.json -keys rpt -vals 16756_1#0 -keys reference_dict_hs -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/picard/hs37d5.fa.dict -keys hs_reference_genome_fasta -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/fasta/hs37d5.fa -keys phix_reference_genome_fasta -vals $dir/references/PhiX/Illumina/all/fasta/phix-illumina.fa -keys alignment_filter_jar -vals $odir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/AlignmentFilter.jar -keys hs_alignment_reference_genome -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/bwa0_6/hs37d5.fa -keys bwa_executable -vals bwa0_6 -keys alignment_method -vals bwa_mem -keys alignment_hs_method -vals bwa_aln \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_humansplit_notargetalign_template.json > run_16756_1#0.json && viv.pl -s -x -v 3 -o viv_16756_1#0.log run_16756_1#0.json } .
    q{ && perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16756,position=>1,sequence_file=>$ARGV[0],tag_index=>0); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/16756_1#0.cram $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/qc && } .
    q{perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16756,position=>1,sequence_file=>$ARGV[0],subset=>q(phix),tag_index=>0); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/16756_1#0_phix.cram $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/qc && } .
    q{ perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16756,position=>1,sequence_file=>$ARGV[0],subset=>q(human),tag_index=>0); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/16756_1#0_human.cram $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/qc } .
    q{&& qc --check alignment_filter_metrics --qc_in $PWD --id_run 16756 --position 1 } .
    qq{--qc_out $dir/150701_HS36_16756_B_C711RANXX/Data/Intensities/BAM_basecalls_20150707-132329/no_cal/archive/lane1/qc --tag_index 0 } .
    q{'};

  lives_ok {$hs_gen->_generate_command_arguments([1])}
     'no error generating hs command arguments';

my @a1 = (split / /, $args->{1002});
my @a2 = (split / /, $hs_gen->_job_args->{1002});
for my $i (0..$#a1) {
	if($a1[$i] ne $a2[$i]) {
		print "DIFF AT ELEM $i:\n$a1[$i]\n$a2[$i]\n";
	}
}

  cmp_deeply ($hs_gen->_job_args, $args,
    'correct command arguments for HiSeq lane 16756_1');
}

{  ##MiSeq, run 16866_1 (nonconsented human split, target alignment)

#################
# already exists?
#################
### my $ref_dir = join q[/],$dir,'references','Plasmodium_falciparum','3D7_Oct11v3','all';
### `mkdir -p $ref_dir/fasta`;
### `mkdir $ref_dir/bwa0_6`;
### `mkdir $ref_dir/picard`;
#################
# already exists?
#################

my $runfolder = q{150713_MS8_16866_A_MS3734403-300V2};
my $runfolder_path = join q[/], $dir, $runfolder;
my $bc_path = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150714-133929/no_cal';
`mkdir -p $bc_path`;
my $cache_dir = join q[/], $runfolder_path, 'Data/Intensities/BAM_basecalls_20150714-133929/metadata_cache_16866';
`mkdir $cache_dir`;

copy("t/data/miseq/16866_RunInfo.xml","$runfolder_path/RunInfo.xml") or die "Copy failed: $!"; #to get information that it is paired end

# default human reference needed for alignment for unconsented human split (should already be done)

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

  my $args = {};

  $args->{1001} = qq{bash -c ' mkdir -p $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/tmp_\$LSB_JOBID/16866_1#1 ; cd $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/tmp_\$LSB_JOBID/16866_1#1 && vtfp.pl -s -keys samtools_executable -vals samtools1 -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` -keys indatadir -vals $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/lane1 -keys outdatadir -vals $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1 -keys af_metrics -vals 16866_1#1.bam_alignment_filter_metrics.json -keys rpt -vals 16866_1#1 -keys reference_dict -vals $dir/references/Plasmodium_falciparum/3D7_Oct11v3/all/picard/Pf3D7_v3.fasta.dict -keys reference_dict_hs -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/picard/hs37d5.fa.dict -keys reference_genome_fasta -vals $dir/references/Plasmodium_falciparum/3D7_Oct11v3/all/fasta/Pf3D7_v3.fasta -keys hs_reference_genome_fasta -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/fasta/hs37d5.fa -keys phix_reference_genome_fasta -vals $dir/references/PhiX/Illumina/all/fasta/phix-illumina.fa -keys alignment_filter_jar -vals $odir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/AlignmentFilter.jar -keys alignment_reference_genome -vals $dir/references/Plasmodium_falciparum/3D7_Oct11v3/all/bwa0_6/Pf3D7_v3.fasta -keys hs_alignment_reference_genome -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/bwa0_6/hs37d5.fa -keys bwa_executable -vals bwa0_6 -keys alignment_method -vals bwa_mem -keys alignment_hs_method -vals bwa_aln \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_humansplit_template.json > run_16866_1#1.json && viv.pl -s -x -v 3 -o viv_16866_1#1.log run_16866_1#1.json } .
    q{ && perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16866,position=>1,sequence_file=>$ARGV[0],tag_index=>1); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/16866_1#1.cram $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/qc && } .
    q{perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16866,position=>1,sequence_file=>$ARGV[0],subset=>q(phix),tag_index=>1); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/16866_1#1_phix.cram $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/qc && } .
    q{ perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16866,position=>1,sequence_file=>$ARGV[0],subset=>q(human),tag_index=>1); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/16866_1#1_human.cram $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/qc } .
    q{&& qc --check alignment_filter_metrics --qc_in $PWD --id_run 16866 --position 1 } .
    qq{--qc_out $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/qc --tag_index 1 } .
    q{'};
  $args->{1002} = qq{bash -c ' mkdir -p $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/tmp_\$LSB_JOBID/16866_1#2 ; cd $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/tmp_\$LSB_JOBID/16866_1#2 && vtfp.pl -s -keys samtools_executable -vals samtools1 -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` -keys indatadir -vals $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/lane1 -keys outdatadir -vals $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1 -keys af_metrics -vals 16866_1#2.bam_alignment_filter_metrics.json -keys rpt -vals 16866_1#2 -keys reference_dict -vals $dir/references/Plasmodium_falciparum/3D7_Oct11v3/all/picard/Pf3D7_v3.fasta.dict -keys reference_dict_hs -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/picard/hs37d5.fa.dict -keys reference_genome_fasta -vals $dir/references/Plasmodium_falciparum/3D7_Oct11v3/all/fasta/Pf3D7_v3.fasta -keys hs_reference_genome_fasta -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/fasta/hs37d5.fa -keys phix_reference_genome_fasta -vals $dir/references/PhiX/Illumina/all/fasta/phix-illumina.fa -keys alignment_filter_jar -vals $odir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/AlignmentFilter.jar -keys alignment_reference_genome -vals $dir/references/Plasmodium_falciparum/3D7_Oct11v3/all/bwa0_6/Pf3D7_v3.fasta -keys hs_alignment_reference_genome -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/bwa0_6/hs37d5.fa -keys bwa_executable -vals bwa0_6 -keys alignment_method -vals bwa_mem -keys alignment_hs_method -vals bwa_aln \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_humansplit_template.json > run_16866_1#2.json && viv.pl -s -x -v 3 -o viv_16866_1#2.log run_16866_1#2.json } .
    q{ && perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16866,position=>1,sequence_file=>$ARGV[0],tag_index=>2); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/16866_1#2.cram $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/qc && } .
    q{perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16866,position=>1,sequence_file=>$ARGV[0],subset=>q(phix),tag_index=>2); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/16866_1#2_phix.cram $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/qc && } .
    q{ perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16866,position=>1,sequence_file=>$ARGV[0],subset=>q(human),tag_index=>2); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/16866_1#2_human.cram $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/qc } .
    q{&& qc --check alignment_filter_metrics --qc_in $PWD --id_run 16866 --position 1 } .
    qq{--qc_out $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/qc --tag_index 2 } .
    q{'};
  $args->{1000} = qq{bash -c ' mkdir -p $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/tmp_\$LSB_JOBID/16866_1#0 ; cd $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/tmp_\$LSB_JOBID/16866_1#0 && vtfp.pl -s -keys samtools_executable -vals samtools1 -keys cfgdatadir -vals \$(dirname \$(readlink -f \$(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2` -keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2` -keys indatadir -vals $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/lane1 -keys outdatadir -vals $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1 -keys af_metrics -vals 16866_1#0.bam_alignment_filter_metrics.json -keys rpt -vals 16866_1#0 -keys reference_dict -vals $dir/references/Plasmodium_falciparum/3D7_Oct11v3/all/picard/Pf3D7_v3.fasta.dict -keys reference_dict_hs -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/picard/hs37d5.fa.dict -keys reference_genome_fasta -vals $dir/references/Plasmodium_falciparum/3D7_Oct11v3/all/fasta/Pf3D7_v3.fasta -keys hs_reference_genome_fasta -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/fasta/hs37d5.fa -keys phix_reference_genome_fasta -vals $dir/references/PhiX/Illumina/all/fasta/phix-illumina.fa -keys alignment_filter_jar -vals $odir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/AlignmentFilter.jar -keys alignment_reference_genome -vals $dir/references/Plasmodium_falciparum/3D7_Oct11v3/all/bwa0_6/Pf3D7_v3.fasta -keys hs_alignment_reference_genome -vals $dir/references/Homo_sapiens/1000Genomes_hs37d5/all/bwa0_6/hs37d5.fa -keys bwa_executable -vals bwa0_6 -keys alignment_method -vals bwa_mem -keys alignment_hs_method -vals bwa_aln \$(dirname \$(dirname \$(readlink -f \$(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_humansplit_template.json > run_16866_1#0.json && viv.pl -s -x -v 3 -o viv_16866_1#0.log run_16866_1#0.json } .
    q{ && perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16866,position=>1,sequence_file=>$ARGV[0],tag_index=>0); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/16866_1#0.cram $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/qc && } .
    q{perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16866,position=>1,sequence_file=>$ARGV[0],subset=>q(phix),tag_index=>0); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/16866_1#0_phix.cram $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/qc && } .
    q{ perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>16866,position=>1,sequence_file=>$ARGV[0],subset=>q(human),tag_index=>0); $o->execute(); $o->store($ARGV[-1]) '"'"' } .
    qq{$dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/16866_1#0_human.cram $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/qc } .
    q{&& qc --check alignment_filter_metrics --qc_in $PWD --id_run 16866 --position 1 } .
    qq{--qc_out $dir/150713_MS8_16866_A_MS3734403-300V2/Data/Intensities/BAM_basecalls_20150714-133929/no_cal/archive/lane1/qc --tag_index 0 } .
    q{'};

  lives_ok {$ms_gen->_generate_command_arguments([1])}
     'no error generating ms command arguments';

my @a1 = (split / /, $args->{1002});
my @a2 = (split / /, $ms_gen->_job_args->{1002});
for my $i (0..$#a1) {
	if($a1[$i] ne $a2[$i]) {
		print "DIFF AT ELEM $i:\n$a1[$i]\n$a2[$i]\n";
	}
}

  cmp_deeply ($ms_gen->_job_args, $args,
    'correct command arguments for HiSeq lane 16866_1');
}

1;
