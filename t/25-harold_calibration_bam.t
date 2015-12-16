use strict;
use warnings;
use Test::More tests => 39;
use Test::Exception::LessClever;
use t::util;
use Cwd;

my $util = t::util->new();

my $curdir = getcwd();
my $repos = join q[/], $curdir, 't/data/sequence';

my $tdir = $util->temp_directory();
$ENV{TEST_DIR} = $tdir;
$ENV{TEST_FS_RESOURCE} = q{nfs_12};
$ENV{NPG_WEBSERVICE_CACHE_DIR} = $curdir . q{/t/data};

my $sp = join q[/], $tdir, 'spatial_filter';
my $java = join q[/], $tdir, 'java';
foreach my $tool (($sp, $java)) {
  `touch $tool`;
  `chmod +x $tool`;
}
local $ENV{PATH} = join q[:], qq[$curdir/t/bin], $tdir, $ENV{PATH};
local $ENV{CLASSPATH} = q{t/bin/software/solexa/bin/aligners/illumina2bam/current};

my $id_run;
my $mem_units = 'MB';

use_ok(q{npg_pipeline::analysis::harold_calibration_bam});

my $runfolder_path = $util->analysis_runfolder_path();
my $bustard_home   = qq{$runfolder_path/Data/Intensities};
my $bustard_rta    = qq{$bustard_home/Bustard_RTA};
my $gerald_rta     = qq{$bustard_rta/GERALD_RTA};
my $config_path    = qq{$runfolder_path/Config};

sub set_staging_analysis_area {
  `rm -rf /tmp/nfs/sf45`;
  `mkdir -p $bustard_rta`;
  `mkdir $config_path`;
  `cp t/data/Recipes/Recipe_GA2_37Cycle_PE_v6.1.xml $runfolder_path/`;
  `cp t/data/Recipes/TileLayout.xml $config_path/`;
  return 1;
}

{
  set_staging_analysis_area();
  my $harold;
  lives_ok {
    $harold = npg_pipeline::analysis::harold_calibration_bam->new({
      id_run => 1234,
      run_folder => q{123456_IL2_1234},
      runfolder_path => $runfolder_path,
      timestamp => q{20091028-101635},
      log_file_path => $runfolder_path,
      log_file_name => q{npg_pipeline_pb_cal_20091028-101635.log},
      verbose => 0,
      repository => $repos,
      no_bsub => 1,
      recalibration => 1,
    });
  } q{create $harold object ok};

  isa_ok($harold, q{npg_pipeline::analysis::harold_calibration_bam}, q{$harold});
  is($harold->pb_calibration_bin, $tdir, 'pb calibration bin is correct');
  is($harold->spatial_filter_path, $sp, 'spatial filter path is correct');
}

{
  set_staging_analysis_area();
  my $harold;
  lives_ok {
    $harold = npg_pipeline::analysis::harold_calibration_bam->new({
      id_run => 1234,
      run_folder => q{123456_IL2_1234},
      runfolder_path => $runfolder_path,
      timestamp => q{20091028-101635},
      verbose => 0,
      repository => $repos,
      dif_files_path   => $bustard_home,
      spatial_filter => 1,
      no_bsub => 1,
      recalibration => 1,
      force_phix_split => 0,
    });
  } q{create $harold object ok};

  my $req_job_completion = q{-w'done(123) && done(321)'};
  my $arg_refs = {
    required_job_completion => $req_job_completion,
  };

  my @job_ids;
  my $mem = 3072;
  my $mem_limit = npg_pipeline::lsf_job->new(memory => $mem, memory_units =>$mem_units)->_scale_mem_limit();
  my $job = qq{bsub -q srpipeline -E 'npg_pipeline_preexec_references --repository $curdir/t/data/sequence' -o $bustard_rta/PB_cal/log/PB_cal_table_1234_4_20091028-101635.%J.out -J PB_cal_table_1234_4_20091028-101635 -R 'select[mem>}.$mem.q{] rusage[mem=}.$mem.q{,nfs_12=8]' -M}.$mem_limit.qq{ -R 'span[hosts=1]' -w'done(123) && done(321)' " cd $bustard_rta/PB_cal && $tdir/pb_calibration --intensity_dir $bustard_home --t_filter 2 --prefix 1234_4 --cstart 1 --bam pb_align_1234_4.bam "};
  lives_ok {
    @job_ids = $harold->generate_calibration_table( $arg_refs );
  } q{no croak submitting calibration table jobs};

  is( scalar @job_ids, 1, q{8 jobs created});

  is( $harold->_calibration_table_bsub_command( {
   dir => $bustard_rta,
   position => 4,
   job_dependencies => $req_job_completion,
   ref_seq => q{phix-illumina.fa},
  } ), $job, q{generated bsub command is correct} );

  my $cal_table = q{1234_4_purity_cycle_caltable.txt};
  is( $harold->calibration_table_name( { id_run => 1234, position=>4 } ), $cal_table, q{generated calibration table name is correct});
  $mem = 1725;
  $mem_limit = npg_pipeline::lsf_job->new(memory => $mem, memory_units =>$mem_units)->_scale_mem_limit();
  $job = qq{bsub -q srpipeline -o $bustard_rta/PB_cal/log/PB_cal_score_1234_3_20091028-101635.%J.out -J PB_cal_score_1234_3_20091028-101635 -R 'select[mem>}.$mem.q{] rusage[mem=}.$mem.q{,nfs_12=8]' -M}.$mem_limit.qq{ -R 'span[hosts=1]' -w'done(123) && done(321)' ' cd $bustard_rta/PB_cal && bash -c '"'"'if [[ -f pb_align_1234_3.bam ]]; then echo phix alignment so merging alignments with 1>&2; set -o pipefail; (if [ -f 1234_4_purity_cycle_caltable.txt ]; then echo  recalibrated qvals 1>&2; $tdir/pb_predictor --u --bam ../1234_3.bam --intensity_dir $bustard_home --cstart 1 --ct 1234_4_purity_cycle_caltable.txt ; else echo no recalibration 1>&2; cat ../1234_3.bam ; fi;) |  ( if [[ -f pb_align_1234_3.bam.filter ]]; then echo applying spatial filter 1>&2; $sp -u -a -f -F pb_align_1234_3.bam.filter - 2> >( tee /dev/stderr | qc --check spatial_filter --id_run 1234 --position 3 --qc_out $bustard_home/Bustard_RTA/PB_cal/archive/qc ); else echo no spatial filter 1>&2; cat; fi;) |  $java -Xmx1024m -jar $curdir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/BamMerger.jar CREATE_MD5_FILE=true VALIDATION_STRINGENCY=SILENT KEEP=true I=/dev/stdin REPLACE_QUAL=true O=1234_3.bam ALIGNED=pb_align_1234_3.bam; else echo symlinking as no phix alignment 1>&2; rm -f 1234_3.bam; ln -s ../1234_3.bam 1234_3.bam; rm -f 1234_3.bam.md5; ln -s ../1234_3.bam.md5 1234_3.bam.md5; fi'"'"' '};
  my $expect_job = $harold->_recalibration_bsub_command( {
    position => 3,
    job_dependencies => $req_job_completion,
    ct => $cal_table,
  } );
  is($expect_job, $job, q{generated bsub command for recalibration job is correct});

  lives_ok {
    @job_ids = $harold->generate_recalibrated_bam($arg_refs);
  } q{no croak submitting recalibration jobs};
  is( scalar @job_ids, 8, q{8 jobs created});
}

{
  set_staging_analysis_area();
  my $harold;
  lives_ok {
    $harold = npg_pipeline::analysis::harold_calibration_bam->new({
      id_run => 8797,
      run_folder => q{121112_HS20_08797_A_C18TEACXX},
      runfolder_path => $runfolder_path,
      timestamp => q{20121112-123456},
      verbose => 0,
      repository => $repos,
      dif_files_path   => $bustard_home,
      spatial_filter => 1,
      no_bsub => 1,
      recalibration => 1,
    });
  } q{create $harold object ok};

  my $req_job_completion = q{-w'done(123) && done(321)'};
  my $arg_refs = {
    required_job_completion => $req_job_completion,
  };

  my @job_ids;
  my $mem = 3072;
  my $mem_limit = npg_pipeline::lsf_job->new(memory => $mem, memory_units =>$mem_units)->_scale_mem_limit();
  my $job = qq{bsub -q srpipeline -E 'npg_pipeline_preexec_references --repository $curdir/t/data/sequence' -o $bustard_rta/PB_cal/log/PB_cal_table_8797_8_20121112-123456.%J.out -J PB_cal_table_8797_8_20121112-123456 -R 'select[mem>}.$mem.q{] rusage[mem=}.$mem.q{,nfs_12=8]' -M}.$mem_limit.qq{ -R 'span[hosts=1]' -w'done(123) && done(321)' " cd $bustard_rta/PB_cal && $tdir/pb_calibration --intensity_dir $bustard_home --t_filter 2 --prefix 8797_8 --cstart 11 --bam pb_align_8797_8.bam "};

  lives_ok {
    @job_ids = $harold->generate_calibration_table( $arg_refs );
  } q{no croak submitting calibration table jobs};

  is( scalar @job_ids, 8, q{8 jobs created});

  is( $harold->_calibration_table_bsub_command( {
   dir => $bustard_rta,
   position => 8,
   job_dependencies => $req_job_completion,
   ref_seq => q{phix-illumina.fa},
  } ), $job, q{generated bsub command is correct} );

  my $cal_table = q{8797_7_purity_cycle_caltable.txt};
  is( $harold->calibration_table_name( { id_run => 8797, position=>7 } ), $cal_table, q{generated calibration table name is correct});
  $mem = 1725;
  $mem_limit = npg_pipeline::lsf_job->new(memory => $mem, memory_units =>$mem_units)->_scale_mem_limit();
  $job = qq{bsub -q srpipeline -o $bustard_rta/PB_cal/log/PB_cal_score_8797_7_20121112-123456.%J.out -J PB_cal_score_8797_7_20121112-123456 -R 'select[mem>}.$mem.q{] rusage[mem=}.$mem.q{,nfs_12=8]' -M}.$mem_limit.qq{ -R 'span[hosts=1]' -w'done(123) && done(321)' ' cd $bustard_rta/PB_cal && bash -c '"'"'if [[ -f pb_align_8797_7.bam ]]; then echo phix alignment so merging alignments with 1>&2; set -o pipefail; (if [ -f 8797_7_purity_cycle_caltable.txt ]; then echo  recalibrated qvals 1>&2; $tdir/pb_predictor --u --bam ../8797_7.bam --intensity_dir $bustard_home --cstart 11 --ct 8797_7_purity_cycle_caltable.txt ; else echo no recalibration 1>&2; cat ../8797_7.bam ; fi;) |  ( if [[ -f pb_align_8797_7.bam.filter ]]; then echo applying spatial filter 1>&2; $sp -u -a -f -F pb_align_8797_7.bam.filter - 2> >( tee /dev/stderr | qc --check spatial_filter --id_run 8797 --position 7 --qc_out $bustard_home/Bustard_RTA/PB_cal/archive/qc ); else echo no spatial filter 1>&2; cat; fi;) |  $java -Xmx1024m -jar $curdir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/BamMerger.jar CREATE_MD5_FILE=true VALIDATION_STRINGENCY=SILENT KEEP=true I=/dev/stdin REPLACE_QUAL=true O=8797_7.bam ALIGNED=pb_align_8797_7.bam; else echo symlinking as no phix alignment 1>&2; rm -f 8797_7.bam; ln -s ../8797_7.bam 8797_7.bam; rm -f 8797_7.bam.md5; ln -s ../8797_7.bam.md5 8797_7.bam.md5; fi'"'"' '} ;
  my $expect_job = $harold->_recalibration_bsub_command( {
    position => 7,
    job_dependencies => $req_job_completion,
    ct => $cal_table,
  } );
  is($expect_job, $job, q{generated bsub command for recalibration job is correct});

  lives_ok {
    @job_ids = $harold->generate_recalibrated_bam($arg_refs);
  } q{no croak submitting recalibration jobs};
  is( scalar @job_ids, 8, q{8 jobs created});
}

{
  set_staging_analysis_area();
  my $harold;
  $id_run = 4846;
  lives_ok {
    $harold = npg_pipeline::analysis::harold_calibration_bam->new({
      id_run => $id_run,
      run_folder => q{123456_IL2_1234},
      runfolder_path => $runfolder_path,
      timestamp => q{20091028-101635},
      verbose => 0,
      repository => $repos,
      bam_basecall_path => $runfolder_path . q{/Data/Intensities/BaseCalls},
      no_bsub => 1,
      recalibration => 1,
      force_phix_split => 0,
    });
  } q{create $harold object ok};

  isa_ok($harold, q{npg_pipeline::analysis::harold_calibration_bam}, q{$harold});

  my @job_ids = $harold->generate_alignment_files({});
  is( scalar @job_ids, 0, q{no job ids for alignment as no spiked phix lane} );

  @job_ids = $harold->generate_calibration_table({});
  is( scalar @job_ids, 0, q{no job ids for calibration table as no spiked phix lane} );

  @job_ids = $harold->generate_recalibrated_bam({});
  is( scalar @job_ids, 8, q{8 job ids for recalibration even if no spiked phix lane} );
}

{
  set_staging_analysis_area();
  my $harold;
  $id_run = 4846;
  lives_ok {
    $harold = npg_pipeline::analysis::harold_calibration_bam->new({
      id_run => $id_run,
      run_folder => q{123456_IL2_1234},
      runfolder_path => $runfolder_path,
      timestamp => q{20091028-101635},
      verbose => 0,
      repository => $repos,
      bam_basecall_path => $runfolder_path . q{/Data/Intensities/BaseCalls},
      no_bsub => 1,
      recalibration => 1,
      force_phix_split => 1,
    });
  } q{create $harold object ok};

  isa_ok($harold, q{npg_pipeline::analysis::harold_calibration_bam}, q{$harold});

  my @job_ids = $harold->generate_alignment_files({});
  is( scalar @job_ids, 8, q{8 job ids for alignment as no spiked phix lane but force phix split} );

  @job_ids = $harold->generate_calibration_table({});
  is( scalar @job_ids, 8, q{8 job ids for calibration table as no spiked phix lane but force phix split} );

  @job_ids = $harold->generate_recalibrated_bam({});
  is( scalar @job_ids, 8, q{8 job ids for recalibration even if no spiked phix lane but force phix split} );
}

{
  set_staging_analysis_area();
  my $harold;
  $id_run = 1234;
  lives_ok {
    $harold = npg_pipeline::analysis::harold_calibration_bam->new({
      id_run => $id_run,
      run_folder => q{123456_IL2_1234},
      runfolder_path => $runfolder_path,
      timestamp => q{20091028-101635},
      verbose => 0,
      repository => $repos,
      dif_files_path => $runfolder_path . q{/Data/Intensities},
      bam_basecall_path => $runfolder_path . q{/Data/Intensities/BaseCalls},
      no_bsub => 1,
      spatial_filter => 1,
      recalibration => 1,
      force_phix_split => 0,
    });
  } q{create $harold object ok};

  my $arg_refs = {
    timestamp => q{20091028-101635},
    position => 1,
    job_dependencies => q{-w 'done(1234) && done(4321)'},
    ref_seq => q{t/data/sequence/references/Human/default/all/bwa/someref.fa.bwt},
  };

  my $mem = 16000;
  my $mem_limit = npg_pipeline::lsf_job->new(memory => $mem, memory_units =>$mem_units)->_scale_mem_limit();
  my $single_read_alignment_command = qq{bsub -q srpipeline -E 'npg_pipeline_preexec_references --repository $curdir/t/data/sequence' -o $bustard_rta/PB_cal/}.q{log/PB_cal_align_1234_1_20091028-101635.%J.out -J PB_cal_align_1234_1_20091028-101635 -R 'select[mem>}.$mem.q{] rusage[mem=}.$mem.q{,nfs_12=4]' -M}.$mem_limit.q{ -R 'span[hosts=1]' -n 6,12 -w 'done(1234) && done(4321)' '} . qq{cd $bustard_rta/PB_cal && $tdir} . q{/pb_align --aln_parms "-t "`npg_pipeline_job_env_to_threads`  --sam_parms "-t "`npg_pipeline_job_env_to_threads --maximum 8`  --spatial_filter --sf_parms "--region_size 200 --region_mismatch_threshold 0.016 --region_insertion_threshold 0.016 --region_deletion_threshold 0.016 --tileviz } . $bustard_home . q{/Bustard_RTA/PB_cal/archive/qc/tileviz/1234_1 " } . qq{--bam_join_jar $curdir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/BamMerger.jar} . q{ --ref t/data/sequence/references/Human/default/all/bwa/someref.fa.bwt --read 0 --bam } . $bustard_home . q{/BaseCalls/1234_1.bam --prefix pb_align_1234_1 --pf_filter'};
  my $paired_read_alignment_command = qq{bsub -q srpipeline -E 'npg_pipeline_preexec_references --repository $curdir/t/data/sequence' -o $bustard_rta/PB_cal/log/PB_cal_align_1234_1_20091028-101635.%J.out -J PB_cal_align_1234_1_20091028-101635 -R 'select[mem>}.$mem.q{] rusage[mem=}.$mem.q{,nfs_12=4]' -M}.$mem_limit.q{ -R 'span[hosts=1]' -n 6,12 -w 'done(1234) && done(4321)' '} . qq{cd $bustard_rta/PB_cal && $tdir} . q{/pb_align --aln_parms "-t "`npg_pipeline_job_env_to_threads`  --sam_parms "-t "`npg_pipeline_job_env_to_threads --maximum 8`  --spatial_filter --sf_parms "--region_size 200 --region_mismatch_threshold 0.016 --region_insertion_threshold 0.016 --region_deletion_threshold 0.016 --tileviz } . $bustard_home . q{/Bustard_RTA/PB_cal/archive/qc/tileviz/1234_1 " } . qq{--bam_join_jar $curdir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/BamMerger.jar} .  qq{ --ref t/data/sequence/references/Human/default/all/bwa/someref.fa.bwt --read1 1 --read2 2 --bam $bustard_home/BaseCalls/1234_1.bam --prefix pb_align_1234_1 --pf_filter'};
  my $spiked_read_alignment_command = qq{bsub -q srpipeline -E 'npg_pipeline_preexec_references --repository $curdir/t/data/sequence' -o $bustard_rta/PB_cal/log/PB_cal_align_1234_1_20091028-101635.%J.out -J PB_cal_align_1234_1_20091028-101635 -R 'select[mem>}.$mem.q{] rusage[mem=}.$mem.q{,nfs_12=4]' -M}.$mem_limit.q{ -R 'span[hosts=1]' -n 6,12 -w 'done(1234) && done(4321)' '} . qq{cd $bustard_rta/PB_cal && $tdir} . q{/pb_align --aln_parms "-t "`npg_pipeline_job_env_to_threads`  --sam_parms "-t "`npg_pipeline_job_env_to_threads --maximum 8`  --spatial_filter --sf_parms "--region_size 200 --region_mismatch_threshold 0.016 --region_insertion_threshold 0.016 --region_deletion_threshold 0.016 --tileviz } . $bustard_home . q{/Bustard_RTA/PB_cal/archive/qc/tileviz/1234_1 " } . qq{--bam_join_jar $curdir/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/BamMerger.jar} .  qq{ --ref t/data/sequence/references/PhiX/default/all/fasta/phix-illumina.fa --read1 1 --read2 2 --bam $bustard_home/BaseCalls/1234_1.bam --prefix pb_align_1234_1 --pf_filter'};

  is( $harold->_alignment_file_bsub_command( $arg_refs ), $single_read_alignment_command, q{single read alignment bsub command is correct} );

  $arg_refs->{is_paired} = 1;
  is( $harold->_alignment_file_bsub_command( $arg_refs ), $paired_read_alignment_command, q{paired read alignment bsub command is correct} );

  $arg_refs->{is_spiked_phix} = 1;
  $arg_refs->{ref_seq} = q{t/data/sequence/references/PhiX/default/all/fasta/phix-illumina.fa};
  is( $harold->_alignment_file_bsub_command( $arg_refs ), $spiked_read_alignment_command, q{paired read alignment bsub command is correct} );

  my @job_ids = $harold->generate_alignment_files({});
  is( scalar @job_ids, 1, q{1 job ids, one spiked phix lane} );
}

{
  set_staging_analysis_area();
  my $harold;
  $id_run = 1234;
  lives_ok {
    $harold = npg_pipeline::analysis::harold_calibration_bam->new({
      id_run => $id_run,
      run_folder => q{123456_IL2_1234},
      runfolder_path => $runfolder_path,
      timestamp => q{20091028-101635},
      verbose => 0,
      repository => $repos,
      bam_basecall_path => $runfolder_path . q{/Data/Intensities/BAM_basecalls},
      no_bsub => 1,
      recalibration => 1,
    });
  } q{create $harold object ok};

  my $arg_refs = {
    timestamp => q{20091028-101635},
    position => 1,
    job_dependencies => q{-w 'done(1234) && done(4321)'},
    ref_seq => q{t/data/sequence/references/Human/default/all/bwa/someref.fa.bwt},
  };
  my $mem = 350;
  my $mem_limit = npg_pipeline::lsf_job->new(memory => $mem, memory_units =>$mem_units)->_scale_mem_limit();
  my $expected_command = q(bsub -q srpipeline -o /nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BAM_basecalls/log/basecall_stats_1234_20091028-101635.%J.out -J basecall_stats_1234_20091028-101635 -R 'select[mem>).$mem.q{] rusage[mem=}.$mem.q{,nfs_12=4]' -M}.$mem_limit.q( -R 'span[hosts=1]' -n 4  " cd /nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BAM_basecalls && if [[ -f Makefile ]]; then echo Makefile already present 1>&2; else echo creating bcl2qseq Makefile 1>&2; /software/solexa/src/OLB-1.9.4/bin/setupBclToQseq.py -b /nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BaseCalls -o /nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BAM_basecalls --overwrite; fi && make -j 4 Matrix Phasing && make -j 4 BustardSummary.x{s,m}l ");
  is( $util->drop_temp_part_from_paths( $harold->_generate_illumina_basecall_stats_command( $arg_refs ) ), $expected_command, q{Illumina basecalls stats generation bsub command is correct} );

  my @job_ids = $harold->generate_illumina_basecall_stats($arg_refs);
  is( scalar @job_ids, 1, q{1 job ids, generate Illumina basecall stats} );
}

1;
