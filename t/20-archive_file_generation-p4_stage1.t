use strict;
use warnings;
use Test::More tests => 3;
use Test::Exception;
use Test::Deep;
use Test::Differences;
use File::Copy;
use File::Path qw(make_path);
use Cwd;
use Perl6::Slurp;
use JSON;
use Data::Dumper;
use t::util;

my $util = t::util->new();
my $dir = $util->temp_directory();
$ENV{TEST_DIR} = $dir;
$ENV{TEST_FS_RESOURCE} = q{nfs_12};
local $ENV{NPG_WEBSERVICE_CACHE_DIR} = 't/data/p4_stage1_analysis';
local $ENV{CLASSPATH} = q{t/bin/software/solexa/bin/aligners/illumina2bam/current};
local $ENV{PATH} = join q[:], q[t/bin], q[t/bin/software/solexa/bin], $ENV{PATH};

use_ok('npg_pipeline::archive::file::generation::p4_stage1_analysis');
my $current = getcwd();

my $new = "$dir/1234_samplesheet.csv";
`cp -r t/data/p4_stage1_analysis/* $dir`;
local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = $new;
local $ENV{NPG_WEBSERVICE_CACHE_DIR} = $dir;

#################################
# mock references
#################################
my $repos_root = $dir . q{/srpipe_references};
`mkdir -p $repos_root/references/PhiX/default/all/bwa0_6`;
`mkdir -p $repos_root/references/PhiX/default/all/fasta`;
`touch $repos_root/references/PhiX/default/all/bwa0_6/phix_unsnipped_short_no_N.fa`;
`touch $repos_root/references/PhiX/default/all/fasta/phix_unsnipped_short_no_N.fa`;

$util->create_analysis();
my $runfolder = $util->analysis_runfolder_path() . '/';
`cp t/data/runfolder/Data/RunInfo.xml $runfolder`;

my $bc_path = q{/nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BaseCalls};

my $bam_generator = npg_pipeline::archive::file::generation::p4_stage1_analysis->new(
    function_list                 => q{t/data/config_files/function_list_p4_stage1.yml},
    conf_path                     => q{t/data/config_files},
    run_folder                    => q{123456_IL2_1234},
    repository                    => $repos_root,
    runfolder_path                => $util->analysis_runfolder_path(),
    timestamp                     => q{20090709-123456},
    verbose                       => 0,
    no_bsub                       => 1,
    id_run                        => 1234,
    _extra_tradis_transposon_read => 1,
    bam_basecall_path             => $util->analysis_runfolder_path() . q{/Data/Intensities/BaseCalls},
   _job_args                      => { _param_vals => { 1=> {}, }},
  );

subtest 'basics' => sub {
  plan tests => 7;

  lives_ok { $bam_generator } q{no croak creating bam_generator object};

  isa_ok($bam_generator, q{npg_pipeline::archive::file::generation::p4_stage1_analysis}, q{$bam_generator});
  is($bam_generator->_extra_tradis_transposon_read, 1, 'TraDIS set');
  $bam_generator->_extra_tradis_transposon_read(0);
  is($bam_generator->_extra_tradis_transposon_read, 0, 'TraDIS not set');
  isa_ok($bam_generator->lims, 'st::api::lims', 'cached lims object');

  my $arg_refs = {
    required_job_completion => q{-w'done(123) && done(321)'}, 
  };
  
  my $alims = $bam_generator->lims->children_ia;
  my $position = 8;

  my $bsub_command = $bam_generator->_command2submit($arg_refs->{required_job_completion});

  is($bam_generator->_get_number_of_plexes_excluding_control($alims->{$position}), 2, 'correct number of plexes');

  $bsub_command = $util->drop_temp_part_from_paths($bsub_command);

  my $expected_cmd = q{bsub -q srpipeline -E 'npg_pipeline_preexec_references --repository /srpipe_references' -R 'select[mem>12000] rusage[mem=12000,nfs_12=5]' -M12000 -R 'span[hosts=1]' -n8,16 -w'done(123) && done(321)' -J 'p4_stage1_analysis_1234_20090709-123456[1]' -o } . $bc_path . q{/log/p4_stage1_analysis_1234_20090709-123456.%I.%J.out 'perl -Mstrict -MJSON -MFile::Slurp -Mopen='"'"':encoding(UTF8)'"'"' -e '"'"'exec from_json(read_file shift@ARGV)->{shift@ARGV} or die q(failed exec)'"'"' } . $bc_path . q{/p4_stage1_analysis_1234_20090709-123456_$LSB_JOBID $LSB_JOBINDEX'};

  eq_or_diff([split" ",$bsub_command], [split" ",$expected_cmd], 'correct bsub command for lane 8');
};

subtest 'check_save_arguments' => sub {
  plan tests => 9;
 
  lives_ok { $bam_generator } q{no croak creating bam_generator object};
  isa_ok($bam_generator, q{npg_pipeline::archive::file::generation::p4_stage1_analysis}, q{$bam_generator});
 
  my $jnr = $bam_generator->job_name_root;
  my $bbp = $bam_generator->bam_basecall_path;
 
  lives_ok{$bam_generator->generate} 'bam_generator generate';
  my $fname;
  lives_ok{$fname = $bam_generator->_save_arguments(42)} 'fetching stage1 analysis file';
  is ($util->drop_temp_part_from_paths($fname), $bc_path. q{/p4_stage1_analysis_1234_20090709-123456_42}, 'file name correct');
  ok (-e $fname, 'file exists');
  my $contents = slurp($fname);

  my $h = from_json($contents);

  my $intensities_dir = $dir . '/nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities';
  my $expected = {
          '1' => 'bash -c \' cd ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane1/log && vtfp.pl -splice_nodes \'"\'"\'bamadapterfind:-bamcollate:\'"\'"\' -prune_nodes \'"\'"\'fs1p_tee_split:__SPLIT_BAM_OUT__-\'"\'"\' -o run_1234_1.json -param_vals ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane1/param_files/1234_1_p4s1_pv_in.json -export_param_vals 1234_1_p4s1_pv_out_${LSB_JOBID}.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads`  $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_1.log run_1234_1.json && qc --check spatial_filter --id_run 1234 --position 1 --qc_out ' . $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/archive/qc < ' . $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/1234_1.bam.filter.stats \'',
          '2' => 'bash -c \' cd ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane2/log && vtfp.pl -splice_nodes \'"\'"\'bamadapterfind:-bamcollate:\'"\'"\' -prune_nodes \'"\'"\'fs1p_tee_split:__SPLIT_BAM_OUT__-\'"\'"\' -o run_1234_2.json -param_vals ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane2/param_files/1234_2_p4s1_pv_in.json -export_param_vals 1234_2_p4s1_pv_out_${LSB_JOBID}.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads`  $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_2.log run_1234_2.json && qc --check spatial_filter --id_run 1234 --position 2 --qc_out ' . $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/archive/qc < ' . $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/1234_2.bam.filter.stats \'',
          '3' => 'bash -c \' cd ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane3/log && vtfp.pl -splice_nodes \'"\'"\'bamadapterfind:-bamcollate:\'"\'"\' -prune_nodes \'"\'"\'fs1p_tee_split:__SPLIT_BAM_OUT__-\'"\'"\' -o run_1234_3.json -param_vals ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane3/param_files/1234_3_p4s1_pv_in.json -export_param_vals 1234_3_p4s1_pv_out_${LSB_JOBID}.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads`  $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_3.log run_1234_3.json && qc --check spatial_filter --id_run 1234 --position 3 --qc_out ' . $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/archive/qc < ' . $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/1234_3.bam.filter.stats \'',
          '4' => 'bash -c \' cd ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane4/log && vtfp.pl -splice_nodes \'"\'"\'bamadapterfind:-bamcollate:\'"\'"\' -prune_nodes \'"\'"\'fs1p_tee_split:__SPLIT_BAM_OUT__-\'"\'"\' -o run_1234_4.json -param_vals ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane4/param_files/1234_4_p4s1_pv_in.json -export_param_vals 1234_4_p4s1_pv_out_${LSB_JOBID}.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads`  $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_4.log run_1234_4.json && qc --check spatial_filter --id_run 1234 --position 4 --qc_out ' . $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/archive/qc < ' . $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/1234_4.bam.filter.stats \'',
          '5' => 'bash -c \' cd ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane5/log && vtfp.pl -splice_nodes \'"\'"\'bamadapterfind:-bamcollate:\'"\'"\' -prune_nodes \'"\'"\'fs1p_tee_split:__SPLIT_BAM_OUT__-\'"\'"\' -o run_1234_5.json -param_vals ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane5/param_files/1234_5_p4s1_pv_in.json -export_param_vals 1234_5_p4s1_pv_out_${LSB_JOBID}.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads`  $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_5.log run_1234_5.json && qc --check spatial_filter --id_run 1234 --position 5 --qc_out ' . $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/archive/qc < ' . $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/1234_5.bam.filter.stats \'',
          '6' => 'bash -c \' cd ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane6/log && vtfp.pl -splice_nodes \'"\'"\'bamadapterfind:-bamcollate:\'"\'"\' -prune_nodes \'"\'"\'fs1p_tee_split:__SPLIT_BAM_OUT__-\'"\'"\' -o run_1234_6.json -param_vals ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane6/param_files/1234_6_p4s1_pv_in.json -export_param_vals 1234_6_p4s1_pv_out_${LSB_JOBID}.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads`  $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_6.log run_1234_6.json && qc --check spatial_filter --id_run 1234 --position 6 --qc_out ' . $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/archive/qc < ' . $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/1234_6.bam.filter.stats \'',
          '7' => 'bash -c \' cd ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane7/log && vtfp.pl -splice_nodes \'"\'"\'bamadapterfind:-bamcollate:\'"\'"\' -prune_nodes \'"\'"\'fs1p_tee_split:__SPLIT_BAM_OUT__-\'"\'"\' -o run_1234_7.json -param_vals ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane7/param_files/1234_7_p4s1_pv_in.json -export_param_vals 1234_7_p4s1_pv_out_${LSB_JOBID}.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads`  $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_7.log run_1234_7.json && qc --check spatial_filter --id_run 1234 --position 7 --qc_out ' . $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/archive/qc < ' . $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/1234_7.bam.filter.stats \'',
          '8' => 'bash -c \' cd ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane8/log && vtfp.pl   -o run_1234_8.json -param_vals ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane8/param_files/1234_8_p4s1_pv_in.json -export_param_vals 1234_8_p4s1_pv_out_${LSB_JOBID}.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads`  $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_8.log run_1234_8.json && qc --check spatial_filter --id_run 1234 --position 8 --qc_out ' . $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/archive/qc < ' . $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/1234_8.bam.filter.stats \'',
  };
 
  cmp_deeply($h, $expected, 'correct json file content (for p4 stage1 file)');

  my $pfname = $dir . $bc_path. q[/p4_stage1_analysis/lane1/param_files/1234_1_p4s1_pv_in.json];
  ok (-e $pfname, 'params file exists');
  $contents = slurp($pfname);

  $h = from_json($contents);

  $expected = {
     'assign' => [
        {
	  'bid_implementation' => 'bambi',
	  'seqchksum_file' => $intensities_dir . '/BaseCalls/1234_1.post_i2b.seqchksum',
	  'scramble_reference_fasta' => $dir . '/srpipe_references/references/PhiX/default/all/fasta/phix_unsnipped_short_no_N.fa',
	  'i2b_rg' => '1234_1',
	  'spatial_filter_stats' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/1234_1.bam.filter.stats',
	  'i2b_pu' => '123456_IL2_1234_1',
	  'tileviz_dir' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/archive/qc/tileviz/1234_1',
	  'i2b_implementation' => 'java',
	  'reference_phix' => $dir . '/srpipe_references/references/PhiX/default/all/bwa0_6/phix_unsnipped_short_no_N.fa',
	  'unfiltered_cram_file' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/1234_1.unfiltered.cram',
	  'qc_check_qc_out_dir' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/archive/qc',
	  'i2b_lane' => '1',
	  'bwa_executable' => 'bwa0_6',
	  'filtered_bam' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/1234_1.bam',
	  'samtools_executable' => 'samtools1',
	  'i2b_library_name' => '51021',
	  'outdatadir' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal',
	  'i2b_run_path' => $dir . q[/nfs/sf45/IL2/analysis/123456_IL2_1234],
	  'teepot_tempdir' => '.',
	  'split_prefix' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/lane1',
	  'illumina2bam_jar' => $current . '/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/Illumina2bam.jar',
	  'i2b_intensity_dir' => $intensities_dir,
	  'i2b_sample_aliases' => 'SRS000147',
	  'phix_alignment_method' => 'bwa_aln_se',
	  'spatial_filter_file' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/1234_1.bam.filter',
	  'md5filename' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/1234_1.bam.md5',
	  'teepot_mval' => '2G',
	  'i2b_runfolder' => '123456_IL2_1234',
	  'i2b_study_name' => '"SRP000031: 1000Genomes Project Pilot 1"',
	  'i2b_basecalls_dir' => $intensities_dir . '/BaseCalls',
	  'teepot_wval' => '500',
	  'qc_check_qc_in_dir' => $intensities_dir . '/BaseCalls',
	  'qc_check_id_run' => '1234',
        },
    ],
  };

  cmp_deeply($h, $expected, 'correct json file content (for p4 stage1 params file)');

 };

1;

