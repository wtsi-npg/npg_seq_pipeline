use strict;
use warnings;
use Test::More tests => 4;
use Test::Exception;
use Cwd;
use File::Copy qw(cp);
use Perl6::Slurp;
use JSON;

use npg_tracking::util::abs_path qw(abs_path);
use t::util;

my $util = t::util->new(clean_temp_directory => 1);
my $dir = $util->temp_directory();

use_ok('npg_pipeline::function::p4_stage1_analysis');
my $current = abs_path(getcwd());

# Copy cache dir to a temp location since a tag file will
# be created there.
my $new = "$dir/1234_samplesheet.csv";
`cp -r t/data/p4_stage1_analysis/* $dir`;
local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = $new;
local $ENV{'http_proxy'} = 'http://wibble.com';
local $ENV{'no_proxy'} = q{};

#################################
# mock references
#################################
my $repos_root = $dir . q{/srpipe_references};
`mkdir -p $repos_root/references/PhiX/default/all/bwa0_6`;
`mkdir -p $repos_root/references/PhiX/default/all/fasta`;
`mkdir -p $repos_root/references/PhiX/default/all/minimap2`;
`touch $repos_root/references/PhiX/default/all/bwa0_6/phix_unsnipped_short_no_N.fa`;
`touch $repos_root/references/PhiX/default/all/fasta/phix_unsnipped_short_no_N.fa`;
`touch $repos_root/references/PhiX/default/all/minimap2/phix_unsnipped_short_no_N.fa.mmi`;

$util->create_analysis();
my $runfolder = $util->analysis_runfolder_path() . '/';
cp('t/data/runfolder/Data/RunInfo.xml', $runfolder) or die 'Failed to copy run info';
#cp('t/data/run_params/runParameters.novaseq.xml', $runfolder . 'runParameters.xml') or
cp('t/data/run_params/runParameters.miseq.xml', $runfolder . 'runParameters.xml') or
  die 'Failed to copy run params';

my $bc_path = q{/nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BaseCalls};

my $bam_generator = npg_pipeline::function::p4_stage1_analysis->new(
    run_folder                    => q{123456_IL2_1234},
    repository                    => $repos_root,
    runfolder_path                => $util->analysis_runfolder_path(),
    timestamp                     => q{20090709-123456},
    verbose                       => 0,
    id_run                        => 1234,
    _extra_tradis_transposon_read => 1,
    bam_basecall_path             => $util->analysis_runfolder_path() . q{/Data/Intensities/BaseCalls},
);

mkdir $bam_generator->bam_basecall_path() or die 'Failed to create directory';
mkdir join(q[/], $bam_generator->bam_basecall_path(), 'metadata_cache_1234')
  or die 'Failed to create directory';

subtest 'basics' => sub {
  plan tests => 5;

  isa_ok($bam_generator, q{npg_pipeline::function::p4_stage1_analysis}, q{$bam_generator});
  is($bam_generator->_extra_tradis_transposon_read, 1, 'TraDIS set');
  $bam_generator->_extra_tradis_transposon_read(0);
  is($bam_generator->_extra_tradis_transposon_read, 0, 'TraDIS not set');
  isa_ok($bam_generator->lims, 'st::api::lims', 'cached lims object');
  
  my $alims = $bam_generator->lims->children_ia;
  my $position = 8;
  is($bam_generator->_get_number_of_plexes_excluding_control($alims->{$position}),
    2, 'correct number of plexes');
};

subtest 'check_save_arguments' => sub {
  plan tests => 29;
 
  my $bbp = $bam_generator->bam_basecall_path;
  my $unique = $bam_generator->_job_id();
 
  my $da = $bam_generator->generate();
  ok ($da && @{$da}==8, 'eight definitions returned');
  my $d = $da->[0];
  isa_ok ($d, 'npg_pipeline::function::definition');
  is ($d->created_by, 'npg_pipeline::function::p4_stage1_analysis', 'created by');
  is ($d->created_on, q{20090709-123456}, 'created on');
  is ($d->identifier, 1234, 'identifier');
  ok (!$d->excluded, 'step is not excluded');
  is ($d->queue, 'default', 'default queue');
  is ($d->job_name, 'p4_stage1_analysis_1234_20090709-123456', 'job name');
  is ($d->fs_slots_num, 4, '4 sf slots');
  is ($d->num_hosts, 1, 'one host');
  is_deeply ($d->num_cpus, [8], 'num cpus as an array');
  is ($d->memory, 20000, 'memory');
  is ($d->command_preexec,
      "npg_pipeline_preexec_references --repository $repos_root",
      'preexec command');
  ok ($d->has_composition, 'composition object is set');
  my $composition = $d->composition;
  isa_ok ($composition, 'npg_tracking::glossary::composition');
  is ($composition->num_components, 1, 'one component');
  my $component = $composition->get_component(0);
  is ($component->id_run, 1234, 'run id correct');
  is ($component->position, 1, 'position correct');
  ok (!defined $component->tag_index, 'tag index undefined');

  my $intensities_dir = $dir . '/nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities';
  my $expected = {
          '1' => 'bash -c \' cd ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane1/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_1.json -param_vals ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane1/param_files/1234_1_p4s1_pv_in.json -export_param_vals 1234_1_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_1.log run_1234_1.json \'',
          '2' => 'bash -c \' cd ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane2/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_2.json -param_vals ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane2/param_files/1234_2_p4s1_pv_in.json -export_param_vals 1234_2_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_2.log run_1234_2.json \'',
          '3' => 'bash -c \' cd ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane3/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_3.json -param_vals ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane3/param_files/1234_3_p4s1_pv_in.json -export_param_vals 1234_3_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_3.log run_1234_3.json \'',
          '4' => 'bash -c \' cd ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane4/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_4.json -param_vals ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane4/param_files/1234_4_p4s1_pv_in.json -export_param_vals 1234_4_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_4.log run_1234_4.json \'',
          '5' => 'bash -c \' cd ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane5/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_5.json -param_vals ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane5/param_files/1234_5_p4s1_pv_in.json -export_param_vals 1234_5_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_5.log run_1234_5.json \'',
          '6' => 'bash -c \' cd ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane6/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_6.json -param_vals ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane6/param_files/1234_6_p4s1_pv_in.json -export_param_vals 1234_6_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_6.log run_1234_6.json \'',
          '7' => 'bash -c \' cd ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane7/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_7.json -param_vals ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane7/param_files/1234_7_p4s1_pv_in.json -export_param_vals 1234_7_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_7.log run_1234_7.json \'',
          '8' => 'bash -c \' cd ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane8/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_8.json -param_vals ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane8/param_files/1234_8_p4s1_pv_in.json -export_param_vals 1234_8_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_8.log run_1234_8.json \'',
  };

  foreach my $d (@{$da}) {
    my $p = $d->composition()->get_component(0)->position();
    is ($d->command, $expected->{$p}, "command correct for lane $p");
  }

  my $pfname = $dir . $bc_path. q[/p4_stage1_analysis/lane1/param_files/1234_1_p4s1_pv_in.json];
  ok (-e $pfname, 'params file exists');
  my $h = from_json(slurp($pfname));

  $expected = {
     'assign' => [
        {
	  'i2b_thread_count' => 8,
	  'seqchksum_file' => $intensities_dir . '/BaseCalls/1234_1.post_i2b.seqchksum',
	  'scramble_reference_fasta' => $dir . '/srpipe_references/references/PhiX/default/all/fasta/phix_unsnipped_short_no_N.fa',
	  'i2b_rg' => '1234_1',
	  'i2b_pu' => '123456_IL2_1234_1',
          'tileviz_dir' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/archive/lane1/tileviz',
          'reference_phix' => $dir . "/srpipe_references/references/PhiX/default/all/bwa0_6/phix_unsnipped_short_no_N.fa",
	  'unfiltered_cram_file' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/1234_1.unfiltered.cram',
	  'qc_check_qc_out_dir' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/archive/lane1/qc',
	  'i2b_lane' => '1',
	  'bwa_executable' => 'bwa0_6',
	  'filtered_bam' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/1234_1.bam',
	  'samtools_executable' => 'samtools',
	  'i2b_library_name' => '51021',
	  'outdatadir' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal',
          'subsetsubpath' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/archive/lane1/.npg_cache_10000',
	  'i2b_run_path' => $dir . q[/nfs/sf45/IL2/analysis/123456_IL2_1234],
	  'teepot_tempdir' => '.',
	  'split_prefix' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal',
	  'i2b_intensity_dir' => $intensities_dir,
	  'i2b_sample_aliases' => 'SRS000147',
	  'phix_alignment_method' => 'bwa_aln_se',
	  'md5filename' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/1234_1.bam.md5',
	  'teepot_mval' => '2G',
	  'i2b_runfolder' => '123456_IL2_1234',
	  'i2b_study_name' => '"SRP000031: 1000Genomes Project Pilot 1"',
	  'i2b_basecalls_dir' => $intensities_dir . '/BaseCalls',
	  'teepot_wval' => '500',
	  'qc_check_qc_in_dir' => $intensities_dir . '/BaseCalls',
	  'qc_check_id_run' => '1234',
          'cluster_count' => '500077065',
          'seed_frac' => '1234.00002000',
          'split_threads_val' => 4,
          'aln_filter_value' => '0x900',
          's1_se_pe' => 'se',
          'fqc1' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/archive/lane1/1234_1_1.fastqcheck',
          'fqc2' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/archive/lane1/1234_1_2.fastqcheck',
          'fqct' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/archive/lane1/1234_1_t.fastqcheck',
        },
    ],
    'ops' => {
      'splice' => [ 'bamadapterfind:-bamcollate:' ],
      'prune' => [ 'tee_split:split_bam-'
      ]
    },
  };

  is_deeply($h, $expected, 'correct json file content (for p4 stage1 params file)');

};

# check_save_arguments_minimap2 test duplicates check_save_arguments, but forces phix_aligment_method to minimap2
$bam_generator = npg_pipeline::function::p4_stage1_analysis->new(
    run_folder                    => q{123456_IL2_1234},
    repository                    => $repos_root,
    runfolder_path                => $util->analysis_runfolder_path(),
    timestamp                     => q{20090709-123456},
    verbose                       => 0,
    id_run                        => 1234,
    bam_basecall_path             => $util->analysis_runfolder_path() . q{/Data/Intensities/BaseCalls},
    p4s1_phix_alignment_method    => q{minimap2},
  );

subtest 'check_save_arguments_minimap2' => sub {
  plan tests => 29;
 
  my $bbp = $bam_generator->bam_basecall_path;
  my $unique = $bam_generator->_job_id();
 
  my $da = $bam_generator->generate();
  ok ($da && @{$da}==8, 'eight definitions returned');
  my $d = $da->[0];
  isa_ok ($d, 'npg_pipeline::function::definition');
  is ($d->created_by, 'npg_pipeline::function::p4_stage1_analysis', 'created by');
  is ($d->created_on, q{20090709-123456}, 'created on');
  is ($d->identifier, 1234, 'identifier');
  ok (!$d->excluded, 'step is not excluded');
  is ($d->queue, 'default', 'default queue');
  is ($d->job_name, 'p4_stage1_analysis_1234_20090709-123456', 'job name');
  is ($d->fs_slots_num, 4, '4 sf slots');
  is ($d->num_hosts, 1, 'one host');
  is_deeply ($d->num_cpus, [8], 'num cpus as an array');
  is ($d->memory, 20000, 'memory');
  is ($d->command_preexec,
      "npg_pipeline_preexec_references --repository $repos_root",
      'preexec command');
  ok ($d->has_composition, 'composition object is set');
  my $composition = $d->composition;
  isa_ok ($composition, 'npg_tracking::glossary::composition');
  is ($composition->num_components, 1, 'one component');
  my $component = $composition->get_component(0);
  is ($component->id_run, 1234, 'run id correct');
  is ($component->position, 1, 'position correct');
  ok (!defined $component->tag_index, 'tag index undefined');

  my $intensities_dir = $dir . '/nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities';
  my $expected = {
          '1' => 'bash -c \' cd ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane1/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_1.json -param_vals ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane1/param_files/1234_1_p4s1_pv_in.json -export_param_vals 1234_1_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_1.log run_1234_1.json \'',
          '2' => 'bash -c \' cd ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane2/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_2.json -param_vals ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane2/param_files/1234_2_p4s1_pv_in.json -export_param_vals 1234_2_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_2.log run_1234_2.json \'',
          '3' => 'bash -c \' cd ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane3/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_3.json -param_vals ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane3/param_files/1234_3_p4s1_pv_in.json -export_param_vals 1234_3_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_3.log run_1234_3.json \'',
          '4' => 'bash -c \' cd ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane4/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_4.json -param_vals ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane4/param_files/1234_4_p4s1_pv_in.json -export_param_vals 1234_4_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_4.log run_1234_4.json \'',
          '5' => 'bash -c \' cd ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane5/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_5.json -param_vals ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane5/param_files/1234_5_p4s1_pv_in.json -export_param_vals 1234_5_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_5.log run_1234_5.json \'',
          '6' => 'bash -c \' cd ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane6/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_6.json -param_vals ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane6/param_files/1234_6_p4s1_pv_in.json -export_param_vals 1234_6_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_6.log run_1234_6.json \'',
          '7' => 'bash -c \' cd ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane7/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_7.json -param_vals ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane7/param_files/1234_7_p4s1_pv_in.json -export_param_vals 1234_7_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_7.log run_1234_7.json \'',
          '8' => 'bash -c \' cd ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane8/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_8.json -param_vals ' . $intensities_dir . '/BaseCalls/p4_stage1_analysis/lane8/param_files/1234_8_p4s1_pv_in.json -export_param_vals 1234_8_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_8.log run_1234_8.json \'',
  };

  foreach my $d (@{$da}) {
    my $p = $d->composition()->get_component(0)->position();
    is ($d->command, $expected->{$p}, "command correct for lane $p");
  }

  my $pfname = $dir . $bc_path. q[/p4_stage1_analysis/lane1/param_files/1234_1_p4s1_pv_in.json];
  ok (-e $pfname, 'params file exists');
  my $h = from_json(slurp($pfname));

  $expected = {
     'assign' => [
        {
	  'i2b_thread_count' => 8,
	  'seqchksum_file' => $intensities_dir . '/BaseCalls/1234_1.post_i2b.seqchksum',
	  'scramble_reference_fasta' => $dir . '/srpipe_references/references/PhiX/default/all/fasta/phix_unsnipped_short_no_N.fa',
	  'i2b_rg' => '1234_1',
	  'i2b_pu' => '123456_IL2_1234_1',
          'tileviz_dir' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/archive/lane1/tileviz',
          'reference_phix' => $dir . '/srpipe_references/references/PhiX/default/all/minimap2/phix_unsnipped_short_no_N.fa.mmi',
	  'unfiltered_cram_file' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/1234_1.unfiltered.cram',
	  'qc_check_qc_out_dir' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/archive/lane1/qc',
	  'i2b_lane' => '1',
	  'bwa_executable' => 'bwa0_6',
	  'filtered_bam' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/1234_1.bam',
	  'samtools_executable' => 'samtools',
	  'i2b_library_name' => '51021',
	  'outdatadir' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal',
          'subsetsubpath' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/archive/lane1/.npg_cache_10000',
	  'i2b_run_path' => $dir . q[/nfs/sf45/IL2/analysis/123456_IL2_1234],
	  'teepot_tempdir' => '.',
	  'split_prefix' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal',
	  'i2b_intensity_dir' => $intensities_dir,
	  'i2b_sample_aliases' => 'SRS000147',
	  'phix_alignment_method' => 'minimap2',
	  'md5filename' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/1234_1.bam.md5',
	  'teepot_mval' => '2G',
	  'i2b_runfolder' => '123456_IL2_1234',
	  'i2b_study_name' => '"SRP000031: 1000Genomes Project Pilot 1"',
	  'i2b_basecalls_dir' => $intensities_dir . '/BaseCalls',
	  'teepot_wval' => '500',
	  'qc_check_qc_in_dir' => $intensities_dir . '/BaseCalls',
	  'qc_check_id_run' => '1234',
          'cluster_count' => '500077065',
          'seed_frac' => '1234.00002000',
          'split_threads_val' => 4,
          'aln_filter_value' => '0x900',
          's1_se_pe' => 'se',
          'fqc1' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/archive/lane1/1234_1_1.fastqcheck',
          'fqc2' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/archive/lane1/1234_1_2.fastqcheck',
          'fqct' => $intensities_dir . '/Bustard1.3.4_09-07-2009_auto/PB_cal/archive/lane1/1234_1_t.fastqcheck',
        },
    ],
    'ops' => {
      'splice' => [ 'bamadapterfind:-bamcollate:' ],
      'prune' => [ 'tee_split:split_bam-'
      ]
    },
  };

  is_deeply($h, $expected, 'correct json file content (for p4 stage1 params file)');
 };

1;

