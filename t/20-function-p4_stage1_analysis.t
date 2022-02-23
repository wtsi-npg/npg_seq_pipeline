use strict;
use warnings;
use Test::More tests => 5;
use Test::Exception;
use Cwd;
use File::Copy qw(cp);
use File::Copy::Recursive qw[dircopy];
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

my $default = {
  default => {
    minimum_cpu => 8,
    memory => 20,
    fs_slots_num => 4,
    queue => "p4stage1"
  }
};

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
    bam_basecall_path             => $util->standard_bam_basecall_path(),
    resource                      => $default
);

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
  is ($d->queue, 'p4stage1', 'special queue');
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
          '1' => 'bash -c \' cd ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane1/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_1.json -param_vals ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane1/param_files/1234_1_p4s1_pv_in.json -export_param_vals 1234_1_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_1.log run_1234_1.json \'',
          '2' => 'bash -c \' cd ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane2/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_2.json -param_vals ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane2/param_files/1234_2_p4s1_pv_in.json -export_param_vals 1234_2_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_2.log run_1234_2.json \'',
          '3' => 'bash -c \' cd ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane3/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_3.json -param_vals ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane3/param_files/1234_3_p4s1_pv_in.json -export_param_vals 1234_3_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_3.log run_1234_3.json \'',
          '4' => 'bash -c \' cd ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane4/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_4.json -param_vals ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane4/param_files/1234_4_p4s1_pv_in.json -export_param_vals 1234_4_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_4.log run_1234_4.json \'',
          '5' => 'bash -c \' cd ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane5/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_5.json -param_vals ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane5/param_files/1234_5_p4s1_pv_in.json -export_param_vals 1234_5_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_5.log run_1234_5.json \'',
          '6' => 'bash -c \' cd ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane6/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_6.json -param_vals ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane6/param_files/1234_6_p4s1_pv_in.json -export_param_vals 1234_6_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_6.log run_1234_6.json \'',
          '7' => 'bash -c \' cd ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane7/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_7.json -param_vals ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane7/param_files/1234_7_p4s1_pv_in.json -export_param_vals 1234_7_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_7.log run_1234_7.json \'',
          '8' => 'bash -c \' cd ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane8/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_8.json -param_vals ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane8/param_files/1234_8_p4s1_pv_in.json -export_param_vals 1234_8_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_8.log run_1234_8.json \'',
  };

  foreach my $d (@{$da}) {
    my $p = $d->composition()->get_component(0)->position();
    is ($d->command, $expected->{$p}, "command correct for lane $p");
  }

  my $pfname = $intensities_dir . q[/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane1/param_files/1234_1_p4s1_pv_in.json];
  ok (-e $pfname, 'params file exists');
  my $h = from_json(slurp($pfname));

  my $no_cal_path = $intensities_dir . '/BAM_basecalls_09-07-2009/no_cal';

  $expected = {
    'assign' => [
        {
          'i2b_thread_count' => 8,
          'seqchksum_file' => $intensities_dir . '/BAM_basecalls_09-07-2009/1234_1.post_i2b.seqchksum',
          'scramble_reference_fasta' => $dir . '/srpipe_references/references/PhiX/default/all/fasta/phix_unsnipped_short_no_N.fa',
          'i2b_rg' => '1234_1',
          'i2b_pu' => '123456_IL2_1234_1',
          'tileviz_dir' => $no_cal_path . '/archive/lane1/tileviz',
          'reference_phix' => $dir . "/srpipe_references/references/PhiX/default/all/bwa0_6/phix_unsnipped_short_no_N.fa",
          'unfiltered_cram_file' => $no_cal_path . '/1234_1.unfiltered.cram',
          'qc_check_qc_out_dir' => $no_cal_path . '/archive/lane1/qc',
          'i2b_lane' => '1',
          'bwa_executable' => 'bwa0_6',
          'filtered_bam' => $no_cal_path . '/1234_1.bam',
          'samtools_executable' => 'samtools',
          'i2b_library_name' => '51021',
          'outdatadir' => $no_cal_path,
          'subsetsubpath' => $no_cal_path . '/archive/lane1/.npg_cache_10000',
          'i2b_run_path' => $dir . q[/nfs/sf45/IL2/analysis/123456_IL2_1234],
          'teepot_tempdir' => '.',
          'split_prefix' => $no_cal_path,
          'i2b_intensity_dir' => $intensities_dir,
          'i2b_sample_aliases' => 'SRS000147',
          'phix_alignment_method' => 'bwa_aln_se',
          'md5filename' => $no_cal_path . '/1234_1.bam.md5',
          'teepot_mval' => '2G',
          'i2b_runfolder' => '123456_IL2_1234',
          'i2b_study_name' => '"SRP000031: 1000Genomes Project Pilot 1"',
          'i2b_basecalls_dir' => $intensities_dir . '/BaseCalls',
          'teepot_wval' => '500',
          'qc_check_qc_in_dir' => $intensities_dir . '/BAM_basecalls_09-07-2009',
          'qc_check_id_run' => '1234',
          'cluster_count' => '500077065',
          'seed_frac' => '1234.00002000',
          'split_threads_val' => 4,
          'aln_filter_value' => '0x900',
          's1_se_pe' => 'se',
          's1_output_format' => 'cram',
          'rpt_list' => '1234:1',
          'lane_archive_path' => $no_cal_path . '/archive/lane1',
        },
    ],
    'ops' => {
      'splice' => [ 'tee_i2b:baf-bamcollate:' ],
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
    bam_basecall_path             => $util->standard_bam_basecall_path(),
    p4s1_phix_alignment_method    => q{minimap2},
    resource                      => $default
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
  is ($d->queue,  'p4stage1', 'special queue');
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
          '1' => 'bash -c \' cd ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane1/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_1.json -param_vals ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane1/param_files/1234_1_p4s1_pv_in.json -export_param_vals 1234_1_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_1.log run_1234_1.json \'',
          '2' => 'bash -c \' cd ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane2/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_2.json -param_vals ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane2/param_files/1234_2_p4s1_pv_in.json -export_param_vals 1234_2_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_2.log run_1234_2.json \'',
          '3' => 'bash -c \' cd ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane3/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_3.json -param_vals ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane3/param_files/1234_3_p4s1_pv_in.json -export_param_vals 1234_3_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_3.log run_1234_3.json \'',
          '4' => 'bash -c \' cd ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane4/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_4.json -param_vals ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane4/param_files/1234_4_p4s1_pv_in.json -export_param_vals 1234_4_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_4.log run_1234_4.json \'',
          '5' => 'bash -c \' cd ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane5/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_5.json -param_vals ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane5/param_files/1234_5_p4s1_pv_in.json -export_param_vals 1234_5_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_5.log run_1234_5.json \'',
          '6' => 'bash -c \' cd ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane6/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_6.json -param_vals ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane6/param_files/1234_6_p4s1_pv_in.json -export_param_vals 1234_6_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_6.log run_1234_6.json \'',
          '7' => 'bash -c \' cd ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane7/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_7.json -param_vals ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane7/param_files/1234_7_p4s1_pv_in.json -export_param_vals 1234_7_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_7.log run_1234_7.json \'',
          '8' => 'bash -c \' cd ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane8/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_1234_8.json -param_vals ' . $intensities_dir . '/BAM_basecalls_09-07-2009/p4_stage1_analysis/lane8/param_files/1234_8_p4s1_pv_in.json -export_param_vals 1234_8_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_1234_8.log run_1234_8.json \'',
  };

  foreach my $d (@{$da}) {
    my $p = $d->composition()->get_component(0)->position();
    is ($d->command, $expected->{$p}, "command correct for lane $p");
  }

  my $pfname = $bbp . q[/p4_stage1_analysis/lane1/param_files/1234_1_p4s1_pv_in.json];
  ok (-e $pfname, 'params file exists');
  my $h = from_json(slurp($pfname));

  my $no_cal_path = $intensities_dir . '/BAM_basecalls_09-07-2009/no_cal';

  $expected = {
     'assign' => [
        {
          'i2b_thread_count' => 8,
          'seqchksum_file' => $intensities_dir . '/BAM_basecalls_09-07-2009/1234_1.post_i2b.seqchksum',
          'scramble_reference_fasta' => $dir . '/srpipe_references/references/PhiX/default/all/fasta/phix_unsnipped_short_no_N.fa',
          'i2b_rg' => '1234_1',
          'i2b_pu' => '123456_IL2_1234_1',
          'tileviz_dir' => $no_cal_path . '/archive/lane1/tileviz',
          'reference_phix' => $dir . '/srpipe_references/references/PhiX/default/all/minimap2/phix_unsnipped_short_no_N.fa.mmi',
          'unfiltered_cram_file' => $no_cal_path . '/1234_1.unfiltered.cram',
          'qc_check_qc_out_dir' => $no_cal_path . '/archive/lane1/qc',
          'i2b_lane' => '1',
          'bwa_executable' => 'bwa0_6',
          'filtered_bam' => $no_cal_path . '/1234_1.bam',
          'samtools_executable' => 'samtools',
          'i2b_library_name' => '51021',
          'outdatadir' => $no_cal_path,
          'subsetsubpath' => $no_cal_path . '/archive/lane1/.npg_cache_10000',
          'i2b_run_path' => $dir . q[/nfs/sf45/IL2/analysis/123456_IL2_1234],
          'teepot_tempdir' => '.',
          'split_prefix' => $no_cal_path,
          'i2b_intensity_dir' => $intensities_dir,
          'i2b_sample_aliases' => 'SRS000147',
          'phix_alignment_method' => 'minimap2',
          'md5filename' => $no_cal_path . '/1234_1.bam.md5',
          'teepot_mval' => '2G',
          'i2b_runfolder' => '123456_IL2_1234',
          'i2b_study_name' => '"SRP000031: 1000Genomes Project Pilot 1"',
          'i2b_basecalls_dir' => $intensities_dir . '/BaseCalls',
          'teepot_wval' => '500',
          'qc_check_qc_in_dir' => $intensities_dir . '/BAM_basecalls_09-07-2009',
          'qc_check_id_run' => '1234',
          'cluster_count' => '500077065',
          'seed_frac' => '1234.00002000',
          'split_threads_val' => 4,
          'aln_filter_value' => '0x900',
          's1_se_pe' => 'se',
          's1_output_format' => 'cram',
          'lane_archive_path' => $no_cal_path . '/archive/lane1',
          'rpt_list' => '1234:1',
        },
    ],
    'ops' => {
      'splice' => [ 'tee_i2b:baf-bamcollate:' ],
      'prune' => [ 'tee_split:split_bam-'
      ]
    },
  };

  is_deeply($h, $expected, 'correct json file content (for p4 stage1 params file)');
 };

# check_duplex-seq test

subtest 'check_duplex-seq' => sub {
  plan tests => 29;

  my $rf_name = '210111_A00513_0447_AHJ55JDSXY';
  my $rfpath  = abs_path(getcwd) . qq{/t/data/novaseq/$rf_name};
  my $copy = join q[/], $dir, $rf_name;
  dircopy $rfpath, $copy or die 'Failed to copy run folder';
  $rfpath = $copy;

  my $id_run  = 36062;
  my $bbp = qq{$rfpath/Data/Intensities/BAM_basecalls_20210113-092146};

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
    qq{$bbp/metadata_cache_36062/samplesheet_36062.csv};

  $bam_generator = npg_pipeline::function::p4_stage1_analysis->new(
    run_folder                    => $rf_name,
    repository                    => $repos_root,
    runfolder_path                => $rfpath,
    timestamp                     => q{20201210-102032},
    verbose                       => 0,
    id_run                        => 36062,
    bam_basecall_path             => $bbp,
    resource                      => $default
  );

  my $unique = $bam_generator->_job_id();

  my $da = $bam_generator->generate();
  ok ($da && @{$da}==4, 'four definitions returned');
  my $d = $da->[0];
  isa_ok ($d, 'npg_pipeline::function::definition');
  is ($d->created_by, 'npg_pipeline::function::p4_stage1_analysis', 'created by');
  is ($d->created_on, q{20201210-102032}, 'created on');
  is ($d->identifier, 36062, 'identifier');
  ok (!$d->excluded, 'step is not excluded');
  is ($d->queue,  'p4stage1', 'special queue');
  is ($d->job_name, 'p4_stage1_analysis_36062_20201210-102032', 'job name');
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
  is ($component->id_run, 36062, 'run id correct');
  is ($component->position, 1, 'position correct');
  ok (!defined $component->tag_index, 'tag index undefined');


  my $intensities_dir = qq{$rfpath/Data/Intensities};
  my $expected = {
          '1' => 'bash -c \' cd ' . $intensities_dir . '/BAM_basecalls_20210113-092146/p4_stage1_analysis/lane1/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_36062_1.json -param_vals ' . $intensities_dir . '/BAM_basecalls_20210113-092146/p4_stage1_analysis/lane1/param_files/36062_1_p4s1_pv_in.json -export_param_vals 36062_1_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_36062_1.log run_36062_1.json \'',
          '2' => 'bash -c \' cd ' . $intensities_dir . '/BAM_basecalls_20210113-092146/p4_stage1_analysis/lane2/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_36062_2.json -param_vals ' . $intensities_dir . '/BAM_basecalls_20210113-092146/p4_stage1_analysis/lane2/param_files/36062_2_p4s1_pv_in.json -export_param_vals 36062_2_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_36062_2.log run_36062_2.json \'',
          '3' => 'bash -c \' cd ' . $intensities_dir . '/BAM_basecalls_20210113-092146/p4_stage1_analysis/lane3/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_36062_3.json -param_vals ' . $intensities_dir . '/BAM_basecalls_20210113-092146/p4_stage1_analysis/lane3/param_files/36062_3_p4s1_pv_in.json -export_param_vals 36062_3_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_36062_3.log run_36062_3.json \'',
          '4' => 'bash -c \' cd ' . $intensities_dir . '/BAM_basecalls_20210113-092146/p4_stage1_analysis/lane4/log && vtfp.pl -template_path $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib -o run_36062_4.json -param_vals ' . $intensities_dir . '/BAM_basecalls_20210113-092146/p4_stage1_analysis/lane4/param_files/36062_4_p4s1_pv_in.json -export_param_vals 36062_4_p4s1_pv_out_' . $unique . '.json -keys cfgdatadir -vals $(dirname $(readlink -f $(which vtfp.pl)))/../data/vtlib/ -keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -2 --divide 3` -keys s2b_mt_val -vals `npg_pipeline_job_env_to_threads --num_threads 8 --exclude -1 --divide 3` -keys bamsormadup_numthreads -vals `npg_pipeline_job_env_to_threads --num_threads 8 --divide 3` -keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --num_threads 8` $(dirname $(dirname $(readlink -f $(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json && viv.pl -s -x -v 3 -o viv_36062_4.log run_36062_4.json \'',
  };

  foreach my $d (@{$da}) {
    my $p = $d->composition()->get_component(0)->position();
    is ($d->command, $expected->{$p}, "command correct for lane $p");
  }

  my $pfname = $bbp . q[/p4_stage1_analysis/lane3/param_files/36062_3_p4s1_pv_in.json];
  ok (-e $pfname, 'params file exists');
  my $h = from_json(slurp($pfname));

  my $no_cal_path = $intensities_dir . '/BAM_basecalls_20210113-092146/no_cal';

  $expected = {
    'assign' => [
       {
         'i2b_intensity_dir' =>  $intensities_dir,
         'qc_check_qc_out_dir' =>  $no_cal_path . '/archive/lane3/qc',
         'bwa_executable' => 'bwa0_6',
         'cluster_count' => '3013541015',
         'split_prefix' => $no_cal_path,
         'barcode_file' => $intensities_dir . '/BAM_basecalls_20210113-092146/metadata_cache_36062/lane_3.taglist',
         'split_threads_val' => '4',
         'i2b_lane' => '3',
         'seed_frac' => '36062.00000332',
         'reference_phix' => $dir . '/srpipe_references/references/PhiX/default/all/minimap2/phix_unsnipped_short_no_N.fa.mmi',
         'i2b_bc_qual_val' => 'rq,mq,bq,QT,QT,rq,mq,bq',
         'lane_archive_path' => $no_cal_path . '/archive/lane3',
         'samtools_executable' => 'samtools',
         'qc_check_id_run' => '36062',
         'i2b_final_0' => '151,318',
         'i2b_sample_aliases' => 'EGAN00002827435,EGAN00002827458,EGAN00002827408,EGAN00002827411,EGAN00002827414,EGAN00002827416,EGAN00002827418,EGAN00002827421,EGAN00002827424,EGAN00002827426,EGAN00002827427,EGAN00002827430,EGAN00002827443,EGAN00002827447,EGAN00002827452,EGAN00002827463,EGAN00002827467,EGAN00002827472,EGAN00002827475,EGAN00002827478,EGAN00002827481,EGAN00002827485,EGAN00002827488,EGAN00002827514,EGAN00002827518,EGAN00002827521,EGAN00002827524,EGAN00002827527',
         'tileviz_dir' => $no_cal_path . '/archive/lane3/tileviz',
         's1_se_pe' => 'pe',
         'aln_filter_value' => '0x900',
         'i2b_runfolder' => $rf_name,
         'subsetsubpath' => $no_cal_path . '/archive/lane3/.npg_cache_10000',
         'outdatadir' => $no_cal_path,
         'i2b_study_name' => '"EGAS00001004066: Bottleneck sequencing of human tissue including neurons, cord blood, sperm This data is part of a pre-publication release. For information on the proper use of pre-publication data shared by the Wellcome Trust Sanger Institute (including details of any publication moratoria), please see http://www.sanger.ac.uk/datasharing/,EGAS00001004604: This study is investigating the effects of chemotherapy drugs used in cancer treatment on the somatic mutational landscape in normal human tissues. The samples used in this study have been taken from rapid autopsies of patients who have undergone chemotherapy using drugs thought to cause mutations in the DNA. Structures from a variety of tissues will be dissected using LCM and whole genome sequences will be produced in order to assess mutational burdens and signatures. "',
         'scramble_reference_fasta' => $dir . '/srpipe_references/references/PhiX/default/all/fasta/phix_unsnipped_short_no_N.fa',
         'i2b_thread_count' => '8',
         'teepot_tempdir' => '.',
         'filtered_bam' => $no_cal_path . '/36062_3.bam',
         'md5filename' => $no_cal_path . '/36062_3.bam.md5',
         'i2b_basecalls_dir' => $intensities_dir . '/BaseCalls',
         'i2b_run_path' => $rfpath,
         'i2b_rg' => '36062_3',
         'rpt_list' => '36062:3',
         'teepot_wval' => '500',
         'i2b_pu' => $rf_name . '_3',
         'i2b_final_index_0' => '3,3,7,159,167,170,170,174',
         'seqchksum_file' => $intensities_dir . '/BAM_basecalls_20210113-092146/36062_3.post_i2b.seqchksum',
         'decoder_metrics' => $intensities_dir . '/BAM_basecalls_20210113-092146/36062_3.bam.tag_decode.metrics',
         'i2b_bc_read' => '1,2,1,1,1,2,1,2',
         'qc_check_qc_in_dir' => $intensities_dir. '/BAM_basecalls_20210113-092146',
         'i2b_bc_seq_val' => 'rb,mb,br,BC,BC,rb,mb,br',
         's1_output_format' => 'cram',
         'phix_alignment_method' => 'minimap2',
         'i2b_first_0' => '8,175',
         'i2b_first_index_0' => '1,1,4,152,160,168,168,171',
         'unfiltered_cram_file' => $no_cal_path . '/36062_3.unfiltered.cram',
         'teepot_mval' => '2G'
       }
     ],
     "ops" => {
       "splice" => ["bamadapterfind"],
       "prune" => ["tee_split:unsplit_bam-"]
      }
    };

  is_deeply($h, $expected, 'correct json file content (for p4 stage1 params file)');

  # and a Targeted NanoSeq Pulldown Twist lane

  $pfname = $bbp . q[/p4_stage1_analysis/lane2/param_files/36062_2_p4s1_pv_in.json];
  ok (-e $pfname, 'params file exists');
  $h = from_json(slurp($pfname));

  $no_cal_path = $intensities_dir . '/BAM_basecalls_20210113-092146/no_cal';

  $expected = {
    'assign' => [
       {
         'i2b_intensity_dir' =>  $intensities_dir,
         'qc_check_qc_out_dir' =>  $no_cal_path . '/archive/lane2/qc',
         'bwa_executable' => 'bwa0_6',
         'cluster_count' => '3029686070',
         'split_prefix' => $no_cal_path,
         'barcode_file' => $intensities_dir . '/BAM_basecalls_20210113-092146/metadata_cache_36062/lane_2.taglist',
         'split_threads_val' => '4',
         'i2b_lane' => '2',
         'seed_frac' => '36062.00000330',
         'reference_phix' => $dir . '/srpipe_references/references/PhiX/default/all/minimap2/phix_unsnipped_short_no_N.fa.mmi',
         'i2b_bc_qual_val' => 'rq,mq,bq,QT,QT,rq,mq,bq',
         'lane_archive_path' => $no_cal_path . '/archive/lane2',
         'samtools_executable' => 'samtools',
         'qc_check_id_run' => '36062',
         'i2b_final_0' => '151,318',
         'i2b_sample_aliases' => 'EGAN00002715626,EGAN00002715646,EGAN00002715660,EGAN00002715671,EGAN00002715675,EGAN00002715685,EGAN00002715692,EGAN00002715705,EGAN00002715707,EGAN00002715709,EGAN00002715712,EGAN00002715714,EGAN00002715721,EGAN00002715734,EGAN00002715743,EGAN00002715751',
         'tileviz_dir' => $no_cal_path . '/archive/lane2/tileviz',
         's1_se_pe' => 'pe',
         'aln_filter_value' => '0x900',
         'i2b_runfolder' => $rf_name,
         'subsetsubpath' => $no_cal_path . '/archive/lane2/.npg_cache_10000',
         'outdatadir' => $no_cal_path,
         'i2b_study_name' => '"EGAS00001004580: My research project aims to use the clonal dynamics of spontaneously occurring somatic mutations to answer fundamental questions about human haematopoietic stem cell (HSC) biology.  The four major questions I will address are: 1. How do age and aging affect normal human HSC dynamics in vivo? 2. How do in vivo perturbations, particularly chemotherapy and increased levels of reactive oxygen species, affect HSC population dynamics? 3. Is response to in vitro perturbation heritable and/or correlated with other features such as age of individual and contribution of the lineage to peripheral blood? 4. How are HSC dynamics altered in people with early driver mutations (clonal haematopoiesis)?"',
         'scramble_reference_fasta' => $dir . '/srpipe_references/references/PhiX/default/all/fasta/phix_unsnipped_short_no_N.fa',
         'i2b_thread_count' => '8',
         'teepot_tempdir' => '.',
         'filtered_bam' => $no_cal_path . '/36062_2.bam',
         'md5filename' => $no_cal_path . '/36062_2.bam.md5',
         'i2b_basecalls_dir' => $intensities_dir . '/BaseCalls',
         'i2b_run_path' => $rfpath,
         'i2b_rg' => '36062_2',
         'rpt_list' => '36062:2',
         'teepot_wval' => '500',
         'i2b_pu' => $rf_name . '_2',
         'i2b_final_index_0' => '3,3,7,159,167,170,170,174',
         'seqchksum_file' => $intensities_dir . '/BAM_basecalls_20210113-092146/36062_2.post_i2b.seqchksum',
         'decoder_metrics' => $intensities_dir . '/BAM_basecalls_20210113-092146/36062_2.bam.tag_decode.metrics',
         'i2b_bc_read' => '1,2,1,1,1,2,1,2',
         'qc_check_qc_in_dir' => $intensities_dir. '/BAM_basecalls_20210113-092146',
         'i2b_bc_seq_val' => 'rb,mb,br,BC,BC,rb,mb,br',
         's1_output_format' => 'cram',
         'phix_alignment_method' => 'minimap2',
         'i2b_first_0' => '8,175',
         'i2b_first_index_0' => '1,1,4,152,160,168,168,171',
         'unfiltered_cram_file' => $no_cal_path . '/36062_2.unfiltered.cram',
         'teepot_mval' => '2G'
       }
     ],
     "ops" => {
       "splice" => ["bamadapterfind"],
       "prune" => ["tee_split:unsplit_bam-"]
      }
    };

  is_deeply($h, $expected, 'correct json file content (for p4 stage1 params file)');

  # and a non Duplex-Seq lane for completeness

  $pfname = $bbp . q[/p4_stage1_analysis/lane1/param_files/36062_1_p4s1_pv_in.json];
  ok (-e $pfname, 'params file exists');
  $h = from_json(slurp($pfname));

  $no_cal_path = $intensities_dir . '/BAM_basecalls_20210113-092146/no_cal';

  $expected = {
    'assign' => [
       {
         'i2b_intensity_dir' =>  $intensities_dir,
         'qc_check_qc_out_dir' =>  $no_cal_path . '/archive/lane1/qc',
         'bwa_executable' => 'bwa0_6',
         'cluster_count' => '3160455277',
         'split_prefix' => $no_cal_path,
         'barcode_file' => $intensities_dir . '/BAM_basecalls_20210113-092146/metadata_cache_36062/lane_1.taglist',
         'split_threads_val' => '4',
         'i2b_lane' => '1',
         'seed_frac' => '36062.00000316',
         'reference_phix' => $dir . '/srpipe_references/references/PhiX/default/all/minimap2/phix_unsnipped_short_no_N.fa.mmi',
         'lane_archive_path' => $no_cal_path . '/archive/lane1',
         'samtools_executable' => 'samtools',
         'qc_check_id_run' => '36062',
         'i2b_sample_aliases' => 'EGAN00002715625,EGAN00002715630,EGAN00002715651,EGAN00002715652,EGAN00002715658,EGAN00002715673,EGAN00002715674,EGAN00002715679,EGAN00002715684,EGAN00002715690,EGAN00002715691,EGAN00002715723,EGAN00002715728,EGAN00002715735,EGAN00002715744,EGAN00002715745',
         'tileviz_dir' => $no_cal_path . '/archive/lane1/tileviz',
         's1_se_pe' => 'pe',
         'aln_filter_value' => '0x900',
         'i2b_runfolder' => $rf_name,
         'subsetsubpath' => $no_cal_path . '/archive/lane1/.npg_cache_10000',
         'outdatadir' => $no_cal_path,
         'i2b_study_name' => '"EGAS00001004580: My research project aims to use the clonal dynamics of spontaneously occurring somatic mutations to answer fundamental questions about human haematopoietic stem cell (HSC) biology.  The four major questions I will address are: 1. How do age and aging affect normal human HSC dynamics in vivo? 2. How do in vivo perturbations, particularly chemotherapy and increased levels of reactive oxygen species, affect HSC population dynamics? 3. Is response to in vitro perturbation heritable and/or correlated with other features such as age of individual and contribution of the lineage to peripheral blood? 4. How are HSC dynamics altered in people with early driver mutations (clonal haematopoiesis)?"',
         'scramble_reference_fasta' => $dir . '/srpipe_references/references/PhiX/default/all/fasta/phix_unsnipped_short_no_N.fa',
         'i2b_thread_count' => '8',
         'teepot_tempdir' => '.',
         'filtered_bam' => $no_cal_path . '/36062_1.bam',
         'md5filename' => $no_cal_path . '/36062_1.bam.md5',
         'i2b_basecalls_dir' => $intensities_dir . '/BaseCalls',
         'i2b_run_path' => $rfpath,
         'i2b_rg' => '36062_1',
         'rpt_list' => '36062:1',
         'teepot_wval' => '500',
         'i2b_pu' => $rf_name . '_1',
         'seqchksum_file' => $intensities_dir . '/BAM_basecalls_20210113-092146/36062_1.post_i2b.seqchksum',
         'decoder_metrics' => $intensities_dir . '/BAM_basecalls_20210113-092146/36062_1.bam.tag_decode.metrics',
         'qc_check_qc_in_dir' => $intensities_dir. '/BAM_basecalls_20210113-092146',
         's1_output_format' => 'cram',
         'phix_alignment_method' => 'minimap2',
         'unfiltered_cram_file' => $no_cal_path . '/36062_1.unfiltered.cram',
         'teepot_mval' => '2G'
       }
     ],
     "ops" => {
       "splice" => ["bamadapterfind"],
       "prune" => ["tee_split:unsplit_bam-"]
      }
    };

  is_deeply($h, $expected, 'correct json file content (for p4 stage1 params file)');
 };

1;

