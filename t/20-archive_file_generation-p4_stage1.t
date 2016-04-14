use strict;
use warnings;
use Test::More tests => 8;
use Test::Exception;
use Test::Differences;
use File::Copy;
use File::Path qw(make_path);
use Cwd;
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

{
  my $new = "$dir/1234_samplesheet.csv";
  copy 't/data/p4_stage1_analysis/1234_samplesheet.csv', $new;
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = $new;
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[];

  $util->create_analysis();
  my $runfolder = $util->analysis_runfolder_path() . '/';
  `cp t/data/runfolder/Data/RunInfo.xml $runfolder`;

  my $bam_generator;

  lives_ok { $bam_generator = npg_pipeline::archive::file::generation::p4_stage1_analysis->new(
    function_list => q{t/data/config_files/function_list_p4_stage1.yml},
    conf_path => q{t/data/config_files},
    run_folder => q{123456_IL2_1234},
    runfolder_path => $util->analysis_runfolder_path(),
    timestamp => q{20090709-123456},
    verbose => 0,
    no_bsub => 1,
    id_run => 1234,
    _extra_tradis_transposon_read => 1,
    bam_basecall_path => $util->analysis_runfolder_path() . q{/Data/Intensities/BaseCalls},
    _job_args => { _param_vals => { 1=> q/dummy/, }},
  ); } q{no croak creating bam_generator object};

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

  my $bc_path = q{/nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BaseCalls};
  my $expected_cmd = q{bsub -q srpipeline -E 'npg_pipeline_preexec_references' -R 'select[mem>12000] rusage[mem=12000,nfs_12=5]' -M12000 -R 'span[hosts=1]' -n8,16 -w'done(123) && done(321)' -J 'p4_stage1_analysis_1234_20090709-123456[1]' -o } . $bc_path . q{/log/p4_stage1_analysis_1234_20090709-123456.%I.%J.out 'perl -Mstrict -MJSON -MFile::Slurp -Mopen='"'"':encoding(UTF8)'"'"' -e '"'"'exec from_json(read_file shift@ARGV)->{shift@ARGV} or die q(failed exec)'"'"' } . $bc_path . q{/p4_stage1_analysis_1234_20090709-123456_$LSB_JOBID $LSB_JOBINDEX'};

  eq_or_diff([split" ",$bsub_command], [split" ",$expected_cmd], 'correct bsub command for lane 8');
}

