use strict;
use warnings;
use Test::More tests => 6;
use Test::Exception;
use t::util;

use_ok('npg_pipeline::analysis::split_bam_by_tag');

my $util = t::util->new();
my $runfolder_path = $util->analysis_runfolder_path();
my $recalibrated = "$runfolder_path/Data/Intensities/Bustard1.3.4_09-07-2009_auto/PB_cal";

$ENV{TEST_DIR} = $util->temp_directory();
$ENV{TEST_FS_RESOURCE} = q{nfs_12};
local $ENV{PATH} = join q[:], q[t/bin], q[t/bin/software/solexa/bin], $ENV{PATH};
local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[];
local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = 't/data/qc/1234_samplesheet_amended.csv';

{ 
  local $ENV{CLASSPATH} = q{t/bin/software/solexa/bin/aligners/illumina2bam/current};
  $util->create_multiplex_analysis();
  my $generator;
  lives_ok { $generator = npg_pipeline::analysis::split_bam_by_tag->new(
    run_folder => q{123456_IL2_1234},
    runfolder_path => $runfolder_path,
    recalibrated_path => $recalibrated,
    timestamp => q{20090709-123456},
    verbose => 0,
    no_bsub => 1,
  ); } q{no croak creating $fastq_generator object};
  isa_ok($generator, q{npg_pipeline::analysis::split_bam_by_tag}, q{$generator});

  my $arg_refs = {
    'position' => q{`echo $LSB_JOBINDEX`},
    'bam' => q{1234_`echo $LSB_JOBINDEX`.bam},
    'output_prefix' => '/tmp/',
    'required_job_completion' => q{-w'done(123) && done(321)'},
    'array_string' => q{[1-4,8]},
  };

  my $bsub_command = $util->drop_temp_part_from_paths( $generator->_generate_bsub_command( $arg_refs ) );
  my $expected_command = q{bsub -q srpipeline -R 'rusage[nfs_12=8]' -w'done(123) && done(321)' -J split_bam_by_tag_1234_20090709-123456[1-4,8] -o /nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/Bustard1.3.4_09-07-2009_auto/PB_cal/log/split_bam_by_tag_1234_20090709-123456.%I.%J.out 'java -Xmx1024m -jar t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/SplitBamByReadGroup.jar CREATE_MD5_FILE=true VALIDATION_STRINGENCY=SILENT  I=1234_`echo $LSB_JOBINDEX`.bam O=/tmp/'};

  is( $bsub_command, $expected_command, q{generated bsub command is correct} );

  my @jids;
  lives_ok { @jids = $generator->generate( $arg_refs ); } q{no croak running generate};
  is(scalar@jids, 1, q{1 jobs id is returned});
}

1;
