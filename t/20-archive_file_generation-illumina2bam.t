use strict;
use warnings;
use Test::More tests => 33;
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
local $ENV{NPG_WEBSERVICE_CACHE_DIR} = 't/data/illumina2bam';
local $ENV{CLASSPATH} = q{t/bin/software/solexa/bin/aligners/illumina2bam/current};
local $ENV{PATH} = join q[:], q[t/bin], q[t/bin/software/solexa/bin], $ENV{PATH};

use_ok('npg_pipeline::archive::file::generation::illumina2bam');
my $current = getcwd();

{
  my $new = "$dir/1234_samplesheet.csv";
  copy 't/data/illumina2bam/1234_samplesheet.csv', $new;
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = $new;
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[];

  $util->create_analysis();
  my $runfolder = $util->analysis_runfolder_path() . '/';
  `cp t/data/runfolder/Data/RunInfo.xml $runfolder`;

  my $bam_generator;

  lives_ok { $bam_generator = npg_pipeline::archive::file::generation::illumina2bam->new(
    run_folder => q{123456_IL2_1234},
    runfolder_path => $util->analysis_runfolder_path(),
    timestamp => q{20090709-123456},
    verbose => 0,
    no_bsub => 1,
    id_run => 1234,
    _extra_tradis_transposon_read => 1,
    bam_basecall_path => $util->analysis_runfolder_path() . q{/Data/Intensities/BaseCalls},
  ); } q{no croak creating bam_generator object};

  isa_ok($bam_generator, q{npg_pipeline::archive::file::generation::illumina2bam}, q{$bam_generator});
  is($bam_generator->_extra_tradis_transposon_read, 1, 'TraDIS set');
  $bam_generator->_extra_tradis_transposon_read(0);
  is($bam_generator->_extra_tradis_transposon_read, 0, 'TraDIS not set');
  isa_ok($bam_generator->lims, 'st::api::lims', 'cached lims object');

  my $arg_refs = {
    required_job_completion => q{-w'done(123) && done(321)'}, 
  };
  
  my $mem = 4000;
  my $cpu = 2;
  my $alims = $bam_generator->lims->children_ia;
  my $position = 8;
  my $bsub_command = $bam_generator->_generate_bsub_commands( $arg_refs , $alims->{$position}, 't/data/taglistfile');

  is( $bam_generator->_get_number_of_plexes_excluding_control($alims->{$position}), 
  1, 'correct number of plexes');

  $bsub_command = $util->drop_temp_part_from_paths( $bsub_command );
  my $expected_cmd = q{bsub -q srpipeline -R 'select[mem>}.$mem.q{] rusage[mem=}.$mem.q{,nfs_12=4]' -M}.$mem.q{ -R 'span[hosts=1]' -n}.$cpu.q{ -w'done(123) && done(321)' -J 'illumina2bam_1234_8_20090709-123456' -o /nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BaseCalls/log/illumina2bam_1234_8_20090709-123456.%J.out /bin/bash -c 'set -o pipefail;java -Xmx1024m -jar t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/Illumina2bam.jar I=/nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities L=8 B=/nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BaseCalls RG=1234_8 PU=123456_IL2_1234_8 LIBRARY_NAME="51021" SAMPLE_ALIAS="SRS000147" STUDY_NAME="SRP000031: 1000Genomes Project Pilot 1" OUTPUT=/dev/stdout COMPRESSION_LEVEL=0 | java -Xmx1024m -jar t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/BamIndexDecoder.jar VALIDATION_STRINGENCY=SILENT I=/dev/stdin  BARCODE_FILE=t/data/taglistfile METRICS_FILE=/nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BaseCalls/1234_8.bam.tag_decode.metrics MAX_NO_CALLS=6 CONVERT_LOW_QUALITY_TO_NO_CALL=true CREATE_MD5_FILE=false OUTPUT=/dev/stdout};
  $expected_cmd .= q{| tee >(bamseqchksum > /nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BaseCalls/1234_8.post_i2b.seqchksum)};
  $expected_cmd .= q{ >(md5sum -b | tr -d '"'"'"'"'"'"'"'"'\n *\-'"'"'"'"'"'"'"'"' > /nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BaseCalls/1234_8.bam.md5)};
  $expected_cmd .= q{ > /nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BaseCalls/1234_8.bam'};

  eq_or_diff([split"=",$bsub_command], [split"=",$expected_cmd], 'correct bsub command for lane 8');


  my @jids;
  lives_ok { @jids = $bam_generator->generate($arg_refs); } q{no croak running generate};
  is(scalar @jids, 8, 'correct number of jobs submitted');
  ok(-f "$dir/lane_8.taglist", 'lane 8 tag list file generated');
  foreach my $lane ((1 .. 7)) {
    ok(!-e "$dir/lane_$lane.taglist", "lane $lane tag list file does not exist");
  }

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[];
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = 't/data';

  lives_ok { $bam_generator = npg_pipeline::archive::file::generation::illumina2bam->new(
    run_folder => q{123456_IL2_1234},
    runfolder_path => $util->analysis_runfolder_path(),
    timestamp => q{20090709-123456},
    verbose => 0,
    id_run => 8033,
    bam_basecall_path => $util->analysis_runfolder_path() . q{/Data/Intensities/BaseCalls},
  ); } q{no croak creating bam_generator object};

  is($bam_generator->_extra_tradis_transposon_read, 1, 'TraDIS set');

  $arg_refs = {
    required_job_completion => q{-w'done(123) && done(321)'}, 
  };
  
  $alims = $bam_generator->lims->children_ia;
  throws_ok {$bam_generator->_generate_bsub_commands( $arg_refs , $alims->{$position})}
    qr/Tag list file path should be defined/,
    'error when tag file name is missing for a pool';

  $bsub_command = $bam_generator->_generate_bsub_commands( $arg_refs , $alims->{$position}, 't/data/lanetagfile');
  is( $bam_generator->_get_number_of_plexes_excluding_control($alims->{$position}), 
  72, 'correct number of plexes');

  $bsub_command = $util->drop_temp_part_from_paths( $bsub_command );
  $expected_cmd = q{bsub -q srpipeline -R 'select[mem>}.$mem.q{] rusage[mem=}.$mem.q{,nfs_12=4]' -M}.$mem.q{ -R 'span[hosts=1]' -n}.$cpu.  q{ -w'done(123) && done(321)' -J 'illumina2bam_8033_8_20090709-123456' -o /nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BaseCalls/log/illumina2bam_8033_8_20090709-123456.%J.out /bin/bash -c 'set -o pipefail;java -Xmx1024m -jar t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/Illumina2bam.jar I=/nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities L=8 B=/nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BaseCalls RG=8033_8 PU=123456_IL2_1234_8 LIBRARY_NAME="5206896" SAMPLE_ALIAS="ERS124385,ERS124386,ERS124387,ERS124388,ERS124389,ERS124390,ERS124391,ERS124392,ERS124393,ERS124394,ERS124395,ERS124396,ERS124397,ERS124398,ERS124399,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124385,ERS124386,ERS124387,ERS124388,ERS124389,ERS124390,ERS124391,ERS124392,ERS124393,ERS124394,ERS124395,ERS124396,ERS124397,ERS124398,ERS124399,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400,ERS124400" STUDY_NAME="mouse PiggyBac sequencing: sites of PiggyBac integration into mouse genome" SEC_BC_SEQ=BC SEC_BC_QUAL=QT BC_SEQ=tr BC_QUAL=tq OUTPUT=/dev/stdout COMPRESSION_LEVEL=0 | java -Xmx1024m -jar t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/BamIndexDecoder.jar VALIDATION_STRINGENCY=SILENT I=/dev/stdin  BARCODE_FILE=t/data/lanetagfile METRICS_FILE=/nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BaseCalls/8033_8.bam.tag_decode.metrics CREATE_MD5_FILE=false OUTPUT=/dev/stdout};
  $expected_cmd .= q{| tee >(bamseqchksum > /nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BaseCalls/8033_8.post_i2b.seqchksum)};
  $expected_cmd .= q{ >(md5sum -b | tr -d '"'"'"'"'"'"'"'"'\n *\-'"'"'"'"'"'"'"'"' > /nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BaseCalls/8033_8.bam.md5)};
  $expected_cmd .= q{ > /nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BaseCalls/8033_8.bam'};

  eq_or_diff([split"=",$bsub_command], [split"=",$expected_cmd], 'correct bsub command for lane 8');

## test of special 3' pulldown RNAseq read 1 index

  lives_ok { $bam_generator = npg_pipeline::archive::file::generation::illumina2bam->new(
    run_folder => q{121112_HS20_08797_A_C18TEACXX},
    runfolder_path => $util->analysis_runfolder_path(), 
    timestamp => q{20121112-123456},
    verbose => 0,
    id_run => 8797,
    bam_basecall_path =>  $util->analysis_runfolder_path(). q{/Data/Intensities/BaseCalls},
  ); } q{no croak creating bam_generator object for run 8797};

  $arg_refs = {
    required_job_completion => q{-w'done(123) && done(321)'}, 
  };
  
  $alims = $bam_generator->lims->associated_child_lims_ia;
  $position = 8;
  $bsub_command = $bam_generator->_generate_bsub_commands( $arg_refs , $alims->{$position}, 't/data/lanetagfile');

  $bsub_command = $util->drop_temp_part_from_paths( $bsub_command );

  $expected_cmd = q{bsub -q srpipeline -R 'select[mem>}.$mem.q{] rusage[mem=}.$mem.q{,nfs_12=4]' -M}.$mem.q{ -R 'span[hosts=1]' -n}.$cpu.  q{ -w'done(123) && done(321)' -J 'illumina2bam_8797_8_20121112-123456' -o /nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BaseCalls/log/illumina2bam_8797_8_20121112-123456.%J.out /bin/bash -c 'set -o pipefail;java -Xmx1024m -jar t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/Illumina2bam.jar I=/nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities L=8 B=/nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BaseCalls RG=8797_8 PU=121112_HS20_08797_A_C18TEACXX_8 LIBRARY_NAME="6045465" SAMPLE_ALIAS="ERS181250,ERS181251,ERS181252,ERS181253,ERS181254,ERS181255" STUDY_NAME="ERP001656: Total RNA was extracted from morpholically abnormal and sibling wild type embryos identified by the Zebrafish Mutation Project (http://www.sanger.ac.uk/Projects/D_rerio/zmp/). The 3prime end of fragmented RNA was pulled down using polyToligos attached to magnetic beads, reverse transcribed, made into Illumina libraries and sequenced using IlluminaHiSeq paired-end sequencing. Protocol: Total RNA was extracted from mouse embryos using Trizol and DNase treated. Chemically fragmented RNA was enriched for the 3prime ends by pulled down using an anchored polyToligo attached to magnetic beads. An RNA oligo comprising part of the Illumina adapter 2 was ligated to the 5prime end of the captured RNA and the RNA was eluted from the beads. Reverse transcription was primed with an anchored polyToligo with part of Illumina adapter 1 at the 5prime end followed by 4 random bases, then an A, C or G base, then one of twelve5 base indexing tags and 14 T bases. An Illumina library with full adapter sequence was produced by 15 cycles of PCR.   This data is part of a pre-publication release. For information on the proper use of pre-publication data shared by the Wellcome Trust Sanger Institute (including details of any publication moratoria), please see http://www.sanger.ac.uk/datasharing/" FIRST_INDEX=6 FINAL_INDEX=10 FIRST_INDEX=1 FINAL_INDEX=5 SEC_BC_SEQ=br SEC_BC_QUAL=qr BC_READ=1 SEC_BC_READ=1 FIRST=11 FINAL=50 OUTPUT=/dev/stdout COMPRESSION_LEVEL=0 | java -Xmx1024m -jar t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/BamIndexDecoder.jar VALIDATION_STRINGENCY=SILENT I=/dev/stdin  BARCODE_FILE=t/data/lanetagfile METRICS_FILE=/nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BaseCalls/8797_8.bam.tag_decode.metrics CREATE_MD5_FILE=false OUTPUT=/dev/stdout};
  $expected_cmd .= q{| tee >(bamseqchksum > /nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BaseCalls/8797_8.post_i2b.seqchksum)};
  $expected_cmd .= q{ >(md5sum -b | tr -d '"'"'"'"'"'"'"'"'\n *\-'"'"'"'"'"'"'"'"' > /nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BaseCalls/8797_8.bam.md5)};
  $expected_cmd .= q{ > /nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BaseCalls/8797_8.bam'};
  eq_or_diff([split"=",$bsub_command], [split"=",$expected_cmd], 'correct bsub command for run 8797 lane 8, special "jecfoo" read1 index');

}

{ ## adapter detection
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = 't/data';
  my $rf = join q[/], $dir, q[131010_HS34_11018_B_H722AADXX];
  my $bc = join q[/], $rf, q[Data/Intensities/BaseCalls];
  my $i = join q[/], $rf, q[Data/Intensities];
  make_path $bc;
  copy q[t/data/example_runfolder/131010_HS34_11018_B_H722AADXX/RunInfo.xml], $rf;

  my $bam_generator;
  lives_ok { $bam_generator = npg_pipeline::archive::file::generation::illumina2bam->new(
    runfolder_path => $rf,
    is_indexed => 0,
    verbose => 0,
    timestamp => q{20131028-155757},
    bam_basecall_path => $bc,
  ); } q{no croak creating bam_generator object for run 11018};

  my $alims = $bam_generator->lims->associated_child_lims_ia;
  my $position = 1;
  my $arg_refs = {
    required_job_completion => q{-w'done(123) && done(321)'},
  };

  my $mem = $bam_generator->general_values_conf()->{illumina2bam_memory};
  my $cpu = $bam_generator->general_values_conf()->{illumina2bam_cpu};
  my $bsub_command = $bam_generator->_generate_bsub_commands( $arg_refs , $alims->{$position});
  #$bsub_command = $util->drop_temp_part_from_paths( $bsub_command );

  my $expected_cmd = q{bsub -q srpipeline -R 'select[mem>}.$mem.q{] rusage[mem=}.$mem.q{,nfs_12=4]' -M}.$mem.q{ -R 'span[hosts=1]' -n} . $cpu . q{ -w'done(123) && done(321)' -J 'illumina2bam_11018_1_20131028-155757' -o } . $bc . q{/log/illumina2bam_11018_1_20131028-155757.%J.out /bin/bash -c 'set -o pipefail;java -Xmx1024m -jar } . $current . q{/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/Illumina2bam.jar I=} . qq{$i L=1 B=$bc RG=11018_1 PU=131010_HS34_11018_B_H722AADXX_1 LIBRARY_NAME="8314075" SAMPLE_ALIAS="ERS333055,ERS333070,ERS333072,ERS333073,ERS333076,ERS333077" STUDY_NAME="ERP000730: llumina sequencing of various Plasmodium species is being carried out for de novo assembly and comparative genomics. This data is part of a pre-publication release. For information on the proper use of pre-publication data shared by the Wellcome Trust Sanger Institute (including details of any publication moratoria), please see http://www.sanger.ac.uk/datasharing/" OUTPUT=/dev/stdout COMPRESSION_LEVEL=0 | bamadapterfind md5=1 md5filename=$bc/11018_1.bam.md5};
  $expected_cmd .= qq{| tee >(bamseqchksum > $bc/11018_1.post_i2b.seqchksum)};
  $expected_cmd .= qq{ > $bc/11018_1.bam'};
  
  eq_or_diff([split"=",$bsub_command], [split"=",$expected_cmd], 'correct bsub command for lane 1 (with adapter detection)');
}

{ ## more testing of special 3' pulldown RNAseq
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = 't/data';

  my $rf = join q[/], $dir, q[121103_HS29_08747_B_C1BV5ACXX];
  my $bc = join q[/], $rf, q[Data/Intensities/BaseCalls];
  my $i = join q[/], $rf, q[Data/Intensities];
  make_path $bc;
  copy q[t/data/example_runfolder/121103_HS29_08747_B_C1BV5ACXX/RunInfo.xml], $rf;

  my $bam_generator;
  lives_ok { $bam_generator = npg_pipeline::archive::file::generation::illumina2bam->new(
    runfolder_path => $rf,
    timestamp => q{20121112-123456},
    bam_basecall_path => $bc,
    verbose => 0,
  ); } q{no croak creating bam_generator object for run 8747};

  my $alims = $bam_generator->lims->associated_child_lims_ia;
  my $position = 4;
  my $arg_refs = {
    required_job_completion => q{-w'done(123) && done(321)'},
  };

  my $mem = 4000;
  my $cpu = 2;
  my $bsub_command = $bam_generator->_generate_bsub_commands( $arg_refs , $alims->{$position}, 't/data/lanetagfile');

  my $expected_cmd = q{bsub -q srpipeline -R 'select[mem>}.$mem.q{] rusage[mem=}.$mem.q{,nfs_12=4]' -M}.$mem.q{ -R 'span[hosts=1]' -n}.$cpu.  q{ -w'done(123) && done(321)' -J 'illumina2bam_8747_4_20121112-123456' -o } . $bc . q{/log/illumina2bam_8747_4_20121112-123456.%J.out /bin/bash -c 'set -o pipefail;java -Xmx1024m -jar } . $current . q{/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/Illumina2bam.jar I=} . $i . q{ L=4 B=} . $bc . q{ RG=8747_4 PU=121103_HS29_08747_B_C1BV5ACXX_4 LIBRARY_NAME="6101244" SAMPLE_ALIAS="ERS183138,ERS183139,ERS183140,ERS183141,ERS183142,ERS183143" STUDY_NAME="ERP001559: Total RNA was extracted from wild type and mutant zebrafish embryos.  Double stranded cDNA representing the 3'"'"'"'"'"'"'"'"' ends of transcripts was made by a variety of methods, including polyT priming and 3'"'"'"'"'"'"'"'"' pull down on magentic beads.   Some samples included indexing test experiments where a sequence barcode was placed within one of the sequence reads.. This data is part of a pre-publication release. For information on the proper use of pre-publication data shared by the Wellcome Trust Sanger Institute (including details of any publication moratoria), please see http://www.sanger.ac.uk/datasharing/" FIRST_INDEX=5 FINAL_INDEX=10 FIRST_INDEX=1 FINAL_INDEX=4 SEC_BC_SEQ=br SEC_BC_QUAL=qr BC_READ=1 SEC_BC_READ=1 FIRST=11 FINAL=75 FIRST=84 FINAL=158 OUTPUT=/dev/stdout COMPRESSION_LEVEL=0 | java -Xmx1024m -jar } . $current . q{/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/BamIndexDecoder.jar VALIDATION_STRINGENCY=SILENT I=/dev/stdin  BARCODE_FILE=t/data/lanetagfile METRICS_FILE=} . $bc . q{/8747_4.bam.tag_decode.metrics CREATE_MD5_FILE=false OUTPUT=/dev/stdout};
  $expected_cmd .= qq{| tee >(bamseqchksum > $bc/8747_4.post_i2b.seqchksum)};
  $expected_cmd .= q{ >(md5sum -b | tr -d '"'"'"'"'"'"'"'"'\n *\-'"'"'"'"'"'"'"'"' > } . qq{$bc/8747_4.bam.md5)};
  $expected_cmd .= qq{ > $bc/8747_4.bam'};

  eq_or_diff([split"=",$bsub_command], [split"=",$expected_cmd], 'correct bsub command for lane 4 of 3 prime pulldown');
}

{ ## more testing of special 3' pulldown RNAseq for non-standard inline index
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = 't/data';

  my $rf = join q[/], $dir, q[130917_MS6_10808_A_MS2030455-300V2];
  my $bc = join q[/], $rf, q[Data/Intensities/BaseCalls];
  my $i = join q[/], $rf, q[Data/Intensities];
  make_path $bc;
  copy q[t/data/example_runfolder/130917_MS6_10808_A_MS2030455-300V2/RunInfo.xml], $rf;

  my $bam_generator;
  lives_ok { $bam_generator = npg_pipeline::archive::file::generation::illumina2bam->new(
    runfolder_path => $rf,
    timestamp => q{20130919-132702},
    bam_basecall_path => $bc,
    verbose => 0,
  ); } q{no croak creating bam_generator object for run 10808};

  my $alims = $bam_generator->lims->associated_child_lims_ia;
  my $position = 1;
  my $arg_refs = {
    required_job_completion => q{-w'done(123) && done(321)'},
  };

  my $mem = 4000;
  my $cpu = 2;
  my $bsub_command = $bam_generator->_generate_bsub_commands( $arg_refs , $alims->{$position}, 't/data/lanetagfile');

  my $expected_cmd = q{bsub -q srpipeline -R 'select[mem>}.$mem.q{] rusage[mem=}.$mem.q{,nfs_12=4]' -M}.$mem.q{ -R 'span[hosts=1]' -n}.$cpu.  q{ -w'done(123) && done(321)' -J 'illumina2bam_10808_1_20130919-132702' -o } . $bc . q{/log/illumina2bam_10808_1_20130919-132702.%J.out /bin/bash -c 'set -o pipefail;java -Xmx1024m -jar } . $current . q{/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/Illumina2bam.jar I=} . qq{$i L=1 B=$bc} . q{ RG=10808_1 PU=130917_MS6_10808_A_MS2030455-300V2_1 LIBRARY_NAME="8115659" SAMPLE_ALIAS="single_cell_1,single_cell_2,single_cell_3,single_cell_4" STUDY_NAME="Transcriptome profiling protocol development: Various test protocols to improve the 3'"'"'"'"'"'"'"'"' pull down transcript profiling protocol, aiming to produce a pipeline library prep protocol. This data is part of a pre-publication release. For information on the proper use of pre-publication data shared by the Wellcome Trust Sanger Institute (including details of any publication moratoria), please see http://www.sanger.ac.uk/datasharing/ " FIRST=1 FINAL=150 FIRST_INDEX=168 FINAL_INDEX=172 FIRST_INDEX=156 FINAL_INDEX=167 SEC_BC_SEQ=br SEC_BC_QUAL=qr BC_READ=2 SEC_BC_READ=2 FIRST=173 FINAL=305 OUTPUT=/dev/stdout COMPRESSION_LEVEL=0 | java -Xmx1024m -jar } . $current . q{/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/BamIndexDecoder.jar VALIDATION_STRINGENCY=SILENT I=/dev/stdin  BARCODE_FILE=t/data/lanetagfile METRICS_FILE=} . $bc . q{/10808_1.bam.tag_decode.metrics CREATE_MD5_FILE=false OUTPUT=/dev/stdout};
  $expected_cmd .= qq{| tee >(bamseqchksum > $bc/10808_1.post_i2b.seqchksum)};
  $expected_cmd .= q{ >(md5sum -b | tr -d '"'"'"'"'"'"'"'"'\n *\-'"'"'"'"'"'"'"'"' > } . qq{$bc/10808_1.bam.md5)};
  $expected_cmd .= qq{ > $bc/10808_1.bam'};
    
  eq_or_diff([split"=",$bsub_command], [split"=",$expected_cmd], 'correct bsub command for lane 1 of 3 prime pulldown');
}

{ ## test of un-equal read lengths
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = 't/data';

  my $rf = join q[/], $dir, q[131021_MS5_11123_A_MS2000187-150V3];
  my $bc = join q[/], $rf, q[Data/Intensities/BaseCalls];
  my $i = join q[/], $rf, q[Data/Intensities];
  make_path $bc;
  copy q[t/data/example_runfolder/131021_MS5_11123_A_MS2000187-150V3/RunInfo.xml], $rf;

  my $bam_generator;
  lives_ok { $bam_generator = npg_pipeline::archive::file::generation::illumina2bam->new(
    runfolder_path => $rf,
    is_indexed => 0,
    timestamp => q{20131022-114117},
    bam_basecall_path => $bc,
    verbose => 0,
  ); } q{no croak creating bam_generator object for run 1123};

  my $alims = $bam_generator->lims->associated_child_lims_ia;
  my $position = 1;
  my $arg_refs = {
    required_job_completion => q{-w'done(123) && done(321)'},
  };

  my $mem = $bam_generator->general_values_conf()->{illumina2bam_memory};
  my $cpu = $bam_generator->general_values_conf()->{illumina2bam_cpu};
  my $bsub_command = $bam_generator->_generate_bsub_commands( $arg_refs , $alims->{$position});

  my $expected_cmd = q{bsub -q srpipeline -R 'select[mem>}.$mem.q{] rusage[mem=}.$mem.q{,nfs_12=4]' -M}. $mem. q{ -R 'span[hosts=1]' -n}.$cpu. q{ -w'done(123) && done(321)' -J 'illumina2bam_11123_1_20131022-114117' -o } . $bc . q{/log/illumina2bam_11123_1_20131022-114117.%J.out /bin/bash -c 'set -o pipefail;java -Xmx1024m -jar } . $current  . q{/t/bin/software/solexa/bin/aligners/illumina2bam/Illumina2bam-tools-1.00/Illumina2bam.jar } . qq{I=$i L=1 B=$bc} . q{ RG=11123_1 PU=131021_MS5_11123_A_MS2000187-150V3_1 LIBRARY_NAME="8111702" SAMPLE_ALIAS="arg404,arg405,arg406,arg407,arg408,arg409,arg410,arg411,arg412,arg413,arg414,arg415,arg416,arg417,arg418,arg419,arg420,arg421,arg422,arg423,arg424,arg425" STUDY_NAME="ERP001151: Data obtained from the sequencing of pools of barcoded P. berghei transgenics is predicted to allow for qualitative and quantitative measurements of individual mutant progeny generated during multiplex transfections. This type of analysis is expected to take P. berghei reverse genetics beyond that of the single-gene level. It aims to explore genetic interactions by measuring the effect on growth rates caused by simultaneous disruption of different genes in diverse genetic backgrounds, as well as potentially becoming a tool to identify essential genes to be prioritised as e.g. potential drug targets, or conversely to be excluded from future gene disruption studies. This data is part of a pre-publication release. For information on the proper use of pre-publication data shared by the Wellcome Trust Sanger Institute (including details of any publication moratoria), please see http://www.sanger.ac.uk/datasharing/" CREATE_MD5_FILE=false OUTPUT=/dev/stdout};
  $expected_cmd .= qq{| tee >(bamseqchksum > $bc/11123_1.post_i2b.seqchksum)};
  $expected_cmd .= q{ >(md5sum -b | tr -d '"'"'"'"'"'"'"'"'\n *\-'"'"'"'"'"'"'"'"' > } . qq{$bc/11123_1.bam.md5)};
  $expected_cmd .= qq{ > $bc/11123_1.bam'};

  eq_or_diff([split"=",$bsub_command], [split"=",$expected_cmd], 'correct bsub command for run with un-equal read lengths');
}

1;
__END__
