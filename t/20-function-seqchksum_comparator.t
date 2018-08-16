use strict;
use warnings;
use Test::More tests => 23;
use Test::Exception;
use Log::Log4perl qw(:levels);
use t::util;

my $util = t::util->new();
my $tmp_dir = $util->temp_directory();

local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_1234.csv];
# if REF_PATH is not set, force using ref defined in the header
local $ENV{REF_PATH} = $ENV{REF_PATH} ? $ENV{REF_PATH} : 'DUMMY';

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $tmp_dir, 'logfile'),
                          utf8   => 1});

use_ok( q{npg_pipeline::function::seqchksum_comparator} );

$util->set_rta_staging_analysis_area();

my $timestamp = q{20100907-142417};
my $analysis_runfolder_path = $util->analysis_runfolder_path();
#$util->create_analysis();
my $bam_basecall_path = $analysis_runfolder_path . "/Data/Intensities/BAM_basecalls_$timestamp/";
my $recalibrated_path = $analysis_runfolder_path. "/Data/Intensities/BAM_basecalls_$timestamp/no_cal";
my $archive_path = $recalibrated_path . q{/archive};

{
  my $object;
  lives_ok {
    $object = npg_pipeline::function::seqchksum_comparator->new(
      run_folder        => q{123456_IL2_1234},
      runfolder_path    => $analysis_runfolder_path,
      archive_path      => $archive_path,
      bam_basecall_path => $bam_basecall_path,
      id_run            => 1234,
      timestamp         => $timestamp,
      lanes             => [1,2],
      is_indexed        => 0,
    );
  } q{object ok};

  isa_ok( $object, q{npg_pipeline::function::seqchksum_comparator});
  my $da = $object->create();
# ok ($da && @{$da} == 2, 'an array with two definitions is returned');
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  my $d = $da->[0];
  isa_ok($d, q{npg_pipeline::function::definition});
  is ($d->created_by, q{npg_pipeline::function::seqchksum_comparator},
    'created_by is correct');
  is ($d->created_on, $object->timestamp, 'created_on is correct');
  is ($d->identifier, 1234, 'identifier is set correctly');
  ok ($d->has_composition, 'composition is set');
  is ($d->composition->num_components, 1, 'one componet in a composition');
  is ($d->composition->get_component(0)->position, 1, 'correct position');
  ok (!defined $d->composition->get_component(0)->tag_index,
    'tag index is not defined');
  ok (!defined $d->composition->get_component(0)->subset,
    'subset is not defined');
  is ($d->job_name, q{seqchksum_comparator_1234_20100907-142417},
    'job_name is correct');
  is ($d->command,
    q{npg_pipeline_seqchksum_comparator --id_run=1234 --archive_path=} .
#   qq{$archive_path --bam_basecall_path=$bam_basecall_path --lanes=1},
    qq{$archive_path --bam_basecall_path=$bam_basecall_path} .
    qq{ --input_globs=$archive_path/lane1/1234_1*.cram} .
    qq{ --input_globs=$archive_path/lane2/1234_2*.cram},
    'command is correct');
  ok (!$d->excluded, 'step not excluded');
  ok (!$d->has_num_cpus, 'number of cpus is not set');
  ok (!$d->has_memory,'memory is not set');
  is ($d->queue, 'default', 'default queue');
  lives_ok {$d->freeze()} 'definition can be serialized to JSON';

# seqchksum_comparator is now done at run-level, so only one definition
# $d = $da->[1];
# is ($d->composition->get_component(0)->position, 2, 'position');
# is ($d->command,
#   q{npg_pipeline_seqchksum_comparator --id_run=1234 --archive_path=} .
#   qq{$archive_path --bam_basecall_path=$bam_basecall_path --lanes=2},
#   'command is correct');

# throws_ok{$object->do_comparison()} qr/Cannot find/,
  throws_ok{$object->do_comparison()} qr/Failed to change directory/,
    q{Doing a comparison with no files throws an exception}; 

#############
#############
#############
#############
##   my $seqchksum_contents1 = <<'END1';
## ###  set count   b_seq name_b_seq  b_seq_qual  b_seq_tags(BC,FI,QT,RT,TC)
## all all 19821774    3a58186f  29528f13  7bf272c0  30e0b9ef
## all pass  19821774    3a58186f  29528f13  7bf272c0  30e0b9ef
##   all 0   1 1 1 1
##   pass  0   1 1 1 1
## 1#0 all 3865560   4aebf9cb  63f4ad67  3d54f814  5c3f971f
## 1#0 pass  3865560   4aebf9cb  63f4ad67  3d54f814  5c3f971f
## 1#2 all 15956214    504ab7d8  28428e9b  643c096e  3cbf1e96
## 1#2 pass  15956214    504ab7d8  28428e9b  643c096e  3cbf1e96};
## END1
## 
##   system "mkdir -p $archive_path/lane1";
##   system "cp -p t/data/runfolder/archive/lane1/1234_1#15.cram $archive_path/lane1";
## 
##   system "mkdir -p $archive_path/lane2";
##   system "cp -p t/data/runfolder/archive/lane1/1234_1#15.cram $archive_path/lane2/1234_2#15.cram";
##   system "cp -p t/data/runfolder/archive/lane1/1234_1#15.seqchksum $archive_path/lane2/1234_2#15.seqchksum";
## 
##   open my $seqchksum_fh1, '>', "$bam_basecall_path/1234_1.post_i2b.seqchksum" or die "Cannot open file for writing";
##   print $seqchksum_fh1 $seqchksum_contents1 or die $!;
##   close $seqchksum_fh1 or die $!;
## 
##   SKIP: {
##     skip 'no tools', 2 if ((not $ENV{TOOLS_INSTALLED}) and (system(q(which bamseqchksum)) or system(q(which scramble))));
##     TODO: { local $TODO= q(scramble doesn't through an exception when converting an empty bam file to cram it just writes a cram files with a @PG ID:scramble .. line);
##       throws_ok{$object->do_comparison()} qr/Failed to run command bamcat /, q{Doing a comparison with empty bam files throws an exception}; 
##     }
## 
##     system "cp -p t/data/seqchksum/sorted.cram $archive_path/lane1/1234_1#15.cram";
##     system "cp -p t/data/seqchksum/sorted.cram $archive_path/lane2/1234_2#15.cram";
## 
##     throws_ok { $object->do_comparison() }
##       qr/seqchksum for post_i2b and product are different/,
##       q{Doing a comparison with different bam files throws an exception}; 
##   }
#############
#############
#############
#############
}

{
  my $object = npg_pipeline::function::seqchksum_comparator->new(
    run_folder        => q{123456_IL2_1234},
    runfolder_path    => $analysis_runfolder_path,
    bam_basecall_path => $bam_basecall_path,
    archive_path      => $archive_path,
    id_run            => 1234,
    lanes             => [1],
    is_indexed        => 0,
  );
  my $da = $object->create();
  ok ($da && @{$da} == 1, 'an array with one definitions is returned');

  $object = npg_pipeline::function::seqchksum_comparator->new(
    run_folder        => q{123456_IL2_1234},
    runfolder_path    => $analysis_runfolder_path,
    bam_basecall_path => $bam_basecall_path,
    archive_path      => $archive_path,
    id_run            => 1234,
    is_indexed        => 0,
  );
  $da = $object->create();
  # seqchksum_comparator is now a run-level function, so only one definition returned
# ok ($da && @{$da} == 8, 'an array with eight definitions is returned');
  ok ($da && @{$da} == 1, 'an array with one definition is returned for eight lanes');

# now that this function is run-level only, lanes attribute is not needed
# throws_ok{ $object->do_comparison() }
#   qr/Lanes have to be given explicitly/,
#   q{lanes attribute is needed to run the comparison};
}

1;
