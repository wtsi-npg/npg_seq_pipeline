use strict;
use warnings;
use Test::More tests => 14;
use Test::Exception;
use Log::Log4perl qw(:levels);
use t::util;

my $util = t::util->new({});
my $tmp_dir = $util->temp_directory();
local $ENV{TEST_DIR} = $tmp_dir;

local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];
local $ENV{PATH} = join q[:], q[t/bin], q[t/bin/software/solexa/bin], $ENV{PATH};

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $tmp_dir, 'logfile'),
                          utf8   => 1});

use_ok( q{npg_pipeline::archive::file::generation::seqchksum_comparator} );

$util->set_rta_staging_analysis_area();

my $timestamp = q{20100907-142417};
my $analysis_runfolder_path = $util->analysis_runfolder_path();
my $bam_basecall_path = $analysis_runfolder_path . "/Data/Intensities/BAM_basecalls_$timestamp/";
my $recalibrated_path = $analysis_runfolder_path. "/Data/Intensities/BAM_basecalls_$timestamp/no_cal";
my $archive_path = $recalibrated_path . q{/archive};

{
  my $object;
  lives_ok {
    $object = npg_pipeline::archive::file::generation::seqchksum_comparator->new(
      run_folder => q{123456_IL2_1234},
      runfolder_path => $analysis_runfolder_path,
      archive_path => $archive_path,
      bam_basecall_path => $bam_basecall_path,
      id_run => 1234,
      timestamp => $timestamp,
      no_bsub => 1,
      lanes => [1,2],
    );
  } q{object ok};

  isa_ok( $object, q{npg_pipeline::archive::file::generation::seqchksum_comparator}, q{$object} );

  my $arg_refs = {
    required_job_completion => q{-w'done(123) && done(321)'},
  };

  my $bsub_command = $util->drop_temp_part_from_paths( qq{bsub -q srpipeline -w'done(123) && done(321)' -J 'npg_pipeline_seqchksum_comparator_1234_20100907-142417[1-2]' -o $archive_path/log/npg_pipeline_seqchksum_comparator_1234_20100907-142417.%I.%J.out 'npg_pipeline_seqchksum_comparator --id_run=1234} .q{ --lanes=`echo $LSB_JOBINDEX` --archive_path=} . qq{$archive_path --bam_basecall_path=$bam_basecall_path'} );
  is( $util->drop_temp_part_from_paths( $object->_generate_bsub_command( $arg_refs ) ), $bsub_command, q{generated bsub command is correct} );

  my @jids = $object->launch( $arg_refs );
  is( scalar @jids, 1, q{1 job id returned} );

  throws_ok{$object->do_comparison()} qr/please check illumina2bam pipeline step/, q{Doing a comparison with no files throws an exception}; 

  is($object->archive_path, $archive_path, "Object has correct archive path");
  is($object->bam_basecall_path, $bam_basecall_path, "Object has correct bam_basecall path");

  my $seqchksum_contents1 = <<'END1';
###  set count   b_seq name_b_seq  b_seq_qual  b_seq_tags(BC,FI,QT,RT,TC)
all all 19821774    3a58186f  29528f13  7bf272c0  30e0b9ef
all pass  19821774    3a58186f  29528f13  7bf272c0  30e0b9ef
  all 0   1 1 1 1
  pass  0   1 1 1 1
1#0 all 3865560   4aebf9cb  63f4ad67  3d54f814  5c3f971f
1#0 pass  3865560   4aebf9cb  63f4ad67  3d54f814  5c3f971f
1#2 all 15956214    504ab7d8  28428e9b  643c096e  3cbf1e96
1#2 pass  15956214    504ab7d8  28428e9b  643c096e  3cbf1e96};
END1

  system "mkdir -p $archive_path/lane1";
  system "cp -p t/data/runfolder/archive/lane1/1234_1#15.cram $archive_path/lane1";

  system "mkdir -p $archive_path/lane2";
  system "cp -p t/data/runfolder/archive/lane1/1234_1#15.cram $archive_path/lane2/1234_2#15.cram";

  open my $seqchksum_fh1, '>', "$bam_basecall_path/1234_1.post_i2b.seqchksum" or die "Cannot open file for writing";
  print $seqchksum_fh1 $seqchksum_contents1 or die $!;
  close $seqchksum_fh1 or die $!;

  SKIP: {
    skip 'no tools', 2 if ((not $ENV{TOOLS_INSTALLED}) and (system(q(which bamseqchksum)) or system(q(which scramble))));
    TODO: { local $TODO= q(scramble doesn't through an exception when converting an empty bam file to cram it just writes a cram files with a @PG ID:scramble .. line);
      throws_ok{$object->do_comparison()} qr/Failed to run command bamcat /, q{Doing a comparison with empty bam files throws an exception}; 
    }

    system "cp -p t/data/seqchksum/sorted.cram $archive_path/lane1/1234_1#15.cram";
    system "cp -p t/data/seqchksum/sorted.cram $archive_path/lane2/1234_2#15.cram";

    throws_ok{$object->do_comparison()} qr/Found a difference in seqchksum for post_i2b and product /, q{Doing a comparison with different bam files throws an exception}; 
  }
}

{
  my $object;
  lives_ok {
    $object = npg_pipeline::archive::file::generation::seqchksum_comparator->new(
      run_folder => q{123456_IL2_1234},
      runfolder_path => $analysis_runfolder_path,
      bam_basecall_path => $bam_basecall_path,
      archive_path => $archive_path,
      id_run => 1234,
      no_bsub => 1,
      lanes => [1],
    );
  } q{object ok};

  my $arg_refs = {
    required_job_completion => q{-w'done(123) && done(321)'},
  };

  my @jids = $object->launch( $arg_refs );
  is( scalar @jids, 1, q{1 job id returned} );

  lives_ok {
    $object = npg_pipeline::archive::file::generation::seqchksum_comparator->new(
      run_folder => q{123456_IL2_1234},
      runfolder_path => $analysis_runfolder_path,
      bam_basecall_path => $bam_basecall_path,
      archive_path => $archive_path,
      id_run => 1234,
      no_bsub => 1,
    );
  } q{object ok};

  lives_ok{$object->launch( $arg_refs )} q{Launching with no positions does not throw an exception};
}

1;
