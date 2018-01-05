use strict;
use warnings;
use Test::More tests => 6;
use Test::Exception;
use t::util;

use_ok('npg_pipeline::archive::file::logs');

$ENV{TEST_FS_RESOURCE} = q{nfs_12};
local $ENV{PATH} = join q[:], q[t/bin], $ENV{PATH};

my $util = t::util->new();
my $tmp_dir = $util->temp_directory();
my $analysis_runfolder_path = $util->analysis_runfolder_path();
my $rfpath = '/nfs/sf45/IL2/outgoing/123456_IL2_1234';
mkdir -p $analysis_runfolder_path;

{
  my $bam_irods;

  lives_ok { $bam_irods = npg_pipeline::archive::file::logs->new(
    run_folder        => q{123456_IL2_1234},
    runfolder_path    => $analysis_runfolder_path,
    recalibrated_path => $analysis_runfolder_path,
    id_run            => 1234,
    timestamp         => q{20090709-123456},
    verbose           => 0
  ) } q{created with run_folder ok};
  isa_ok($bam_irods , q{npg_pipeline::archive::file::logs}, q{object test});
  #create_analysis();

  my $arg_refs = {
    required_job_completion => q{-w'done(123) && done(321)'},
  };

  my @jids;
  lives_ok { @jids = $bam_irods->submit_to_lsf($arg_refs); } q{no croak submitting job to lsf};

  is(scalar@jids, 1, q{only one job submitted});

  my $bsub_command = $util->drop_temp_part_from_paths( $bam_irods ->_generate_bsub_command($arg_refs) );
  my $expected_command = qq{bsub -q lowload -w'done(123) && done(321)' -J npg_publish_illumina_logs.pl_1234_20090709-123456 -R 'rusage[nfs_12=1,seq_irods=15]' -o ${rfpath}/log/npg_publish_illumina_logs.pl_1234_20090709-123456.out -E "[ -d '$rfpath' ]" 'npg_publish_illumina_logs.pl --runfolder_path $rfpath --id_run 1234'};
  is( $bsub_command, $expected_command, q{generated bsub command is correct});
}

1;
__END__
