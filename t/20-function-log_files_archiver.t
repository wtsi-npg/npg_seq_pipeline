use strict;
use warnings;
use Test::More tests => 8;
use Test::Exception;
use Log::Log4perl qw(:levels);
use t::util;

my $util = t::util->new();
my $tmp_dir = $util->temp_directory();
Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $tmp_dir, 'logfile'),
                          utf8   => 1});

use_ok('npg_pipeline::function::log_files_archiver');

$ENV{TEST_FS_RESOURCE} = q{nfs_12};
local $ENV{PATH} = join q[:], q[t/bin], $ENV{PATH};

my $rfpath = $util->analysis_runfolder_path();

{
  my $a  = npg_pipeline::function::log_files_archiver->new(
    run_folder        => q{123456_IL2_1234},
    runfolder_path    => $rfpath,
    recalibrated_path => $rfpath,
    id_run            => 1234,
    timestamp         => q{20090709-123456},
  );
  isa_ok ($a , q{npg_pipeline::function::log_files_archiver});

  my @jids = $a->submit_to_lsf();
  is (scalar @jids, 1, q{one job submitted});
  my $bsub_command = $a ->_generate_bsub_command();
  my $expected_command = qq{bsub -q lowload -J npg_publish_illumina_logs.pl_1234_20090709-123456 -R 'rusage[nfs_12=1,seq_irods=15]' -o ${rfpath}/log/npg_publish_illumina_logs.pl_1234_20090709-123456.out -E "[ -d '$rfpath' ]" 'npg_publish_illumina_logs.pl --runfolder_path $rfpath --id_run 1234'};
  $expected_command =~ s/analysis/outgoing/g;
  is( $bsub_command, $expected_command, q{generated bsub command is correct});

  $a  = npg_pipeline::function::log_files_archiver->new(
    run_folder        => q{123456_IL2_1234},
    runfolder_path    => $rfpath,
    recalibrated_path => $rfpath,
    id_run            => 1234,
    no_irods_archival => 1,
  );
  ok ($a->no_irods_archival, q{archival switched off});
  @jids = $a->submit_to_lsf();
  is (scalar @jids, 0, q{no jobs submitted});

  $a  = npg_pipeline::function::log_files_archiver->new(
    run_folder        => q{123456_IL2_1234},
    runfolder_path    => $rfpath,
    recalibrated_path => $rfpath,
    id_run            => 1234,
    local             => 1,
  );
  ok ($a->no_irods_archival, q{archival switched off});
  @jids = $a->submit_to_lsf();
  is (scalar @jids, 0, q{no jobs submitted});
}

1;
