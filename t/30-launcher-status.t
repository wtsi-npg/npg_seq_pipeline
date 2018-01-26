use strict;
use warnings;
use Test::More tests => 9;
use Test::Exception;
use File::Temp qw{ tempdir };

local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];

use_ok('npg_pipeline::launcher::status');

my $temp = tempdir(CLEANUP => 1);
my $run_folder_path = join q[/], $temp, 'analysis';
mkdir $run_folder_path;
my $status_dir = join q[/], $run_folder_path, 'status';
my $log_dir = join q[/], $status_dir, 'log';

{
  throws_ok {
    npg_pipeline::launcher::status->new(
      id_run => 1234,
      run_folder => 'myfolder',
    );
  } qr/Attribute[ ][(]status[)][ ]is[ ]required/, q{error on missing status attribute};

  my $sr;
  lives_ok {
    $sr = npg_pipeline::launcher::status->new(
      status     => 'run archived',
      id_run     => 1234,
      run_folder => 'myfolder',
      runfolder_path    => $run_folder_path,
      bam_basecall_path => $run_folder_path,
      timestamp  => q{20090709-123456},
    )
  } 'status runner object created';
  isa_ok($sr, q{npg_pipeline::launcher::status});
  ok( !$sr->lane_status_flag, 'lane status flag is false by default');

  my $bsub_command = $sr->_generate_bsub_command();
  my $expected_cmd =  q{bsub -J save_run_status_1234_run_archived_20090709-123456 -q small -o } . $log_dir . q{/save_run_status_1234_run_archived_20090709-123456.out 'npg_status2file --id_run 1234 --status "run archived" --dir_out }. $status_dir . q{'};
  is($bsub_command, $expected_cmd, q{bsub command ok when updating status to "run archived"});

  $sr = npg_pipeline::launcher::status->new(
      status     => 'qc complete',
      id_run     => 1234,
      run_folder => 'myfolder',
      runfolder_path    => $run_folder_path,
      bam_basecall_path => $run_folder_path,
      timestamp  => q{20090709-123456},
  );
  my $ostatus_dir = $status_dir;
  my $olog_dir = $log_dir;
  $bsub_command = $sr->_generate_bsub_command();
  $expected_cmd =  q{bsub -J save_run_status_1234_qc_complete_20090709-123456 -q small -o } . $olog_dir . q{/save_run_status_1234_qc_complete_20090709-123456.out 'npg_status2file --id_run 1234 --status "qc complete" --dir_out }. $ostatus_dir . q{'};
  is($bsub_command, $expected_cmd, q{bsub command ok when updating status to "qc complete"});

  $sr = npg_pipeline::launcher::status->new(
      status            => 'analysis in progress',
      lane_status_flag  => 1,
      id_run            => 1234,
      run_folder => 'myfolder',
      runfolder_path    => $run_folder_path,
      bam_basecall_path => $run_folder_path,
      timestamp         => q{20090709-123456},
      lanes => [3 .. 5],
  );

  $bsub_command = $sr->_generate_bsub_command();
  $expected_cmd =  q{bsub -J save_lane_status_1234_analysis_in_progress_20090709-123456 -q small -o } . $log_dir . q{/save_lane_status_1234_analysis_in_progress_20090709-123456.out 'npg_status2file --id_run 1234 --status "analysis in progress" --dir_out } . $status_dir . q{ --lanes 3 --lanes 4 --lanes 5'};

  is($bsub_command, $expected_cmd, q{bsub command ok when updating selected lanes status to "analysis in progress"});

  $sr = npg_pipeline::launcher::status->new(
      status            => 'analysis in progress',
      lane_status_flag  => 1,
      id_run            => 1234,
      run_folder => 'myfolder',
      runfolder_path    => $run_folder_path,
      bam_basecall_path => $run_folder_path,
      timestamp         => q{20090709-123456},
  );

  $bsub_command = $sr->_generate_bsub_command();
  $expected_cmd =  q{bsub -J save_lane_status_1234_analysis_in_progress_20090709-123456 -q small -o } . $log_dir . q{/save_lane_status_1234_analysis_in_progress_20090709-123456.out 'npg_status2file --id_run 1234 --status "analysis in progress" --dir_out } . $status_dir . q{ --lanes 1 --lanes 2 --lanes 3 --lanes 4 --lanes 5 --lanes 6 --lanes 7 --lanes 8'};

  is($bsub_command, $expected_cmd, q{bsub command ok when updating all lanes status to "analysis in progress"});
}

1;
