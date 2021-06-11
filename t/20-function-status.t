use strict;
use warnings;
use Test::More tests => 3;
use Test::Exception;
use File::Temp qw{ tempdir };

use_ok('npg_pipeline::function::status');

local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_1234.csv];

my $temp = tempdir(CLEANUP => 1);
my $run_folder_path = join q[/], $temp, 'analysis';
mkdir $run_folder_path;
my $status_dir = join q[/], $run_folder_path, 'status';
my $log_dir = join q[/], $status_dir, 'log';

my %init = (
  id_run              => 1234,
  run_folder          => 'myfolder',
  runfolder_path      => $run_folder_path,
  bam_basecall_path   => $run_folder_path,
  timestamp           => q{20090709-123456},
  resource          => {
    default => {
      memory => 2,
      minimum_cpu => 0,
      queue => 'small'
    }
  }
);

subtest 'no_db_status_update option is false (default)' => sub {
  plan tests => 27;

  throws_ok {
    npg_pipeline::function::status->new(
      id_run     => 1234,
      run_folder => 'myfolder',
    );
  } qr/Attribute \(status\) is required/, q{error on missing status attribute};

  my $sr;
  lives_ok {
    $sr = npg_pipeline::function::status->new(
      %init,
      status => 'run archived'
    )
  } 'status runner object created';
  isa_ok ($sr, 'npg_pipeline::function::status');
  ok ( !$sr->lane_status_flag, 'lane status flag is false by default');
  ok ( !$sr->no_db_status_update, q['no_db_status_update' is unset by default]);
  my $da = $sr->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  my $d = $da->[0];
  isa_ok($d, 'npg_pipeline::function::definition');
  is ($d->created_by, 'npg_pipeline::function::status',
    'created_by is correct');
  is ($d->created_on, $sr->timestamp, 'created_on is correct');
  is ($d->identifier, 1234, 'identifier is set correctly');
  is ($d->job_name, 'save_run_status_1234_run_archived_20090709-123456',
    'job_name is correct');
  is ($d->command,
    'npg_status2file --id_run 1234 --status "run archived" --dir_out '
    . $status_dir . q[ --db_save],
    'command is correct');
  ok (!$d->has_composition, 'composition not set');
  ok (!$d->excluded, 'step not excluded');
  ok ($d->has_num_cpus, 'number of cpus is set');
  is_deeply ($d->num_cpus, [0], 'zero cpus');
  is ($d->queue, 'small', 'small queue');
  lives_ok {$d->freeze()} 'definition can be serialized to JSON';

  $sr = npg_pipeline::function::status->new(
    %init,
    status => 'qc complete'
  );
  my $ostatus_dir = $status_dir;
  my $olog_dir = $log_dir;

  $da = $sr->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  $d = $da->[0];
  is ($d->job_name, 'save_run_status_1234_qc_complete_20090709-123456',
    'job_name is correct');
  is ($d->command,
    'npg_status2file --id_run 1234 --status "qc complete" --dir_out '
    . $ostatus_dir . q[ --db_save],
    'command is correct');

  $sr = npg_pipeline::function::status->new(
    %init,
    status            => 'analysis in progress',
    lane_status_flag  => 1,
    lanes             => [3 .. 5]
  );

  $da = $sr->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  $d = $da->[0];
  is ($d->job_name, 'save_lane_status_1234_analysis_in_progress_20090709-123456',
    'job_name is correct');
  is ($d->command,
    'npg_status2file --id_run 1234 --status "analysis in progress" --dir_out '
    . $status_dir .  q' --lanes 3 --lanes 4 --lanes 5 --db_save',
    'command is correct');

  $sr = npg_pipeline::function::status->new(
    %init,
    status            => 'analysis in progress',
    lane_status_flag  => 1
  );

  $da = $sr->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  $d = $da->[0];
  is ($d->job_name, 'save_lane_status_1234_analysis_in_progress_20090709-123456',
    'job_name is correct');
  is ($d->command,
    'npg_status2file --id_run 1234 --status "analysis in progress" --dir_out '
    . $status_dir .
    ' --lanes 1 --lanes 2 --lanes 3 --lanes 4 --lanes 5' .
    ' --lanes 6 --lanes 7 --lanes 8 --db_save',
    'command is correct');
};

subtest 'no_db_status_update option is true' => sub {
  plan tests => 4;

  my $status = 'run archived';
  my $expected_command = sprintf
    'npg_status2file --id_run 1234 --status "%s" --dir_out %s',
    $status, $status_dir;

  my $sr = npg_pipeline::function::status->new(
    %init,
    status              => $status,
    no_db_status_update => 1  
  );
  ok (!$sr->local, q['local' is false]);
  my $da = $sr->create();
  is ($da->[0]->command, $expected_command,
    q[command is correct when 'no_db_status_update' is set to true explicilty]);

  $sr = npg_pipeline::function::status->new(
    %init,
    status => $status,
    local  => 1  
  );
  ok ($sr->no_db_status_update, q['no_db_status_update' is set to true]);
  $da = $sr->create();
  is ($da->[0]->command, $expected_command,
    q[command is correct when 'local' is set to true explicilty]);
};

1;
