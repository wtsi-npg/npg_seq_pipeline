use strict;
use warnings;
use Test::More tests => 21;
use Test::Exception;
use t::util;

use_ok('npg_pipeline::function::autoqc_archiver');

local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_1234.csv];

my $util = t::util->new();
my $analysis_runfolder_path = $util->analysis_runfolder_path();
my $pbcal = $util->standard_analysis_recalibrated_path();

{
  $util->create_analysis();
  my $aaq;
  $util->create_run_info();

  lives_ok { $aaq = npg_pipeline::function::autoqc_archiver->new(
    run_folder     => q{123456_IL2_1234},
    runfolder_path => $analysis_runfolder_path,
    timestamp      => q{20090709-123456},
  ); } q{created with run_folder ok};
  isa_ok($aaq, q{npg_pipeline::function::autoqc_archiver});

  my $da = $aaq->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  my $d = $da->[0];
  isa_ok($d, q{npg_pipeline::function::definition});

  is ($d->created_by, q{npg_pipeline::function::autoqc_archiver},
    'created_by is correct');
  is ($d->created_on, $aaq->timestamp, 'created_on is correct');
  is ($d->identifier, 1234, 'identifier is set correctly');
  is ($d->job_name, q{autoqc_loader_1234_20090709-123456},
    'job_name is correct');
  is ($d->command,
    qq{npg_qc_autoqc_data.pl --id_run=1234 --path=$pbcal/archive/qc},
    'command is correct');
  ok (!$d->has_composition, 'composition not set');
  ok (!$d->excluded, 'step not excluded');
  ok (!$d->has_num_cpus, 'number of cpus is not set');
  ok (!$d->has_memory,'memory is not set');
  is ($d->queue, 'lowload', 'queue');
  is ($d->fs_slots_num, 1, 'one fs slot is set');
  lives_ok {$d->freeze()} 'definition can be serialized to JSON';
}

{
  $util->create_multiplex_analysis({'qc_dir' => [2,3,5]});
  my $command = q{npg_qc_autoqc_data.pl --id_run=1234};
  $command .= qq{ --path=$pbcal/archive/qc};
  for my $p ((2,3,5)) {
    $command .= qq{ --path=$pbcal/archive/lane${p}/qc};
  }

  my $aaq = npg_pipeline::function::autoqc_archiver->new(
    run_folder     => q{123456_IL2_1234},
    runfolder_path => $analysis_runfolder_path,
    timestamp      => q{20090709-123456},
    is_indexed     => 1,
  );

  my $da = $aaq->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  my $d = $da->[0];
  is ($d->job_name, 'autoqc_loader_1234_20090709-123456',
    'job_name is correct');
  is ($d->command, $command, 'command is correct');
  is ($d->fs_slots_num, 1, 'one fs slot is set');
}

1;
