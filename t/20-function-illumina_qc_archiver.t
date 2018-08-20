use strict;
use warnings;
use Test::More tests => 18;
use Test::Exception;
use t::util;

use_ok('npg_pipeline::function::illumina_qc_archiver');

my $util = t::util->new();
my $tmp_dir = $util->temp_directory();
my $analysis_runfolder_path = $util->analysis_runfolder_path();

{
  $util->create_analysis();

  my $ia_loader;
  lives_ok { $ia_loader = npg_pipeline::function::illumina_qc_archiver->new(
    run_folder => q{123456_IL2_1234},
    runfolder_path => $analysis_runfolder_path,
    timestamp => q{20090709-123456}
  ) } q{created with run_folder ok};
  isa_ok($ia_loader, q{npg_pipeline::function::illumina_qc_archiver});

  my $da = $ia_loader->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  my $d = $da->[0];
  isa_ok($d, q{npg_pipeline::function::definition});

  is ($d->created_by, q{npg_pipeline::function::illumina_qc_archiver},
    'created_by is correct');
  is ($d->created_on, $ia_loader->timestamp, 'created_on is correct');
  is ($d->identifier, 1234, 'identifier is set correctly');
  is ($d->job_name, q{illumina_analysis_loader_1234_20090709-123456},
    'job_name is correct');
  my $command = qq{npg_qc_illumina_analysis_loader --id_run 1234 --run_folder 123456_IL2_1234 --runfolder_path $tmp_dir/nfs/sf45/IL2/analysis/123456_IL2_1234};
  is ($d->command, $command, 'command is correct');
  is ($d->command_preexec,
    q{npg_pipeline_script_must_be_unique_runner -job_name="illumina_analysis_loader" -own_job_name="illumina_analysis_loader_1234_20090709-123456"},
    'preexec command is correct');
  ok (!$d->has_composition, 'composition not set');
  ok (!$d->excluded, 'step not excluded');
  ok (!$d->has_num_cpus, 'number of cpus is not set');
  ok (!$d->has_memory,'memory is not set');
  is ($d->queue, 'lowload', 'queue');
  is ($d->fs_slots_num, 1, 'one fs slot is set');
  lives_ok {$d->freeze()} 'definition can be serialized to JSON';
}

1;
