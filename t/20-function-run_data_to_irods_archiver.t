use strict;
use warnings;
use Test::More tests => 3;
use Test::Exception;
use File::Copy;
use t::util;

use_ok('npg_pipeline::function::run_data_to_irods_archiver');

my $util = t::util->new();

my $tmp_dir = $util->temp_directory();
my $script = q{npg_publish_illumina_run.pl};
my $includes = qr/--include 'RunInfo\.xml' --include '\[Rr\]unParameters\.xml' --include InterOp/;

subtest 'MiSeq run' => sub {
  plan tests => 27;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q{t/data/miseq/samplesheet_16850.csv};

  my $id_run  = 16850;
  my $rf_name = '150710_MS2_16850_A_MS3014507-500V2';
  my $paths = $util->create_runfolder(
    join(q[/], $tmp_dir, 'analysis'),
    {runfolder_name => $rf_name,
     analysis_path  => 'BAM_basecalls_20181130'}
  );
  my $rfpath  = $paths->{'runfolder_path'};
  for my $name (qw/RunInfo runParameters/) {
    my $copied = copy qq(t/data/miseq/${id_run}_${name}.xml),
                      qq($rfpath/${name}.xml);
    ok($copied, "$name copied");
  }
  my $archive_path = $paths->{'archive_path'};
  my $col = qq{/seq/$id_run};
  my $restart_file = qr/${archive_path}\/publish_run_data2irods_${id_run}_20181204-\d+\.restart_file\.json/;

  my $a = npg_pipeline::function::run_data_to_irods_archiver->new(
    run_folder        => $rf_name,
    runfolder_path    => $rfpath,  
    id_run            => $id_run,
    timestamp         => q{20181204}
  );
  isa_ok($a, q{npg_pipeline::function::run_data_to_irods_archiver}, q{object test});
  ok (!$a->no_irods_archival, 'no_irods_archival flag is unset');

  my $da = $a->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  my $d = $da->[0];
  isa_ok($d, q{npg_pipeline::function::definition});

  is ($d->created_by, q{npg_pipeline::function::run_data_to_irods_archiver},
    'created_by is correct');
  is ($d->created_on, $a->timestamp, 'created_on is correct');
  is ($d->identifier, $id_run, 'identifier is set correctly');
  is ($d->job_name, qq{publish_run_data2irods_${id_run}_20181204},
    'job_name is correct');
  like ($d->command,
    qr/$script --restart_file $restart_file --max_errors 10 --collection $col --source_directory $rfpath $includes --id_run $id_run/,
    'command is correct');
  is ($d->command_preexec,
    'npg_pipeline_script_must_be_unique_runner -job_name="publish_run_data2irods_16850"',
    'preexec command is correct');
  ok (!$d->has_composition, 'composition not set');
  ok (!$d->excluded, 'step not excluded');
  ok (!$d->has_num_cpus, 'number of cpus is not set');
  ok (!$d->has_memory,'memory is not set');
  is ($d->queue, 'lowload', 'queue');
  is ($d->fs_slots_num, 1, 'one fs slot is set');
  ok ($d->reserve_irods_slots, 'iRODS slots to be reserved');
  lives_ok {$d->freeze()} 'definition can be serialized to JSON';

  $a = npg_pipeline::function::run_data_to_irods_archiver->new(
    run_folder        => $rf_name,
    runfolder_path    => $rfpath,  
    id_run            => $id_run,
    timestamp         => q{20181204},
    no_irods_archival => 1
  );
  ok ($a->no_irods_archival, 'no_irods_archival flag is set');
  $da = $a->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  $d = $da->[0];
  isa_ok($d, q{npg_pipeline::function::definition});
  ok ($d->excluded, 'step is excluded');

  $a = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder        => $rf_name,
    runfolder_path    => $rfpath,  
    id_run            => $id_run,
    timestamp         => q{20181204},
    local              => 1
  );
  ok ($a->no_irods_archival, 'no_irods_archival flag is set');
  $da = $a->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  $d = $da->[0];
  ok ($d->excluded, 'step is excluded');
};

subtest 'NovaSeq run' => sub {
  plan tests => 3;

  my $id_run  = 26291;
  my $rf_name = '180709_A00538_0010_BH3FCMDRXX';
  my $rfpath  = qq{t/data/novaseq/$rf_name};
  my $bbc_path = qq{$rfpath/Data/Intensities/BAM_basecalls_20180805-013153};
  my $archive_path = qq{$bbc_path/no_cal/archive};
  my $col = qq{/seq/illumina/runs/$id_run};
  my $restart_file = qr/${archive_path}\/publish_run_data2irods_${id_run}_20181204-\d+\.restart_file\.json/;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
    qq{$bbc_path/metadata_cache_26291/samplesheet_26291.csv};

  my $a  = npg_pipeline::function::run_data_to_irods_archiver->new(
    run_folder        => $rf_name,
    runfolder_path    => $rfpath,  
    id_run            => $id_run,
    timestamp         => q{20181204}
  );
  my $da = $a->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  my $d = $da->[0];
  isa_ok($d, q{npg_pipeline::function::definition});
  like ($d->command,
    qr/$script --restart_file $restart_file --max_errors 10 --collection $col --source_directory $rfpath $includes --id_run $id_run/,
    'command is correct');
};

1;

