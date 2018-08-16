use strict;
use warnings;
use Test::More tests => 33;
use Test::Exception;
use t::util;

use_ok('npg_pipeline::function::seq_to_irods_archiver');

my $util = t::util->new();
local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_1234.csv];

my $tmp_dir = $util->temp_directory();

my $analysis_runfolder_path = $util->analysis_runfolder_path();
my $pb_cal = q[/Data/Intensities/Bustard1.3.4_09-07-2009_auto/PB_cal];
my $pb_cal_path = $analysis_runfolder_path . $pb_cal;

sub create_analysis {
  `mkdir -p $analysis_runfolder_path/$pb_cal/archive`;
  `mkdir -p $analysis_runfolder_path/Config`;
  `cp t/data/Recipes/TileLayout.xml $analysis_runfolder_path/Config/`;
  `ln -s $pb_cal $analysis_runfolder_path/Latest_Summary`;

  my $archive_root = qq{$analysis_runfolder_path/$pb_cal/archive/};
  foreach my $i (1..7) {
    foreach my $extension (qw{bam}) {
      my $file = $archive_root . qq{1234_$i}.qq{.$extension};
      `touch $file`;
    }
  }

  `touch $archive_root/1234_8_human.cram`;
  `touch $archive_root/1234_8.cram`;

  return 1;
}

create_analysis();

{
  my $archive_path = "$pb_cal_path/archive";
  my $bam_irods;
  lives_ok { $bam_irods = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder        => q{123456_IL2_1234},
    runfolder_path    => $analysis_runfolder_path,
    timestamp         => q{20090709-123456},
    verbose           => 0,
    recalibrated_path => $pb_cal_path,
  ); } q{created with run_folder ok};
  isa_ok($bam_irods , q{npg_pipeline::function::seq_to_irods_archiver}, q{object test});
  ok (!$bam_irods->no_irods_archival, 'no_irods_archival flag is unset');

  my $da = $bam_irods->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  my $d = $da->[0];
  isa_ok($d, q{npg_pipeline::function::definition});

  is ($d->created_by, q{npg_pipeline::function::seq_to_irods_archiver},
    'created_by is correct');
  is ($d->created_on, $bam_irods->timestamp, 'created_on is correct');
  is ($d->identifier, 1234, 'identifier is set correctly');
  is ($d->job_name, q{publish_illumina_run_1234_20090709-123456},
    'job_name is correct');
  like ($d->command,
    qr/npg_publish_illumina_run\.pl --archive_path $archive_path --runfolder_path $analysis_runfolder_path --restart_file ${archive_path}\/publish_illumina_run_1234_20090709-123456-\d+\.restart_file\.json --max_errors 10/,
    'command is correct');
  is ($d->command_preexec,
    'npg_pipeline_script_must_be_unique_runner -job_name="publish_illumina_run_1234"',
    'preexec command is correct');
  ok (!$d->has_composition, 'composition not set');
  ok (!$d->excluded, 'step not excluded');
  ok (!$d->has_num_cpus, 'number of cpus is not set');
  ok (!$d->has_memory,'memory is not set');
  is ($d->queue, 'lowload', 'queue');
  is ($d->fs_slots_num, 1, 'one fs slot is set');
  ok ($d->reserve_irods_slots, 'iRODS slots to be reserved');
  lives_ok {$d->freeze()} 'definition can be serialized to JSON';
  
  $bam_irods = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder        => q{123456_IL2_1234},
    runfolder_path    => $analysis_runfolder_path,
    verbose           => 0,
    no_irods_archival => 1,
    recalibrated_path => $pb_cal_path,
  );
  ok ($bam_irods->no_irods_archival, 'no_irods_archival flag is set');
  $da = $bam_irods->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  $d = $da->[0];
  isa_ok($d, q{npg_pipeline::function::definition});
  ok ($d->excluded, 'step is excluded');

  $bam_irods = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder        => q{123456_IL2_1234},
    runfolder_path    => $analysis_runfolder_path,
    verbose           => 0,
    local              => 1,
    recalibrated_path => $pb_cal_path,
  );
  ok ($bam_irods->no_irods_archival, 'no_irods_archival flag is set');
  $da = $bam_irods->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  $d = $da->[0];
  ok ($d->excluded, 'step is excluded');

  $bam_irods = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder        => q{123456_IL2_1234},
    runfolder_path    => $analysis_runfolder_path,
    recalibrated_path => $pb_cal_path,
    timestamp         => q{20090709-123456},
    verbose           => 0,
    lanes             => [8],
  );
  $da = $bam_irods->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  $d = $da->[0];
  like ($d->command,
    qr/npg_publish_illumina_run\.pl --archive_path $archive_path --runfolder_path $analysis_runfolder_path --restart_file ${archive_path}\/publish_illumina_run_1234_20090709-123456-\d+\.restart_file\.json --max_errors 10 --positions 8/,
    'command is correct');

  $bam_irods = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder        => q{123456_IL2_1234},
    runfolder_path    => $analysis_runfolder_path,
    id_flowcell_lims  => q{1023456789111},
    recalibrated_path => $pb_cal_path,
    timestamp         => q{20090709-123456},
    verbose           => 0,
  );
  $da = $bam_irods->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  $d = $da->[0];
  like ($d->command,
    qr/npg_publish_illumina_run\.pl --archive_path $archive_path --runfolder_path $analysis_runfolder_path --restart_file ${archive_path}\/publish_illumina_run_1234_20090709-123456-\d+\.restart_file\.json --max_errors 10 --alt_process qc_run/,
    'command is correct');

  $bam_irods = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder        => q{123456_IL2_1234},
    runfolder_path    => $analysis_runfolder_path,
    recalibrated_path => $pb_cal_path,
    timestamp         => q{20090709-123456},
    verbose           => 0,
    lims_driver_type  => 'samplesheet',
  );
  $da = $bam_irods->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  $d = $da->[0];
  like ($d->command,
    qr/npg_publish_illumina_run\.pl --archive_path $archive_path --runfolder_path $analysis_runfolder_path --restart_file ${archive_path}\/publish_illumina_run_1234_20090709-123456-\d+\.restart_file\.json --max_errors 10 --driver-type samplesheet/,
    'command is correct');
}

1;

