use strict;
use warnings;
use Test::More tests => 3;
use Test::Exception;
use File::Copy;
use Log::Log4perl qw[:levels];
use File::Slurp;
use t::util;

use_ok('npg_pipeline::function::seq_to_irods_archiver');

my $util = t::util->new();

my $tmp_dir = $util->temp_directory();
my $script = q{npg_publish_illumina_run.pl};
my $config_dir = join q[/], $tmp_dir, 'config';
mkdir $config_dir;
copy 't/data/release/config/archive_on/product_release.yml', $config_dir;
copy 'data/config_files/general_values.ini', $config_dir;
my $pconfig = join q[/], $config_dir, 'product_release.yml';

Log::Log4perl->easy_init({level  => $INFO,
                          layout => '%d %p %m %n',
                          file   => join(q[/], $tmp_dir, 'logfile')});

subtest 'MiSeq run' => sub {
  plan tests => 41;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q{t/data/miseq/samplesheet_16850.csv};
  my $pconfig_content = read_file $pconfig;
  my $study_id = 3573;
  ok ($pconfig_content !~ /study_id: \"$study_id\"/xms,
    'no product release config for this run study');

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
  my $restart_file = qr/${archive_path}\/publish_seq_data2irods_${id_run}_20181204-\d+\.restart_file\.json/;

  my $a = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder     => $rf_name,
    runfolder_path => $rfpath,
    conf_path      => $config_dir,
    id_run         => $id_run,
    timestamp      => q{20181204}
   );
  isa_ok($a, q{npg_pipeline::function::seq_to_irods_archiver}, q{object test});
  ok (!$a->no_irods_archival, 'no_irods_archival flag is unset');

  my $da = $a->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  my $d = $da->[0];
  isa_ok($d, q{npg_pipeline::function::definition});

  is ($d->created_by, q{npg_pipeline::function::seq_to_irods_archiver},
    'created_by is correct');
  is ($d->created_on, $a->timestamp, 'created_on is correct');
  is ($d->identifier, $id_run, 'identifier is set correctly');
  is ($d->job_name, qq{publish_seq_data2irods_${id_run}_20181204},
    'job_name is correct');

  like ($d->command, qr/\Abash -c 'set -e;/, 'correct command start');
  like ($d->command,
    qr/$script --restart_file $restart_file --max_errors 10 --collection $col --source_directory $archive_path\/lane1\/plex0;/,
    'contains command for tag zero');
  like ($d->command,
     qr/$script --restart_file $restart_file --max_errors 10 --collection $col --source_directory $archive_path\/lane1\/plex1;/,
    'contains command for tag 1');
  like ($d->command,
     qr/$script --restart_file $restart_file --max_errors 10 --collection $col --source_directory $archive_path\/lane1\/plex2/,
    'contains command for tag 2');
  like ($d->command, qr/plex2'\Z/, 'correct command end');

  # Make study explicitly configured to be archived to iRODS
  $pconfig_content =~ s/study_id: \"1000\"/study_id: \"$study_id\"/;
  write_file($pconfig, $pconfig_content);

  $a = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder     => $rf_name,
    runfolder_path => $rfpath,
    conf_path      => $config_dir,
    id_run         => $id_run,
    timestamp      => q{20181204}
  );

  $da = $a->create();
  $d = $da->[0];
  like ($d->command, qr/\Abash -c 'set -e;/, 'correct command start');
  like ($d->command,
    qr/$script --restart_file $restart_file --max_errors 10 --collection $col --source_directory $archive_path\/lane1\/plex0;/,
    'contains command for tag zero');
  like ($d->command,
     qr/$script --restart_file $restart_file --max_errors 10 --collection $col --source_directory $archive_path\/lane1\/plex1;/,
    'contains command for tag 1');
  like ($d->command,
     qr/$script --restart_file $restart_file --max_errors 10 --collection $col --source_directory $archive_path\/lane1\/plex2/,
    'contains command for tag 2');
  like ($d->command, qr/plex2'\Z/, 'correct command end');

  is ($d->command_preexec,
    'npg_pipeline_script_must_be_unique_runner -job_name="publish_seq_data2irods_16850"',
    'preexec command is correct');
  ok (!$d->has_composition, 'composition not set');
  ok (!$d->excluded, 'step not excluded');
  ok (!$d->has_num_cpus, 'number of cpus is not set');
  ok (!$d->has_memory,'memory is not set');
  is ($d->queue, 'lowload', 'queue');
  is ($d->fs_slots_num, 1, 'one fs slot is set');
  ok ($d->reserve_irods_slots, 'iRODS slots to be reserved');
  lives_ok {$d->freeze()} 'definition can be serialized to JSON';

  $a = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder        => $rf_name,
    runfolder_path    => $rfpath,
    conf_path         => $config_dir,  
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
    run_folder     => $rf_name,
    runfolder_path => $rfpath,
    conf_path      => $config_dir,
    id_run         => $id_run,
    timestamp      => q{20181204},
    local          => 1
  );
  ok ($a->no_irods_archival, 'no_irods_archival flag is set');
  $da = $a->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  $d = $da->[0];
  ok ($d->excluded, 'step is excluded');

  $a = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder       => $rf_name,
    runfolder_path   => $rfpath,
    conf_path        => $config_dir,
    id_run           => $id_run,
    timestamp        => q{20181204},
    id_flowcell_lims => q{1023456789111}
  );
  $da = $a->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  $d = $da->[0];
  like ($d->command,
    qr/$script --restart_file $restart_file --max_errors 10 --alt_process qc_run --collection $col --source_directory $archive_path\/lane1\/plex0/,
    'command is correct for qc run');

  $a = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder       => $rf_name,
    runfolder_path   => $rfpath,
    conf_path        => $config_dir,  
    id_run           => $id_run,
    timestamp        => q{20181204},
    lims_driver_type => 'samplesheet'
  );
  $da = $a->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  $d = $da->[0];
  like ($d->command,
    qr/$script --restart_file $restart_file --max_errors 10 --driver-type samplesheet --collection $col --source_directory $archive_path\/lane1\/plex0/,
    'command is correct');
};

subtest 'NovaSeq run' => sub {
  plan tests => 5;

  my $id_run  = 26291;
  my $rf_name = '180709_A00538_0010_BH3FCMDRXX';
  my $rfpath  = qq{t/data/novaseq/$rf_name};
  my $bbc_path = qq{$rfpath/Data/Intensities/BAM_basecalls_20180805-013153};
  my $archive_path = qq{$bbc_path/no_cal/archive};
  my $col = qq{/seq/illumina/runs/$id_run};
  my $restart_file = qr/${archive_path}\/publish_seq_data2irods_${id_run}_20181204-\d+\.restart_file\.json/;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
    qq{$bbc_path/metadata_cache_26291/samplesheet_26291.csv};

  my $a  = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder     => $rf_name,
    runfolder_path => $rfpath,
    conf_path      => $config_dir,  
    id_run         => $id_run,
    timestamp      => q{20181204}
  );
  my $da = $a->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  my $d = $da->[0];
  isa_ok($d, q{npg_pipeline::function::definition});
  like ($d->command,
    qr/$script --restart_file $restart_file --max_errors 10 --collection $col\/plex0 --source_directory $archive_path\/plex0;$script --restart_file $restart_file --max_errors 10 --collection $col\/plex888 --source_directory $archive_path\/plex888/,
    'command is correct for merged lanes');

  $a  = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder     => $rf_name,
    runfolder_path => $rfpath,
    conf_path      => $config_dir,
    id_run         => $id_run,
    timestamp      => q{20181204}, 
    lanes          => [2]
  );

  $da = $a->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  $d = $da->[0];
  like ($d->command,
    qr/$script --restart_file $restart_file --max_errors 10 --collection $col\/lane2\/plex0 --source_directory $archive_path\/lane2\/plex0;$script --restart_file $restart_file --max_errors 10 --collection $col\/lane2\/plex888 --source_directory $archive_path\/lane2\/plex888/,
    'command is correct for a single lane');
};

1;

