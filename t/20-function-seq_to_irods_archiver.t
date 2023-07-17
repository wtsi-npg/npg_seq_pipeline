use strict;
use warnings;
use Test::More tests => 4;
use Test::Exception;
use File::Copy;
use Log::Log4perl qw[:levels];
use File::Slurp;
use Cwd;
use File::Copy::Recursive qw(dircopy);

use npg_tracking::util::abs_path qw(abs_path);
use t::util;

use_ok('npg_pipeline::function::seq_to_irods_archiver');

my $util = t::util->new();

my $tmp_dir = $util->temp_directory();
my $script = q{npg_publish_illumina_run.pl};
my $config_dir = join q[/], $tmp_dir, 'config';
mkdir $config_dir;
copy 't/data/release/config/archive_on/product_release.yml', $config_dir;
copy 'data/config_files/general_values.ini', $config_dir;
copy 'data/config_files/log4perl_publish_illumina.conf', $config_dir;
my $pconfig = join q[/], $config_dir, 'product_release.yml';
my $syslog_conf = join q[/], $config_dir, 'log4perl_publish_illumina.conf';

Log::Log4perl->easy_init({level  => $INFO,
                          layout => '%d %p %m %n',
                          file   => join(q[/], $tmp_dir, 'logfile')});

my $defaults = {
  default => {
    minimum_cpu => 1,
    memory => 2,
    reserve_irods_slots => 1,
    queue => 'lowload',
    fs_slots_num => 1
  }
};


subtest 'MiSeq run' => sub {
  plan tests => 42;

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
  my $archive_path  = $paths->{'archive_path'};
  my $analysis_path = $paths->{'analysis_path'};
  my $col = qq{/seq/$id_run};
  my $restart_file = qr/${analysis_path}\/irods_publisher_restart_files\/publish_seq_data2irods_${id_run}_20181204-\d+_\w+\.restart_file\.json/;
  my $irods_location_file = qr/${analysis_path}\/irods_locations_files\/16850_1\#\d+\.seq_to_irods_archiver\.json/;

  my $a = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder     => $rf_name,
    runfolder_path => $rfpath,
    conf_path      => $config_dir,
    id_run         => $id_run,
    timestamp      => q{20181204},
    resource       => $defaults
  );
  isa_ok($a, q{npg_pipeline::function::seq_to_irods_archiver}, q{object test});
  ok (!$a->no_irods_archival, 'no_irods_archival flag is unset');

  my $da = $a->create();
  ok ($da && @{$da} == 3, 'an array with three definitions is returned');
  my $d = $da->[0];
  isa_ok($d, q{npg_pipeline::function::definition});

  is ($d->created_by, q{npg_pipeline::function::seq_to_irods_archiver},
    'created_by is correct');
  is ($d->created_on, $a->timestamp, 'created_on is correct');
  is ($d->identifier, $id_run, 'identifier is set correctly');
  is ($d->job_name, qq{publish_seq_data2irods_${id_run}_20181204},
    'job_name is correct');
  is ($d->composition->get_component(0)->tag_index, 1, 'tag index 1 job');
  like ($d->command,
    qr/\A$script --max_errors 10 --restart_file $restart_file --collection $col --source_directory $archive_path\/lane1\/plex1 --mlwh_json $irods_location_file --logconf $syslog_conf\Z/,
    'command for tag 1');

  $d = $da->[1];
  is ($d->composition->get_component(0)->tag_index, 2, 'tag index 2 job');
  like ($d->command,
     qr/\A$script --max_errors 10 --restart_file $restart_file --collection $col --source_directory $archive_path\/lane1\/plex2 --mlwh_json $irods_location_file --logconf $syslog_conf\Z/,
    'command for tag 2');

  $d = $da->[2];
  is ($d->composition->get_component(0)->tag_index, 0, 'tag index 0 job');
  like ($d->command,
     qr/\A$script --max_errors 10 --restart_file $restart_file --collection $col --source_directory $archive_path\/lane1\/plex0 --mlwh_json $irods_location_file --logconf $syslog_conf\Z/,
    'command for tag 0');

  # Make study explicitly configured to be archived to iRODS
  $pconfig_content =~ s/study_id: \"1000\"/study_id: \"$study_id\"/;
  write_file($pconfig, $pconfig_content);

  $a = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder     => $rf_name,
    runfolder_path => $rfpath,
    conf_path      => $config_dir,
    id_run         => $id_run,
    timestamp      => q{20181204},
    resource       => $defaults
  );

  $da = $a->create();
  ok ($da && @{$da} == 3, 'an array with three definitions is returned');
  $d = $da->[0];
  like ($d->command,
    qr/\A$script --max_errors 10 --restart_file $restart_file --collection $col --source_directory $archive_path\/lane1\/plex1 --mlwh_json $irods_location_file --logconf $syslog_conf\Z/,
    'command for tag 1');
  $d = $da->[1];
  like ($d->command,
     qr/\A$script --max_errors 10 --restart_file $restart_file --collection $col --source_directory $archive_path\/lane1\/plex2 --mlwh_json $irods_location_file --logconf $syslog_conf\Z/,
    'command for tag 2');
  $d = $da->[2];
  like ($d->command,
     qr/\A$script --max_errors 10 --restart_file $restart_file --collection $col --source_directory $archive_path\/lane1\/plex0 --mlwh_json $irods_location_file --logconf $syslog_conf\Z/,
    'command for tag 0');

  is ($d->command_preexec,
    'npg_pipeline_script_must_be_unique_runner -job_name="publish_seq_data2irods_16850"',
    'preexec command is correct');
  ok (!$d->excluded, 'step not excluded');
  is ($d->queue, 'lowload', 'queue');
  is ($d->fs_slots_num, 1, 'one fs slot is set');
  ok ($d->reserve_irods_slots, 'iRODS slots to be reserved');
  lives_ok {$d->freeze()} 'definition can be serialized to JSON';

  $a = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder     => $rf_name,
    runfolder_path => $rfpath,
    conf_path      => $config_dir,
    id_run         => $id_run,
    timestamp      => q{20181204},
    is_indexed     => 0,
    resource       => $defaults
  );
  $da = $a->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  $d = $da->[0];
  is ($d->composition->num_components, 1, 'one component');
  is ($d->composition->get_component(0)->tag_index, undef, 'tag index is undefined');
  my $ifile = "${analysis_path}/irods_locations_files/16850_1.seq_to_irods_archiver.json";
  like ($d->command,
    qr/\A$script --max_errors 10 --restart_file $restart_file --collection $col --source_directory $archive_path\/lane1 --mlwh_json $ifile --logconf $syslog_conf\Z/,
    'command for lane 1');

  $a = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder        => $rf_name,
    runfolder_path    => $rfpath,
    conf_path         => $config_dir,
    id_run            => $id_run,
    timestamp         => q{20181204},
    no_irods_archival => 1,
    resource          => $defaults
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
    local          => 1,
    resource       => $defaults
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
    id_flowcell_lims => q{1023456789111},
    resource         => $defaults
  );
  $da = $a->create();
  ok ($da && @{$da} == 3, 'an array with three definitions is returned');
  $d = $da->[0];
  like ($d->command,
    qr/\A$script --max_errors 10 --restart_file $restart_file --collection $col --source_directory $archive_path\/lane1\/plex1 --mlwh_json $irods_location_file --logconf $syslog_conf\Z/,
    'command is correct for qc run');

  $a = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder       => $rf_name,
    runfolder_path   => $rfpath,
    conf_path        => $config_dir,
    id_run           => $id_run,
    timestamp        => q{20181204},
    lims_driver_type => 'samplesheet',
    resource         => $defaults
  );
  $da = $a->create();
  ok ($da && @{$da} == 3, 'an array with three definitions is returned');
  $d = $da->[0];
  like ($d->command,
    qr/\A$script --max_errors 10 --driver-type samplesheet --restart_file $restart_file --collection $col --source_directory $archive_path\/lane1\/plex1 --mlwh_json $irods_location_file --logconf $syslog_conf\Z/,
    'command is correct for the samplesheet driver');
};

subtest 'NovaSeq run' => sub {
  plan tests => 11;

  my $id_run  = 26291;
  my $rf_name = '180709_A00538_0010_BH3FCMDRXX';
  my $rfpath  = abs_path(getcwd) . qq{/t/data/novaseq/$rf_name};
  my $bbc_path = qq{$rfpath/Data/Intensities/BAM_basecalls_20180805-013153};
  my $col = qq{/seq/illumina/runs/26/$id_run};

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
    qq{$bbc_path/metadata_cache_26291/samplesheet_26291.csv};

  my $a  = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder     => $rf_name,
    runfolder_path => $rfpath,
    analysis_path  => $bbc_path,
    conf_path      => $config_dir,
    id_run         => $id_run,
    timestamp      => q{20181204},
    resource       => $defaults
  );
  my $da = $a->create();
  ok ($da && @{$da} == 1, 'an array with one definitions is returned');
  my $d = $da->[0];
  isa_ok($d, q{npg_pipeline::function::definition});
  ok ($d->excluded, 'step is excluded');

  ok ($a->per_product_archive(), 'per-product archival'); 
  is ($a->irods_destination_collection(), $col, 'correct run collection');

  $a  = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder     => $rf_name,
    runfolder_path => $rfpath,
    conf_path      => $config_dir,
    id_run         => $id_run,
    timestamp      => q{20181204},
    lanes          => [2],
    resource       => $defaults
  );
  $da = $a->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  $d = $da->[0];
  isa_ok($d, q{npg_pipeline::function::definition});
  ok ($d->excluded, 'step is excluded');

  $a  = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder     => $rf_name,
    runfolder_path => $rfpath,
    conf_path      => $config_dir,
    id_run         => $id_run,
    timestamp      => q{20181204},
    lanes          => [2],
    merge_lanes    => 0,
    is_indexed     => 0,
    resource       => $defaults
  );
  $da = $a->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  $d = $da->[0];
  ok (!$d->has_composition, 'does not have composition object defined');
  ok ($d->excluded, 'step is excluded');
};

subtest 'NovaSeqX run' => sub {
  plan tests => 3,
  my $id_run  = 47515;
  my $rf_name = '20230622_LH00210_0007_A225TMTLT3';
  my $rfpath_test  = abs_path(getcwd) . qq{/t/data/novaseqx/$rf_name};
  my $rfpath = "$tmp_dir/$rf_name";
  dircopy($rfpath_test, $rfpath);
  my $bbc_path = qq{$rfpath/Data/Intensities/BAM_basecalls_20230703-150003};

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
    qq{$bbc_path/metadata_cache_47515/samplesheet_47515.csv};

  my $col = qq{/seq/illumina/runs/47/$id_run}; 

  my $a  = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder     => $rf_name,
    runfolder_path => $rfpath,
    analysis_path  => $bbc_path,
    conf_path      => $config_dir,
    id_run         => $id_run,
    timestamp      => q{20230702},
    resource       => $defaults
  );

  ok ($a->per_product_archive(), 'per-product archival'); 
  is ($a->irods_destination_collection(), $col, 'correct run collection');
  my $da = $a->create();
  my $d = $da->[0];
  like ($d->command,
    qr{--collection \S+illumina/runs\S+lane1\/plex1},
    'command has per product iRODS destination collection');
};

1;

