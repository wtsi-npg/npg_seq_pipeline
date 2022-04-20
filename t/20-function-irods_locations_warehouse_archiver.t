use strict;
use warnings;
use Test::More tests => 16;
use Log::Log4perl qw(:levels);

use t::util;

my $util = t::util->new();
Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $util->temp_directory(), 'logfile'),
                          utf8   => 1});

my $paths = $util->create_runfolder(undef, {analysis_path => 'analysis'});
my $runfolder_path = $paths->{runfolder_path};
my $analysis_path  = $paths->{analysis_path};

use_ok('npg_pipeline::function::irods_locations_warehouse_archiver');

my $default = {
  default => {
    minimum_cpu => 0,
    queue       => 'lowload',
    memory      => 1
  }
};

my $locations_archiver =
  npg_pipeline::function::irods_locations_warehouse_archiver->new(
    id_run         => 25955,
    runfolder_path => $runfolder_path,
    analysis_path  => $analysis_path,
    resource       => $default
  );
isa_ok ($locations_archiver,
  'npg_pipeline::function::irods_locations_warehouse_archiver');

my $json_dir = join q[/], $analysis_path, q[irods_locations_files];
ok (!-e $json_dir, 'directory for locations JSON files does not exist');
my $d = $locations_archiver->create();
ok (-d $json_dir, 'directory for locations JSON files has been created');

is (@{$d}, 1, 'one definition is returned');
isa_ok ($d->[0], 'npg_pipeline::function::definition');
is ($d->[0]->created_by,
  'npg_pipeline::function::irods_locations_warehouse_archiver', 'created_by');
like ($d->[0]->job_name, qr/npg_irods_locations2ml_warehouse_25955_\d+-\d+/,
  'job name');
ok (!$d->[0]->excluded, 'the job is not excluded');
is ($d->[0]->command,
  "npg_irods_locations2ml_warehouse --path $json_dir --verbose", 'command');
is ($d->[0]->queue, 'lowload', 'queue');
is_deeply ($d->[0]->num_cpus, [0], 'zero CPU required');
ok (!$d->[0]->has_command_preexec, 'preexec command not defined');

$locations_archiver =
  npg_pipeline::function::irods_locations_warehouse_archiver->new(
    id_run         => 25955,
    runfolder_path => $runfolder_path,
    analysis_path  => $analysis_path,
    resource       => $default,
    no_irods_archival => 1
  );
$d = $locations_archiver->create();
is (@{$d}, 1, 'one definition is returned');
ok ($d->[0]->excluded, 'the job is excluded');
is ($d->[0]->command, undef, 'the command is undefined');

1;
