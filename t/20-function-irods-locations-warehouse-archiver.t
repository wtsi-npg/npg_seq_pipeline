use strict;
use warnings;
use Test::More tests => 9;
use Log::Log4perl qw(:levels);

use t::util;

my $util = t::util->new();
Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $util->temp_directory(), 'logfile'),
                          utf8   => 1});

my $runfolder_path = $util->analysis_runfolder_path();

use_ok('npg_pipeline::function::irods_locations_warehouse_archiver');

my $default = {
  default => {
    minimum_cpu => 1,
    queue       => 'lowload',
    memory      => 1
  }
};

my $locations_archiver = npg_pipeline::function::irods_locations_warehouse_archiver->new(
  runfolder_path => $runfolder_path,
  analysis_path  => $runfolder_path,
  resource       => $default
);
isa_ok ($locations_archiver, 'npg_pipeline::function::irods_locations_warehouse_archiver');

my $d = $locations_archiver->create();

isa_ok ($d->[0], 'npg_pipeline::function::definition');
is ($d->[0]->created_by, 'npg_pipeline::function::irods_locations_warehouse_archiver', 'created_by');
is ($d->[0]->job_name, 'npg_irods_locations2ml_warehouse_1234_', 'job name');
is ($d->[0]->command, "npg_irods_locations2ml_warehouse --path $runfolder_path/irods_locations_files --verbose", 'command');
is ($d->[0]->queue, 'lowload', 'queue');
is_deeply ($d->[0]->num_cpus, [1], 'one CPU required');
ok (!$d->[0]->has_command_preexec, 'preexec command not defined');

1;