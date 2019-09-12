use strict;
use warnings;
use Test::More tests => 4;
use Test::Exception;
use File::Copy;
use Log::Log4perl qw(:levels);
use t::util;

my $util = t::util->new();
my $tmp_dir = $util->temp_directory();
Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $tmp_dir, 'logfile'),
                          utf8   => 1});

use_ok('npg_pipeline::function::log_files_archiver');

subtest 'MiSeq run' => sub {
  plan tests => 32;

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

  my $orfpath = $rfpath;
  $orfpath    =~ s/analysis/outgoing/xms;

  my $a  = npg_pipeline::function::log_files_archiver->new(
    run_folder        => $rf_name,
    runfolder_path    => $rfpath,  
    id_run            => $id_run,
    timestamp         => q{20181204},
  );
  isa_ok ($a , q{npg_pipeline::function::log_files_archiver});

  my $da = $a->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  my $d = $da->[0];
  isa_ok($d, q{npg_pipeline::function::definition});

  is ($d->created_by, q{npg_pipeline::function::log_files_archiver},
    'created_by is correct');
  is ($d->created_on, $a->timestamp, 'created_on is correct');
  is ($d->identifier, $id_run, 'identifier is set correctly');
  is ($d->job_name, qq{publish_logs_${id_run}_20181204},
    'job_name is correct');
  is ($d->command, join(q[ ], 'npg_publish_illumina_logs.pl',
    qq{--collection \/seq\/$id_run/log},
    qq{--runfolder_path $orfpath --id_run $id_run}),
    'command is correct');
  ok (!$d->has_composition, 'composition not set');
  ok (!$d->excluded, 'step not excluded');
  ok (!$d->has_num_cpus, 'number of cpus is not set');
  ok (!$d->has_memory,'memory is not set');
  is ($d->queue, 'lowload', 'queue');
  is ($d->fs_slots_num, 1, 'one fs slot is set');
  ok ($d->reserve_irods_slots, 'iRODS slots to be reserved');
  lives_ok {$d->freeze()} 'definition can be serialized to JSON';

  $a  = npg_pipeline::function::log_files_archiver->new(
    run_folder        => $rf_name,
    runfolder_path    => $rfpath,  
    id_run            => $id_run,
    timestamp         => q{20181204},
    no_irods_archival => 1
  );

  ok ($a->no_irods_archival, q{archival switched off});
  $da = $a->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  $d = $da->[0];
  isa_ok($d, q{npg_pipeline::function::definition});
  is ($d->created_by, q{npg_pipeline::function::log_files_archiver},
    'created_by is correct');
  is ($d->created_on, $a->timestamp, 'created_on is correct');
  is ($d->identifier, $id_run, 'identifier is set correctly');
  ok ($d->excluded, 'step is excluded');

  $a  = npg_pipeline::function::log_files_archiver->new(
    run_folder        => $rf_name,
    runfolder_path    => $rfpath,  
    id_run            => $id_run,
    timestamp         => q{20181204},
    local             => 1
  );
  ok ($a->no_irods_archival, q{archival switched off});
  $da = $a->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  $d = $da->[0];
  isa_ok($d, q{npg_pipeline::function::definition});
  is ($d->created_by, q{npg_pipeline::function::log_files_archiver},
    'created_by is correct');
  is ($d->created_on, $a->timestamp, 'created_on is correct');
  is ($d->identifier, $id_run, 'identifier is set correctly');
  ok ($d->excluded, 'step is excluded');
};

subtest 'NovaSeq run' => sub {
  plan tests => 3;

  my $id_run  = 26291;
  my $rf_name = '180709_A00538_0010_BH3FCMDRXX';
  my $rfpath  = qq{t/data/novaseq/$rf_name};

  my $a  = npg_pipeline::function::log_files_archiver->new(
    run_folder        => $rf_name,
    runfolder_path    => $rfpath,  
    id_run            => $id_run,
  );

  my $da = $a->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  my $d = $da->[0];
  isa_ok($d, q{npg_pipeline::function::definition});
  is ($d->command, join(q[ ], 'npg_publish_illumina_logs.pl',
    qq{--collection \/seq\/illumina\/runs\/26\/$id_run\/log},
    qq{--runfolder_path $rfpath --id_run $id_run}),
    'command is correct');
};

subtest 'pipeline for a product' => sub {
  plan tests => 1;

  my $a  = npg_pipeline::function::log_files_archiver->new(
    runfolder_path   => q{t/data/novaseq},
    label            => 'my_label',
    product_rpt_list => '123:4:5'
  );
  throws_ok { $a->create() }
    qr/Not implemented for individual products/,
    'functionality for individual products not implemented - error'; 
};

1;
