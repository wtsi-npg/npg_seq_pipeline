use strict;
use warnings;
use Test::More tests => 19;
use Test::Exception;
use t::util;

use_ok('npg_pipeline::function::autoqc_archiver');

local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_1234.csv];

my $util = t::util->new();

my $rfh = $util->create_runfolder(undef, {'analysis_path' => 'BAM_basecalls_3445'});
my $arpath = $rfh->{'archive_path'};

{
  my $aqc_archiver = npg_pipeline::function::autoqc_archiver->new(
    id_run         => 1234,
    archive_path   => $arpath,
    merge_lanes    => 0,
    is_indexed     => 0,
    timestamp      => q{20090709-123456},
  );
  isa_ok($aqc_archiver, q{npg_pipeline::function::autoqc_archiver});

  my $da = $aqc_archiver->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  my $d = $da->[0];
  isa_ok($d, q{npg_pipeline::function::definition});

  is ($d->created_by, q{npg_pipeline::function::autoqc_archiver},
    'created_by is correct');
  is ($d->created_on, $aqc_archiver->timestamp, 'created_on is correct');
  is ($d->identifier, 1234, 'identifier is set correctly');
  is ($d->job_name, q{autoqc_loader_1234_20090709-123456},
    'job_name is correct');
  is ($d->command,
    qq{npg_qc_autoqc_data.pl --id_run 1234 --archive_path $arpath} .
    q{ --lane 1 --lane 2 --lane 3 --lane 4 --lane 5 --lane 6 --lane 7 --lane 8},
    'command is correct');
  ok (!$d->has_composition, 'composition not set');
  ok (!$d->excluded, 'step not excluded');
  ok (!$d->has_num_cpus, 'number of cpus is not set');
  ok (!$d->has_memory,'memory is not set');
  is ($d->queue, 'lowload', 'queue');
  is ($d->fs_slots_num, 1, 'one fs slot is set');
  lives_ok {$d->freeze()} 'definition can be serialized to JSON';

  $aqc_archiver = npg_pipeline::function::autoqc_archiver->new(
    id_run         => 1234,
    lanes          => [2,3],
    archive_path   => $arpath,
    merge_lanes    => 0,
    is_indexed     => 0,
    timestamp      => q{20090709-123456},
  );
  $da = $aqc_archiver->create();
  is ($da->[0]->command,
    qq{npg_qc_autoqc_data.pl --id_run 1234 --archive_path $arpath} .
    q{ --lane 2 --lane 3},
    'command is correct');
}

{
  my $aqc_archiver = npg_pipeline::function::autoqc_archiver->new(
    label            => 'myjob',
    lanes            => [2,3],
    archive_path     => $arpath,
    product_rpt_list => '1234:1',
    timestamp        => q{20090709-123456},
  );
  my $da = $aqc_archiver->create();
  ok ($da && @{$da} == 1, 'one definition returned');
  is ($da->[0]->command,
    qq{npg_qc_autoqc_data.pl --path $arpath/lane1/qc},
    'command is correct');
}

1;
