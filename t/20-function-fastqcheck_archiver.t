use strict;
use warnings;
use Test::More tests => 17;
use Test::Exception;
use t::util;

my $util = t::util->new();
my $rfh = $util->create_runfolder(undef, {'analysis_path' => 'BAM_basecalls_3445'});
my $arpath = $rfh->{'archive_path'};

use_ok(q{npg_pipeline::function::fastqcheck_archiver});
local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_1234.csv];
{
  my $fq_loader = npg_pipeline::function::fastqcheck_archiver->new(
    id_run         => 1234,
    archive_path   => $arpath,
    merge_lanes    => 0,
    is_indexed     => 0,
    timestamp      => q{20090709-123456},
  );
  isa_ok( $fq_loader, q{npg_pipeline::function::fastqcheck_archiver});

  my $da = $fq_loader->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  my $d = $da->[0];
  isa_ok($d, q{npg_pipeline::function::definition});

  is ($d->created_by, q{npg_pipeline::function::fastqcheck_archiver},
    'created_by is correct');
  is ($d->created_on, $fq_loader->timestamp, 'created_on is correct');
  is ($d->identifier, 1234, 'identifier is set correctly');
  is ($d->job_name, q{fastqcheck_loader_1234_20090709-123456},
    'job_name is correct');
  my $command = qq{npg_qc_save_files.pl --path=$arpath/lane1 --path=$arpath/lane2 --path=$arpath/lane3 --path=$arpath/lane4 --path=$arpath/lane5 --path=$arpath/lane6 --path=$arpath/lane7 --path=$arpath/lane8 --path=$arpath};
  is ($d->command, $command, 'command is correct');
  ok (!$d->has_composition, 'composition not set');
  ok (!$d->excluded, 'step not excluded');
  ok (!$d->has_num_cpus, 'number of cpus is not set');
  ok (!$d->has_memory,'memory is not set');
  is ($d->queue, 'lowload', 'queue');
  is ($d->fs_slots_num, 1, 'one fs slot is set');
  lives_ok {$d->freeze()} 'definition can be serialized to JSON';

  $fq_loader = npg_pipeline::function::fastqcheck_archiver->new(
    id_run       => 1234,
    archive_path => $arpath,
    merge_lanes  => 1,
    is_indexed   => 0,
    lanes        => [2, 3],
    timestamp    => q{20090709-123456},
  );
  $da = $fq_loader->create();
  $d = $da->[0];
  $command = qq{npg_qc_save_files.pl --path=$arpath/lane2 --path=$arpath/lane3 --path=$arpath};
  is ($d->command, $command, 'command is correct - two lane-level directories');
}

1;
