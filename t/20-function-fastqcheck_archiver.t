use strict;
use warnings;
use Test::More tests => 21;
use Test::Exception;
use t::util;

my $util = t::util->new();
my $pbcal_path = $util->temp_directory() . q{/nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/Bustard1.3.4_09-07-2009_auto/PB_cal};

use_ok(q{npg_pipeline::function::fastqcheck_archiver});

{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_1234.csv];
  $util->create_multiplex_analysis( { qc_dir => [1..7], } );

  my $fh;
  my $rf = $util->analysis_runfolder_path();
  my $runinfofile = qq[$rf/RunInfo.xml];
  open($fh, '>', $runinfofile) or die "Could not open file '$runinfofile' $!";
  print $fh <<"ENDXML";
<?xml version="1.0"?>
<RunInfo xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" Version="3">
  <Run>
    <Reads>
    <Read Number="1" NumCycles="76" IsIndexedRead="N" />
    <Read NumCycles="11" Number="2" IsIndexedRead="Y" />
    </Reads>
    <FlowcellLayout LaneCount="8" SurfaceCount="2" SwathCount="1" TileCount="60">
    </FlowcellLayout>
  </Run>
</RunInfo>
ENDXML
  close $fh;

  my $fq_loader;
  lives_ok {
    $fq_loader = npg_pipeline::function::fastqcheck_archiver->new(
      run_folder => q{123456_IL2_1234},
      runfolder_path => $rf,
      timestamp => q{20090709-123456},
      is_indexed => 1,
    );
  } q{fq_loader created ok};
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
  my $command = qq{npg_qc_save_files.pl --path=$pbcal_path/archive --path=$pbcal_path/archive/lane1 --path=$pbcal_path/archive/lane2 --path=$pbcal_path/archive/lane3 --path=$pbcal_path/archive/lane4 --path=$pbcal_path/archive/lane5 --path=$pbcal_path/archive/lane6 --path=$pbcal_path/archive/lane7};
  is ($d->command, $command, 'command is correct');
  ok (!$d->has_composition, 'composition not set');
  ok (!$d->excluded, 'step not excluded');
  ok (!$d->has_num_cpus, 'number of cpus is not set');
  ok (!$d->has_memory,'memory is not set');
  is ($d->queue, 'lowload', 'queue');
  is ($d->fs_slots_num, 1, 'one fs slot is set');
  lives_ok {$d->freeze()} 'definition can be serialized to JSON';
}

{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_1234.csv];
  $util->create_analysis( {'qc_dir' => 1} );
  my $fq_loader = npg_pipeline::function::fastqcheck_archiver->new(
    run_folder => q{123456_IL2_1234},
    runfolder_path => $util->analysis_runfolder_path(),
    timestamp => q{20090709-123456},
  );

  my $da = $fq_loader->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  my $d = $da->[0];
  isa_ok($d, q{npg_pipeline::function::definition});
  is ($d->job_name, q{fastqcheck_loader_1234_20090709-123456},
    'job_name is correct');
  is ($d->command, qq{npg_qc_save_files.pl --path=$pbcal_path/archive},
    'command is correct');
}

1;
