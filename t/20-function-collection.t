use strict;
use warnings;
use Test::More tests => 2;
use Test::Exception;

use t::util;

my $util = t::util->new();
my $runfolder_path = $util->analysis_runfolder_path();

use_ok('npg_pipeline::function::collection');

subtest ' bam2fastqcheck_and_cached_fastq' => sub {
  plan tests => 112;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q{t/data/samplesheet_1234.csv};

  my $ds = npg_pipeline::function::collection->new(
    id_run            => 1234,
    recalibrated_path => $runfolder_path,
    runfolder_path    => $runfolder_path,
    timestamp         => q{22-May},
  )->bam2fastqcheck_and_cached_fastq();
  ok ($ds && scalar @{$ds} == 8, 'eight definitions returned');

  my $count = 0;
  foreach my $d (@{$ds}) {
    $count++;
    my $command = "generate_cached_fastq --path ${runfolder_path}/archive" .
                  " --file ${runfolder_path}/1234_${count}.bam";
    isa_ok ($d, 'npg_pipeline::function::definition');
    is ($d->identifier, '1234', 'identifier set to run id');
    is ($d->created_by, 'npg_pipeline::function::collection', 'created_by');
    ok (!$d->excluded, 'function is not excluded');
    is ($d->command, $command, 'command');
    is ($d->job_name, 'bam2fastqcheck_and_cached_fastq_1234_22-May', 'job name');
    is ($d->fs_slots_num, 1, 'one fs slot');
    ok ($d->has_composition, 'composition is set');
    is ($d->composition->num_components, 1, 'one componet in a composition');
    is ($d->composition->get_component(0)->position, $count, 'correct position');
    ok (!defined $d->composition->get_component(0)->tag_index,
      'tag index is not defined');
    ok (!defined $d->composition->get_component(0)->subset,
      'subset is not defined');
    is ($d->queue, 'default', 'default queue');  
  }

  $ds = npg_pipeline::function::collection->new(
    id_run            => 1234,
    recalibrated_path => $runfolder_path,
    runfolder_path    => $runfolder_path,
    lanes             => [2, 5]
  )->bam2fastqcheck_and_cached_fastq();
  ok ($ds && scalar @{$ds} == 2, 'two definitions returned');
  is ($ds->[0]->composition->get_component(0)->position, 2,
    'definition for position 2');
  is ($ds->[1]->composition->get_component(0)->position, 5,
    'definition for position 5');

  $ds = npg_pipeline::function::collection->new(
    id_run            => 1234,
    recalibrated_path => $runfolder_path,
    runfolder_path    => $runfolder_path,
    lanes             => [5, 8, 3]
  )->bam2fastqcheck_and_cached_fastq();
  ok ($ds && scalar @{$ds} == 3, 'two definitions returned');
  is ($ds->[0]->composition->get_component(0)->position, 3,
    'definition for position 3');
  is ($ds->[1]->composition->get_component(0)->position, 5,
    'definition for position 5');
  is ($ds->[2]->composition->get_component(0)->position, 8,
    'definition for position 8');
};
