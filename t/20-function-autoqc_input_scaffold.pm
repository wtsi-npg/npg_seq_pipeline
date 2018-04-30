use strict;
use warnings;
use Test::More tests => 17;
use t::util;

use_ok('npg_pipeline::function::autoqc_input_scaffold');

my $util = t::util->new();
my $runfolder_path = $util->analysis_runfolder_path();
$util->create_analysis();

{
  my $s = npg_pipeline::function::autoqc_input_scaffold->new(
    id_run            => 1234,
    run_folder        => q{123456_IL2_1234},
    runfolder_path    => $runfolder_path,
    timestamp         => q{20090709-123456},
  );
  isa_ok($s, 'npg_pipeline::function::autoqc_input_scaffold');

  my $da = $s->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  my $d = $da->[0];
  isa_ok($d, q{npg_pipeline::function::definition});
  is ($d->created_by, q{npg_pipeline::function::autoqc_input_scaffold},
    'created_by is correct');
  is ($d->created_on, $s->timestamp, 'created_on is correct');
  ok ($d->immediate_mode, 'immediate mode is true');
}

{
  my $s = npg_pipeline::function::autoqc_input_scaffold->new(
    id_run            => 1234,
    run_folder        => q{123456_IL2_1234},
    runfolder_path    => $runfolder_path,
  );  
  is ($s->fq_filename(3, undef), '1234_3.fastq', 'correct filename');
  is ($s->fq_filename(3, undef, 1), '1234_3_1.fastq', 'correct filename');
  is ($s->fq_filename(3, undef, 2), '1234_3_2.fastq', 'correct filename');
  is ($s->fq_filename(3, undef, 't'), '1234_3_t.fastq', 'correct filename');
  is ($s->fq_filename(3, 5), '1234_3#5.fastq', 'correct filename');
  is ($s->fq_filename(3, 5, 1), '1234_3_1#5.fastq', 'correct filename');
  is ($s->fq_filename(3, 5, 2), '1234_3_2#5.fastq', 'correct filename');
  is ($s->fq_filename(3, 0), '1234_3#0.fastq', 'correct filename');
  is ($s->fq_filename(3, 0, 1), '1234_3_1#0.fastq', 'correct filename');
  is ($s->fq_filename(3, 0, 2), '1234_3_2#0.fastq', 'correct filename');
}

1;
