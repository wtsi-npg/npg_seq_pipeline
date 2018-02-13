use strict;
use warnings;
use Test::More tests => 12;

use_ok('npg_pipeline::function::autoqc_input_scaffold');

my $s = npg_pipeline::function::autoqc_input_scaffold->new();
isa_ok($s, 'npg_pipeline::function::autoqc_input_scaffold');

{
  is ($s->fq_filename(3, undef), '4_3.fastq');
  is ($s->fq_filename(3, undef, 1), '4_3_1.fastq');
  is ($s->fq_filename(3, undef, 2), '4_3_2.fastq');
  is ($s->fq_filename(3, undef, 't'), '4_3_t.fastq');
  is ($s->fq_filename(3, 5), '4_3#5.fastq');
  is ($s->fq_filename(3, 5, 1), '4_3_1#5.fastq');
  is ($s->fq_filename(3, 5, 2), '4_3_2#5.fastq');
  is ($s->fq_filename(3, 0), '4_3#0.fastq');
  is ($s->fq_filename(3, 0, 1), '4_3_1#0.fastq');
  is ($s->fq_filename(3, 0, 2), '4_3_2#0.fastq');
}

1;
