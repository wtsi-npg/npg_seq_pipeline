use strict;
use warnings;
use Test::More tests => 8;

use_ok('npg_pipeline::function::remove_intermediate_data');

my $o=npg_pipeline::function::remove_intermediate_data->new(
  id_run => q[26291],
  recalibrated_path => q[/a/recalibrated/path/no_cal],
  timestamp         => q{2019},
  resource => {
    default => {
      minimum_cpu => 1,
      memory => 2
    }
  }
);
my $defs = $o->create;

my $expected_command = q[rm -fv /a/recalibrated/path/no_cal/*.cram];

ok ($defs && @{$defs} == 1, 'array of 1 definition is returned');
my $def = $defs->[0];
isa_ok ($def, 'npg_pipeline::function::definition');
is ($def->created_by, 'npg_pipeline::function::remove_intermediate_data', 'created by correct');
is ($def->created_on, '2019', 'timestamp');
is ($def->identifier, 26291, 'identifier is set correctly');
is ($def->job_name, 'remove_intermediate_data_26291_2019', 'job name');
is ($def->command, $expected_command, 'correct command');

1;
