use strict;
use warnings;
use Test::More tests => 34;
use Test::Exception;
use Cwd;

use_ok('npg_pipeline::lsf_job');

my $lsf_path = `which lsadmin`;
 
SKIP: {
  if ($lsf_path eq q{}) {
    skip 'not running any live LSF', 2;
  }
  my $expected_memory = 7000;
  my $lsf_job = npg_pipeline::lsf_job->new(memory => $expected_memory);

  my $unit = ($lsf_path =~ /7/) ? q{KB} : q{MB};
  my $expected_memory_limit = ($unit eq 'KB') ? $expected_memory * 1000 : $expected_memory;
  my $expected_memory_string = qq{-R 'select[mem>$expected_memory] rusage[mem=$expected_memory]' -M$expected_memory_limit};

  is($lsf_job->memory_spec(), $expected_memory_string, q{Using default memory units gives correct memory spec});
  is($lsf_job->_scale_mem_limit(), $expected_memory_limit, qq{memory limit is $expected_memory_limit});
} # end of skip

local $ENV{PATH} = join q[:], q[t/bin], q[t/bin/software/solexa/bin], $ENV{PATH};
{
  my $response = `lsadmin`;
  like($response, qr/LSF_UNIT_FOR_LIMITS = MB/, q{local lsadmin (LSF 9.x) is available});
  my $expected_memory_string = q{-R 'select[mem>8000] rusage[mem=8000]' -M8000};
  is(npg_pipeline::lsf_job->new(memory => 8000)->memory_spec(), $expected_memory_string,
    q{Using default memory units gives correct memory spec});

  local $ENV{PATH} = join q[:], q[t/bin/lenny], $ENV{PATH};
  $response = `lsadmin`;
  like($response, qr/LSF_UNIT_FOR_LIMITS = KB/, q{local lsadmin (LSF 7.x) is available});
  $expected_memory_string = q{-R 'select[mem>8000] rusage[mem=8000]' -M8000000};
  is(npg_pipeline::lsf_job->new(memory => 8000)->memory_spec(),
    $expected_memory_string, q{Using default memory units gives correct memory spec});
}

{
  my $expected_memory = 8_000_000;
  my $expected_memory_in_mb = 8_000;
  my $expected_memory_units = q{KB};

  my $lsf_job = npg_pipeline::lsf_job->new(
    memory => $expected_memory,
    memory_units => qq{$expected_memory_units},
  );
  isa_ok($lsf_job, q{npg_pipeline::lsf_job}, q{$lsf_job});
  is($lsf_job->memory, $expected_memory, q{memory set correctly});
  is($lsf_job->memory_in_mb, $expected_memory_in_mb, q{memory in mb set correctly});
  is($lsf_job->_is_valid_memory(), 1, qq{memory $expected_memory is valid});
  is($lsf_job->_is_valid_memory_unit(), 1, qq{memory unit $expected_memory_units is valid});
  is($lsf_job->_scale_mem_limit(), 8000, qq{memory limit correct});

  is($lsf_job->memory_spec(), q{-R 'select[mem>8000] rusage[mem=8000]' -M8000},
    q{$lsf_job->memory_spec() constructed correctly});
}

{
  my $expected_memory = 8000;
  my $expected_memory_units = q{MB};

  my $lsf_job = npg_pipeline::lsf_job->new(
    memory => $expected_memory,
    memory_units => qq{$expected_memory_units},
  );
  is($lsf_job->_is_valid_memory(), 1, qq{memory $expected_memory is valid});
  is($lsf_job->_is_valid_memory_unit(), 1, qq{memory unit $expected_memory_units is valid});
  is($lsf_job->_scale_mem_limit(), 8000, qq{memory limit is correct});
  is($lsf_job->memory_spec(), q{-R 'select[mem>8000] rusage[mem=8000]' -M8000}, q{memory spec constructed correctly});
}

{
  my $expected_memory = 8;
  my $expected_memory_units = q{GB};

  my $lsf_job = npg_pipeline::lsf_job->new(
    memory => $expected_memory,
    memory_units => qq{$expected_memory_units},
  );
  is($lsf_job->_is_valid_memory(), 1, qq{memory $expected_memory is valid});
  ok($lsf_job->_is_valid_memory_unit(), qq{memory unit $expected_memory_units is valid});
  is($lsf_job->memory_spec(), q{-R 'select[mem>8000] rusage[mem=8000]' -M8000}, q{memory spec constructed correctly});
}

{
  is(npg_pipeline::lsf_job->new(memory => 8000)->memory_spec(),
    q{-R 'select[mem>8000] rusage[mem=8000]' -M8000}, q{Using default memory units gives correct memory spec});
}

{
  my $expected_memory_units = 'TB';
  my $lsf_job = npg_pipeline::lsf_job->new( memory => 8000, memory_units => $expected_memory_units,);
  ok(!($lsf_job->_is_valid_memory_unit()), qq{memory unit $expected_memory_units is NOT valid}); 
  ok(!($lsf_job->_is_valid_lsf_memory_unit($expected_memory_units)), qq{memory unit $expected_memory_units is NOT valid as an LSF memory unit}); 
  throws_ok {
    $lsf_job->memory_spec();
    } qr/lsf_job does not recognise requested memory unit/, q{croak if memory units are not recognised};

  throws_ok {
    npg_pipeline::lsf_job->new(memory => -8000)->memory_spec();
  } qr/failed/, q{Using negative memory is rejected};

  $lsf_job = npg_pipeline::lsf_job->new( memory => 8000000, memory_units => qq{MB},);
  is($lsf_job->_is_valid_memory(), 0, qq{memory is NOT valid}); 
  throws_ok {
    $lsf_job->memory_spec();
    } qr/lsf_job cannot handle request for memory /, q{croak if memory is silly};
}

{
  my $expected_memory = 8_550_050;
  my $expected_memory_units = 'KB';
  my $lsf_job = npg_pipeline::lsf_job->new(memory => $expected_memory, memory_units =>$expected_memory_units);
  my $expected_memory_string = qq{-R 'select[mem>8550] rusage[mem=8550]' -M8550};
  is($lsf_job->memory_spec(), $expected_memory_string, q{Using default memory units gives correct memory spec});
  is($lsf_job->_scale_mem_limit(), 8_550, qq{memory limit is correct});
}

{
  local $ENV{PATH} = join q[:], q[t/bin/dodo], $ENV{PATH};
  my $response = `lsadmin`;
  like($response, qr/LSF_UNIT_FOR_LIMITS = dodo/, q{local lsadmin is broken});

  my $expected_memory = 8000;
  my $lsf_job = npg_pipeline::lsf_job->new(memory => $expected_memory);
  ok(!$lsf_job->_is_valid_lsf_memory_unit(q{dodo}), q{dodo is NOT a valid LSF memory unit});

  throws_ok {
    $lsf_job->_find_memory_units();
    $lsf_job->memory_spec();
  } qr/Cannot/, q{Error is thrown from _find_memory_units when local units are set to dodo};

  throws_ok {
    $lsf_job->memory_spec();
  } qr/Cannot/, q{Error is thrown when asking for memory spec if local units are set to dodo};

}

1;
