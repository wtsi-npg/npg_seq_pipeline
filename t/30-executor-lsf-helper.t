use strict;
use warnings;
use Test::More tests => 4;
use Test::Exception;
use File::Temp qw{ tempdir };

use_ok('npg_pipeline::executor::lsf::helper');

subtest 'object creation and default values of attributes' => sub {
  plan tests => 3;

  my $h = npg_pipeline::executor::lsf::helper->new();
  isa_ok ($h, 'npg_pipeline::executor::lsf::helper');
  ok (!$h->no_bsub, 'default - use bsub');
  ok (!$h->lsf_conf, 'lsf config not defined by default');
};

subtest 'memory' => sub {
  plan tests => 12;
  
  my $h = npg_pipeline::executor::lsf::helper->new();
  my $expected = q{-R 'select[mem>7000] rusage[mem=7000]' -M7000};;
  is($h->memory_spec(7000), $expected,
    q{Using default memory units gives correct memory spec});
  is($h->memory_spec(7000, q{MB}), $expected,
    q{Using MB memory units gives correct memory spec});

  is($h->memory_in_mb(8_000_000, q{KB}), 8_000, q{memory in mb});
  is($h->memory_spec(8_000_000, q{KB}), q{-R 'select[mem>8000] rusage[mem=8000]' -M8000},
    q{memory spec from KB is correct});
  is($h->memory_spec(8_000_050, q{KB}), q{-R 'select[mem>8000] rusage[mem=8000]' -M8000},
    q{memory spec from KB is correct});

  is($h->memory_in_mb(8, q{GB}), 8_000, q{memory in mb});
  is($h->memory_spec(8, q{GB}), q{-R 'select[mem>8000] rusage[mem=8000]' -M8000},
    q{memory spec from GB is correct});

  throws_ok { $h->memory_spec(1, q{TB}) }
    qr/Memory unit TB is not recognised/,
    q{error if memory units are not recognised};

  throws_ok { $h->memory_spec(-8000) }
    qr/Memory -8000 MB out of bounds/, q{negative memory is rejected};
  throws_ok { $h->memory_spec(0) }
    qr/Memory required/, q{zero memory is rejected};
  throws_ok { $h->memory_spec(600.5) }
    qr/Memory should be an integer/, q{floating point memory is rejected};
  throws_ok { $h->memory_spec('some') }
    qr/Argument \"some\" isn't numeric in int/,
    q{Memory cannot be a string of characters};
};

subtest 'lsf command execution' => sub {
  plan tests => 14;
 
  my $h = npg_pipeline::executor::lsf::helper->new(lsf_conf => {});

  my $e = qr/command have to be a non-empty string/;
  throws_ok {$h->execute_lsf_command()} $e, 'command has to be defined';
  throws_ok {$h->execute_lsf_command(q[])} $e, 'command cannot be an empty string';
  throws_ok {$h->execute_lsf_command(qq[ \n])} $e,
    'command cannot be a string of white sspace characters';
  
  throws_ok {$h->execute_lsf_command('echo 3')}
    qr/'echo' is not one of supported LSF commands/,
    'error if the command is not supported';
  throws_ok {$h->execute_lsf_command('bmod 3')}
    qr/'bmod' is not one of supported LSF commands/,
    'bmod LSF command is not supported';
  throws_ok {$h->execute_lsf_command(' hostname ')}
    qr/'hostname' is not one of supported LSF commands/,
    'bmod LSF command is not supported';

  # mock supported LSF commands
  local $ENV{'PATH'} = join q[:], 't/bin', $ENV{'PATH'};
  is ($h->execute_lsf_command('bsub some'), 30, 'mock LSF job submitted');
  is ($h->execute_lsf_command('bkill some'), q[],
    'mock bkill command is executed, empty string returned');  
  is ($h->execute_lsf_command('bresume some'), q[],
    'mock bresume command is executed, empty string returned');

  $h = npg_pipeline::executor::lsf::helper->new(no_bsub => 1);
  is ($h->execute_lsf_command('bsub some'), 50,
    'bsub command is not executed, default job id is returned');
  is ($h->execute_lsf_command('bkill some'), q[],
    'bkill command is not executed, empty string returned');  
  is ($h->execute_lsf_command('bresume some'), q[],
    'bresume command is not executed, empty string returned');

  # soft-link bsub and bkill commands to /bin/false so that they fail
  my $tmp = tempdir(CLEANUP => 1);
  symlink '/bin/false', "$tmp/bsub";
  symlink '/bin/false', "$tmp/bkill";
  local $ENV{'PATH'} = join q[:], $tmp, $ENV{'PATH'};
  $h = npg_pipeline::executor::lsf::helper->new(lsf_conf => {min_sleep => 1, max_tries => 2});
  my $job_id;
  throws_ok { $job_id = $h->execute_lsf_command('bsub some') }
    qr/Failed to submit command to LSF/, 'error on failure to execute'; 
  throws_ok { $job_id = $h->execute_lsf_command('bkill some') }
    qr/Failed to submit command to LSF/, 'error on failure to execute'; 
};

1;
