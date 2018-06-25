use strict;
use warnings;
use Test::More tests => 3;
use Test::Exception;
use File::Temp qw(tempdir);
use Graph::Directed;
use Log::Log4perl qw(:levels);

use_ok('npg_pipeline::executor::lsf');

my $tmp = tempdir(CLEANUP => 1);

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $tmp, 'logfile'),
                          utf8   => 1});

my $ref = {
  lsf_conf             => {},
  function_definitions => {},
  function_graph       => Graph::Directed->new(),
          };

subtest 'object creation and default values of attributes' => sub {
  plan tests => 2;

  my $l = npg_pipeline::executor::lsf->new($ref);
  isa_ok ($l, 'npg_pipeline::executor::lsf');
  ok (!$l->no_bsub, 'default - use bsub');
};

subtest 'lsf command execution' => sub {
  plan tests => 14;
 
  my $l = npg_pipeline::executor::lsf->new($ref);

  my $e = qr/command has to be a non-empty string/;
  throws_ok {$l->_execute_lsf_command()} $e, 'command has to be defined';
  throws_ok {$l->_execute_lsf_command(q[])} $e, 'command cannot be an empty string';
  throws_ok {$l->_execute_lsf_command(qq[ \n])} $e,
    'command cannot be a string of white sspace characters';
  
  throws_ok {$l->_execute_lsf_command('echo 3')}
    qr/'echo' is not one of supported LSF commands/,
    'error if the command is not supported';
  throws_ok {$l->_execute_lsf_command('bmod 3')}
    qr/'bmod' is not one of supported LSF commands/,
    'bmod LSF command is not supported';
  throws_ok {$l->_execute_lsf_command(' hostname ')}
    qr/'hostname' is not one of supported LSF commands/,
    'bmod LSF command is not supported';

  # mock supported LSF commands
  local $ENV{'PATH'} = join q[:], 't/bin', $ENV{'PATH'};
  is ($l->_execute_lsf_command('bsub some'), 30, 'mock LSF job submitted');
  is ($l->_execute_lsf_command('bkill some'), q[],
    'mock bkill command is executed, empty string returned');  
  is ($l->_execute_lsf_command('bresume some'), q[],
    'mock bresume command is executed, empty string returned');

  $ref->{'no_bsub'} = 1;
  $l = npg_pipeline::executor::lsf->new($ref);
  delete $ref->{'no_bsub'};
  is ($l->_execute_lsf_command('bsub some'), 50,
    'bsub command is not executed, default job id is returned');
  is ($l->_execute_lsf_command('bkill some'), q[],
    'bkill command is not executed, empty string returned');  
  is ($l->_execute_lsf_command('bresume some'), q[],
    'bresume command is not executed, empty string returned');

  # soft-link bsub and bkill commands to /bin/false so that they fail
  symlink '/bin/false', "$tmp/bsub";
  symlink '/bin/false', "$tmp/bkill";
  local $ENV{'PATH'} = join q[:], $tmp, $ENV{'PATH'};
  $ref->{'lsf_conf'} = {'min_sleep' => 1, 'max_tries' => 2};
  $l = npg_pipeline::executor::lsf->new($ref);
  $ref->{'lsf_conf'} = {};
  throws_ok { $l->_execute_lsf_command('bsub some') }
    qr/Failed to submit command to LSF/, 'error on failure to execute'; 
  throws_ok { $l->_execute_lsf_command('bkill some') }
    qr/Failed to submit command to LSF/, 'error on failure to execute'; 
};

1;
