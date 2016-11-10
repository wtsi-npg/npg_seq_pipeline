use strict;
use warnings;
use Test::More tests => 21;
use Test::Exception;
use Cwd;
use Log::Log4perl qw(:levels);
use t::util;

local $ENV{PATH} = join q[:], q[t/bin], q[t/bin/software/solexa/bin], $ENV{PATH};

use_ok('npg_pipeline::pluggable');

my $util = t::util->new();
my $test_dir = $util->temp_directory();
$ENV{TEST_DIR} = $test_dir;
$ENV{OWNING_GROUP} = q{staff};
local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $test_dir, 'logfile'),
                          utf8   => 1});

{
  my $pluggable = npg_pipeline::pluggable->new(
    id_run => 1234,
    runfolder_path => $test_dir,
  );
  isa_ok($pluggable, q{npg_pipeline::pluggable}, q{$pluggable});
  is($pluggable->pipeline_name, 'pluggable', 'pipeline name');
  is($pluggable->interactive, 0, 'interactive false');
  is(join(q[ ], @{$pluggable->function_order}), 'lsf_start lsf_end', '2 functions added implicitly');
}

{
  my $pluggable = npg_pipeline::pluggable->new(
    id_run => 1234,
    runfolder_path => $test_dir,
    function_order => [],
  );
  my $rv;
  lives_ok { $rv = $pluggable->_finish(); 1; } q{no croak with $pluggable->_finish()};
  is($rv, undef, q{return value of $pluggable->_finish() is correct});
  lives_ok { $pluggable->lsf_start() } q{no croak with $pluggable->lsf_start()};
  lives_ok { $pluggable->lsf_end() } q{no croak with $pluggable->lsf_end()};
  is(join(q[ ], @{$pluggable->function_order}), 'lsf_start lsf_end', '2 functions added implicitly to an empty list');
}

{
  my $pluggable = npg_pipeline::pluggable->new(
    id_run => 1234,
    lanes   => [1],
    runfolder_path => q{Data/found/here},
    no_bsub => 1,
    function_order => ['my_function'],
  );

  is($pluggable->id_run(), 1234, q{$pluggable->id_run() populated on new});
  is($pluggable->script_name(), q{t/10-pluggable.t}, q{$pluggable->script_name() obtained});
  is($pluggable->conf_path, join(q[/], getcwd, 't/../data/config_files'), 'local conf path is built');
  is(join(q[ ], @{$pluggable->function_order}), 'lsf_start my_function lsf_end', '2 functions added implicitly');
  throws_ok {$pluggable->main()} qr{Error submitting jobs: Can't locate object method "my_function" via package "npg_pipeline::pluggable"} , 'error when unknown function is used';
  my $finish;
  lives_ok { $finish = $pluggable->_finish(); 1; } q{no croak with $pluggable->_finish()};
  is($finish, undef, q{return value of $pluggable->_finish() is correct});
  my @ids;
  lives_ok { @ids = $pluggable->lsf_start() } q{no croak with $pluggable->lsf_start()};
  is(join(q[ ], @ids), '50', 'test start job id is correct');
  lives_ok { @ids = $pluggable->lsf_end() } q{no croak with $pluggable->lsf_end()};
  is(join(q[ ], @ids), '50', 'test end job id is correct');
}

1;
