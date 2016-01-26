use strict;
use warnings;
use Test::More tests => 8;
use Cwd;

use_ok('npg_tracking::daemon::analysis');
{
    my $r = npg_tracking::daemon::analysis->new();
    isa_ok($r, 'npg_tracking::daemon::analysis');
}

{
    my $command = 'npg_pipeline_analysis_runner';
    my $log_dir = join(q[/],getcwd(), 'logs');
    my $r = npg_tracking::daemon::analysis->new(timestamp => '2013');
    is(join(q[ ], @{$r->hosts}), q[sf2-farm-srv1 sf2-farm-srv2], 'default host names');
    is($r->command, $command, 'command to run');
    is($r->daemon_name, 'npg_pipeline_analysis_runner', 'default daemon name');

    my $host = q[sf-1-1-01];
    my $test = q{[[ -d } . $log_dir . q{ && -w } . $log_dir . q{ ]] && };
    my $error = q{ || echo Log directory } .  $log_dir . q{ for staging host } . $host . q{ cannot be written to};
    my $action = $test . qq[daemon -i -r -a 10 -n $command --umask 002 -A 10 -L 10 -M 10 -o $log_dir/$command-$host-2013.log -- $command] . $error;

    is($r->start($host), $action, 'start command');
    is($r->ping, q[daemon --running -n npg_pipeline_analysis_runner && ((if [ -w /tmp/npg_pipeline_analysis_runner.pid ]; then touch -mc /tmp/npg_pipeline_analysis_runner.pid; fi) && echo -n 'ok') || echo -n 'not ok'], 'ping command');
    is($r->stop, q[daemon --stop -n npg_pipeline_analysis_runner], 'stop command');
}

1;
