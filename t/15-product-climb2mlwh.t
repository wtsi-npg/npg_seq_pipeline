use strict;
use warnings;
use Test::More tests => 4;
use Test::Exception;
use File::Temp qw/tempdir/;
use File::Path qw/make_path/;
use File::Copy;
use File::Slurp;
use Log::Log4perl qw/:levels/;
use Moose::Meta::Class;
use DateTime;
use t::dbic_util;

my $dir = tempdir( CLEANUP => 0);

use_ok('npg_pipeline::product::heron::upload::climb2mlwh');

# Create dummy command file
my $exec = join q[/], $dir, 'wsi-npg-ssh-restricted';
open my $fh1, '>', $exec or die 'failed to open file for writing';
print $fh1 qq[
printf '1605550073.2791698770\\t201112_A00971_0093_AHVCTKDRXX/QEUH-B14165/35466_2#312.mapped.bam\\n'
printf '1605547023.4205982210\\t201112_A00971_0093_AHVCTKDRXX/QEUH-B0DE3B/35466_1#13.mapped.bam\\n'
printf '1605548414.2048339100\\201112_A00971_0093_AHVCTKDRXX/QEUH-B0FB7E/35466_1#370.mapped.bam\\n'
] or warn 'failed to print';
close $fh1 or warn 'failed to close file handle';
chmod 0755, $exec;
local $ENV{PATH} = join q[:], $dir, $ENV{PATH};

my $logfile = join q[/], $dir, 'logfile';

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => $logfile,
                          utf8   => 1});

# Create dummy pkey_file
my $pkey_file = join q[/], $dir, 'pkey_file';
open my $pkey_fh, '>', $pkey_file or die 'failed to open pkey file for writing';
print $pkey_fh 'dummy pkey file' or warn 'failed to print to pkey_file';
close $pkey_fh or warn 'failed to close pkey_file';

# Create test database
my $schema = t::dbic_util->new()->test_schema_mlwh('t/data/fixtures/mlwh');

my $init = {
  user          => 'fred',
  run_folder    => 'runfolder',
  pkey_file     => $pkey_file,
  host          => 'localhost',
  _schema       => $schema,
  dry_run       => 1
};

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG});
my $logger = Log::Log4perl->get_logger();
my $command = [$exec];

my $f = npg_pipeline::product::heron::upload::climb2mlwh->new($init);
$f->_update_mlwh($logger, $command);
my $s = $schema->resultset(q(IseqHeronProductMetric))->search({supplier_sample_name=>q{QEUH-B14165}});
is ($s, 0, 'nothing updated for dry_run');

$init->{dry_run} = 0;
$f = npg_pipeline::product::heron::upload::climb2mlwh->new($init);
$f->_update_mlwh($logger, $command);
$s = $schema->resultset(q(IseqHeronProductMetric))->search({supplier_sample_name=>q{QEUH-B14165}});
is($s, 1, 'sample name added');

$f->_update_mlwh($logger, $command);
$s = $schema->resultset(q(IseqHeronProductMetric))->search({supplier_sample_name=>q{QEUH-B14165}});
is($s, 1, 'sample name updated');

1;
