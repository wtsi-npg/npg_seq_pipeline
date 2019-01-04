use strict;
use warnings;
use Test::More tests => 3;
use Log::Log4perl;
use Moose::Meta::Class;

Log::Log4perl::init_once('./t/log4perl_test.conf');

use_ok('npg_pipeline::validation::autoqc_files');

my $schema = Moose::Meta::Class->create_anon_class(roles => [qw/npg_testing::db/])
                            ->new_object()->create_test_db(q[npg_qc::Schema]);
my $logger = Log::Log4perl->get_logger('dnap');

my $validator = npg_pipeline::validation::autoqc_files->new(
        is_paired_read => 1,
        logger         => $logger,
        skip_checks    => [qw/adaptor samtools_stats+phix+human/],
        _qc_schema     => $schema,
        staging_files  => {});

isa_ok($validator, 'npg_pipeline::validation::autoqc_files');

my $expected = {'adaptor' => [], 'samtools_stats' => [qw/phix human/]};
is_deeply($validator->_skip_checks_wsubsets(), $expected, 'excluded checks parsed correctly');

1;
