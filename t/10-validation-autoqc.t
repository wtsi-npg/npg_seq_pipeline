use strict;
use warnings;
use Test::More tests => 6;
use Test::Exception;
use Log::Log4perl;
use Moose::Meta::Class;

Log::Log4perl::init_once('./t/log4perl_test.conf');

use_ok('npg_pipeline::product');
use_ok('npg_pipeline::validation::entity');
use_ok('npg_pipeline::validation::autoqc');

my $schema = Moose::Meta::Class->create_anon_class(roles => [qw/npg_testing::db/])
                            ->new_object()->create_test_db(q[npg_qc::Schema]);
my $logger = Log::Log4perl->get_logger('dnap');

throws_ok { npg_pipeline::validation::autoqc->new(
            is_paired_read   => 1,
            logger           => $logger,
            qc_schema        => $schema,
            product_entities => []) }
  qr/product_entities array cannot be empty/, 'object construction failed';

my $irrelevant_entity =  npg_pipeline::validation::entity->new(
  staging_archive_root => q[t],
  target_product       => npg_pipeline::product->new(rpt_list => q[2:3])
);

my $validator = npg_pipeline::validation::autoqc->new(
        is_paired_read   => 1,
        logger           => $logger,
        skip_checks      => [qw/adaptor samtools_stats+phix+human/],
        qc_schema        => $schema,
        product_entities => [ $irrelevant_entity]);

isa_ok($validator, 'npg_pipeline::validation::autoqc');

my $expected = {'adaptor' => [], 'samtools_stats' => [qw/phix human/]};
is_deeply($validator->_skip_checks_wsubsets(), $expected, 'excluded checks parsed correctly');

1;
