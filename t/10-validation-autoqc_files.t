use strict;
use warnings;
use Test::More tests => 14;
use Log::Log4perl;
use Moose::Meta::Class;

use WTSI::NPG::iRODS;

Log::Log4perl::init_once('./t/log4perl_test.conf');
my $logger = Log::Log4perl->get_logger('dnap');
my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0, logger => $logger);

use_ok('npg_validation::runfolder::deletable::autoqc');

my $qc  = Moose::Meta::Class->create_anon_class(roles => [qw/npg_testing::db/])
                            ->new_object()->create_test_db(q[npg_qc::Schema]);

my $validator = npg_validation::runfolder::deletable::autoqc->new
       (id_run      => 1234,
        irods       => $irods,
        verbose     => 1,
        skip_checks => [qw/adaptor samtools_stats+phix+human/],
        _qc_schema  => $qc);

isa_ok($validator, 'npg_validation::runfolder::deletable::autoqc');

my $expected = {'adaptor' => [], 'samtools_stats' => [qw/phix human/]};
is_deeply($validator->_parse_excluded_checks(), $expected, 'excluded checks parsed correctly');

$validator = npg_validation::runfolder::deletable::autoqc->new
       (id_run     => 1234,
        irods      => $irods,
        verbose    => 1,
        _qc_schema => $qc);
is_deeply($validator->_parse_excluded_checks(), {}, 'excluded checks parsed correctly');

is($validator->_query_to_be_skipped(
  {'check' => 'pig'}, $expected), 0, 'no skip');
is($validator->_query_to_be_skipped(
  {'check' => 'pig', 'subset' => 'phix'}, $expected), 0, 'no skip');
is($validator->_query_to_be_skipped(
  {'check' => 'adaptor'}, $expected), 1, 'skip');
is($validator->_query_to_be_skipped(
  {'check' => 'adaptor', 'subset' => 'all'}, $expected), 1, 'skip');
is($validator->_query_to_be_skipped(
  {'check' => 'adaptor'}, {}), 0, 'no skip');
is($validator->_query_to_be_skipped(
  {'check' => 'samtools_stats'}, $expected), 0, 'no skip');
is($validator->_query_to_be_skipped(
  {'check' => 'samtools_stats', 'subset' => 'target'}, $expected), 0, 'no skip');
is($validator->_query_to_be_skipped(
  {'check' => 'samtools_stats', 'subset' => 'phix'}, $expected), 1, 'skip');
is($validator->_query_to_be_skipped(
  {'check' => 'samtools_stats', 'subset' => 'human'}, $expected), 1, 'skip');
is($validator->_query_to_be_skipped(
  {'check' => 'samtools_stats', 'subset' => 'human'}, {}), 0, 'no skip');

1;
