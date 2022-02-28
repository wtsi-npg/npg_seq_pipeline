use strict;
use warnings;
use Test::More tests => 6;
use Test::Exception;
use Log::Log4perl qw(:levels);
use Moose::Meta::Class;

use st::api::lims;
use t::util;

my $test_dir = t::util->new()->temp_directory();

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $test_dir, 'logfile'),
                          utf8   => 1});

# Need the norm_fit executable on the PATH,
# dependency of npg_qc::autoqc::checks::insert_size 
my $tool = qq[$test_dir/norm_fit];
open my $fh, '>', $tool or die 'cannot open file for writing';
print $fh $tool or die 'cannot print';
close $fh or warn 'failed to close file handle';
chmod 0755, $tool;
local $ENV{'PATH'} = join q[:], $test_dir, $ENV{'PATH'};

use_ok('npg_pipeline::product');
use_ok('npg_pipeline::validation::entity');
use_ok('npg_pipeline::validation::autoqc');

my $schema = Moose::Meta::Class
  ->create_anon_class(roles => [qw/npg_testing::db/])
  ->new_object()->create_test_db(q[npg_qc::Schema]);
my $logger = Log::Log4perl->get_logger('dnap');

my @CHECKS = qw/
    qX_yield adapter gc_fraction sequence_error
    spatial_filter tag_metrics
    bam_flagstats samtools_stats sequence_summary
/; 

subtest 'object construction' => sub {
  plan tests => 3;

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
  is_deeply($validator->_skip_checks_wsubsets(), $expected,
    'excluded checks parsed correctly');
};

subtest 'insert_size checks validation' => sub {
  plan tests => 9;

  local $ENV{NPG_REPOSITORY_ROOT} = q[t];

  is ($schema->resultset('InsertSize')->count(), 0,
    'no insert size db results');

  my @skip_checks = (@CHECKS, 'ref_match');

  ######
  # MiSeq run
  # Primer panel is set for all samples,
  # two different primer panels across the pool, resulting in
  # the primer_panel value not being set on lane level and tag zero.
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
    q[t/data/primer_panel/samplesheet_24135_gbs.csv];

  my $product = npg_pipeline::product->new(
    rpt_list => q[24135:1:0],
    lims => st::api::lims->new(id_run => 24135, position => 1, tag_index => 0)
  );
  my $entity = npg_pipeline::validation::entity->new(
    staging_archive_root => q[t], target_product => $product);
  my $validator = npg_pipeline::validation::autoqc->new(
    is_paired_read   => 1,
    logger           => $logger,
    qc_schema        => $schema,
    skip_checks      => \@skip_checks,
    product_entities => [$entity]
  );
  ok (!$validator->fully_archived, 'tag zero is not fully archived');

  $validator = npg_pipeline::validation::autoqc->new(
    is_paired_read   => 0,
    logger           => $logger,
    qc_schema        => $schema,
    skip_checks      => \@skip_checks,
    product_entities => [$entity]
  ); 
  ok ($validator->fully_archived, 'non-paired run, tag zero is fully archived');

  $product = npg_pipeline::product->new(
    rpt_list => q[24135:1:1],
    lims => st::api::lims->new(id_run => 24135, position => 1, tag_index => 1)
  );
  $entity = npg_pipeline::validation::entity->new(
    staging_archive_root => q[t], target_product => $product);
  $validator = npg_pipeline::validation::autoqc->new(
    is_paired_read   => 1,
    logger           => $logger,
    qc_schema        => $schema,
    skip_checks      => \@skip_checks,
    product_entities => [$entity]
  );
  ok ($validator->fully_archived, 'tag 1 is fully archived');

  $product = npg_pipeline::product->new(
    rpt_list => q[24135:1],
    lims => st::api::lims->new(id_run => 24135, position => 1)
  );
  $entity = npg_pipeline::validation::entity->new(
    staging_archive_root => q[t], target_product => $product);
  $validator = npg_pipeline::validation::autoqc->new(
    is_paired_read   => 1,
    logger           => $logger,
    qc_schema        => $schema,
    skip_checks      => \@skip_checks,
    product_entities => [$entity]
  );
  ok (!$validator->fully_archived, 'lane 1 is not fully archived');

  ######
  # NovaSeq run, two lanes
  # Primer panel is set for all samples,
  # the primer panels across each pool, resulting in
  # the primer_panel value being set on lane level and tag zero.
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
    q[t/data/primer_panel/samplesheet_36214.csv];
  
  $product = npg_pipeline::product->new(
    rpt_list => q[36214:2:0],
    lims => st::api::lims->new(id_run => 36214, position => 2, tag_index => 0)
  );
  $entity = npg_pipeline::validation::entity->new(
    staging_archive_root => q[t], target_product => $product);
  $validator = npg_pipeline::validation::autoqc->new(
    is_paired_read   => 1,
    logger           => $logger,
    qc_schema        => $schema,
    skip_checks      => \@skip_checks,
    product_entities => [$entity]
  );
  ok ($validator->fully_archived, 'tag zero is fully archived');

  $product = npg_pipeline::product->new(
    rpt_list => q[24135:1:1],
    lims => st::api::lims->new(id_run => 36214, position => 1, tag_index => 2)
  );
  $entity = npg_pipeline::validation::entity->new(
    staging_archive_root => q[t], target_product => $product);
  $validator = npg_pipeline::validation::autoqc->new(
    is_paired_read   => 1,
    logger           => $logger,
    qc_schema        => $schema,
    skip_checks      => \@skip_checks,
    product_entities => [$entity]
  );
  ok ($validator->fully_archived, 'tag 2 is fully archived');

  $product = npg_pipeline::product->new(
    rpt_list => q[24135:1],
    lims => st::api::lims->new(id_run => 36214, position => 1)
  );
  $entity = npg_pipeline::validation::entity->new(
    staging_archive_root => q[t], target_product => $product);
  $validator = npg_pipeline::validation::autoqc->new(
    is_paired_read   => 1,
    logger           => $logger,
    qc_schema        => $schema,
    skip_checks      => \@skip_checks,
    product_entities => [$entity]
  );
  ok ($validator->fully_archived, 'lane 1 is fully archived');

  my @entities = ();
  for my $lane ((1, 2)) {
    for my $tag ((0 .. 5)) {
      my $p = npg_pipeline::product->new(
        rpt_list => join(q[:], 24135, $lane, $tag),
        lims => st::api::lims->new(
          id_run => 36214, position => $lane, tag_index => $tag) 
      );
      push @entities, $entity = npg_pipeline::validation::entity->new(
        staging_archive_root => q[t], target_product => $p);
    }
  }
  $validator = npg_pipeline::validation::autoqc->new(
    is_paired_read   => 1,
    logger           => $logger,
    qc_schema        => $schema,
    skip_checks      => \@skip_checks,
    product_entities => \@entities
  );
  ok ($validator->fully_archived, 'run is fully archived');
};

subtest 'ref_match checks validation' => sub {
  plan tests => 5;

  local $ENV{NPG_REPOSITORY_ROOT} = q[t];

  is ($schema->resultset('RefMatch')->count(), 0,
    'no ref_match db results');

  my @skip_checks = (@CHECKS, 'insert_size');

  ######
  # MiSeq GBS run
  # Primer panel is set for all samples,
  # two different primer panels across the pool, resulting in
  # the primer_panel value not being set on lane level and tag zero.
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
    q[t/data/primer_panel/samplesheet_24135_gbs.csv];

  my $product = npg_pipeline::product->new(
    rpt_list => q[24135:1:0],
    lims => st::api::lims->new(id_run => 24135, position => 1, tag_index => 0)
  );
  my $entity = npg_pipeline::validation::entity->new(
    staging_archive_root => q[t], target_product => $product);
  my $validator = npg_pipeline::validation::autoqc->new(
    is_paired_read   => 1,
    logger           => $logger,
    qc_schema        => $schema,
    skip_checks      => \@skip_checks,
    product_entities => [$entity]
  );
  ok (!$validator->fully_archived, 'tag zero is not fully archived');

  $product = npg_pipeline::product->new(
    rpt_list => q[24135:1:1],
    lims => st::api::lims->new(id_run => 24135, position => 1, tag_index => 1)
  );
  $entity = npg_pipeline::validation::entity->new(
    staging_archive_root => q[t], target_product => $product);
  $validator = npg_pipeline::validation::autoqc->new(
    is_paired_read   => 1,
    logger           => $logger,
    qc_schema        => $schema,
    skip_checks      => \@skip_checks,
    product_entities => [$entity]
  );
  ok ($validator->fully_archived, 'tag 1 is fully archived');

  $product = npg_pipeline::product->new(
    rpt_list => q[24135:1],
    lims => st::api::lims->new(id_run => 24135, position => 1)
  );
  $entity = npg_pipeline::validation::entity->new(
    staging_archive_root => q[t], target_product => $product);
  $validator = npg_pipeline::validation::autoqc->new(
    is_paired_read   => 1,
    logger           => $logger,
    qc_schema        => $schema,
    skip_checks      => \@skip_checks,
    product_entities => [$entity]
  );
  ok (!$validator->fully_archived, 'lane 1 is not fully archived');

  ######
  # NovaSeq run, two lanes
  # Primer panel is set for all samples, this is not a GBS run.
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
    q[t/data/primer_panel/samplesheet_36214.csv];
  
  $product = npg_pipeline::product->new(
    rpt_list => q[24135:1:1],
    lims => st::api::lims->new(id_run => 36214, position => 1, tag_index => 2)
  );
  $entity = npg_pipeline::validation::entity->new(
    staging_archive_root => q[t], target_product => $product);
  $validator = npg_pipeline::validation::autoqc->new(
    is_paired_read   => 1,
    logger           => $logger,
    qc_schema        => $schema,
    skip_checks      => \@skip_checks,
    product_entities => [$entity]
  );
  ok (!$validator->fully_archived, 'tag 2 is not fully archived');
};

1;
