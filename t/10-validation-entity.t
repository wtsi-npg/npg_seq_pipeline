use strict;
use warnings;
use Test::More tests => 8;
use Test::Exception;

use_ok('npg_pipeline::product');
use_ok('npg_pipeline::validation::entity');

subtest 'object creation' => sub {
  plan tests => 2;

  my $product = npg_pipeline::product->new(rpt_list => q[22:2:3;22:3:3]);

  my $entity = npg_pipeline::validation::entity->new(
    staging_archive_root => q[t],
    target_product       => $product
  );
  isa_ok($entity, 'npg_pipeline::validation::entity');
  is ($entity->per_product_archive, 1, 'defaults to per-product archive');
};

subtest 'related products' => sub {
  plan tests => 8;

  my $rpt_list = q[22:2:3;22:3:3];
  my $product = npg_pipeline::product->new(rpt_list => $rpt_list);
  my $entity = npg_pipeline::validation::entity->new(
    staging_archive_root => q[t],
    target_product       => $product
  );
  is (scalar @{$entity->related_products}, 0, 'no subsets - empty related_products array');

  $entity = npg_pipeline::validation::entity->new(
    staging_archive_root => q[t],
    subsets              => [qw/phix human/],   
    target_product       => $product
  );
  is (scalar @{$entity->related_products}, 2,
    'two subsets - two items in the related_products array');
  map { isa_ok ($_, 'npg_pipeline::product') } @{$entity->related_products};
  
  is ($entity->related_products->[0]->composition->get_component(0)->subset,
     'phix', 'subset of the first product is phix');
  is ($entity->related_products->[0]->composition->freeze2rpt, $rpt_list,
     'correct rpt list');
  is ($entity->related_products->[1]->composition->get_component(0)->subset,
     'human', 'subset of the first product is human');
  is ($entity->related_products->[1]->composition->freeze2rpt, $rpt_list,
     'correct rpt list');
};

subtest 'description' => sub {
  plan tests => 1;

  my $product = npg_pipeline::product->new(rpt_list => '1:2');
  my $entity = npg_pipeline::validation::entity->new(
    staging_archive_root => q[t],
    target_product       => $product
  );
  is ($entity->description, '{"components":[{"id_run":1,"position":2}]}',
    'correct description');
};

subtest 'relative_path' => sub {
  plan tests => 6;

  my $product = npg_pipeline::product->new(rpt_list => '1:2');
  my $entity = npg_pipeline::validation::entity->new(
    staging_archive_root => q[t],
    per_product_archive  => 0,
    target_product       => $product
  );
  is ($entity->entity_relative_path, q[], 'common archive, empty relative path');

  $product = npg_pipeline::product->new(rpt_list => '1:2:1');
  $entity = npg_pipeline::validation::entity->new(
    staging_archive_root => q[t],
    per_product_archive  => 0,
    target_product       => $product
  );
  is ($entity->entity_relative_path, q[lane2], 'common archive, lane2 relative path');

  $product = npg_pipeline::product->new(rpt_list => '1:2');
  $entity = npg_pipeline::validation::entity->new(
    staging_archive_root => q[t],
    target_product       => $product
  );
  is ($entity->entity_relative_path, q[lane2], 'lane2 relative path');  

  $product = npg_pipeline::product->new(rpt_list => '1:2:1');
  $entity = npg_pipeline::validation::entity->new(
    staging_archive_root => q[t],
    target_product       => $product
  );
  is ($entity->entity_relative_path, q[lane2/plex1], 'lane2/plex1 relative path');

  $product = npg_pipeline::product->new(rpt_list => q[22:2:3;22:3:3]);
  $entity = npg_pipeline::validation::entity->new(
    staging_archive_root => q[t],
    target_product       => $product
  );
  is ($entity->entity_relative_path, q[plex3], 'plex13 relative path');

  $product = npg_pipeline::product->new(
    rpt_list => q[22:2:3;22:3:3],
    selected_lanes => 1
  );
  $entity = npg_pipeline::validation::entity->new(
    staging_archive_root => q[t],
    target_product       => $product
  );
  is ($entity->entity_relative_path, q[lane2-3/plex3],
    'lane2-3/plex13 relative path for a partial merge');
};

subtest 'entity_staging_path' => sub {
  plan tests => 2;

  my $product = npg_pipeline::product->new(rpt_list => '1:2');
  my $entity = npg_pipeline::validation::entity->new(
    staging_archive_root => q[t],
    per_product_archive  => 0,
    target_product       => $product
  );
  is ($entity->entity_staging_path, q[t], 'top-level path without change');

  $product = npg_pipeline::product->new(rpt_list => '1:2:1');
  $entity = npg_pipeline::validation::entity->new(
    staging_archive_root => q[t],
    per_product_archive  => 0,
    target_product       => $product
  );
  is ($entity->entity_staging_path, q[t/lane2], '/lane2 appended to the top-level path');  
};

subtest 'staging_files' => sub {
  plan tests => 3;

  my $product = npg_pipeline::product->new(rpt_list => '1:2:1');
  my $entity = npg_pipeline::validation::entity->new(
    staging_archive_root => q[t],  
    target_product       => $product
  );
  throws_ok { $entity->staging_files() } qr/Extension required/, 'extension required';
  is (join(q[,], $entity->staging_files(q[cram])), 't/lane2/plex1/1_2#1.cram',
    'single cram file path');

  $entity = npg_pipeline::validation::entity->new(
    staging_archive_root => q[t],
    per_product_archive  => 0,
    subsets              => [qw/phix human/],   
    target_product       => $product
  );
  is (join(q[, ], $entity->staging_files(q[cram])), 
    't/lane2/1_2#1.cram, t/lane2/1_2#1_phix.cram, t/lane2/1_2#1_human.cram',
    'paths for three cram files');  
};

1;
