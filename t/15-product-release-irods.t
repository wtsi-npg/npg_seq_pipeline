use strict;
use warnings;
use Test::More tests => 11;
use Test::Exception;


my $package = 'npg_pipeline::product::release::irods';
use_ok('npg_pipeline::product');
use_ok($package);

# There tests cover use of package-level methods only.

throws_ok {$package->irods_collection4run_rel()} qr/Run id should be given/,
  'error if run is is not given';
is ($package->irods_collection4run_rel(44567), '44567',
  'non NovaSeq run path');
is ($package->irods_collection4run_rel(44567, 0), '44567',
  'non NovaSeq run path'); 
is ($package->irods_collection4run_rel(44567, 1), 'illumina/runs/44/44567',
  'NovaSeq run path');

throws_ok {$package->irods_product_destination_collection_norf()}
  qr/Run collection iRODS path is required/,
  'error when no arguments are given';
throws_ok {$package->irods_product_destination_collection_norf('/seq/4567')}
  qr/Product object is required/,
  'error when the rpoduct object argument is not given';

my $product = npg_pipeline::product->new(rpt_list => '26219:1:3');
is ($package->irods_product_destination_collection_norf(
  '/seq/26219', $product), '/seq/26219',
  'path for a product for a non NovaSeq run');
is ($package->irods_product_destination_collection_norf(
  '/seq/26219', $product, 0), '/seq/26219',
  'path for a product for a non NovaSeq run');
is ($package->irods_product_destination_collection_norf(
  '/seq/illumina/runs/26/26219', $product, 1),
  '/seq/illumina/runs/26/26219/lane1/plex3',
  'path for a product for a NovaSeq run');

1;

