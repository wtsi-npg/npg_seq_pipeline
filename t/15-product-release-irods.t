use strict;
use warnings;
use Test::More tests => 4;
use Test::Exception;
use Moose::Meta::Class;

use st::api::lims;

my $package = 'npg_pipeline::product::release::irods';
use_ok('npg_pipeline::product');
use_ok($package);

subtest 'iRODS collection path' => sub {
  plan tests => 9;
  # These tests cover use of package-level methods only.

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
};

subtest 'publishing to iRODS pp archive' => sub {
  plan tests => 11;

  my $path = 't/data/novaseq/200709_A00948_0157_AHM2J2DRXX' .
             '/Data/Intensities/BAM_basecalls_20200710-105415';
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = join q[/], $path,
        'metadata_cache_34576/samplesheet_34576.csv';

  my $product = npg_pipeline::product->new(rpt_list => '34576:1:3',
    lims => st::api::lims->new(id_run => 34576, position => 1, tag_index =>3));
  my $class = Moose::Meta::Class->create_anon_class(roles => [$package]);

  my $obj = $class->new_object(product_conf_file_path =>
    q[t/data/release/config/pp_archival/product_release.yml]);
  ok ($obj->is_for_pp_irods_release($product),
    'this product should be in the pp iRODS archive');
  my $filters = $obj->glob_filters4publisher($product);
  ok ($filters, 'filters are returned');
  is (keys %{$filters}, 2,'two types of filters');
  is (@{$filters->{include}}, 4, '5 include filters');
  is (@{$filters->{exclude}}, 1, '1 exclude filter');

  $obj = $class->new_object(product_conf_file_path =>
    q[t/data/release/config/pp_archival/product_release_no_include.yml]);
  throws_ok {$obj->glob_filters4publisher($product)}
    qr/No 'include' filter/, 'include filter should be present';

  $obj = $class->new_object(product_conf_file_path =>
    q[t/data/release/config/pp_archival/product_release_no_filters.yml]);
  ok ($obj->is_for_pp_irods_release($product),
    'this product should be in the pp iRODS archive');
  lives_ok {$filters = $obj->glob_filters4publisher($product)}
    'no error when filters section is not present';
  is ($filters, undef, 'undefined value is returned');
  
  $obj = $class->new_object(product_conf_file_path =>
    q[t/data/release/config/pp_archival/product_release_no_array.yml]);
  throws_ok {$obj->glob_filters4publisher($product)}
    qr/Malformed configuration for filter \'include\'/,
    'each type of filters should be a list';

  $obj = $class->new_object(product_conf_file_path =>
    q[t/data/release/config/archive_on/product_release.yml]);  
  ok (!$obj->is_for_pp_irods_release($product),
    'this product should not be in the pp iRODS archive');
};

1;

