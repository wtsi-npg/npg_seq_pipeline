use strict;
use warnings;
use Test::More tests => 5;
use Test::Exception;
use Moose::Meta::Class;
use File::Copy;

use st::api::lims;
use t::util;

my $package = 'npg_pipeline::product::release::irods';
use_ok('npg_pipeline::product');
use_ok($package);

subtest 'iRODS collection paths, package-level methods' => sub {
  plan tests => 10;

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

  is ($package->irods_pp_root_collection(), q[illumina/pp/runs],
    'pp relative root collection');
};

subtest 'destination collection - instance methods and attributes' => sub {
  plan tests => 10;
  
  my $class = Moose::Meta::Class->create_anon_class(
    roles => [$package], superclasses => ['npg_pipeline::base']);

  # Tracking db handle is unset explicitly to preven access
  # to the database, which otherwise might be made.

  my $obj = $class->new_object(
    id_run => 26219,
    per_product_archive => 1,
    npg_tracking_schema => undef
  );
  is ($obj->irods_destination_collection(),
    q[/seq/illumina/runs/26/26219], 'per-product archive');
  $obj = $class->new_object(
    id_run => 26219,
    runfolder_path => 't/data/novaseq/180709_A00538_0010_BH3FCMDRXX',
    npg_tracking_schema => undef
  );
  is ($obj->irods_destination_collection(),
    q[/seq/illumina/runs/26/26219], 'per-product archive');
  ok ($obj->per_product_archive(),
    'per-product archive flag is set correctly'); 

  $obj = $class->new_object(
    id_run => 26219,
    per_product_archive => 0,
    npg_tracking_schema => undef
  );
  is ($obj->irods_destination_collection(),
    q[/seq/26219], 'flat run-level archive');

  my $util = t::util->new();
  my $paths = $util->create_runfolder(undef,
    {runfolder_name => '220128_HX9_43416_A_HHCCCCCX2'});
  my $path = $paths->{runfolder_path};
  copy 't/data/hiseqx/43416_runParameters.xml', $path .q[/runParameters.xml];
  $obj = $class->new_object(
    id_run => 43416,
    runfolder_path => $path,
    npg_tracking_schema => undef
  );
  is ($obj->irods_destination_collection(),
    q[/seq/43416], 'flat run-level archive');
  ok (!$obj->per_product_archive(),
    'per-product archive flag is set correctly');

  $obj = $class->new_object(
    id_run => 43416,
    per_product_archive => 1,
    runfolder_path => $path,
    npg_tracking_schema => undef
  );
  is ($obj->irods_destination_collection(),
    q[/seq/illumina/runs/43/43416],
    'per-product archive flag takes precedence over the instrument type');
   
  my $pp_root = $obj->irods_pp_root_collection();
  is ($pp_root, q[illumina/pp/runs], 'pp relative root collection');

  $obj = $class->new_object(
    id_run => 26219,
    per_product_archive => 1,
    irods_root_collection_ns => $pp_root,  
    npg_tracking_schema => undef
  );
  is ($obj->irods_destination_collection(),
    q[/seq/illumina/pp/runs/26/26219], 'per-product archive for pp data'); 

  $obj = $class->new_object(
    id_run => 43416,
    per_product_archive => 1,
    runfolder_path => $path,
    irods_root_collection_ns => $pp_root,
    npg_tracking_schema => undef
  );
  is ($obj->irods_destination_collection(),
    q[/seq/illumina/pp/runs/43/43416],
    'per-product archive flag takes precedenceover the instrument type');
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

