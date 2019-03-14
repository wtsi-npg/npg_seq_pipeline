use strict;
use warnings;
use Test::More tests => 4;
use Log::Log4perl qw/ :levels/;
use File::Copy;
use File::Slurp qw/read_file/;

use st::api::lims;
use t::util;

use_ok('npg_pipeline::product');
use_ok('npg_pipeline::validation::entity');
use_ok ('npg_pipeline::validation::s3');

my $util = t::util->new();
my $dir  = $util->temp_directory();
Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $WARN,
                          file   => join(q[/], $dir, 'logfile'),
                          utf8   => 1});
my $logger = Log::Log4perl->get_logger(q[]);

subtest 'is archivable' => sub {
  plan tests => 3;

  my $config_dir = join q[/], $dir, 'config';
  mkdir $config_dir or die "Failed to create $config_dir";
  copy 't/data/release/config/archive_on/product_release.yml', $config_dir;
  copy 'data/config_files/general_values.ini', $config_dir;

  my $pconfig_content = read_file join(q[/], $config_dir, 'product_release.yml');
  my $study_id = 3573;
  ok ($pconfig_content !~ /study_id: \"$study_id\"/xms,
    'no product release config for this run study');

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q{t/data/miseq/samplesheet_16850.csv};

  my @ets = map {
    npg_pipeline::validation::entity->new(
      staging_archive_root => q[t],
      target_product => npg_pipeline::product->new(
        rpt_list => $_,
        lims     => st::api::lims->new(rpt_list => $_)
      )
    )
  } map { qq[16850:1:$_] } (0 .. 2);

  my $v = npg_pipeline::validation::s3->new(
    product_entities  => \@ets,
    logger            => $logger,
    file_extension    => 'cram',
    conf_path         => $config_dir,
  );
  ok ($v->fully_archived, 'nothing in a run is archivable to s3 - archived');

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
    q{t/data/novaseq/180709_A00538_0010_BH3FCMDRXX/Data/Intensities/} .
    q{BAM_basecalls_20180805-013153/metadata_cache_26291/samplesheet_26291.csv};

  @ets = map {
    npg_pipeline::validation::entity->new(
      staging_archive_root => q[t],
      target_product => npg_pipeline::product->new(
        rpt_list => $_,
        lims     => st::api::lims->new(rpt_list => $_)
      )
    )
  } map { qq[26291:1:$_;26291:2:$_] } (0 .. 12,888);  

  $v = npg_pipeline::validation::s3->new(
    product_entities  => \@ets,
    logger            => $logger,
    file_extension    => 'cram',
    conf_path         => $config_dir,
  );
  ok (!$v->fully_archived, 'some plexes are archivable to s3 - not archived');
};

1;