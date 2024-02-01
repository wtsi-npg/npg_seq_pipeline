use strict;
use warnings;
use Test::More tests => 8;
use Test::Exception;
use Test::Warn;
use Test::Trap qw/ :warn /;
use File::Path qw/ make_path /;
use File::Slurp qw/ write_file /;
use File::Copy;
use Log::Log4perl qw/ :levels /;
use File::Temp qw/ tempdir /;
use Moose::Meta::Class;

use t::util;
use t::dbic_util;

use_ok ('npg_pipeline::validation');

my $util = t::util->new();
my $logfile = join q[/], $util->temp_directory(), 'logfile';
Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $WARN,
                          file   => $logfile,
                          utf8   => 1});

my $qc_schema = Moose::Meta::Class->create_anon_class(
                  roles => [qw/npg_testing::db/])->new_object()
                ->create_test_db(q[npg_qc::Schema]);
my $tracking_schema = t::dbic_util->new()
                                  ->test_schema('t/data/dbic_fixtures/');
$tracking_schema->resultset('Run')->create({
  id_run               => 8747,
  id_instrument_format => 10,
  id_instrument        => 67,
  actual_cycle_count   => 300,
  expected_cycle_count => 300,
  id_run_pair          => 0,
  is_paired            => 1,
  team                 => 'A'
});
for ((1 .. 8)) {
  $tracking_schema->resultset('RunLane')->create({id_run     => 8747,
                                                  position   => $_,
                                                  tile_count => 120,
                                                  tracks     => 4});
}

sub _create_test_runfolder_8747 {
  my $rfh = $util->create_runfolder(
            tempdir(CLEANUP => 1), {analysis_path => 'analysis'});
  copy 't/data/run_params/runParameters.hiseq.xml',
       join(q[/], $rfh->{'runfolder_path'}, 'runParameters.xml');
  copy 't/data/hiseq/16756_RunInfo.xml', 
       join(q[/], $rfh->{'runfolder_path'}, 'RunInfo.xml');
  return $rfh;
}

sub _populate_test_runfolder {
  my ($archive_path, $products) = @_;
  my @letters  = (q(a)..q(z));
  foreach my $p (@{$products}) {
    my $path = $p->path($archive_path);
    make_path($path);
    my @names = ($p->file_name);
    my $ti = $p->composition->get_component(0)->tag_index;
    if (!defined $ti || ($ti != 168)) {
      push @names, join(q[_], $names[0], 'phix');
    }
    for my $ext (qw/.cram .cram.crai/) {
      for my $file_name (@names) {
        my $content = join(q[], map {$letters[rand(26)]} (1 .. 30));
        write_file(join(q[/], $path, $file_name.$ext), $content);
      }
    }
  }
  return;
}

subtest 'create object' => sub {
  plan tests => 16;

  my $v = npg_pipeline::validation->new(
    qc_schema           => $qc_schema,
    npg_tracking_schema => $tracking_schema,
    min_keep_days       => 30,
    pp_files_number     => 4);
  isa_ok ($v, 'npg_pipeline::validation');

  for my $flag (qw/ignore_lims ignore_npg_status ignore_time_limit
                   ignore_autoqc ignore_irods remove_staging_tag/) {
    ok (!$v->$flag, "$flag is false by default");
  }
  ok ($v->use_cram, 'cram files are used by default');
  is ($v->file_extension, 'cram', 'default file extension is cram');
  is ($v->index_file_extension, 'crai', 'default index file extension is crai');
  is ($v->min_keep_days(), 30,
    'min_keep_days attribute value as set in the constructor');
  is ($v->lims_driver_type, 'samplesheet', 'default driver type is samplesheet');
  is ($v->pp_files_number(), 4, 'pp_files_number as set');

  $v = npg_pipeline::validation->new(use_cram => 0);
  is ($v->file_extension, 'bam', 'file extension is bam');
  is ($v->index_file_extension, 'bai', 'index file extension is bai');
  is ($v->pp_files_number(), 10000, 'default pp_files_number');
};

subtest 'deletion delay' => sub {
  plan tests => 9;

  local $ENV{'NPG_CACHED_SAMPLESHEET_FILE'} = 't/data/samplesheet_8747.csv';
  note 'Samples from three studies in the run data used in this test';

  my $rfh = _create_test_runfolder_8747();

  my $ref = {
    id_run              => 8747,
    runfolder_path      => $rfh->{'runfolder_path'},
    analysis_path       => $rfh->{'analysis_path'},
    archive_path        => $rfh->{'archive_path'},
    qc_schema           => $qc_schema,
    npg_tracking_schema => $tracking_schema,
    conf_path           => 't/data/release/config/archive_on',
  };

  my $v = npg_pipeline::validation->new($ref);
  throws_ok { $v->min_keep_days } qr/Current run status is undefined/,
    'error for a run with no associated status';
  # Assign current status to 'qc complete'.
  $v->tracking_run->update_run_status('qc complete');
  $v = npg_pipeline::validation->new($ref);
  is ($v->min_keep_days, 14,'no config - falling back on hardcoded defalt');

  $ref->{conf_path} = 't/data/release/config/archive_off';
  $v = npg_pipeline::validation->new($ref);
  is ($v->min_keep_days, 12, 'value from default product config');
  
  # Reassign current status to 'run cancelled'.
  sleep 1 and $v->tracking_run->update_run_status('run cancelled');
  $v = npg_pipeline::validation->new($ref);
  is ($v->min_keep_days, 14, 'default value for a cancelled run');
  # Reassign current status to 'data discarded'.
  sleep 1 and $v->tracking_run->update_run_status('data discarded');
  $v = npg_pipeline::validation->new($ref);
  is ($v->min_keep_days, 14, 'default value for a run with discarded data');
  # Reassign current status back to 'qc complete'.
  sleep 1 and $v->tracking_run->update_run_status('qc complete');

  $ref->{conf_path} = 't/data/release/config/notify_on';
  $v = npg_pipeline::validation->new($ref);
  is ($v->min_keep_days, 15,
    'from config for study 1713, which is longer than default config');

  $ref->{conf_path} = 't/data/release/config/notify_off';
  $v = npg_pipeline::validation->new($ref);
  is ($v->min_keep_days, 12,
    'from default config which is longer than in config for study 1713');

  $ref->{conf_path} = 't/data/release/config/bqsr_on_study_specific';
  $v = npg_pipeline::validation->new($ref);
  is ($v->min_keep_days, 14,
    'from hardcoded default which is longer than in config for study 1713');

  $ref->{conf_path} = 't/data/release/config/bqsr_off';
  $v = npg_pipeline::validation->new($ref);
  is ($v->min_keep_days, 3, 'largest number across all three configured studies');
};

subtest 'lims and staging deletable' => sub {
  plan tests => 12;

  local $ENV{'NPG_CACHED_SAMPLESHEET_FILE'} = 't/data/samplesheet_8747.csv';

  my $rfh = _create_test_runfolder_8747();

  my $archive_path = $rfh->{'archive_path'};
  my $ref = {
    id_run => 8747,
    runfolder_path      => $rfh->{'runfolder_path'},
    analysis_path       => $rfh->{'analysis_path'},
    archive_path        => $archive_path,
    qc_schema           => $qc_schema,
    npg_tracking_schema => $tracking_schema
  };

  my $v = npg_pipeline::validation->new($ref);

  _populate_test_runfolder($archive_path, $v->products->{'data_products'});

  is ($v->_lims_deletable, 1, 'deletable');

  # Remove on of the cram files
  my $file = $archive_path . '/lane6/plex0/8747_6#0_phix.cram';
  my $moved = $file . '_moved';
  rename $file, $moved or die "Failed to rename $file to $moved";

  $v = npg_pipeline::validation->new($ref);
  my $deletable;
  warning_like { $deletable = $v->_lims_deletable }
    qr/File $file is missing/, 'warning - file missing';
  is ($deletable, 0, 'not deletable, file missing');
  # Restore the file
  rename $moved, $file or die "Failed to rename $moved to $file";

  # Create unexpected cram file
  my $extra = $file . '23.cram';
  copy $file, $extra or die "Faile to copy $file to $extra";
  $v = npg_pipeline::validation->new($ref);
  ok ($v->_lims_deletable, 'lims deletable');
  warning_like { $deletable = $v->_staging_deletable }
    qr/Staging file $extra is not expected/, 'warning - unexpected cram file';
  is ($deletable, 0, 'not deletable, unexpected cram file');

  # Make it unexpected crai file
  my $extra_i = $extra . '.crai';
  move $extra, $extra_i or die "failed to move $extra to $extra_i";
  $v = npg_pipeline::validation->new($ref);
  ok ($v->_lims_deletable, 'lims deletable');
  warning_like { $deletable = $v->_staging_deletable }
    qr/Staging index file $extra_i is not expected/, 'warning - unexpected index file';
  is ($deletable, 0, 'not deletable, unexpected index file');
  unlink $extra_i or die "Failed to delete $extra_i";
  
  # Remove on of index files
  $file = $archive_path . '/lane6/plex0/8747_6#0_phix.cram.crai';
  $moved = $file . '_moved';
  rename $file, $moved or die "Failed to rename $file to $moved";
  $v = npg_pipeline::validation->new($ref);
  ok ($v->_lims_deletable, 'lims deletable');
  $file =~ s/\.crai\Z//;
  warning_like { $deletable = $v->_staging_deletable }
    qr/Staging index file is missing for $file/, 'warning - missing index file';
  is ($deletable, 0, 'not deletable, unexpected index file'); 
};

subtest 'xarchive validation' => sub {
  plan tests => 8;

  local $ENV{'NPG_CACHED_SAMPLESHEET_FILE'} = 't/data/samplesheet_8747.csv';

  my $rfh = _create_test_runfolder_8747();
  my $ref = {
    id_run => 8747,
    runfolder_path      => $rfh->{'runfolder_path'},
    analysis_path       => $rfh->{'analysis_path'},
    archive_path        => $rfh->{'archive_path'},
    qc_schema           => $qc_schema,
    npg_tracking_schema => $tracking_schema
  };

  my $v = npg_pipeline::validation->new($ref);
  ok (!@{$v->eligible_product_entities},
    'no eligible products prior to running validation for file archives');
  my $deletable = 1;
  $deletable = trap { $v->_file_archive_deletable() };
  like ( $trap->stderr, qr/Product not available in any of file archives/,
    'warnings about data absent from archive'); 
  ok (!$deletable, 'not deletable prior to running validation for file archives');

  $v = npg_pipeline::validation->new(%{$ref}, ignore_irods => 1);
  ok ($v->_irods_seq_deletable(), 'no irods archival - irods deletable');
  my $num_products = scalar @{$v->product_entities};
  ok (scalar @{$v->eligible_product_entities} == $num_products,
    'number pf products in eligible products');
  is ($v->_file_archive_deletable(), 1, 'is deletable');

  while (scalar @{$v->eligible_product_entities} > ($num_products - 1)) {
    pop @{$v->eligible_product_entities};
  }
  $deletable = 1;
  warning_like { $deletable = $v->_file_archive_deletable() }
    qr/Product not available in any of file archives/,
    'warnings about one product missing from archives';
  ok (!$deletable, 'not deletable');
};

subtest 'flagged as not deletable' => sub {
  plan tests => 2;

  my $rfh = _create_test_runfolder_8747();
  my $ref = {
    id_run => 8747,
    runfolder_path      => $rfh->{'runfolder_path'},
    analysis_path       => $rfh->{'analysis_path'},
    archive_path        => $rfh->{'archive_path'},
    qc_schema           => $qc_schema,
    npg_tracking_schema => $tracking_schema
  };
  mkdir join q[/], $rfh->{'runfolder_path'}, 'npg_do_not_delete';

  my $v = npg_pipeline::validation->new($ref);
  ok ($v->_flagged_as_not_deletable(),
    'detected that the run folder is flagged as not deletable');
  ok (!$v->run(), 'run is not deletable');
};

subtest 'per product flag and iRODS locations' => sub {
  plan tests => 10;

  my $rfh = _create_test_runfolder_8747();
  my $ref = {
    id_run => 8747,
    runfolder_path      => $rfh->{'runfolder_path'},
    analysis_path       => $rfh->{'analysis_path'},
    archive_path        => $rfh->{'archive_path'},
    qc_schema           => $qc_schema,
    npg_tracking_schema => $tracking_schema
  };

  my $v = npg_pipeline::validation->new($ref);
  ok ($v->per_product_staging_archive,
    'per product archive is true by default');
  ok (!$v->per_product_archive, 'computed per product flag for iRODS is false');
  is ($v->irods_destination_collection, '/seq/8747', 'flat iRODS collection');

  $ref->{per_product_staging_archive} = 0;
  $v = npg_pipeline::validation->new($ref);
  ok (!$v->per_product_archive, 'computed per product flag for iRODS');
  is ($v->irods_destination_collection, '/seq/8747', 'iRODS collection');

  delete $ref->{per_product_staging_archive};
  $ref->{per_product_archive} = 1;
  $v = npg_pipeline::validation->new($ref);
  ok ($v->per_product_staging_archive, 'per product archive is true');
  ok ($v->per_product_archive, 'per product iRODS is true');
  is ($v->irods_destination_collection, '/seq/illumina/runs/8/8747',
    'iRODS collection');

  $ref->{irods_destination_collection} = '/seq-dev/8747';
  $v = npg_pipeline::validation->new($ref);
  ok ($v->per_product_archive, 'per product iRODS is true');
  is ($v->irods_destination_collection, '/seq-dev/8747',
    'iRODS collection as set');
};

subtest 'presence of onboard analysis results' => sub {
  plan tests => 2;

  my $rf_path = tempdir(CLEANUP => 1);

  copy('t/data/run_params/RunParameters.novaseqx.xml',
    "$rf_path/RunParameters.xml");
  my $v = npg_pipeline::validation->new(
    id_run => 47539,
    runfolder_path => $rf_path
  );
  ok (!$v->_irods_seq_onboard_deletable(),
    'NovaSeqX with onboard analysis - not deletable');

  copy('t/data/run_params/runParameters.novaseq.xml',
    "$rf_path/RunParameters.xml"); 
  $v = npg_pipeline::validation->new(
    id_run => 9999,
    runfolder_path => $rf_path
  );
  ok ($v->_irods_seq_onboard_deletable(),
    'NovaSeq - no onboard analysis - deletable');
};

1;
