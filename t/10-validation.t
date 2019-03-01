use strict;
use warnings;
use Test::More tests => 3;
use Test::Exception;
use Test::Warn;
use File::Path qw/ make_path /;
use File::Slurp qw/ write_file /;
use File::Copy;
use Log::Log4perl qw/ :levels /;;
use t::util;

use_ok ('npg_pipeline::validation');

my $util = t::util->new();
Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $WARN,
                          file   => join(q[/], $util->temp_directory(), 'logfile'),
                          utf8   => 1});

sub _create_test_runfolder {
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
  plan tests => 14;

  my $v = npg_pipeline::validation->new();
  isa_ok ($v, 'npg_pipeline::validation');

  for my $flag (qw/ignore_lims ignore_npg_status ignore_time_limit
                   ignore_autoqc ignore_irods remove_staging_tag/) {
    ok (!$v->$flag, "$flag is false by default");
  }
  ok ($v->use_cram, 'cram files are used by default');
  is ($v->file_extension, 'cram', 'default file extension is cram');
  is ($v->index_file_extension, 'crai', 'default index file extension is crai');
  is ($v->min_keep_days, 14, '14 days after qc complete data to ve retained');
  is ($v->lims_driver_type, 'samplesheet', 'default driver type is samplesheet');

  $v = npg_pipeline::validation->new(use_cram => 0);
  is ($v->file_extension, 'bam', 'file extension is bam');
  is ($v->index_file_extension, 'bai', 'index file extension is bai');
};

subtest 'lims and staging deletable' => sub {
  plan tests => 12;

  local $ENV{'NPG_CACHED_SAMPLESHEET_FILE'} = 't/data/samplesheet_8747.csv';

  my $rfh = $util->create_runfolder(
            $util->temp_directory(), {analysis_path => 'analysis'});
  copy 't/data/run_params/runParameters.hiseq.xml',
       join(q[/], $rfh->{'runfolder_path'}, 'runParameters.xml');
  copy 't/data/hiseq/16756_RunInfo.xml', 
       join(q[/], $rfh->{'runfolder_path'}, 'RunInfo.xml');

  my $archive_path = $rfh->{'archive_path'};
  my $ref = {
    id_run => 8747,
    runfolder_path => $rfh->{'runfolder_path'},
    analysis_path  => $rfh->{'analysis_path'},
    archive_path   => $archive_path
  };

  my $v = npg_pipeline::validation->new($ref);

  _create_test_runfolder($archive_path, $v->products->{'data_products'});

  is ($v->_lims_deletable, 1, 'deletable');
  #diag `find $archive_path`;

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

1;