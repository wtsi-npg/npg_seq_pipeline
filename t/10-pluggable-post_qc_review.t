use strict;
use warnings;
use Test::More tests => 2;
use Log::Log4perl qw(:levels);

use t::util;

my $util = t::util->new();
my $runfolder_path = $util->analysis_runfolder_path();
local $ENV{PATH} = join q[:], q[t/bin], $ENV{PATH};

my $temp = $util->temp_directory();

$ENV{TEST_DIR} = $temp;
$ENV{TEST_FS_RESOURCE} = q{nfs_12};

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $temp, 'logfile'),
                          utf8   => 1});

my $runfolfer_path = $util->analysis_runfolder_path;
$util->set_staging_analysis_area({with_latest_summary => 1});

use_ok('npg_pipeline::pluggable::post_qc_review');
my $p = npg_pipeline::pluggable::post_qc_review->new(
      runfolder_path      => $runfolder_path,
      no_irods_archival   => 1,
      no_warehouse_update => 1
  );
isa_ok($p, q{npg_pipeline::pluggable::post_qc_review});

1;
