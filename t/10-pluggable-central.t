use strict;
use warnings;
use Test::More tests => 15;
use Test::Exception;
use Cwd qw(getcwd);
use Log::Log4perl qw(:levels);

use npg_tracking::util::abs_path qw(abs_path);
use t::util;

local $ENV{http_proxy} = 'http://wibble';
local $ENV{no_proxy} = q[];

my $util = t::util->new();
my $cwd = getcwd();
my $tdir = $util->temp_directory();

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $tdir, 'logfile'),
                          utf8   => 1});

local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];

my $central = q{npg_pipeline::pluggable::central};
use_ok($central);

my $runfolder_path = $util->analysis_runfolder_path();

{
  $util->set_staging_analysis_area();
  my $pipeline;
  lives_ok {
    $pipeline = $central->new(
      runfolder_path => $runfolder_path,
    );
  } q{no croak creating new object};
  isa_ok($pipeline, $central);
}

{
  my $pb;
  lives_ok {
    $pb = $central->new(
      function_order => [qw(qc_qX_yield qc_insert_size)],
      runfolder_path => $runfolder_path,
    );
  } q{no croak on creation};
  $util->set_staging_analysis_area({with_latest_summary => 1});
  is(join(q[ ], @{$pb->function_order()}), 'qc_qX_yield qc_insert_size',
    'function_order set on creation');
}

{
  local $ENV{CLASSPATH} = undef;
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];

  my $pb;
  $util->set_staging_analysis_area();
  my $init = {
      function_order => [qw{qc_qX_yield qc_adapter update_warehouse qc_insert_size}],
      lanes => [4],
      runfolder_path => $runfolder_path,
      no_bsub => 1,
      repository => 't/data/sequence',
      spider  => 0,
  };
 
  lives_ok { $pb = $central->new($init); } q{no croak on new creation};
  mkdir $pb->archive_path;
  mkdir $pb->qc_path;
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];
  lives_ok { $pb->main() } q{no croak running qc->main()};
}

my $rf = join q[/], $tdir, 'myfolder';
mkdir $rf;
{
  my $init = {
      id_run => 1234,
      run_folder => 'myfolder',
      runfolder_path => $rf,
      no_bsub => 1,
      timestamp => '22-May',
  };
  my $pb;
  lives_ok { $pb = $central->new($init); $pb->_set_paths() }
    q{no error on object creation and analysis paths set for a flattened runfolder};
  is ($pb->intensity_path, $rf, 'intensities path is set to runfolder');
  is ($pb->basecall_path, $rf, 'basecall path is set to runfolder');
  is ($pb->bam_basecall_path, join(q[/],$rf,q{BAM_basecalls_22-May}), 'bam basecall path is created');
  is ($pb->recalibrated_path, join(q[/],$pb->bam_basecall_path, 'no_cal'), 'recalibrated path set');
  my $status_path = $pb->status_files_path();
  is ($status_path, join(q[/],$rf,q{BAM_basecalls_22-May}, q{status}), 'status directory path');
  ok(-d $status_path, 'status directory created');
  ok(-d "$status_path/log", 'log directory for status jobs created');
}

1;
