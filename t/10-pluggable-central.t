use strict;
use warnings;
use Test::More tests => 27;
use Test::Exception;
use Log::Log4perl qw(:levels);
use File::Copy qw(cp);
use File::Path qw(make_path remove_tree);

use npg_tracking::util::abs_path qw(abs_path);
use t::util;

my $util = t::util->new();
my $tdir = $util->temp_directory();
my @tools = map { "$tdir/$_" } qw/bamtofastq blat norm_fit/;
foreach my $tool (@tools) {
  open my $fh, '>', $tool or die 'cannot open file for writing';
  print $fh $tool or die 'cannot print';
  close $fh or warn 'failed to close file handle';
}
chmod 0755, @tools;
local $ENV{'PATH'} = join q[:], $tdir, $ENV{'PATH'};

my $product_config = q[t/data/release/config/archive_on/product_release.yml];

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $tdir, 'logfile'),
                          utf8   => 1});

my $central = q{npg_pipeline::pluggable::central};
use_ok($central);

my $runfolder_path = $util->analysis_runfolder_path();

{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_1234.csv];
  $util->create_analysis();
  my $pipeline;
  lives_ok {
    $pipeline = $central->new(
      runfolder_path => $runfolder_path,
    );
  } q{no croak creating new object};
  isa_ok($pipeline, $central);
}

{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_1234.csv];
  my $pb;
  lives_ok {
    $pb = $central->new(
      function_order => [qw(qc_qX_yield qc_insert_size)],
      runfolder_path => $runfolder_path,
    );
  } q{no croak on creation};
  $util->create_analysis();
  is(join(q[ ], @{$pb->function_order()}), 'qc_qX_yield qc_insert_size',
    'function_order set on creation');
}

{
  local $ENV{CLASSPATH} = undef;
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_1234.csv];
  my $pb;
  $util->create_analysis();
  cp 't/data/run_params/runParameters.hiseq.xml',
    join(q[/], $runfolder_path, 'runParameters.xml');

  $util->create_run_info();
  my $config_dir = 'data/config_files';
  my $init = {
      function_order   => [qw{qc_qX_yield qc_adapter update_warehouse qc_insert_size}],
      lanes            => [4],
      runfolder_path   => $runfolder_path,
      function_list => "$config_dir/function_list_central.json",
      id_flowcell_lims => 2015,
      no_bsub          => 1,
      repository       => 't/data/sequence',
      spider           => 0,
      no_sf_resource   => 1,
      product_conf_file_path => $product_config,
  };

  lives_ok { $pb = $central->new($init); } q{no croak on new creation};
  mkdir $pb->archive_path;
  lives_ok { $pb->main() } q{no croak running qc->main()};
}

{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_1234.csv];
  my $rf = join q[/], $tdir, 'myfolder';
  mkdir $rf;
  cp 't/data/run_params/runParameters.hiseq.xml',
    join(q[/], $rf, 'runParameters.xml');
  my $init = {
      id_run         => 1234,
      run_folder     => 'myfolder',
      runfolder_path => $rf,
      timestamp      => '22-May',
      spider         => 0,
      is_indexed     => 0,
      product_conf_file_path => $product_config,
  };
  my $pb = $central->new($init);
  is ($pb->intensity_path, "$rf/Data/Intensities", 'intensities path');
  is ($pb->basecall_path, "$rf/Data/Intensities/BaseCalls", 'basecalls path');
  throws_ok { $pb->prepare() }
    qr/does not exist, either bam_basecall_path or analysis_path should be given/,
    q{error scaffolding the run folder};

  make_path "$rf/Data/Intensities";
  $pb = $central->new($init);
  is ($pb->intensity_path, "$rf/Data/Intensities", 'intensities path');
  is ($pb->basecall_path, "$rf/Data/Intensities/BaseCalls", 'basecalls path');
  lives_ok { $pb->prepare() } 'prepare runs fine';
  my $expected_pb_cal = join q[/],$rf,q{Data/Intensities/BAM_basecalls_22-May};
  is ($pb->bam_basecall_path, $expected_pb_cal, 'bam basecall path is set');
  ok (-d $pb->bam_basecall_path, 'directory exists');
  my $expected_no_cal_path = join q[/],$pb->bam_basecall_path,'no_cal';
  is ($pb->recalibrated_path, $expected_no_cal_path, 'recalibrated path');
  ok (-d $pb->recalibrated_path, 'directory exists');
  is ($pb->analysis_path, $pb->bam_basecall_path, 'analysis path');

  $init->{'bam_basecall_path'} = $expected_pb_cal;
  $pb = $central->new($init);
  $pb->prepare();
  is ($pb->bam_basecall_path, $expected_pb_cal, 'bam basecall path is set');
  is ($pb->analysis_path, $expected_pb_cal, 'analysis path');

  delete $init->{'bam_basecall_path'};
  $init->{'analysis_path'} = $expected_pb_cal;
  $pb = $central->new($init);
  $pb->prepare();
  is ($pb->bam_basecall_path, $expected_pb_cal, 'bam basecall path is set');

  delete $init->{'analysis_path'};
  $init->{'archive_path'} = qq{$expected_no_cal_path/archive};
  $pb = $central->new($init);
  $pb->prepare();
  is ($pb->bam_basecall_path, $expected_pb_cal, 'bam basecall path is set');

  delete $init->{'runfolder_path'};
  delete $init->{'runfolder'};
  $pb = $central->new($init);
  throws_ok { $pb->prepare() }
    qr/Nothing looks like a run_folder in any given subpath/,
    'error since the given subpath does not exist';

  make_path $init->{'archive_path'};
  throws_ok { $pb->prepare() }
    qr/Nothing looks like a run_folder in any given subpath/,
    'error since Config does not exist';

  make_path "$rf/Config";
  lives_ok { $pb->prepare() } 'no error';
  is ($pb->bam_basecall_path, $expected_pb_cal, 'bam basecall path is set');
  is ($pb->runfolder_path, $rf, 'run folder path is set');
}

1;
