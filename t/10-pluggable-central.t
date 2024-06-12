use strict;
use warnings;
use Test::More tests => 5;
use Test::Exception;
use Log::Log4perl qw(:levels);
use File::Copy qw(cp);
use File::Path qw(make_path);
use File::Temp qw(tempdir);
use File::Slurp qw(read_file write_file);
use JSON;

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
local $ENV{'HOME'} = 't';

my $product_config = q[t/data/release/config/archive_on/product_release.yml];

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $tdir, 'logfile'),
                          utf8   => 1});

my $test_data_dir_47995 = 't/data/novaseqx/20231017_LH00210_0012_B22FCNFLT3';
sub _setup_runfolder_47995 {
  my $tmp_dir = tempdir(CLEANUP => 1);
  my @dirs = split q[/], $test_data_dir_47995;
  my $rf_name = pop @dirs;
  my $rf_info = $util->create_runfolder($tmp_dir, {'runfolder_name' => $rf_name});
  my $rf = $rf_info->{'runfolder_path'};
  for my $file (qw(RunInfo.xml RunParameters.xml)) {
    if (cp("$test_data_dir_47995/$file", "$rf/$file") == 0) {
      die "Failed to copy $file";
    }
  }
  return $rf_info;
}

my $central = q{npg_pipeline::pluggable::central};
use_ok($central);

subtest 'test object creation' => sub {
  plan tests => 4;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = "$test_data_dir_47995/samplesheet_47995.csv";
  my $pipeline;
  lives_ok {
    $pipeline = $central->new(
      runfolder_path => $tdir,
    );
  } q{no error creating new object};
  isa_ok($pipeline, $central);

  lives_ok {
    $pipeline = $central->new(
      function_order => [qw(qc_qX_yield qc_insert_size)],
      runfolder_path => $tdir,
    );
  } q{no error on creation};
  is(join(q[ ], @{$pipeline->function_order()}), 'qc_qX_yield qc_insert_size',
    'function_order set on creation');
};

subtest 'execute main() with a merge' => sub {
  plan tests => 7;

  local $ENV{CLASSPATH} = undef;
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = "$test_data_dir_47995/samplesheet_47995.csv";
  my $rf_info = _setup_runfolder_47995();
  my $config_dir = 'data/config_files';

  my $pb;
  lives_ok { $pb = $central->new(
    id_run           => 47995,
    function_order   => [qw{qc_qX_yield qc_adapter update_ml_warehouse qc_insert_size}],
    lanes            => [3,4],
    run_folder       => $rf_info->{'runfolder_name'},
    runfolder_path   => $rf_info->{'runfolder_path'},
    function_list    => "$config_dir/function_list_central.json",
    id_flowcell_lims => 17089,
    no_bsub          => 1,
    repository       => 't/data/sequence',
    spider           => 0,
    product_conf_file_path => $product_config,
  ); } q{no croak on new creation};

  lives_ok { $pb->main() } q{no croak running qc->main()};

  my $rf = $rf_info->{'runfolder_path'};
  my %dirs =
    map  { $_ => 1 }
    map  { /(lane.+\z)/ }
    grep { -d }
    glob "$rf/Data/Intensities/BAM_basecalls_*/no_cal/archive/lane*";
  is (scalar keys %dirs, 3, 'three directories for lanes 3 and 4 are created');
  # Presence of lane3-4 dir indicates that data from lanes 3 and 4 will be merged.
  for my $name (qw(lane3 lane4 lane3-4)) {
    ok (exists $dirs{$name}, "directory '$name' exists");
  }
  my @files = grep { -f }
    glob "$rf/Data/Intensities/BAM_basecalls_*/metadata_cache_47995/*.json";
  is (@files, 0, 'No JSON files in the metadata cache directory');
};

subtest 'execute main() with merge supressed' => sub {
  plan tests => 13;

  local $ENV{CLASSPATH} = undef;
  my $samplesheet = "$test_data_dir_47995/samplesheet_47995.csv";
  my $rf_info = _setup_runfolder_47995();
  my $rf = $rf_info->{'runfolder_path'};
  my $config_dir = 'data/config_files';

  my $init = {
    id_run           => 47995,
    function_order   => [qw{qc_qX_yield qc_adapter update_ml_warehouse qc_insert_size}],
    lanes            => [3,4],
    process_separately_lanes => [4,3],
    run_folder       => $rf_info->{'runfolder_name'},
    runfolder_path   => $rf,
    function_list    => "$config_dir/function_list_central.json",
    id_flowcell_lims => 17089,
    no_bsub          => 1,
    repository       => 't/data/sequence',
    spider           => 0,
    product_conf_file_path => $product_config, 
  };
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = $samplesheet;
  my $pb = $central->new($init);
  lives_ok { $pb->main() } 'no error running qc->main()';
  my $bam_basecall_path = $pb->bam_basecall_path();

  my %dirs =
    map  { $_ => 1 }
    map  { /(lane.+\z)/ }
    grep { -d }
    glob "$rf/Data/Intensities/BAM_basecalls_*/no_cal/archive/lane*";
  is (scalar keys %dirs, 2, 'two directories for lanes 3 and 4 are created');
  # Absence of lane3-4 dir indicates that data from lanes 3 and 4 will
  # not be merged.
  for my $name (qw(lane3 lane4)) {
    ok (exists $dirs{$name}, "directory '$name' exists");
  }

  my $file_with_cache = "$bam_basecall_path/metadata_cache_47995/analysis_options.json";
  ok (-f $file_with_cache, 'A file with cached no-merge options exists');
  is_deeply (decode_json(read_file($file_with_cache))->{'process_separately_lanes'},
    [3,4], 'no-merge options are correctly cached');

  # Run once more. Reuse bam_basecalls directory. Expect no change.
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = $samplesheet;
  $init->{'bam_basecall_path'} = $bam_basecall_path;
  $pb = $central->new($init);
  lives_ok { $pb->main() } 'no error running qc->main() with the same options';
  ok (-f $file_with_cache, 'A file with cached no-merge options is retained');

  # Run once more with different lanes not to merge.
  my $error = 'Lane list from process_separately_lanes attribute is ' .
              'inconsistent with cached value';
  $init->{'process_separately_lanes'} = [8,7];
  $pb = $central->new($init);
  throws_ok { $pb->main() } qr/$error/,
    'error running qc->main() with different no-merge options';

  # The file exists, but the no-merge option is not captured;
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = $samplesheet;
  $init->{'process_separately_lanes'} = [4,3];
  write_file($file_with_cache,
    encode_json({'some option' => 'some option value'}));
  $pb = $central->new($init);
  lives_ok { $pb->main() } 'no error running qc->main()';
  is_deeply (decode_json(read_file($file_with_cache)),
    {'some option' => 'some option value', 'process_separately_lanes' => [3,4]},
    'the no-merged option has been added to the file'
  );

  # The file exists, the no-merge option is captured as an empty list.
  write_file($file_with_cache,
    encode_json({'option' => 'value', 'process_separately_lanes' => []}));
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = $samplesheet;
  $init->{'process_separately_lanes'} = [3,4];
  $pb = $central->new($init);
  lives_ok { $pb->main() } 'no error running qc->main()';
  is_deeply (decode_json(read_file($file_with_cache)),
    {'option' => 'value', 'process_separately_lanes' => [3,4]},
    'the no-merged option has been added to the file'
  );
};

subtest 'execute prepare()' => sub {
  plan tests => 12;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = "$test_data_dir_47995/samplesheet_47995.csv";
  my $rf_info = _setup_runfolder_47995();
  my $rf = $rf_info->{'runfolder_path'};
  my $init = {
      id_run         => 47995,
      run_folder     => $rf_info->{'runfolder_name'},
      runfolder_path => $rf,
      timestamp      => '22-May',
      spider         => 0,
      is_indexed     => 0,
      product_conf_file_path => $product_config,
  };
  my $pb = $central->new($init);
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
};

1;
