use strict;
use warnings;
use Test::More tests => 6;
use Test::Exception;
use t::util;
use Cwd qw/getcwd/;

local $ENV{PATH} = join q[:], q[t/bin], q[t/bin/software/solexa/bin], $ENV{PATH};
my $util = t::util->new();
my $conf_path = $util->conf_path();

$ENV{TEST_DIR} = $util->temp_directory();
$ENV{TEST_FS_RESOURCE} = q{nfs_12};


my $tmp_dir = $util->temp_directory();
use_ok('npg_pipeline::run::folder::link');

my $runfolder_path = $util->analysis_runfolder_path();

{
  my $rfl;

  lives_ok {
    $rfl = npg_pipeline::run::folder::link->new({
      analysis_type => q{full},
      run_folder => q{123456_IL2_1234},
      runfolder_path => $runfolder_path,
      conf_path => $conf_path,
      domain => q{test},
    });
  } q{no croak with all attributes provided };
  `mkdir -p $tmp_dir/nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/Bustard1.3.4_10-07-2009_auto/GERALD_09-07-2009_auto`;
  `touch $tmp_dir/nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/Bustard1.3.4_10-07-2009_auto/GERALD_09-07-2009_auto/touch_file`;
  lives_ok {
    $rfl->make_link();
    1;
  } q{no croak creating link - overriding single to auto};

  lives_ok {
    $rfl->make_link();
    1;
  } q{no croak creating link - no change since link already auto};

  `mv $tmp_dir/nfs/sf45/IL2/analysis $tmp_dir/nfs/sf45/IL2/outgoing`;
  $rfl->_set_folder(q{outgoing});
  lives_ok {
    $rfl->make_link();
    1;
  } q{no croak creating link - changed as folder route has changed};

  `rm -f $tmp_dir/nfs/sf45/IL2/outgoing/123456_IL2_1234/Latest_Summary`;
  `mkdir -p $tmp_dir/nfs/sf45/IL2/outgoing/123456_IL2_1234/Data/Intensities/Bustard_auto/GERALD_auto`;
  `touch $tmp_dir/nfs/sf45/IL2/outgoing/123456_IL2_1234/Data/Intensities/Bustard_auto/GERALD_auto/touch_file`;
  lives_ok {
    $rfl->make_link();
    1;
  } q{no croak creating link - changed as folder now using specified route};
}

1;
