use strict;
use warnings;
use Test::More tests => 5;
use Test::Exception;

use_ok('npg_pipeline::runfolder_scaffold');
use_ok('npg_tracking::illumina::runfolder');

package npg_test::runfolder_scaffold;
use Moose;
extends 'npg_tracking::illumina::runfolder';
with 'npg_pipeline::runfolder_scaffold'; 
sub positions {
  return (1 .. 8);
}
sub is_multiplexed_lane {
  my ($self,$num)=@_;
  return $num%2;
}
1;

package main;
use t::util;

my $util = t::util->new();
local $ENV{NPG_WEBSERVICE_CACHE_DIR} = 't/data';

{
  $util->create_analysis({skip_archive_dir => 1});
  my $rfs;
  lives_ok {
    $rfs = npg_test::runfolder_scaffold->new(
      run_folder     => q{123456_IL2_1234},
      runfolder_path => $util->analysis_runfolder_path(),
      is_indexed     => 1,      
    );
  } q{scaffolder created OK};

  lives_ok { $rfs->create_analysis_level() } q{no error scaffolding runfolder};
  lives_ok { $rfs->create_analysis_level() } q{no error scaffolding directories which already exists};
}

1;
