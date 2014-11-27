use strict;
use warnings;
use English qw{-no_match_vars};
use Test::More tests => 15;
use Test::Exception;
use t::util;
use Cwd qw/getcwd/;

local $ENV{PATH} = join q[:], q[t/bin], q[t/bin/software/solexa/bin], $ENV{PATH};

my $util = t::util->new({});
my $conf_path = $util->conf_path();

$ENV{TEST_DIR} = $util->temp_directory();

my $tmp_dir = $util->temp_directory();

use_ok('npg_pipeline::run::folder::move');

{
  my $rfm;
  my $run_folder = q{123456_IL2_1234};
  my $run_name   = q{IL2_1234};
  my $runfolder_path = $util->analysis_runfolder_path();
  lives_ok {
    $rfm = npg_pipeline::run::folder::move->new( {
      run_folder => $run_folder,
      conf_path => $conf_path,
      domain => q{test},
      runfolder_path => $runfolder_path,
      id_run => 1234,
    } );
  } q{no croak as run folder provided};

  my @path_comp = split q[/],$runfolder_path;
  pop @path_comp; 
  my $in_dir = join q[/], @path_comp;
  is($rfm->get_instrument_dir( $runfolder_path ), $in_dir, q{new cache ok});

  my @dirs = split m{/}xms, $in_dir;
  shift @dirs;
  `rm -rf /$dirs[0]/$dirs[1]/$dirs[2]`;
  `mkdir -p /$dirs[0]/$dirs[1]/$dirs[2]/$dirs[3]/$dirs[4]/outgoing`;
  `mkdir -p $runfolder_path`;

  lives_ok {
    $rfm->move_runfolder();
  } q{no croak moving folder};

  throws_ok {
    $rfm->move_runfolder();
  } qr{Failed[ ]to[ ]find[ ]$runfolder_path[ ]to[ ]move}, q{croaked as no folder in analysis to be found to move};

  `mkdir -p $runfolder_path`;

  throws_ok {
    $rfm->move_runfolder();
  } qr{/tmp/.*?/nfs/sf45/IL2/outgoing/123456_IL2_1234[ ]already[ ]exists}, q{croaked as folder already exists in outgoing};

  `rm -rf /$dirs[0]/$dirs[1]`;
}

{
  my $rfm;
  my $run_folder = q{123456_IL2_1234};
  my $folder     = q{outgoing};
  lives_ok {
    $rfm = npg_pipeline::run::folder::move->new({
      run_folder => $run_folder,
      folder     => $folder,
      timestamp  => q{20090709-123456},
      verbose    => 1,
      conf_path => $conf_path,
      domain => q{test},
      runfolder_path => $util->analysis_runfolder_path(),
      id_run => 1234,
    });
  } q{folder attribute populated on new ok};
  my $arg_refs = {
    required_job_completion => q{-w'done(123) && done(321)'},
  };

  my $bsub_command = $util->drop_temp_part_from_paths( $rfm->_generate_bsub_command($arg_refs) );
  my $expected_cmd = q{bsub -w'done(123) && done(321)' -J move_run_folder_1234_123456_IL2_1234_to_outgoing -q small -o /nfs/sf45/IL2/outgoing/log/move_run_folder_1234_123456_IL2_1234_to_outgoing_20090709-123456.out 'move_run_folder --folder outgoing --run_folder 123456_IL2_1234 --runfolder_path /nfs/sf45/IL2/analysis/123456_IL2_1234'};
  is($bsub_command, $expected_cmd, q{bsub command ok when moving to outgoing dir to be used});

  `mkdir -p $tmp_dir/nfs/sf45/IL2/analysis`;

  $rfm->_set_folder(q{analysis});
  $bsub_command = $util->drop_temp_part_from_paths( $rfm->_generate_bsub_command($arg_refs) );
  $expected_cmd = q{bsub -w'done(123) && done(321)' -J move_run_folder_1234_123456_IL2_1234_to_analysis -q small -o /nfs/sf45/IL2/analysis/log/move_run_folder_1234_123456_IL2_1234_to_analysis_20090709-123456.out 'move_run_folder --folder analysis --run_folder 123456_IL2_1234 --runfolder_path /nfs/sf45/IL2/analysis/123456_IL2_1234'};
  is($bsub_command, $expected_cmd, q{bsub command ok when moving to analysis dir to be used});

  lives_ok { $rfm->submit_move_run_folder($arg_refs) } q{job id returned ok from $rfm->submit_move_run_folder()};
  `rm -rf $tmp_dir/nfs/sf45`;
}

{
  my $rfm;
  my $run_folder = q{123456_IL2_2341};
  my $run_name   = q{IL2_2341};
  my $rf_path = $util->analysis_runfolder_path();
  $rf_path =~ s/1234\z/2341/xms;

  lives_ok {
    $rfm = npg_pipeline::run::folder::move->new({
      run_folder => $run_folder,
      conf_path => $conf_path,
      domain => q{test},
      runfolder_path => $rf_path,
    });
  } q{no croak as run folder provided};

  my $in_dir;
  lives_ok {
    $in_dir = $rfm->get_instrument_dir($rf_path);
  } q{no croak obtaining instrument_dir};

  my @dirs = split m{/}xms, $in_dir;
  shift @dirs;
  `rm -rf /$dirs[0]/$dirs[1]/$dirs[2]/$dirs[3]`;
  `mkdir -p /$dirs[0]/$dirs[1]/$dirs[2]/$dirs[3]/$dirs[4]/analysis/$run_folder; mkdir /$dirs[0]/$dirs[1]/$dirs[2]/$dirs[3]/$dirs[4]/analysis/123456_IL2_3412; mkdir /$dirs[0]/$dirs[1]/$dirs[2]/$dirs[3]/$dirs[4]/outgoing; `;

  lives_ok {
    $rfm->move_runfolder();
  } q{no croak moving folders};
}

{
  my $run_folder = q{123456_IL2_2341};
  my $run_name   = q{IL2_2341};
  my $rf_path    = $util->analysis_runfolder_path();
  $rf_path =~ s{analysis/.*}{analysis/$run_folder}xms;
  my $rfm = npg_pipeline::run::folder::move->new({
      run_folder => $run_folder,
      conf_path => $conf_path,
      domain => q{test},
      runfolder_path => $rf_path,
    });

  my $in_dir = $rfm->get_instrument_dir($rf_path);
  my @dirs = split m{/}xms, $in_dir;
  shift @dirs;
  `rm -rf $rf_path`;
  `mkdir -p $rf_path/Data/Intensities/Bustard_RTA/GERALD_RTA`;
  $rf_path =~ s{analysis/.*\z}{outgoing}xms;
  `rm -rf $rf_path`;
  `mkdir $rf_path`;
  $rf_path =~ s{outgoing}{analysis/$run_folder}xms;
  my $script_path = join q[/], getcwd, q[bin], q[move_run_folder];
  lives_ok { qx[$script_path --folder=outgoing --run_folder=$run_folder --runfolder_path=$rf_path]; } q{no croak running move_run_folder};
  ok(!$CHILD_ERROR, qq{Error code ok : $CHILD_ERROR});
}
1;
