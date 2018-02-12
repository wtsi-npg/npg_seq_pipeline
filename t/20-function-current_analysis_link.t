use strict;
use warnings;
use Test::More tests => 20;
use Test::Exception;
use File::Path qw(make_path);
use t::util;

my $util = t::util->new();
my $tmp_dir = $util->temp_directory();

use_ok('npg_pipeline::function::current_analysis_link');

my $runfolder_path = $util->analysis_runfolder_path();
my $link_to = 'Data/Intensities/BAM_basecalls_20150608-091427/no_cal';
my $recalibrated_path = join q[/], $runfolder_path, $link_to;
make_path($recalibrated_path);

{
  my $rfl;
  my $link = "$runfolder_path/Latest_Summary";
  ok(!-e $link, 'link does not exist - test prerequisite');

  $rfl = npg_pipeline::function::current_analysis_link->new(
      run_folder        => q{123456_IL2_1234},
      runfolder_path    => $runfolder_path,
      recalibrated_path => $recalibrated_path,
      no_summary_link   => 1
  );
  ok(!$rfl->submit_create_link(), 'jobs ids are not returned');

  $rfl = npg_pipeline::function::current_analysis_link->new(
      run_folder        => q{123456_IL2_1234},
      runfolder_path    => $runfolder_path,
      recalibrated_path => $recalibrated_path,
      local             => 1
  );
  ok($rfl->no_summary_link, 'summary link switched off');
  ok(!$rfl->submit_create_link(), 'jobs ids are not returned');

  $rfl = npg_pipeline::function::current_analysis_link->new(
      run_folder        => q{123456_IL2_1234},
      runfolder_path    => $runfolder_path,
      recalibrated_path => $recalibrated_path,
  );
  ok (!$rfl->no_summary_link, 'no_summary_link flag false by default');
  ok (!$rfl->local, 'local flag false by default');

  $rfl = npg_pipeline::function::current_analysis_link->new(
      run_folder        => q{123456_IL2_1234},
      runfolder_path    => $runfolder_path,
      recalibrated_path => $recalibrated_path,
      no_bsub           => 1,
  );
  ok ($rfl->no_summary_link, 'no_summary_link flag is set');
  ok ($rfl->local, 'local flag is set');

  lives_ok { $rfl->make_link(); } q{no croak creating link};
  ok(-l $link, 'link exists');
  is(readlink $link, $link_to, 'correct link target');
  lives_ok { $rfl->make_link(); } q{no croak creating link when it already exists};
  ok(-l $link, 'link exists');

  rename "$tmp_dir/nfs/sf45/IL2/analysis", "$tmp_dir/nfs/sf45/IL2/outgoing";
  $link              =~ s/analysis/outgoing/;
  $runfolder_path    =~ s/analysis/outgoing/;
  $recalibrated_path =~ s/analysis/outgoing/;

  $rfl = npg_pipeline::function::current_analysis_link->new(
    run_folder        => q{123456_IL2_1234},
    runfolder_path    => $runfolder_path,
    recalibrated_path => $recalibrated_path,
  );
  lives_ok { $rfl->make_link();} q{no croak creating link in outgoing when it already exists};
  ok(-l $link, 'link exists');
  unlink $link;
  ok(!-e $link, 'link deleted - test prerequisite');
  lives_ok { $rfl->make_link(); } q{no croak creating link in outgoing};
  ok(-l $link, 'link exists');
  is(readlink $link, $link_to, 'correct link target');
}

1;
