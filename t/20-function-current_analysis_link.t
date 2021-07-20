use strict;
use warnings;
use Test::More tests => 24;
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
  my $test = sub {
    my ($obj) = @_;
    my $ds = $obj->create();
    ok($ds && scalar @{$ds} == 1 && $ds->[0]->excluded,
      'creating summary link switched off');
    isa_ok ($ds->[0], 'npg_pipeline::function::definition');
  };

  my $rfl;
  $rfl = npg_pipeline::function::current_analysis_link->new(
      run_folder        => q{123456_IL2_1234},
      runfolder_path    => $runfolder_path,
      recalibrated_path => $recalibrated_path,
      no_summary_link   => 1,
      resource => {
        default => {
          minimum_cpu => 1,
          memory => 1
        }
      }
  );
  $test->($rfl);

  $rfl = npg_pipeline::function::current_analysis_link->new(
      run_folder        => q{123456_IL2_1234},
      runfolder_path    => $runfolder_path,
      recalibrated_path => $recalibrated_path,
      local             => 1,
      resource => {
        default => {
          minimum_cpu => 1,
          memory => 1
        }
      }
  );
  $test->($rfl);

  $rfl = npg_pipeline::function::current_analysis_link->new(
      run_folder        => q{123456_IL2_1234},
      runfolder_path    => $runfolder_path,
      recalibrated_path => $recalibrated_path,
      resource => {
        default => {
          minimum_cpu => 1,
          memory => 1,
          queue => 'small'
        }
      }
  );
  my $ds = $rfl->create();
  ok($ds && scalar @{$ds} == 1 && !$ds->[0]->excluded,
    'creating summary link is enabled');
  my $d = $ds->[0];
  isa_ok ($d, 'npg_pipeline::function::definition');
  is ($d->identifier, '1234', 'identifier set to run id');
  is ($d->created_by, 'npg_pipeline::function::current_analysis_link',
    'created_by');
  my $command = 'npg_pipeline_create_summary_link ' .
                '--run_folder 123456_IL2_1234 ' .
                "--runfolder_path $runfolder_path " .
                "--recalibrated_path $recalibrated_path";
  is ($d->command, $command, 'command');
  is ($d->job_name, 'create_latest_summary_link_1234_123456_IL2_1234',
    'job name');
  is ($d->queue, 'small', 'small queue');
}

{
  my $link = "$runfolder_path/Latest_Summary";
  ok(!-e $link, 'link does not exist - test prerequisite');

  my $rfl = npg_pipeline::function::current_analysis_link->new(
      run_folder        => q{123456_IL2_1234},
      runfolder_path    => $runfolder_path,
      recalibrated_path => $recalibrated_path,
      resource => {
        default => {
          minimum_cpu => 1,
          memory => 1
        }
      }
  );

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
