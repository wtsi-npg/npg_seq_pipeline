use strict;
use warnings;
use Test::More tests => 23;
use Test::Exception;
use File::Copy::Recursive qw(dircopy fmove);
use File::Temp qw(tempdir);

use_ok('npg_pipeline::function::current_analysis_link');

my $temp_dir = tempdir(CLEANUP => 1);

my $rf_name = q[210415_A00971_0162_AHNNTMDSXY];
my $test_rf = q[t/data/novaseq/] . $rf_name;
my $analysis_dir = join q[/], $temp_dir,
  q[esa-sv-20201215-02/IL_seq_data/analysis];
my $runfolder_path = join q[/], $analysis_dir, $rf_name;
dircopy($test_rf, $runfolder_path);
my $nocall_relative = q[Data/Intensities/BAM_basecalls_20210417-080715/no_cal];
my $nocall_path = join q[/], $runfolder_path, $nocall_relative;
mkdir $nocall_path;

my $id_run = 37416;
for my $file (qw(RunInfo.xml RunParameters.xml)) {
  my $source = join q[/], $runfolder_path, "${id_run}_${file}";
  my $target = join q[/], $runfolder_path, $file;
  fmove($source, $target);
}

my $resource = {default => {minimum_cpu => 1, memory => 1, queue => 'small'}};

sub test_job_skipped {
  my $obj = shift;
  my $ds = $obj->create();
  ok($ds && scalar @{$ds} == 1 && $ds->[0]->excluded,
    'creating summary link switched off');
  isa_ok ($ds->[0], 'npg_pipeline::function::definition');
}

{
  my $rfl = npg_pipeline::function::current_analysis_link->new(
    runfolder_path      => $runfolder_path,
    no_summary_link     => 1,
    resource            => $resource,
    npg_tracking_schema => undef
  );
  test_job_skipped($rfl);

  $rfl = npg_pipeline::function::current_analysis_link->new(
    runfolder_path      => $runfolder_path,
    local               => 1,
    resource            => $resource,
    npg_tracking_schema => undef
  );
  test_job_skipped($rfl);

  $rfl = npg_pipeline::function::current_analysis_link->new(
    runfolder_path      => $runfolder_path,
    resource            => $resource,
    npg_tracking_schema => undef
  );
  my $ds = $rfl->create();
  ok($ds && scalar @{$ds} == 1 && !$ds->[0]->excluded,
    'creating summary link is enabled');
  my $d = $ds->[0];
  is ($d->identifier, $id_run, 'identifier set to run id');
  is ($d->created_by, 'npg_pipeline::function::current_analysis_link',
    'created_by');
  my $command = 'npg_pipeline_create_summary_link ' .
                "--run_folder $rf_name " .
                "--runfolder_path $runfolder_path " .
                "--recalibrated_path $nocall_path";
  is ($d->command, $command, 'command');
  is ($d->job_name, "create_latest_summary_link_${id_run}_${rf_name}", 'job name');
  is ($d->queue, 'small', 'small queue');
}

{
  my $link = "$runfolder_path/Latest_Summary";
  ok(!-e $link, 'link does not exist - test prerequisite');

  my $rfl = npg_pipeline::function::current_analysis_link->new(
    runfolder_path      => $runfolder_path,
    resource            => $resource,
    npg_tracking_schema => undef
  );

  lives_ok { $rfl->make_link() } q{no error creating link};
  ok(-l $link, 'link exists');
  is(readlink $link, $nocall_relative, 'correct link target');
  lives_ok { $rfl->make_link() } q{no error creating link when it already exists};
  ok(-l $link, 'link exists');

  my $outgoing_dir = $analysis_dir;
  $outgoing_dir =~ s/analysis/outgoing/;
  rename $analysis_dir, $outgoing_dir;
  $link              =~ s/analysis/outgoing/;
  $runfolder_path    =~ s/analysis/outgoing/;

  $rfl = npg_pipeline::function::current_analysis_link->new(
    runfolder_path      => $runfolder_path,
    npg_tracking_schema => undef
  );
  lives_ok { $rfl->make_link() }
    q{no error creating link in outgoing when it already exists};
  ok(-l $link, 'link exists');
  unlink $link;
  ok(!-e $link, 'link deleted - test prerequisite');
  lives_ok { $rfl->make_link() } q{no error creating link in outgoing};
  ok(-l $link, 'link exists');
  is(readlink $link, $nocall_relative, 'correct link target');
}

1;
