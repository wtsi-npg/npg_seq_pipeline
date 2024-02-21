use strict;
use warnings;
use Test::More tests => 4;
use Test::Exception;
use Log::Log4perl qw(:levels);
use File::Temp qw(tempdir);
use File::Copy::Recursive qw(dircopy);

my $temp_dir = tempdir(CLEANUP => 1);
Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $temp_dir, 'logfile'),
                          utf8   => 1});

my $pqq_suffix = q[_post_qc_complete];
my @wh_methods = qw/update_ml_warehouse/;
@wh_methods = map {$_, $_ . $pqq_suffix} @wh_methods;
my $default = {
  default => {
    minimum_cpu => 0,
    memory => 2,
    queue => 'lowload'
  }
};

my $rf_name = q[210415_A00971_0162_AHNNTMDSXY];
my $test_rf = q[t/data/novaseq/] . $rf_name;
my $runfolder_path = join q[/], $temp_dir,
  q[esa-sv-20201215-02/IL_seq_data/analysis], $rf_name;
dircopy($test_rf, $runfolder_path);
my $nocal_path = join q[/], $runfolder_path,
  q[Data/Intensities/BAM_basecalls_20210417-080715/no_cal];
mkdir $nocal_path;
symlink $nocal_path, "$runfolder_path/Latest_Summary";

my $id_run = 37416;

use_ok('npg_pipeline::function::warehouse_archiver');

subtest 'warehouse updates' => sub {
  plan tests => 19;

  my $c = npg_pipeline::function::warehouse_archiver->new(
    runfolder_path      => $runfolder_path,
    resource            => $default
  );
  isa_ok ($c, 'npg_pipeline::function::warehouse_archiver');

  my $recalibrated_path = $c->recalibrated_path();
  my $recalibrated_path_in_outgoing = $recalibrated_path;
  $recalibrated_path_in_outgoing =~ s{/analysis/}{/outgoing/}smx;


  foreach my $m (@wh_methods) {

    my $postqcc  = $m =~ /$pqq_suffix/smx;
    my $command  = 'npg_runs2mlwarehouse';
    my $job_name = join q[_], $command, $id_run, 'pname';
    $command    .= " --verbose --id_run $id_run";
    if ($postqcc) {
      $job_name .= '_postqccomplete';
    } else {
      $command .= " && npg_run_params2mlwarehouse --id_run $id_run --path_glob " .
        "'$runfolder_path/{r,R}unParameters.xml'";
    }

    my $ds = $c->$m('pname');
    ok ($ds && scalar @{$ds} == 1 && !$ds->[0]->excluded,
      'update to warehouse is enabled');
    my $d = $ds->[0];
    isa_ok ($d, 'npg_pipeline::function::definition');

    is ($d->identifier, $id_run, 'identifier set to run id');
    is ($d->created_by, 'npg_pipeline::function::warehouse_archiver', 'created_by');
    is ($d->command, $command, "command for $m");
    is ($d->job_name, $job_name, "job name for $m");
    is ($d->queue, 'lowload', 'queue');
    is_deeply ($d->num_cpus, [0], 'zero CPUs required');
    ok (!$d->has_command_preexec, "preexec command not defined for $m");
  }
};

subtest 'warehouse updates disabled' => sub {
  plan tests => 6;

  my $test_method = sub {
    my ($f, $method, $switch) = @_;
    my $d = $f->$method();
    ok($d && scalar @{$d} == 1 &&
      ($switch eq 'off' ? $d->[0]->excluded : !$d->[0]->excluded),
      $method . ': update to warehouse switched ' . $switch);
  };

  foreach my $m (@wh_methods) {
    my $c = npg_pipeline::function::warehouse_archiver->new(
      runfolder_path      => $runfolder_path,
      no_warehouse_update => 1,
      resource            => $default
    );
    $test_method->($c, $m, 'off');

    $c = npg_pipeline::function::warehouse_archiver->new(
      runfolder_path    => $runfolder_path,
      local             => 1,
      resource          => $default
    );
    $test_method->($c, $m, 'off');

    $c = npg_pipeline::function::warehouse_archiver->new(
      runfolder_path      => $runfolder_path,
      local               => 1,
      no_warehouse_update => 0,
      resource            => $default
    );
    $test_method->($c, $m, 'on');
  }
};

subtest 'mlwh updates for a product' => sub {
  plan tests => 7;

  my $rpt_list = join(q[:], $id_run, 4, 5);
  my $wa = npg_pipeline::function::warehouse_archiver->new(
    runfolder_path    => $runfolder_path,
    label             => 'my_label',
    product_rpt_list  => $rpt_list,
    resource          => $default
  );

  my $ds = $wa->update_ml_warehouse('pname');
  ok ($ds && scalar @{$ds} == 1 && !$ds->[0]->excluded,
    'update to warehouse is enabled');
  my $d = $ds->[0];
  isa_ok ($d, 'npg_pipeline::function::definition');
  is ($d->identifier, 'my_label', 'identifier set to the label value');
  is ($d->command,
    "npg_products2mlwarehouse --verbose --rpt_list '$rpt_list'", 'command');
  is ($d->job_name, 'npg_runs2mlwarehouse_my_label_pname', 'job name');
  is ($d->queue, 'lowload', 'queue');
  is_deeply ($d->num_cpus, [0], 'zero CPUs required');
};

1;
