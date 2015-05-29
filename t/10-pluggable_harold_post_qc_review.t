use strict;
use warnings;
use Test::More tests => 17;
use Test::Deep;
use Test::Exception;
use Cwd;

use t::util;
use t::dbic_util;

my $util = t::util->new();
my $runfolder_path = $util->analysis_runfolder_path();
local $ENV{PATH} = join q[:], q[t/bin], $ENV{PATH};

my $temp = $util->temp_directory();
$ENV{TEST_DIR} = $temp;
$ENV{TEST_FS_RESOURCE} = q{nfs_12};

use_ok('npg_pipeline::pluggable::harold::post_qc_review');

{
  my $post_qc_review;
  my @functions_in_order = qw(
    run_archival_in_progress
    archive_to_irods
    upload_illumina_analysis_to_qc_database
    upload_auto_qc_to_qc_database
    move_to_outgoing
    run_run_archived
    run_qc_complete
    update_warehouse
    );
  my @original = @functions_in_order;
  unshift @original, 'lsf_start';
  push @original, 'lsf_end';
  $util->set_staging_analysis_area({with_latest_summary => 1});

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = cwd() . q[/t/data];

  lives_ok {
    $post_qc_review = npg_pipeline::pluggable::harold::post_qc_review->new(
      function_list => q{post_qc_review},
      function_order   => \@functions_in_order,
      runfolder_path   => $runfolder_path,
      spider           => 0,
    );
  } q{no croak on creation};

  isa_ok($post_qc_review, q{npg_pipeline::pluggable::harold::post_qc_review}, q{$post_qc_review});

  is($post_qc_review->id_run, 1234, 'run id set correctly');
  lives_ok { $post_qc_review->main(); } q{no croak running harold->main()};
  is(join(q[ ], @{$post_qc_review->function_order()}), join(q[ ], @original), q{$post_qc_review->function_order() set on creation});

  my $timestamp = $post_qc_review->timestamp;
  my $recalibrated_path = $post_qc_review->recalibrated_path();
  my $log_dir = $post_qc_review->make_log_dir( $recalibrated_path );
  $log_dir =~ s/\/analysis\//\/outgoing\//smx;
  my $unset_string = 'unset NPG_WEBSERVICE_CACHE_DIR; unset NPG_CACHED_SAMPLESHEET_FILE;';
  my $expected = q[bsub -q lowload 50 -J warehouse_loader_1234_post_qc_review ] .
                qq[-o $log_dir/warehouse_loader_1234_post_qc_review_] . $timestamp . 
                qq[.out '$unset_string warehouse_loader --id_run 1234'];
  is($post_qc_review->_update_warehouse_command(50,'warehouse_loader', $unset_string),
    $expected, 'update warehouse command');

  $expected = q[bsub -q lowload 50 -J npg_runs2mlwarehouse_1234_post_qc_review ] .
             qq[-o $log_dir/npg_runs2mlwarehouse_1234_post_qc_review_] . $timestamp . 
              q[.out 'npg_runs2mlwarehouse --id_run 1234'];
  is($post_qc_review->_update_warehouse_command(50,'npg_runs2mlwarehouse'),
    $expected, 'update ml_warehouse command');

  $log_dir = $post_qc_review->make_log_dir( $runfolder_path );
  is($post_qc_review->_interop_command, qq[bsub -q lowload  -J interop_1234_post_qc_review -R 'rusage[nfs_12=1,seq_irods=15]' -o $log_dir/interop_1234_post_qc_review_] . $timestamp . qq[.out 'irods_interop_loader.pl --id_run 1234 --runfolder_path $runfolder_path'], 'irods_interop_loader.pl command');
}

{
  my $p = npg_pipeline::pluggable::harold::post_qc_review->new(
      runfolder_path => $runfolder_path,
      no_irods_archival => 1,
      no_warehouse_update => 1,
    );
  ok(!$p->archive_to_irods(), 'archival to irods switched off');
  ok(!$p->update_warehouse(), 'update to warehouse switched off');
}

{
  my $p = npg_pipeline::pluggable::harold::post_qc_review->new(
      runfolder_path => $runfolder_path,
      local => 1,
    );
  ok(!$p->archive_to_irods(), 'archival to irods switched off');
  ok(!$p->update_warehouse(), 'update to warehouse switched off');
  is($p->no_summary_link,1, 'summary_link switched off');
}

{
  my $p = npg_pipeline::pluggable::harold::post_qc_review->new(
      runfolder_path => $runfolder_path,
      local => 1,
      no_warehouse_update => 0,
    );
  ok(!$p->archive_to_irods(), 'archival to irods switched off');
  ok($p->update_warehouse(), 'update to warehouse switched on');
  is($p->no_summary_link,1, 'summary_link switched off');
}

1;
