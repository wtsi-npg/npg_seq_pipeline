use strict;
use warnings;
use Test::More tests => 21;
use Test::Deep;
use Test::Exception;
use t::util;
use npg::api::request;

my $util = t::util->new();
my $conf_path = $util->conf_path();

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

  lives_ok {
    $post_qc_review = npg_pipeline::pluggable::harold::post_qc_review->new(
      id_run => 1234,
      function_order => \@functions_in_order,
      runfolder_path => $runfolder_path,
      run_folder => q{123456_IL2_1234},
      verbose => 1,
      conf_path => $conf_path,
      domain => q{test},
      script_name => q{npg_pipeline_post_qc_review},
    );
  } q{no croak on creation};
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];
  lives_ok { $post_qc_review->main(); } q{no croak running harold->main()};
  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[];
  isa_ok($post_qc_review, q{npg_pipeline::pluggable::harold::post_qc_review}, q{$post_qc_review});
  is(join(q[ ], @{$post_qc_review->function_order()}), join(q[ ], @original), q{$post_qc_review->function_order() set on creation});

  my $timestamp = $post_qc_review->timestamp;
  my $recalibrated_path = $post_qc_review->recalibrated_path();
  my $log_dir = $post_qc_review->make_log_dir( $recalibrated_path );
  $log_dir =~ s/\/analysis\//\/outgoing\//smx;
  my $expected =  qq[bsub -q test  -J warehouse_loader_1234_post_qc_review -o $log_dir/warehouse_loader_1234_post_qc_review_] . $timestamp . 
     q[.out 'unset NPG_WEBSERVICE_CACHE_DIR; unset NPG_CACHED_SAMPLESHEET_FILE; warehouse_loader --id_run 1234'];
  is($post_qc_review->_update_warehouse_command(undef,'warehouse_loader'), $expected, 'update warehouse command');
  $expected =  qq[bsub -q test  -J ml_warehouse_loader_1234_post_qc_review -o $log_dir/ml_warehouse_loader_1234_post_qc_review_] . $timestamp . 
     q[.out 'unset NPG_WEBSERVICE_CACHE_DIR; unset NPG_CACHED_SAMPLESHEET_FILE; ml_warehouse_loader --id_run 1234'];
  is($post_qc_review->_update_warehouse_command(undef,'ml_warehouse_loader'), $expected, 'update ml_warehouse command');

  $log_dir = $post_qc_review->make_log_dir( $runfolder_path );
  is($post_qc_review->_interop_command, qq[bsub -q test  -J interop_1234_post_qc_review -R 'rusage[nfs_12=1,seq_irods=15]' -o $log_dir/interop_1234_post_qc_review_] . $timestamp . qq[.out 'irods_interop_loader.pl --id_run 1234 --runfolder_path $runfolder_path'], 'irods_interop_loader.pl command');
}

{
  my $p = npg_pipeline::pluggable::harold::post_qc_review->new(
      id_run => 1234,
      runfolder_path => $runfolder_path,
      run_folder => q{123456_IL2_1234},
      no_irods_archival => 1,
      no_warehouse_update => 1,
    );
  ok(!$p->archive_to_irods(), 'archival to irods switched off');
  ok(!$p->update_warehouse(), 'update to warehouse switched off');
}

{
  my $p = npg_pipeline::pluggable::harold::post_qc_review->new(
      id_run => 1234,
      runfolder_path => $runfolder_path,
      run_folder => q{123456_IL2_1234},
      local => 1,
    );
  ok(!$p->archive_to_irods(), 'archival to irods switched off');
  ok(!$p->update_warehouse(), 'update to warehouse switched off');
  is($p->no_summary_link,1, 'summary_link switched off');
}

{
  my $p = npg_pipeline::pluggable::harold::post_qc_review->new(
      id_run => 1234,
      runfolder_path => $runfolder_path,
      run_folder => q{123456_IL2_1234},
      local => 1,
      no_warehouse_update => 0,
    );
  ok(!$p->archive_to_irods(), 'archival to irods switched off');
  ok($p->update_warehouse(), 'update to warehouse switched on');
  is($p->no_summary_link,1, 'summary_link switched off');
}

{
  my $p = npg_pipeline::pluggable::harold::post_qc_review->new(
      id_run => 1234,
      runfolder_path => $runfolder_path,
      run_folder => q{123456_IL2_1234},
      local => 1,
      bam_basecall_path => $runfolder_path,
  );
  my $cache = $runfolder_path . q{/metadata_cache_1234};
  qx{mkdir -p $cache};
  lives_ok {$p->spider();} q{spider runs ok};
  is( $ENV{ npg::api::request->cache_dir_var_name() }, $cache, q{environment variable set to cache} );

  local $ENV{ npg::api::request->cache_dir_var_name() } = q{t/data};
  lives_ok {$p->spider();} q{spider runs ok};
  is( $ENV{ npg::api::request->cache_dir_var_name() },qq{$runfolder_path/metadata_cache_1234},
    q{environment variable is set to local cache} );
  ok(-d qq{$runfolder_path/metadata_cache_1234}, 'local cache directory exists');
}

1;
