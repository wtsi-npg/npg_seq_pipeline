use strict;
use warnings;
use Test::More tests => 34;
use Test::Exception;

use_ok('npg_pipeline::pluggable::registry');

my $r = npg_pipeline::pluggable::registry->new();

throws_ok { $r->get_function_implementor() }
  qr/Non-empty function name string is required/,
  'error if function name is not given';
throws_ok { $r->get_function_implementor('my_function') }
  qr/Handler for 'my_function' is not registered/,
  'error if function name is not registered';

my $i = $r->get_function_implementor('pipeline_start');
is (ref $i, 'HASH', 'hash ref returned');
is ($i->{'module'}, 'start_stop', 'module name');
is ($i->{'method'}, 'pipeline_start', 'method name');
ok (!exists $i->{'params'}, 'params not defined');

$i = $r->get_function_implementor('update_warehouse');
is ($i->{'module'}, 'warehouse_archiver', 'module name');
is ($i->{'method'}, 'update_warehouse', 'method name');
ok (!exists $i->{'params'}, 'params not defined');

$i = $r->get_function_implementor('update_ml_warehouse');
is ($i->{'module'}, 'warehouse_archiver', 'module name');
is ($i->{'method'}, 'update_ml_warehouse', 'method name');
ok (!exists $i->{'params'}, 'params not defined');

$i = $r->get_function_implementor('update_warehouse_post_qc_complete');
is ($i->{'module'}, 'warehouse_archiver', 'module name');
is ($i->{'method'}, 'update_warehouse_post_qc_complete', 'method name');
ok (!exists $i->{'params'}, 'params not defined');

$i = $r->get_function_implementor('update_ml_warehouse_post_qc_complete');
is ($i->{'module'}, 'warehouse_archiver', 'module name');
is ($i->{'method'}, 'update_ml_warehouse_post_qc_complete', 'method name');
ok (!exists $i->{'params'}, 'params not defined');

$i = $r->get_function_implementor('run_analysis_in_progress');
is ($i->{'module'}, 'status', 'module name');
is ($i->{'method'}, 'create', 'method name');
is_deeply ($i->{'params'}, {'status' => 'analysis in progress',
                           'lane_status_flag' => 0}, 'params');

$i = $r->get_function_implementor('lane_analysis_in_progress');
is ($i->{'module'}, 'status', 'module name');
is ($i->{'method'}, 'create', 'method name');
is_deeply ($i->{'params'}, {'status' => 'analysis in progress',
                           'lane_status_flag' => 1}, 'params');

$i = $r->get_function_implementor('qc_adapter');
is ($i->{'module'}, 'autoqc', 'module name');
is ($i->{'method'}, 'create', 'method name');
is_deeply ($i->{'params'}, {'qc_to_run'  => 'adapter'}, 'params');

$i = $r->get_function_implementor('archive_to_irods_samplesheet');
is ($i->{'module'}, 'seq_to_irods_archiver', 'module name');
is ($i->{'method'}, 'create', 'method name');
is_deeply ($i->{'params'}, {'lims_driver_type' => 'samplesheet'}, 'params');

$i = $r->get_function_implementor('archive_to_irods_ml_warehouse');
is ($i->{'module'}, 'seq_to_irods_archiver', 'module name');
is ($i->{'method'}, 'create', 'method name');
is_deeply ($i->{'params'}, {'lims_driver_type' => 'ml_warehouse_fc_cache'}, 'params');

1;
