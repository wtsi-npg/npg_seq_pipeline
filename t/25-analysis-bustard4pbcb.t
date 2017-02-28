use strict;
use warnings;
use Test::More tests => 18;
use Test::Exception;
use Log::Log4perl qw(:levels);
use t::util;

my $util = t::util->new();
my $tmp_dir = $util->temp_directory();

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $tmp_dir, 'logfile'),
                          utf8   => 1});

$ENV{TEST_DIR} = $tmp_dir;
$ENV{TEST_FS_RESOURCE} = q{nfs_12};
local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];
local $ENV{PATH} = join q[:], q[t/bin], q[t/bin/software/solexa/bin], $ENV{PATH};
my $mem_units = 'MB';

use_ok(q{npg_pipeline::analysis::bustard4pbcb});

my $runfolder_path = $util->analysis_runfolder_path();
my $bustard_home   = qq{$runfolder_path/Data/Intensities};
my $bustard_rta    = qq{$bustard_home/Bustard_RTA};
my $config_path    = qq{$runfolder_path/Config};

my $req_job_completion = q{-w'done(123) && done(321)'};

sub set_staging_analysis_area {
  `rm -rf $tmp_dir/nfs/sf45`;
  `mkdir -p $bustard_home`;
  `mkdir $config_path`;
  return 1;
}

{
  set_staging_analysis_area();
  my $bustard;
  lives_ok {
    $bustard = npg_pipeline::analysis::bustard4pbcb->new(
      id_run => 1234,
      bustard_home => $bustard_home,
    );
  } q{no croak creating new object with id_run and bustard_home attributes};
  isa_ok($bustard, q{npg_pipeline::analysis::bustard4pbcb}, q{$bustard});

  require "npg_pipeline/pluggable.pm";
  lives_ok {
    $bustard = npg_pipeline::analysis::bustard4pbcb->new(
      pipeline => npg_pipeline::pluggable->new(id_run=>1),
      id_run => 1234,
      bustard_home => $bustard_home,
      bustard_dir  => join ($bustard_home, 'BUSTARD_NPG'),
    );
  } q{no croak creating new object with pipeline, id_run, bustard_home and bustard_dir attributes};
  is($bustard->script_path, '/software/solexa/src/OLB-1.9.4/bin/bustard.py', 'live bustard script path');

  throws_ok {
    npg_pipeline::analysis::bustard4pbcb->new(
      pipeline => $util,
      id_run => 1234,
      bustard_home => $bustard_home,
      bustard_dir  => join ($bustard_home, 'BUSTARD_NPG'),
  )} qr/Validation failed for 'NpgPipelinePluggableObject'/, 'error when pipeline object has wrong type';
}

{
  my $bustard = npg_pipeline::analysis::bustard4pbcb->new(
      pipeline => npg_pipeline::pluggable->new(),
      id_run => 1234,
      bustard_home => $bustard_home,
      bustard_dir  => join ($bustard_home, 'BUSTARD_NPG'),
      timestamp    => '20091028-101635',
      script_path  => '/bin/true',
  );

  my $lsf_index_string = $bustard->lsb_jobindex();

  my $expected_cmd = qq{LOGNAME=101635 /bin/true --make --CIF --keep-dif-files --no-eamss --phasing=lane --matrix=lane --tiles=s_1,s_2,s_3,s_4,s_5,s_6,s_7,s_8 $bustard_home > $bustard_home/bustard_output_20091028-101635.txt 2>&1};
  is( $bustard->_bustard_command(), $expected_cmd, q{bustard command});

  my $mem = 13800;
  my $mem_limit = npg_pipeline::lsf_job->new(memory => $mem, memory_units =>$mem_units)->_scale_mem_limit();
  $expected_cmd = q{bsub -n 8,16 -q srpipeline -o log/bustard_basecalls_all_1234_20091028-101635.%J.out -J bustard_basecalls_all_1234_20091028-101635 -R 'select[mem>}.$mem.q{] rusage[mem=}.$mem.q{,nfs_12=8]' -M}.$mem_limit.q{ -R 'span[hosts=1]' -w'done(123) && done(321)' 'make -j `npg_pipeline_job_env_to_threads` all'};
  is( $bustard->_make_command('basecalls_all', $req_job_completion), $expected_cmd, q{command for basecalls all generated correctly});

  $expected_cmd = q{bsub -n 8,16 -q srpipeline -o log/bustard_basecalls_lanes_1234_20091028-101635.%I.%J.out -J bustard_basecalls_lanes_1234_20091028-101635[1,2,3,4,5,6,7,8] -R 'select[mem>}.$mem.q{] rusage[mem=}.$mem.q{,nfs_12=8]' -M}.$mem_limit.q{ -R 'span[hosts=1]' -w'done(123) && done(321)' 'make -j `npg_pipeline_job_env_to_threads` s_} . $lsf_index_string . q{'};
  is( $bustard->_make_command('basecalls_lanes', $req_job_completion), $expected_cmd, q{command for basecall lanes generated correctly});

  $bustard = npg_pipeline::analysis::bustard4pbcb->new(
      pipeline => npg_pipeline::pluggable->new(),
      id_run => 1234,
      bustard_home => $bustard_home,
      bustard_dir  => join ($bustard_home, 'BUSTARD_NPG'),
      timestamp    => '20091028-101635',
      script_path  => 'bustard_script',
      lanes => [1,3,5],
  );

  $expected_cmd = qq{LOGNAME=101635 bustard_script --make --CIF --keep-dif-files --no-eamss --phasing=lane --matrix=lane --tiles=s_1,s_3,s_5 $bustard_home > $bustard_home/bustard_output_20091028-101635.txt 2>&1};
  is( $bustard->_bustard_command(), $expected_cmd, q{bustard command});

  $expected_cmd = q{bsub -n 8,16 -q srpipeline -o log/bustard_matrix_lanes_1234_20091028-101635.%I.%J.out -J bustard_matrix_lanes_1234_20091028-101635[1,3,5] -R 'select[mem>}.$mem.q{] rusage[mem=}.$mem.q{,nfs_12=8]' -M}.$mem_limit.q{ -R 'span[hosts=1]' 'make -j `npg_pipeline_job_env_to_threads` matrix_`echo $LSB_JOBINDEX`_finished.txt'};
  is ($bustard->_make_command('matrix_lanes'), $expected_cmd, 'matrix lane command');

  $expected_cmd = q{bsub -n 8,16 -q srpipeline -o log/bustard_phasing_all_1234_20091028-101635.%J.out -J bustard_phasing_all_1234_20091028-101635 -R 'select[mem>}.$mem.q{] rusage[mem=}.$mem.q{,nfs_12=8]' -M}.$mem_limit.q{ -R 'span[hosts=1]' 'make -j `npg_pipeline_job_env_to_threads` phasing_finished.txt'};
  is ($bustard->_make_command('phasing_all'), $expected_cmd, 'phasing all command');
}

{ 
  my $bustard = npg_pipeline::analysis::bustard4pbcb->new(
      id_run => 1234,
      bustard_home => $bustard_home,
      timestamp    => '20091028-101635',
      script_path  => 'none',
  );
  throws_ok { $bustard->bustard_dir } qr/ not found/, 'error when bustard command not found';

  $bustard = npg_pipeline::analysis::bustard4pbcb->new(
      id_run => 1234,
      bustard_home => $bustard_home,
      script_path  => '/bin/true',
  );
  throws_ok { $bustard->bustard_dir } qr/No bustard output in/, 'error when bustard output file is empty';

  $bustard = npg_pipeline::analysis::bustard4pbcb->new(
      id_run => 1234,
      bustard_home => $bustard_home,
      script_path  => '/bin/true',
  );
  throws_ok { $bustard->_get_bustard_dir(qw/one two three/) } qr/No record about bustard directory/, 'error when bustard output file does not contain the bustard directory name';

  my $dir;
  lives_ok { $dir =  $bustard->_get_bustard_dir('one', 'Sequence folder: folder', 'three')} 'parsing bustard output lives';
  is ($dir, 'folder', 'correct bustard directory extracted');
  is($bustard->_get_bustard_dir('one', 'Sequence folder:folder', 'three'), undef, 'undef returned if line format is wrong');
}

1;
