use strict;
use warnings;
use Test::More tests => 39;
use Test::Exception;
use t::util;
use Cwd;
use Log::Log4perl qw(:levels);

use npg_tracking::util::abs_path qw(abs_path);
my $util = t::util->new();

my $curdir = abs_path(getcwd());
my $repos = join q[/], $curdir, 't/data/sequence';

my $tdir = $util->temp_directory();
$ENV{TEST_DIR} = $tdir;
$ENV{TEST_FS_RESOURCE} = q{nfs_12};
$ENV{NPG_WEBSERVICE_CACHE_DIR} = $curdir . q{/t/data};

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $tdir, 'logfile'),
                          utf8   => 1});

local $ENV{PATH} = join q[:], qq[$curdir/t/bin], $tdir, $ENV{PATH};

my $id_run;
my $mem_units = 'MB';

use_ok(q{npg_pipeline::analysis::harold_calibration_bam});

my $runfolder_path = $util->analysis_runfolder_path();
my $bustard_home   = qq{$runfolder_path/Data/Intensities};
my $bustard_rta    = qq{$bustard_home/Bustard_RTA};
my $gerald_rta     = qq{$bustard_rta/GERALD_RTA};
my $config_path    = qq{$runfolder_path/Config};

sub set_staging_analysis_area {
  `rm -rf /tmp/nfs/sf45`;
  `mkdir -p $bustard_rta`;
  `mkdir -p $config_path`;
  `cp t/data/Recipes/Recipe_GA2_37Cycle_PE_v6.1.xml $runfolder_path/`;
  `cp t/data/Recipes/TileLayout.xml $config_path/`;
  return 1;
}

{
  set_staging_analysis_area();
  my $harold;
  $id_run = 1234;
  lives_ok {
    $harold = npg_pipeline::analysis::harold_calibration_bam->new({
      id_run => $id_run,
      run_folder => q{123456_IL2_1234},
      runfolder_path => $runfolder_path,
      timestamp => q{20091028-101635},
      verbose => 0,
      repository => $repos,
      bam_basecall_path => $runfolder_path . q{/Data/Intensities/BAM_basecalls},
      no_bsub => 1,
      recalibration => 1,
    });
  } q{create $harold object ok};

  my $arg_refs = {
    timestamp => q{20091028-101635},
    position => 1,
    job_dependencies => q{-w 'done(1234) && done(4321)'},
    ref_seq => q{t/data/sequence/references/Human/default/all/bwa/someref.fa.bwt},
  };
  my $mem = 350;
  my $mem_limit = npg_pipeline::lsf_job->new(memory => $mem, memory_units =>$mem_units)->_scale_mem_limit();
  my $expected_command = q(bsub -q srpipeline -o /nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BAM_basecalls/log/basecall_stats_1234_20091028-101635.%J.out -J basecall_stats_1234_20091028-101635 -R 'select[mem>).$mem.q{] rusage[mem=}.$mem.q{,nfs_12=4]' -M}.$mem_limit.q( -R 'span[hosts=1]' -n 4  " cd /nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BAM_basecalls && if [[ -f Makefile ]]; then echo Makefile already present 1>&2; else echo creating bcl2qseq Makefile 1>&2; /software/solexa/src/OLB-1.9.4/bin/setupBclToQseq.py -b /nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BaseCalls -o /nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BAM_basecalls --overwrite; fi && make -j 4 Matrix Phasing && make -j 4 BustardSummary.x{s,m}l ");
  is( $util->drop_temp_part_from_paths( $harold->_generate_illumina_basecall_stats_command( $arg_refs ) ), $expected_command, q{Illumina basecalls stats generation bsub command is correct} );

  my @job_ids = $harold->generate_illumina_basecall_stats($arg_refs);
  is( scalar @job_ids, 1, q{1 job ids, generate Illumina basecall stats} );
}

1;
