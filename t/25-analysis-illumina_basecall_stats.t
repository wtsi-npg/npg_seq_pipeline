use strict;
use warnings;
use Test::More tests => 4;
use Test::Exception;
use Cwd;
use Log::Log4perl qw(:levels);

use npg_tracking::util::abs_path qw(abs_path);
use t::util;

my $util = t::util->new();

my $curdir = abs_path(getcwd());
my $tdir = $util->temp_directory();

local $ENV{NPG_WEBSERVICE_CACHE_DIR} = $curdir . q{/t/data};
local $ENV{TEST_FS_RESOURCE} = q{nfs_12};

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $tdir, 'logfile'),
                          utf8   => 1});

my $e = join q[/], $tdir, 'setupBclToQseq.py';
open my $fh, '>', $e;
print "#!/usr/bin/env python\n";
close $fh;
chmod 0755, $e;

local $ENV{PATH} = join q[:], qq[$curdir/t/bin], $tdir, $ENV{PATH};

use_ok(q{npg_pipeline::analysis::illumina_basecall_stats});

{
  my $runfolder_path = $util->analysis_runfolder_path();
  my $bustard_rta    = qq{$runfolder_path/Data/Intensities/Bustard_RTA};
  `mkdir -p $bustard_rta`;

  my $obj;
  my $id_run = 1234;
  my $bam_basecall_path = $runfolder_path . q{/Data/Intensities/BAM_basecalls};
  my $basecall_path = $runfolder_path . q{/Data/Intensities/BaseCalls};
  lives_ok {
    $obj = npg_pipeline::analysis::illumina_basecall_stats->new({
      id_run => $id_run,
      run_folder => q{123456_IL2_1234},
      runfolder_path => $runfolder_path,
      timestamp => q{20091028-101635},
      verbose => 0,
      bam_basecall_path => $bam_basecall_path,
      no_bsub => 1,
    })
  } q{create object ok};

  my $arg_refs = {
    timestamp => q{20091028-101635},
    position => 1,
    job_dependencies => q{-w 'done(1234) && done(4321)'},
  };
  my $mem = 350;
  my $mem_limit = npg_pipeline::lsf_job->new(memory => $mem, memory_units =>'MB')->_scale_mem_limit();
  my $expected_command = qq(bsub -q srpipeline -o $bam_basecall_path/log/basecall_stats_1234_20091028-101635.%J.out -J basecall_stats_1234_20091028-101635 -R 'select[mem>).$mem.q{] rusage[mem=}.$mem.q{,nfs_12=4]' -M} . $mem_limit . qq( -R 'span[hosts=1]' -n 4  " cd $bam_basecall_path && if [[ -f Makefile ]]; then echo Makefile already present 1>&2; else echo creating bcl2qseq Makefile 1>&2; ) . qq($tdir/setupBclToQseq.py -b $basecall_path -o $bam_basecall_path --overwrite; fi && make -j 4 Matrix Phasing && make -j 4 BustardSummary.x{s,m}l ");
  is( $obj->_generate_command( $arg_refs ), $expected_command,
    q{Illumina basecalls stats generation bsub command is correct} );

  my @job_ids = $obj->generate($arg_refs);
  is( scalar @job_ids, 1, q{1 job ids, generate Illumina basecall stats} );
}

1;
