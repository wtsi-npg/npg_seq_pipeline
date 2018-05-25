use strict;
use warnings;
use Test::More tests => 21;
use Test::Exception;
use Cwd;
use Log::Log4perl qw(:levels);

use npg_tracking::util::abs_path qw(abs_path);
use t::util;

my $util = t::util->new();

my $curdir = abs_path(getcwd());
my $tdir = $util->temp_directory();

local $ENV{NPG_WEBSERVICE_CACHE_DIR} = $curdir . q{/t/data};

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $tdir, 'logfile'),
                          utf8   => 1});

my $e = join q[/], $tdir, 'setupBclToQseq.py';
open my $fh, '>', $e;
print $fh "#!/usr/bin/env python\n";
close $fh;
chmod 0755, $e;

local $ENV{PATH} = join q[:], $tdir, $ENV{PATH};

use_ok(q{npg_pipeline::function::illumina_basecall_stats});

{
  my $runfolder_path = $util->analysis_runfolder_path();
  my $bustard_rta    = qq{$runfolder_path/Data/Intensities/Bustard_RTA};
  `mkdir -p $bustard_rta`;

  my $obj;
  my $id_run = 1234;
  my $bam_basecall_path = $runfolder_path . q{/Data/Intensities/BAM_basecalls};
  my $basecall_path = $runfolder_path . q{/Data/Intensities/BaseCalls};
  lives_ok {
    $obj = npg_pipeline::function::illumina_basecall_stats->new(
      id_run => $id_run,
      run_folder => q{123456_IL2_1234},
      runfolder_path => $runfolder_path,
      timestamp => q{20091028-101635},
      bam_basecall_path => $bam_basecall_path,
    )
  } q{create object ok};
  isa_ok ($obj, q{npg_pipeline::function::illumina_basecall_stats});
  my $da = $obj->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  my $d = $da->[0];
  isa_ok($d, q{npg_pipeline::function::definition});
  is ($d->created_by, q{npg_pipeline::function::illumina_basecall_stats},
    'created_by is correct');
  is ($d->created_on, $obj->timestamp, 'created_on is correct');
  is ($d->identifier, 1234, 'identifier is set correctly');
  is ($d->job_name, q{basecall_stats_1234_20091028-101635},
    'job_name is correct');
  ok (!$d->has_composition, 'composition not set');
  ok (!$d->excluded, 'step not excluded');
  is_deeply ($d->num_cpus, [4], 'number of cpus');
  is ($d->num_hosts, 1, 'number of hosts');
  is ($d->fs_slots_num, 4, 'fs slots number');
  is ($d->memory, 350, 'memory');
  is ($d->queue, 'default', 'default queue');
  lives_ok {$d->freeze()} 'definition can be serialized to JSON';

  my $command = "cd $bam_basecall_path && if [[ -f Makefile ]]; then echo Makefile already present 1>&2; else echo creating bcl2qseq Makefile 1>&2; $tdir/setupBclToQseq.py -b $basecall_path -o $bam_basecall_path --overwrite; fi && make -j 4 Matrix Phasing && make -j 4 BustardSummary.x{s,m}l";
  is ($d->command, $command, 'command is correct');

  local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data/hiseqx];
  $obj = npg_pipeline::function::illumina_basecall_stats->new(
    id_run => 13219,
    run_folder => 'folder',
    runfolder_path => $runfolder_path
  );
  $da = $obj->create();
  ok ($da && @{$da} == 1, 'an array with one definition is returned');
  $d = $da->[0];
  isa_ok($d, q{npg_pipeline::function::definition});
  ok ($d->excluded, 'illumina_basecall_stats step is skipped for HiSeqX run');
}

1;
