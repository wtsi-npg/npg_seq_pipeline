use strict;
use warnings;
use English qw{-no_match_vars};
use Test::More tests => 5;
use Test::Exception;
use t::util;
use Cwd;

local $ENV{PATH} = join q[:], q[t/bin], q[t/bin/software/solexa/bin], $ENV{PATH};

my $util = t::util->new();

my $curdir = cwd();
my $tmp_dir = $util->temp_directory();
local $ENV{TEST_DIR} = $tmp_dir;
my $bin = $curdir . q[/bin];
my $conf_path = $curdir . q{/data/config_files};

my $analysis_runfolder_path = $util->analysis_runfolder_path();
$util->set_rta_staging_analysis_area();

# Script failures
{
  my $out = `perl $bin/npg_pipeline_PB_cal_bam --no_bsub --runfolder_path $tmp_dir/nfs/sf45/IL2/analysis/123456_IL2_1234 --function_order dodo`;
  like($out, qr/Error submitting jobs: Can't locate object method "dodo" via package "npg_pipeline::pluggable::harold::PB_cal_bam"/,
  'error when function does not exist');
}

# Script passes
{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q{t/data/samplesheet_1234.csv};
  my $bustard_path = join q[/], $analysis_runfolder_path, q{Data/Intensities/Bustard1.5.1_09-07-2009_RTA};
  rename $bustard_path, join(q[/], $analysis_runfolder_path, 'moved');
  lives_ok { diag qx{perl $bin/npg_pipeline_post_qc_review --domain test --conf_path $conf_path --no_folder_moves --runfolder_path $analysis_runfolder_path --bam_basecall_path $analysis_runfolder_path}; } q{ran bin/npg_pipeline_post_qc_review};
  ok(!$CHILD_ERROR, qq{Return code of $CHILD_ERROR});
}

{
  lives_ok { qx{perl $bin/npg_pipeline_seqchksum_comparator --id_run=1234 --archive_path=$tmp_dir/nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BAM_basecalls_20140815-114817/no_cal/archive --bam_basecall_path=$tmp_dir/nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BAM_basecalls_20140815-114817 --lanes=1 };} q{ran bin/npg_pipeline_seqchksum_comparator with analysis and bam_basecall_path};
  ok($CHILD_ERROR, qq{Return code of $CHILD_ERROR as no files found});
}

1;

