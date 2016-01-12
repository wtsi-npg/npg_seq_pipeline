use strict;
use warnings;
use English qw{-no_match_vars};
use Test::More tests => 12;
use Test::Exception;
use t::util;
use Cwd;

local $ENV{PATH} = join q[:], q[t/bin], q[t/bin/software/solexa/bin], $ENV{PATH};
local $ENV{http_proxy} = 'http://wibble';
local $ENV{no_proxy} = q[];

my $util = t::util->new();

my $curdir = cwd();
my $bin = $curdir . q[/bin];

# Script failures
{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q{/does/not/exist.csv};

  my $util = t::util->new();
  $util->set_rta_staging_analysis_area();
  my $tmp_dir = $util->temp_directory();
  local $ENV{TEST_DIR} = $tmp_dir;

  my $out = `perl $bin/npg_pipeline_central --spider --no_bsub --runfolder_path $tmp_dir/nfs/sf45/IL2/analysis/123456_IL2_1234 --function_order dodo`;
  like($out,
  qr/Error initializing pipeline: Error while spidering/,
  'error in spidering when pre-set samplesheet does not exist');
}

{
  my $util = t::util->new();
  $util->set_rta_staging_analysis_area();
  my $tmp_dir = $util->temp_directory();
  local $ENV{TEST_DIR} = $tmp_dir;

  my $out = `perl $bin/npg_pipeline_central --no-spider --no_bsub --runfolder_path $tmp_dir/nfs/sf45/IL2/analysis/123456_IL2_1234 --function_order dodo`;
  like($out,
  qr/Can't locate object method "dodo" via package "npg_pipeline::pluggable::harold::central"/,
  'error when function does not exist');
}

# Script passes
{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q{t/data/samplesheet_1234.csv};
  local $ENV{NPG_WEBSERVICE_CACHE_DIR}    = q{t}; # no chache here

  my $util = t::util->new();
  $util->set_rta_staging_analysis_area();
  my $tmp_dir = $util->temp_directory();
  local $ENV{TEST_DIR} = $tmp_dir;
 
  lives_ok { diag qx{
    perl $bin/npg_pipeline_post_qc_review --runfolder_path $tmp_dir/nfs/sf45/IL2/analysis/123456_IL2_1234};}
    q{ran bin/npg_pipeline_post_qc_review};
  ok(!$CHILD_ERROR, qq{Return code of $CHILD_ERROR});

  lives_ok { diag qx{
    perl $bin/npg_pipeline_post_qc_review --runfolder_path $tmp_dir/nfs/sf45/IL2/analysis/123456_IL2_1234  --gclp}; }
    q{ran bin/npg_pipeline_post_qc_review with gclp flag};
  ok(!$CHILD_ERROR, qq{Return code of $CHILD_ERROR});

  lives_ok { diag qx{
    perl $bin/npg_pipeline_post_qc_review --runfolder_path $tmp_dir/nfs/sf45/IL2/analysis/123456_IL2_1234  --function_list gclp}; }
    q{ran bin/npg_pipeline_post_qc_review with gclp function list};
  ok(!$CHILD_ERROR, qq{Return code of $CHILD_ERROR});

  lives_ok { diag qx{
    perl $bin/npg_pipeline_post_qc_review --runfolder_path $tmp_dir/nfs/sf45/IL2/analysis/123456_IL2_1234  --function_list some}; }
    q{ran bin/npg_pipeline_post_qc_review with non-exisitng function list};
  ok($CHILD_ERROR, qq{Child error $CHILD_ERROR});
}

{
  my $util = t::util->new();
  $util->set_rta_staging_analysis_area();
  my $tmp_dir = $util->temp_directory();
  local $ENV{TEST_DIR} = $tmp_dir;

  lives_ok { qx{perl $bin/npg_pipeline_seqchksum_comparator --id_run=1234 --archive_path=$tmp_dir/nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BAM_basecalls_20140815-114817/no_cal/archive --bam_basecall_path=$tmp_dir/nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BAM_basecalls_20140815-114817 --lanes=1 };} q{ran bin/npg_pipeline_seqchksum_comparator with analysis and bam_basecall_path};
  ok($CHILD_ERROR, qq{Return code of $CHILD_ERROR as no files found});
}

1;
