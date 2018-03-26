use strict;
use warnings;
use English qw{-no_match_vars};
use Test::More tests => 8;
use Test::Exception;
use Cwd;

use t::util;

my $bin = cwd() . q[/bin];
local $ENV{PATH} = join q[:], $bin, $ENV{PATH};

local $ENV{'http_proxy'} = q[http://wibble];
local $ENV{'no_proxy'}   = q[];

{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q{/does/not/exist.csv};

  my $util = t::util->new();
  $util->set_rta_staging_analysis_area();
  my $tmp_dir = $util->temp_directory();

  my $out = `$bin/npg_pipeline_central --spider --no_bsub --no_sf_resource --runfolder_path $tmp_dir/nfs/sf45/IL2/analysis/123456_IL2_1234 --function_order dodo 2>&1`;
  like($out,
  qr/Error initializing pipeline: Error while spidering/,
  'error in spidering when pre-set samplesheet does not exist');
}

{
  my $util = t::util->new();
  $util->set_rta_staging_analysis_area();
  my $tmp_dir = $util->temp_directory();

  my $out = `$bin/npg_pipeline_central --no-spider --no_bsub --no_sf_resource --runfolder_path $tmp_dir/nfs/sf45/IL2/analysis/123456_IL2_1234 --function_order dodo 2>&1`;
  like($out,
  qr/Handler for 'dodo' is not registered/,
  'error when function does not exist');
}

{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q{t/data/samplesheet_1234.csv};
  local $ENV{NPG_WEBSERVICE_CACHE_DIR}    = q{t}; # no chache here

  my $util = t::util->new();
  $util->set_rta_staging_analysis_area();
  my $tmp_dir = $util->temp_directory();
 
  lives_ok { qx{
    $bin/npg_pipeline_post_qc_review --no_bsub --no_sf_resource --runfolder_path $tmp_dir/nfs/sf45/IL2/analysis/123456_IL2_1234};}
    q{ran bin/npg_pipeline_post_qc_review};
  ok(!$CHILD_ERROR, qq{Return code of $CHILD_ERROR});

  lives_ok { qx{
    $bin/npg_pipeline_post_qc_review --no_bsub --no_sf_resource --runfolder_path $tmp_dir/nfs/sf45/IL2/analysis/123456_IL2_1234  --function_list some}; }
    q{ran bin/npg_pipeline_post_qc_review with non-exisitng function list};
  ok($CHILD_ERROR, qq{Child error $CHILD_ERROR});
}

{
  my $util = t::util->new();
  $util->set_rta_staging_analysis_area();
  my $tmp_dir = $util->temp_directory();

  lives_ok { qx{$bin/npg_pipeline_seqchksum_comparator --id_run=1234 --archive_path=$tmp_dir/nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BAM_basecalls_20140815-114817/no_cal/archive --bam_basecall_path=$tmp_dir/nfs/sf45/IL2/analysis/123456_IL2_1234/Data/Intensities/BAM_basecalls_20140815-114817 --lanes=1 };} q{ran bin/npg_pipeline_seqchksum_comparator with analysis and bam_basecall_path};
  ok($CHILD_ERROR, qq{Return code of $CHILD_ERROR as no files found});
}

1;
