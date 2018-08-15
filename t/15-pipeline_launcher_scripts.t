use strict;
use warnings;
use English qw{-no_match_vars};
use Test::More tests => 10;
use Test::Exception;
use Cwd;

use t::util;

my $util    = t::util->new();
my $tmp_dir = $util->temp_directory();
my $bin     = cwd() . q[/bin];

my @tools = map { "$tmp_dir/$_" } qw/bamtofastq blat norm_fit/;
foreach my $tool (@tools) {
  open my $fh, '>', $tool or die 'cannot open file for writing';
  print $fh $tool or die 'cannot print';
  close $fh or warn 'failed to close file handle';
}
chmod 0755, @tools;

local $ENV{'PATH'}       = join q[:], $tmp_dir, $bin, $ENV{'PATH'};
local $ENV{'http_proxy'} = q[http://wibble];
local $ENV{'no_proxy'}   = q[];

my $rf = $util->analysis_runfolder_path;
my $bbp = "$rf/bam_basecall_path";
{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q{/does/not/exist.csv};
  $util->set_rta_staging_analysis_area();

  my $out = `$bin/npg_pipeline_central --spider --no_bsub --no_sf_resource --runfolder_path $rf --function_order dodo 2>&1`;
  like($out,
  qr/Error initializing pipeline: Error while spidering/,
  'error in spidering when pre-set samplesheet does not exist');
}

{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q{t/data/samplesheet_1234.csv};
  $util->set_rta_staging_analysis_area();

  $util->create_run_info();

  my $out = `$bin/npg_pipeline_central --no-spider --no_bsub --no_sf_resource --runfolder_path $rf --bam_basecall_path $bbp --function_order dodo 2>&1`;
  like($out,
  qr/Handler for 'dodo' is not registered/,
  'error when function does not exist');
}

{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q{t/data/samplesheet_1234.csv};

  lives_ok { qx{
    $bin/npg_pipeline_post_qc_review --no_bsub --no_sf_resource --runfolder_path $rf --bam_basecall_path $bbp};}
    q{ran bin/npg_pipeline_post_qc_review};
  ok(!$CHILD_ERROR, qq{Return code of $CHILD_ERROR});

  lives_ok { qx{
    $bin/npg_pipeline_post_qc_review --no_bsub --no_sf_resource --runfolder_path $rf  --bam_basecall_path $bbp --function_list some}; }
    q{ran bin/npg_pipeline_post_qc_review with non-exisitng function list};
  ok($CHILD_ERROR, qq{Child error $CHILD_ERROR});
}

{
  $util->set_rta_staging_analysis_area();

  $util->create_run_info();

  lives_ok { qx{$bin/npg_pipeline_seqchksum_comparator --id_run=1234 --archive_path=$rf/Data/Intensities/BAM_basecalls_20140815-114817/no_cal/archive --bam_basecall_path=$rf/Data/Intensities/BAM_basecalls_20140815-114817 --lanes=1 };} q{ran bin/npg_pipeline_seqchksum_comparator with analysis and bam_basecall_path};
  ok($CHILD_ERROR, qq{Return code of $CHILD_ERROR as no files found});
}

{
  `bin/npg_pipeline_preexec_references --repository t/data/sequence/refs 2>/dev/null`;
  ok( $CHILD_ERROR, qq{failed as could not locate references directory - $CHILD_ERROR} );

  qx{bin/npg_pipeline_preexec_references --repository t/data/sequence};
  ok( ! $CHILD_ERROR, q{script runs OK} );
}

1;
