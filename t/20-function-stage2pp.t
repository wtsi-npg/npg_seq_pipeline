use strict;
use warnings;
use Test::More tests => 5;
use Test::Exception;
use File::Temp qw/tempdir/;
use File::Path qw/make_path/;
use File::Copy;
use Log::Log4perl qw/:levels/;
use File::Slurp;

my $dir = tempdir( CLEANUP => 1);

use_ok('npg_pipeline::function::stage2pp');

for my $name (qw/npg_simple_robo4artic nextflow/) {
  my $exec = join q[/], $dir, $name;
  open my $fh1, '>', $exec or die 'failed to open file for writing';
  print $fh1 'echo "$name mock"' or warn 'failed to print';
  close $fh1 or warn 'failed to close file handle';
  chmod 755, $exec;
}
local $ENV{PATH} = join q[:], $dir, $ENV{PATH};

my $logfile = join q[/], $dir, 'logfile';

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => $logfile,
                          utf8   => 1});

# setup reference repository
my $ref_dir = join q[/],$dir,'references';
my $base_dir = join q[/], $ref_dir, 'SARS-CoV-2/MN908947.3/all';
my $fasta_dir = "$base_dir/fasta";
make_path $fasta_dir;
my $ref_fasta = "$fasta_dir/MN908947.3.fa";
open my $fh, '>', $ref_fasta or die 'failed to open file for writing';
print $fh 'test reference' or warn 'failed to print';
close $fh or warn 'failed to close file handle';
my $bwa_dir = "$base_dir/bwa0_6";
make_path $bwa_dir;

# setup primer panel repository
my $pp_repository = join q[/],$dir,'primer_panel', 'nCoV-2019';
my @dirs = map { join q[/], $pp_repository, $_ } qw/default V2 V3/; 
@dirs = map {join q[/], $_, 'SARS-CoV-2/MN908947.3'} @dirs;
for (@dirs) {
  make_path $_;
  my $bed = join q[/], $_, 'nCoV-2019.bed';
  open my $fh, '>', $bed or die 'failed to open file for writing';
  print $fh 'test bed file' or warn 'failed to print';
  close $fh or warn 'failed to close file handle';
}

# setup runfolder
my $runfolder_path = join q[/], $dir, 'novaseq', '180709_A00538_0010_BH3FCMDRXX';
my $archive_path = join q[/], $runfolder_path,
  'Data/Intensities/BAM_basecalls_20180805-013153/no_cal/archive';
my $no_archive_path = join q[/], $runfolder_path,
'Data/Intensities/BAM_basecalls_20180805-013153/no_archive';
my $pp_archive_path = join q[/], $runfolder_path,
'Data/Intensities/BAM_basecalls_20180805-013153/pp_archive';

make_path $archive_path;
make_path $no_archive_path;
copy('t/data/novaseq/180709_A00538_0010_BH3FCMDRXX/RunInfo.xml', "$runfolder_path/RunInfo.xml") or die
'Copy failed';
copy('t/data/novaseq/180709_A00538_0010_BH3FCMDRXX/RunParameters.xml', "$runfolder_path/runParameters.xml")
or die 'Copy failed';

my $timestamp = q[20180701-123456];
my $repo_dir = q[t/data/portable_pipelines/ncov2019-artic-nf/cf01166c42a];
my $product_conf = qq[$repo_dir/product_release.yml];

subtest 'error on missing data in LIMS' => sub {
  plan tests => 2;

  my $text = read_file(q[t/data/samplesheet_33990.csv]);
  my $file = qq[$dir/samplesheet_33990.csv];
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = $file;

  my $text1 = $text;
  $text1 =~ s/SARS-CoV-2\ \(MN908947\.3\)//g;
  write_file($file, $text1);
  
  my $ppd = npg_pipeline::function::stage2pp->new(
    product_conf_file_path => $product_conf,
    archive_path           => $archive_path,
    runfolder_path         => $runfolder_path,
    id_run                 => 26291,
    timestamp              => $timestamp,
    repository             => $dir);
  throws_ok { $ppd->create } 
    qr/bwa reference is not found for/,
    'error if reference is not defined for one of the products';

  $text1 = $text;
  $text1 =~ s/nCoV-2019\/V3//g;
  write_file($file, $text1);

  $ppd = npg_pipeline::function::stage2pp->new(
    product_conf_file_path => $product_conf,
    archive_path           => $archive_path,
    runfolder_path         => $runfolder_path,
    id_run                 => 26291,
    timestamp              => $timestamp,
    repository             => $dir);
  throws_ok { $ppd->create } 
    qr/Bed file is not found for/,
    'error if primer panel is not defined for one of the products';
};

subtest 'definition generation' => sub {
  plan tests => 16;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_33990.csv];
  my $nf_dir = q[t/data/portable_pipelines/ncov2019-artic-nf/cf01166c42a];
  my @out_dirs = map { qq[$pp_archive_path/plex] . $_ . q[/ncov2019_artic_nf/cf01166c42a]}
                 qw/1 2 3/;
  map { ok (!(-e $_), "output dir $_ does not exists") } @out_dirs;

  my $ppd = npg_pipeline::function::stage2pp->new(
    product_conf_file_path => $product_conf,
    archive_path           => $archive_path,
    runfolder_path         => $runfolder_path,
    id_run                 => 26291,
    timestamp              => $timestamp,
    repository             => $dir);

  my $ds = $ppd->create;

  map { ok (-d $_, "output dir $_ exists") } @out_dirs;

  is (scalar @{$ds}, 3, '3 definitions are returned');
  isa_ok ($ds->[0], 'npg_pipeline::function::definition');
  is ($ds->[0]->excluded, undef, 'function is not excluded');

  my $command =          "($dir/nextflow run $nf_dir " .
                         '-profile singularity,sanger ' .
                         '--illumina --cram --prefix 26291 ' .
                         "--ref $bwa_dir/MN908947.3.fa " .
                         "--bed $pp_repository/default/SARS-CoV-2/MN908947.3/nCoV-2019.bed";

  my $summary_file = join q[/], $out_dirs[0], '26291.qc.csv';
  my $c  = "$command --directory $no_archive_path/plex1/stage1 --outdir $out_dirs[0])" .
           ' && ' .
           "( ([ -f $summary_file ] && echo 'Found $summary_file') || (echo 'Not found $summary_file' && /bin/false) )" .
           ' && ' .
           "(cat $summary_file | $dir/npg_simple_robo4artic $archive_path/plex1/qc)";
  my $c0 = $c;
  is ($ds->[0]->command, $c, 'correct command for plex 1');
  is ($ds->[0]->job_name, 'stage2pp_ncov2cf011_26291', 'job name');

  my $c_copy = $command;
  $c_copy =~ s/default/V2/;
  $summary_file = join q[/], $out_dirs[1], '26291.qc.csv';
  $c  =    "$c_copy --directory $no_archive_path/plex2/stage1 --outdir $out_dirs[1])"  .
           ' && ' .
           "( ([ -f $summary_file ] && echo 'Found $summary_file') || (echo 'Not found $summary_file' && /bin/false) )" .
           ' && ' .
           "(cat $summary_file | $dir/npg_simple_robo4artic $archive_path/plex2/qc)";
  is ($ds->[1]->command, $c, 'correct command for plex 2');

  $c_copy = $command;
  $c_copy =~ s/default/V3/;
  $summary_file = join q[/], $out_dirs[2], '26291.qc.csv';
  $c  =    "$c_copy --directory $no_archive_path/plex3/stage1 --outdir $out_dirs[2])"  .
           ' && ' .
           "( ([ -f $summary_file ] && echo 'Found $summary_file') || (echo 'Not found $summary_file' && /bin/false) )" .
           ' && ' .
           "(cat $summary_file | $dir/npg_simple_robo4artic $archive_path/plex3/qc)";
  is ($ds->[2]->command, $c, 'correct command for plex 3');

  $ppd = npg_pipeline::function::stage2pp->new(
    product_conf_file_path => qq[$repo_dir/../v.3/product_release_two_pps.yml],
    archive_path           => $archive_path,
    runfolder_path         => $runfolder_path,
    id_run                 => 26291,
    timestamp              => $timestamp,
    repository             => $dir);

  $ds = $ppd->create;
  is (scalar @{$ds}, 6, '6 definitions are returned');
  is ($ds->[0]->command, $c0, 'correct command for plex 1');
  $c0 =~ s/cf01166c42a/v.3/g;
  is ($ds->[1]->command, $c0, 'correct command for plex 1 for the second pipeline');
};

subtest 'step skipped' => sub {
  plan tests => 5;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_33990.csv];

  my $ppd = npg_pipeline::function::stage2pp->new(
    product_conf_file_path => qq[$repo_dir/product_release_no_pp.yml],
    archive_path           => $archive_path,
    runfolder_path         => $runfolder_path,
    id_run                 => 26291,
    timestamp              => $timestamp,
    repository             => $dir);

  my $ds = $ppd->create;
  is (scalar @{$ds}, 1, 'one definition is returned');
  isa_ok ($ds->[0], 'npg_pipeline::function::definition');
  is ($ds->[0]->excluded, 1, 'function is excluded');

 $ppd = npg_pipeline::function::stage2pp->new(
    product_conf_file_path => qq[$repo_dir/product_release_no_study.yml],
    archive_path           => $archive_path,
    runfolder_path         => $runfolder_path,
    id_run                 => 26291,
    timestamp              => $timestamp,
    repository             => $dir);

  $ds = $ppd->create;
  is (scalar @{$ds}, 1, 'one definition is returned');
  is ($ds->[0]->excluded, 1, 'function is excluded');
};

subtest 'skip unknown pipeline' => sub {
  plan tests => 2;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_33990.csv];
  my $ppd = npg_pipeline::function::stage2pp->new(
    product_conf_file_path => qq[$repo_dir/product_release_unknown_pp.yml],
    archive_path           => $archive_path,
    runfolder_path         => $runfolder_path,
    id_run                 => 26291,
    timestamp              => $timestamp,
    repository             => $dir);

  my $ds;
  lives_ok { $ds = $ppd->create }
    'no error when job definition creation for a pipeline is not implemented';
  is (scalar @{$ds}, 3, '3 definitions are returned');
};

1;
