use strict;
use warnings;
use Test::More tests => 6;
use Test::Exception;
use File::Temp qw/tempdir/;
use File::Path qw/make_path/;
use File::Copy;
use Log::Log4perl qw/:levels/;
use File::Slurp;

my $dir = tempdir( CLEANUP => 1);

use_ok('npg_pipeline::function::stage2pp');

for my $name (qw/npg_autoqc_generic4artic
                 npg_simple_robo4artic
                 nextflow
                 samtools
                 plot-ampliconstats
                 qc/) {
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

subtest 'definition generation, ncov2019_artic_nf pp' => sub {
  plan tests => 21;

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

  is ($ppd->pipeline_type, 'stage2pp', 'default pipeline type');

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
           "(cat $summary_file | $dir/npg_simple_robo4artic $archive_path/plex1/qc)" .
           ' && ' .
           "(cat $summary_file | $dir/npg_autoqc_generic4artic --qc_out $archive_path/plex1/qc --pp_version cf01166c42a)";
  my $c0 = $c;
  is ($ds->[0]->command, $c, 'correct command for plex 1');
  is ($ds->[0]->job_name, 'stage2pp_ncov2cf011_26291', 'job name');
  is ($ds->[0]->memory, 5000, 'memory');
  is_deeply ($ds->[0]->num_cpus, [4], 'number of CPUs');

  my $c_copy = $command;
  $c_copy =~ s/default/V2/;
  $summary_file = join q[/], $out_dirs[1], '26291.qc.csv';
  $c  =    "$c_copy --directory $no_archive_path/plex2/stage1 --outdir $out_dirs[1])"  .
           ' && ' .
           "( ([ -f $summary_file ] && echo 'Found $summary_file') || (echo 'Not found $summary_file' && /bin/false) )" .
           ' && ' .
           "(cat $summary_file | $dir/npg_simple_robo4artic $archive_path/plex2/qc)" .
           ' && ' .
           "(cat $summary_file | $dir/npg_autoqc_generic4artic --qc_out $archive_path/plex2/qc --pp_version cf01166c42a)";
  is ($ds->[1]->command, $c, 'correct command for plex 2');

  $c_copy = $command;
  $c_copy =~ s/default/V3/;
  $summary_file = join q[/], $out_dirs[2], '26291.qc.csv';
  $c  =    "$c_copy --directory $no_archive_path/plex3/stage1 --outdir $out_dirs[2])"  .
           ' && ' .
           "( ([ -f $summary_file ] && echo 'Found $summary_file') || (echo 'Not found $summary_file' && /bin/false) )" .
           ' && ' .
           "(cat $summary_file | $dir/npg_simple_robo4artic $archive_path/plex3/qc)" .
           ' && ' .
           "(cat $summary_file | $dir/npg_autoqc_generic4artic --qc_out $archive_path/plex3/qc --pp_version cf01166c42a)";
  is ($ds->[2]->command, $c, 'correct command for plex 3');

  $ppd = npg_pipeline::function::stage2pp->new(
    product_conf_file_path => $product_conf,
    pipeline_type          => 'stage2pp',
    archive_path           => $archive_path,
    runfolder_path         => $runfolder_path,
    id_run                 => 26291,
    merge_lanes            => 0,
    timestamp              => $timestamp,
    repository             => $dir);
  $ds = $ppd->create;
  is (scalar @{$ds}, 6, '6 definitions are returned');
  my $out_dir = qq[$pp_archive_path/lane1/plex1/ncov2019_artic_nf/cf01166c42a];
  $summary_file = join q[/], $out_dir, '26291.qc.csv';
  $c  = "$command --directory $no_archive_path/lane1/plex1/stage1 --outdir $out_dir)" .
        ' && ' .
        "( ([ -f $summary_file ] && echo 'Found $summary_file') || (echo 'Not found $summary_file' && /bin/false) )" .
        ' && ' .
        "(cat $summary_file | $dir/npg_simple_robo4artic $archive_path/lane1/plex1/qc)" .
        ' && ' .
        "(cat $summary_file | $dir/npg_autoqc_generic4artic --qc_out $archive_path/lane1/plex1/qc " .
        "--rpt_list 26291:1:1 --tm_json_file $archive_path/lane1/qc/26291_1.tag_metrics.json --pp_version cf01166c42a)";
  is ($ds->[0]->command, $c, 'correct command for unmerged plex 1');

  $ppd = npg_pipeline::function::stage2pp->new(
    product_conf_file_path => qq[$repo_dir/../v.3/product_release_two_pps.yml],
    pipeline_type          => 'stage2pp',
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

subtest q(definition generation, 'ncov2019_artic_nf ampliconstats' pp) => sub {
  plan tests => 30;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_33990.csv];

  my $ppd = npg_pipeline::function::stage2pp->new(
    product_conf_file_path => $product_conf,
    pipeline_type          => 'stage2App',
    archive_path           => $archive_path,
    runfolder_path         => $runfolder_path,
    id_run                 => 26291,
    merge_lanes            => 1,
    timestamp              => $timestamp,
    repository             => $dir);

  is ($ppd->pipeline_type, 'stage2App', 'pipeline type');

  my $ds = $ppd->create;
  is (@{$ds}, 1, 'one definition is returned');
  is ($ds->[0]->excluded, 1, 'merged product, the function is excluded');

  my @commands = ();
  my @replacement_files = ();
  my @astats_sections = qw(FREADS FPCOV-1 FPCOV-10 FPCOV-20 FPCOV-100);

  my $count = 0;
  for my $p ((1, 2, 1)) {

    $count++;
    my @s = @astats_sections;
    $count == 3 and pop @s;
    my $sections = join q[ ], map { q[--ampstats_section ] . $_ } @s;
 
    my $pp_path = qq(${pp_archive_path}/lane${p}) .
      qq(/ncov2019_artic_nf_ampliconstats/0.1/);
    my $glob = $pp_archive_path . qq(/lane${p}) .
      q(/plex*/ncov2019_artic_nf/cf01166c42a) .
      q(/ncovIlluminaCram_ncovIllumina_sequenceAnalysis_trimPrimerSequences) .
      q(/*primertrimmed.sorted.bam);
    my $astats_file = $pp_path . qq(26291_${p}.astats);
    my $replacement_map_file = $pp_path . q(replacement_map.txt);
    push @replacement_files, $replacement_map_file;
    push @commands,
                '(' .
      qq(! ls $glob) .
                ') || (' .
                '(' .
      $dir . q(/samtools ampliconstats -@1 -t 50 -d 1,10,20) .
      ($count == 3 ? q( ) : q(,100 )) .
      $dir . q(/primer_panel/nCoV-2019/default/SARS-CoV-2/MN908947.3/nCoV-2019.bed ) .
      $glob . q( > ) . $astats_file .
                ') && (' .
      q[perl -e 'use strict;use warnings;use File::Slurp; my%h=map{(split qq(\t))} (read_file shift, chomp=>1); map{print} map{s/\b(?:\w+_)?(\d+_\d(#\d+))\S*\b/($h{$1} || q{unknown}).$2/e; $_} (read_file shift)'] .
      qq( $replacement_map_file $astats_file | ) .
      q(plot-ampliconstats -page 48 ) .
      $archive_path . qq(/lane${p}/qc/ampliconstats/26291_${p}) .
                ') && (' .
      $dir . q(/qc --check generic --spec ampliconstats ) .
      qq(--rpt_list 26291:${p} --input_files $astats_file ) .
      q(--pp_name 'ncov2019-artic-nf ampliconstats' --pp_version 0.1 ) .
      qq($sections ) .
      q(--qc_out ) . $archive_path . qq(/lane${p}/qc ) .
      q(--sample_qc_out ') . $archive_path . qq(/lane${p}/plex*/qc') .
                '))';
  }

  @commands = map { [(split q[ ])] } @commands;

  ok (!-e $replacement_files[0], 'replacement file for lane 1 does not exist');
  ok (!-e $replacement_files[1], 'replacement file for lane 2 does not exist');  

  $ppd = npg_pipeline::function::stage2pp->new(
    product_conf_file_path => $product_conf,
    pipeline_type          => 'stage2App',
    archive_path           => $archive_path,
    runfolder_path         => $runfolder_path,
    id_run                 => 26291,
    merge_lanes            => 0,
    timestamp              => $timestamp,
    repository             => $dir);

  $ds = $ppd->create;
  is (@{$ds}, 2, 'two definitions are returned');
  
  my $d = $ds->[0];
  isa_ok ($d, 'npg_pipeline::function::definition');
  ok (!$d->excluded, 'the function is not excluded');
  ok ($d->composition, 'composition is defined');
  is ($d->composition->num_components, 1, 'composition is for one component');
  my $component = $d->composition->get_component(0);
  ok ((($component->position == 1) and not defined $component->tag_index),
    'definition for lane 1 job'); 
  is_deeply ([(split q[ ], $d->command)], $commands[0], 'correct command for lane 1');
  ok (-f $replacement_files[0], 'lane 1 replacement file created');
  is (read_file($replacement_files[0]), join(qq[\n] ,
    "26291_1#1\tA1",
    "26291_1#2\tB1",
    "26291_1#3\tC1"), 'lane 1 replacement file content is correct');
  is ($d->job_name, 'stage2App_ncov20.1_26291', 'job name');
  is ($d->memory, 1000, 'memory');
  is_deeply ($d->num_cpus, [2], 'number of CPUs');

  $d = $ds->[1];
  isa_ok ($d, 'npg_pipeline::function::definition');
  ok (!$d->excluded, 'the function is not excluded');
  ok ($d->composition, 'composition is defined');
  is ($d->composition->num_components, 1, 'composition is for one component');
  $component = $d->composition->get_component(0);
  ok ((($component->position == 2) and not defined $component->tag_index),
    'definition for lane 2 job');
  ok (-f $replacement_files[1], 'lane 2 replacement file created');
  is (read_file($replacement_files[1]), join(qq[\n] ,
    "26291_2#1\tA1",
    "26291_2#2\tB1",
    "26291_2#3\tC1"), 'lane 2 replacement file content is correct');
  is_deeply ([(split q[ ], $d->command)], $commands[1], 'correct command for lane 2');
  is ($d->job_name, 'stage2App_ncov20.1_26291', 'job name');
  is ($d->memory, 1000, 'memory');
  is_deeply ($d->num_cpus, [2], 'number of CPUs');

  $ppd = npg_pipeline::function::stage2pp->new(
    product_conf_file_path => qq[$repo_dir/product_release_explicit_astats_depth.yml],
    pipeline_type          => 'stage2App',
    archive_path           => $archive_path,
    runfolder_path         => $runfolder_path,
    id_run                 => 26291,
    merge_lanes            => 0,
    timestamp              => $timestamp,
    repository             => $dir);

  $ds = $ppd->create;
  is (@{$ds}, 2, 'two definitions are returned');
  $d = $ds->[0];
  is_deeply ([(split q[ ], $d->command)], $commands[2], 'correct command for lane 1');
};

1;
