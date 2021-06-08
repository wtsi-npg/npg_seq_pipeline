use strict;
use warnings;
use Test::More tests => 4;
use Test::Exception;
use File::Temp qw/tempdir/;
use File::Path qw/make_path/;
use File::Copy;
use Log::Log4perl qw/:levels/;
use File::Slurp;

my $dir = tempdir( CLEANUP => 1);

my $exec = join q[/], $dir, 'qc';
open my $fh1, '>', $exec or die 'failed to open file for writing';
print $fh1 'echo "$name mock"' or warn 'failed to print';
close $fh1 or warn 'failed to close file handle';
chmod 755, $exec;

local $ENV{PATH} = join q[:], $dir, $ENV{PATH};

my $logfile = join q[/], $dir, 'logfile';

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => $logfile,
                          utf8   => 1});

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

my $default = {
  default => {
    minimum_cpu => 4,
    memory => 5,
    fs_slots_num => 1
  }
};

use_ok('npg_pipeline::function::autoqc::generic');

subtest 'definition generation, artic spec' => sub {
  plan tests => 19;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_33990.csv];

  my $ag = npg_pipeline::function::autoqc::generic->new(
    product_conf_file_path => $product_conf,
    archive_path           => $archive_path,
    runfolder_path         => $runfolder_path,
    id_run                 => 26291,
    timestamp              => $timestamp,
    merge_lanes            => 0,
    spec                   => 'artic',
    portable_pipeline_name => 'ncov2019-artic-nf',
    resource               => $default
  );
  my $ds = $ag->create;
  is (scalar @{$ds}, 2, '2 definitions are returned');
  for my $p ((1,2)) {
    my $i = $p - 1;
    isa_ok ($ds->[$i], 'npg_pipeline::function::definition');
    is ($ds->[$i]->excluded, undef, 'function is not excluded');
    my $command = "$dir/qc --check generic --spec artic " .
      "--rpt_list 26291:$p " .
      q[--pp_name 'ncov2019-artic-nf' --pp_version 'cf01166c42a' ] .
      "--tm_json_file '$archive_path/lane$p/qc/26291_$p.tag_metrics.json' " .
      "--input_files_glob '$pp_archive_path/lane$p/plex*/" .
        q[ncov2019_artic_nf/cf01166c42a/*.qc.csv' ] .
      "--sample_qc_out '$archive_path/lane$p/plex*/qc'";
    is ($ds->[$i]->command, $command, "correct command for lane $p");
    is ($ds->[$i]->job_name, 'qc_generic_artic_26291_20180701-123456', 'job name');
    my $c = $ds->[$i]->composition;
    isa_ok ($c, 'npg_tracking::glossary::composition');
    is ($c->num_components, 1, 'one-component composition');
    my $comp = $c->get_component(0);
    is ($comp->id_run, 26291, 'component - run 26291');
    is ($comp->position, $p, "component: position $p");
    is ($comp->tag_index, undef, 'component: tag index is undefined');
  }
};

subtest 'step skipped' => sub {
  plan tests => 7;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_33990.csv];

  # pipeline name mismatch
  my $ag = npg_pipeline::function::autoqc::generic->new(
    product_conf_file_path => $product_conf,
    archive_path           => $archive_path,
    runfolder_path         => $runfolder_path,
    id_run                 => 26291,
    timestamp              => $timestamp,
    merge_lanes            => 0,
    spec                   => 'artic',
    portable_pipeline_name => 'ncov2019_artic_nf',
    resource               => $default
  );
  my $ds = $ag->create;
  is (scalar @{$ds}, 1, 'one definition is returned');
  isa_ok ($ds->[0], 'npg_pipeline::function::definition');
  is ($ds->[0]->excluded, 1, 'function is excluded');

  $ag = npg_pipeline::function::autoqc::generic->new(
    product_conf_file_path => qq[$repo_dir/product_release_no_pp.yml],
    archive_path           => $archive_path,
    runfolder_path         => $runfolder_path,
    id_run                 => 26291,
    timestamp              => $timestamp,
    merge_lanes            => 0,
    spec                   => 'artic',
    portable_pipeline_name => 'ncov2019_artic_nf',
    resource               => $default
  );
  $ds = $ag->create;
  is (scalar @{$ds}, 1, 'one definition is returned');
  is ($ds->[0]->excluded, 1, 'function is excluded');

  $ag = npg_pipeline::function::autoqc::generic->new(
    product_conf_file_path => $product_conf,
    archive_path           => $archive_path,
    runfolder_path         => $runfolder_path,
    id_run                 => 26291,
    timestamp              => $timestamp,
    merge_lanes            => 0,
    spec                   => 'artic',
    portable_pipeline_name => 'ncov2019_artic_nf',
    resource               => $default
  );

  $ag = npg_pipeline::function::autoqc::generic->new(
    product_conf_file_path => qq[$repo_dir/product_release_no_study.yml],
    archive_path           => $archive_path,
    runfolder_path         => $runfolder_path,
    id_run                 => 26291,
    timestamp              => $timestamp,
    merge_lanes            => 0,
    spec                   => 'artic',
    portable_pipeline_name => 'ncov2019_artic_nf',
    resource               => $default
  );
  $ds = $ag->create;
  is (scalar @{$ds}, 1, 'one definition is returned');
  is ($ds->[0]->excluded, 1, 'function is excluded');
};

subtest 'definition generation, ampliconstats spec' => sub {
  plan tests => 19;

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/samplesheet_33990.csv];

  my $ag = npg_pipeline::function::autoqc::generic->new(
    product_conf_file_path => $product_conf,
    archive_path           => $archive_path,
    runfolder_path         => $runfolder_path,
    id_run                 => 26291,
    merge_lanes            => 0,
    timestamp              => $timestamp,
    spec                   => 'ampliconstats',
    portable_pipeline_name => 'ncov2019-artic-nf ampliconstats',
    resource               => $default
  );
  my $ds = $ag->create;
  is (scalar @{$ds}, 2, 'two definitions are returned');
  for my $p ((1, 2)) {
    my $i = $p - 1;
    isa_ok ($ds->[$i], 'npg_pipeline::function::definition');
    is ($ds->[$i]->excluded, undef, 'function is not excluded');
    my $command = "$dir/qc --check generic --spec ampliconstats " .
      "--rpt_list 26291:$p " .
      q[--pp_name 'ncov2019-artic-nf ampliconstats' ] .
      q[--pp_version '0.1' ] .
      q[--ampstats_section 'FREADS' ] .
      q[--ampstats_section 'FPCOV-1' ] .
      q[--ampstats_section 'FPCOV-10' ] .
      q[--ampstats_section 'FPCOV-20' ] .
      q[--ampstats_section 'FPCOV-100' ] .
      "--input_files '$pp_archive_path/lane$p/ncov2019_artic_nf_ampliconstats/0.1/26291_$p.astats' " .
      "--qc_out '$archive_path/lane$p/qc' " .
      "--sample_qc_out '$archive_path/lane$p/plex*/qc'";
    is ($ds->[$i]->command, $command, "correct command for lane $p");
    is ($ds->[$i]->job_name,
      'qc_generic_ampliconstats_26291_20180701-123456', 'job name');
    my $c = $ds->[$i]->composition;
    isa_ok ($c, 'npg_tracking::glossary::composition');
    is ($c->num_components, 1, 'one-component composition');
    my $comp = $c->get_component(0);
    is ($comp->id_run, 26291, 'component - run 26291');
    is ($comp->position, $p, "component: position $p");
    is ($comp->tag_index, undef, 'component: tag index is undefined');
  }
};
1;
