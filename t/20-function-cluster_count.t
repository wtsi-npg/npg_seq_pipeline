use strict;
use warnings;
use English qw{-no_match_vars};
use Test::More tests => 43;
use Test::Exception;
use Log::Log4perl qw(:levels);
use t::util;

use_ok( q{npg_pipeline::function::cluster_count} );

my $util = t::util->new();
my $dir = $util->temp_directory();
local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q[t/data];

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $dir, 'logfile'),
                          utf8   => 1});

$util->create_multiplex_analysis();
my $analysis_runfolder_path = $util->analysis_runfolder_path();
my $bam_basecall_path = $util->standard_bam_basecall_path();
my $recalibrated_path = $util->standard_analysis_recalibrated_path();
my $archive_path = $recalibrated_path . q{/archive};

{
  my $object;
  lives_ok {
    $object = npg_pipeline::function::cluster_count->new(
      run_folder        => q{123456_IL2_1234},
      runfolder_path    => $analysis_runfolder_path,
      bam_basecall_path => $bam_basecall_path,
      id_run            => 1234,
      timestamp         => q{20100907-142417},
    );
  } q{obtain object ok};

  isa_ok( $object, q{npg_pipeline::function::cluster_count});

  my $da = $object->create();
  ok ($da && @{$da} == 8, 'an array with eight definitions is returned');

  my $d = $da->[0];
    is ($d->created_by, 'npg_pipeline::function::cluster_count',
    'created_by is correct');
  is ($d->created_on, $object->timestamp, 'created_on is correct');
  is ($d->identifier, 1234, 'identifier is set correctly');
  ok (!$d->excluded, 'step not excluded');
  ok (!$d->has_num_cpus, 'number of cpus is not set');
  ok (!$d->has_memory,'memory is not set');
  ok ($d->has_composition, 'composition is set');
  is ($d->composition->num_components, 1, 'one componet in a composition');
  is ($d->composition->get_component(0)->position, 1, 'correct position');
  ok (!defined $d->composition->get_component(0)->tag_index,
    'tag index is not defined');
  ok (!defined $d->composition->get_component(0)->subset,
    'subset is not defined');
  lives_ok {$d->freeze()} 'definition can be serialized to JSON';

  my $values = {};
  map {$values->{ref $_} += 1} @{$da};
  is ($values->{'npg_pipeline::function::definition'}, 8,
    'eight definition objects returned');

  map {$values->{$_->job_name} += 1} @{$da};
  is ($values->{'npg_pipeline_check_cluster_count_1234_20100907-142417'}, 8,
    'the same job name for all definitions');
  
  map {$values->{$_->queue} += 1} @{$da};
  is ($values->{'default'}, 8, 'the same default queue for all definitions');
  
  is (join(q[ ], map {$_->composition->get_component(0)->position} @{$da}),
    '1 2 3 4 5 6 7 8', 'positions');

  my $command = sub {
    my $p = shift;
    return q{npg_pipeline_check_cluster_count --id_run=1234} .
      qq{ --position=$p --runfolder_path=$analysis_runfolder_path} .
      qq{ --qc_path=$archive_path/qc --bam_basecall_path=$bam_basecall_path};
  };
  
  for my $d (@{$da}) {
    my $position = $d->composition->get_component(0)->position;
    is ($d->command, $command->($position), 'correct command');
  }
}

{
  my $analysis_runfolder_path = 't/data/example_runfolder/121103_HS29_08747_B_C1BV5ACXX';
  my $bam_basecall_path = "$analysis_runfolder_path/Data/Intensities/BAM_basecalls_20130122-085552";
  my $archive_path = "$bam_basecall_path/PB_cal_bam/archive";

  my $object;
  lives_ok{
    $object = npg_pipeline::function::cluster_count->new(
      id_run => 8747,
      position => 1,
      runfolder_path => $analysis_runfolder_path,
      bam_basecall_path => $bam_basecall_path,
      archive_path => $archive_path,
    );
  } q{obtain object ok};

  is( $object->_bustard_pf_cluster_count(),  150694669, q{correct pf_cluster_count obtained from TileMetricsOut.bin}  );
  is( $object->_bustard_raw_cluster_count(), 158436062, q{correct raw_cluster_count obtained from TileMetricsOut.bin} );

  lives_ok {
    $object->run_cluster_count_check();
  } q{check returns ok};
}

{
  my $object;
  lives_ok{
    $object = npg_pipeline::function::cluster_count->new(
      id_run => 1234,
      runfolder_path => $analysis_runfolder_path,
      position => 3,
      bam_basecall_path => $bam_basecall_path,
      archive_path => $archive_path,
    );
  } q{obtain object ok};
  
  ok( !$object->_bam_cluster_count_total({}), 'no bam cluster count total returned');
  
  qx{mkdir $archive_path/qc};
  qx{cp t/data/bam_flagstats/1234_3_bam_flagstats.json $archive_path/qc };
  qx{cp t/data/bam_flagstats/1234_3_phix_bam_flagstats.json $archive_path//qc/1234_3_phix_bam_flagstats.json};

  my $is_indexed = 1;
  qx{mkdir -p $archive_path/lane3/qc};
  qx{cp t/data/bam_flagstats/1234_3_bam_flagstats.json $archive_path/lane3/qc/1234_3#0_bam_flagstats.json};
  qx{cp t/data/bam_flagstats/1234_3_bam_flagstats.json $archive_path/lane3/qc/1234_3#1_bam_flagstats.json};
  
  is( $object->_bam_cluster_count_total( {plex=>$is_indexed} ), 32, 'correct bam cluster count total for plexes');

  qx{cp t/data/bam_flagstats/1234_3_phix_bam_flagstats.json $archive_path/lane3/qc/1234_3#0_phix_bam_flagstats.json};
  
  is( $object->_bam_cluster_count_total( {plex=>$is_indexed} ), 46, 'correct bam cluster count total for plexes');

}

{
  my $analysis_runfolder_path = 't/data/example_runfolder/121103_HS29_08747_B_C1BV5ACXX';
  my $bam_basecall_path = "$analysis_runfolder_path/Data/Intensities/BAM_basecalls_20130122-085552";
  my $archive_path = "$bam_basecall_path/PB_cal_bam/archive";

  my $object;
  lives_ok{
    $object = npg_pipeline::function::cluster_count->new(
      id_run => 8747,
      position => 1,
      runfolder_path => $analysis_runfolder_path,
      bam_basecall_path => $bam_basecall_path,
      archive_path => $archive_path,
    );
  } q{obtain object ok};

  is( $object->_bam_cluster_count_total({plex=>1}), 301389338, 'correct bam cluster count total');
  rename "$archive_path/lane1/qc/8747_1#0_bam_flagstats.json", "$archive_path/lane1/qc/8747_1#0_bam_flagstats.json.RENAMED";
  throws_ok {$object->run_cluster_count_check()}  qr{Cluster count in bam files not as expected}, 'Cluster count in bam files not as expected';
  rename "$archive_path/lane1/qc/8747_1#0_bam_flagstats.json.RENAMED", "$archive_path/lane1/qc/8747_1#0_bam_flagstats.json";
  ok($object->run_cluster_count_check(), 'Cluster count in bam files as expected');
}

{
  my $analysis_runfolder_path = 't/data/example_runfolder/121103_HS29_08747_B_C1BV5ACXX';
  my $bam_basecall_path = "$analysis_runfolder_path/Data/Intensities/BAM_basecalls_20130122-085552";
  my $qc_path = "$bam_basecall_path/PB_cal_bam/archive/qc";

  my $common_command = "$EXECUTABLE_NAME bin/npg_pipeline_check_cluster_count --id_run 8747 --bam_basecall_path $bam_basecall_path --qc_path $qc_path --position ";
  note `$common_command 1 2>&1`;
  ok( ! $CHILD_ERROR, q{script runs ok when no spatial filter json} );
  note `$common_command 4 2>&1`;
  ok( ! $CHILD_ERROR, q{script runs ok when spatial filter has failed reads} );
  note `$common_command 6 2>&1`;
  ok( ! $CHILD_ERROR, q{script runs ok when no spatial filter has no PF reads} );
}

1;
