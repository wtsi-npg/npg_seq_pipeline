package t::util;

use Moose;
use Carp;
use English qw{-no_match_vars};
use File::Temp qw{ tempdir };
use Readonly;
use Cwd qw(getcwd);
use npg::api::request;

Readonly::Scalar our $TEMP_DIR => q{/tmp};
Readonly::Scalar our $NFS_STAGING_DISK => q{/nfs/sf45};

has q{cwd} => (
  isa => q{Str},
  is => q{ro},
  lazy_build => 1,
);

sub _build_cwd {
  my ( $self ) = @_;
  return getcwd();
}

# for getting a temporary directory which will clean up itself, and should not clash with other people attempting to run the tests
has q{temp_directory} => (
  isa => q{Str},
  is => q{ro},
  lazy_build => 1,
);
sub _build_temp_directory {
  my ( $self ) = @_;

  my $tempdir = tempdir(
    DIR => $TEMP_DIR,
    CLEANUP => 1,
  );
  return $tempdir;
}

###############
# path setups

Readonly::Scalar our $DEFAULT_RUNFOLDER => q{123456_IL2_1234};

Readonly::Scalar our $ANALYSIS_RUNFOLDER_PATH => $NFS_STAGING_DISK . q{/IL2/analysis/} . $DEFAULT_RUNFOLDER;
Readonly::Scalar our $OUTGOING_RUNFOLDER_PATH => $NFS_STAGING_DISK . q{/IL2/outgoing/} . $DEFAULT_RUNFOLDER;
Readonly::Scalar our $BUSTARD_PATH            => qq{$ANALYSIS_RUNFOLDER_PATH/Data/Intensities/Bustard1.3.4_09-07-2009_auto};
Readonly::Scalar our $RECALIBRATED_PATH       => qq{$BUSTARD_PATH/PB_cal};

sub default_runfolder {
  my ( $self ) = @_;
  return $DEFAULT_RUNFOLDER;
}

sub test_run_folder {
  my ($self) = @_;
  my $test_run_folder_path = $self->temp_directory() . $ANALYSIS_RUNFOLDER_PATH;
  my ($run_folder) = $test_run_folder_path =~ /(\d+_IL\d+_\d+)/xms;
  return $run_folder;
}

sub analysis_runfolder_path {
  my ( $self ) = @_;
  return $self->temp_directory() . $ANALYSIS_RUNFOLDER_PATH;
}

sub standard_analysis_bustard_path {
  my ( $self ) = @_;
  return $self->temp_directory() . $BUSTARD_PATH;
}

sub standard_analysis_recalibrated_path {
  my ( $self ) = @_;
  return $self->temp_directory() . $RECALIBRATED_PATH;
}

sub create_analysis {
  my ($self, $args) = @_;
  $args ||= {};
  $self->remove_staging();
  my $analysis_runfolder_path = $self->temp_directory() . $ANALYSIS_RUNFOLDER_PATH;
  my $recalibrated_path = $self->temp_directory() . $RECALIBRATED_PATH;
  `mkdir -p $recalibrated_path`;
  if (!$args->{skip_archive_dir}) {
    `mkdir -p $recalibrated_path/archive/log`;
  }
  if ($args->{qc_dir}) {
    `mkdir -p $recalibrated_path/archive/log`;
    `mkdir -p $recalibrated_path/archive/qc/log`;
  }
  `mkdir $analysis_runfolder_path/Config`;
  `cp t/data/Recipes/Recipe_GA2_37Cycle_PE_v6.1.xml $analysis_runfolder_path/`;
  `cp t/data/Recipes/TileLayout.xml $analysis_runfolder_path/Config/`;
  `mkdir $analysis_runfolder_path/Data/Intensities/archive`;
  `ln -s Data/Intensities/Bustard1.3.4_09-07-2009_auto/PB_cal $analysis_runfolder_path/Latest_Summary`;
  return 1;
}

sub create_multiplex_analysis {
  my ($self, $args) = @_;
  $args ||= {};

  $self->create_analysis($args);
  my $analysis_runfolder_path = $self->temp_directory() . $ANALYSIS_RUNFOLDER_PATH;
  my $recalibrated_path = $self->temp_directory() . $RECALIBRATED_PATH;
  `rm $analysis_runfolder_path/Recipe_GA2_37Cycle_PE_v6.1.xml`;
  `cp t/data/Recipes/Recipe_GA2-PEM_MP_2x76Cycle+8_v7.7.xml $analysis_runfolder_path/`;

  if ($args->{qc_dir}) {
    foreach my $lane (@{$args->{qc_dir}}) {
      `mkdir -p $recalibrated_path/archive/lane$lane/qc`;
    }
  }
  return 1;
}

sub set_staging_analysis_area {
  my ($self, $args) = @_;
  $args ||= {};
  $self->remove_staging();
  my $analysis_runfolder_path = $self->temp_directory() . $ANALYSIS_RUNFOLDER_PATH;
  my $bustard_path = $self->temp_directory() . $BUSTARD_PATH;
  my $recalibrated_path = $self->temp_directory() . $RECALIBRATED_PATH;
  `mkdir -p $recalibrated_path`;
  `mkdir $analysis_runfolder_path/Config`;
  `mkdir $analysis_runfolder_path/t`;
  `cp t/data/Recipes/Recipe_GA2_37Cycle_PE_v6.1.xml $analysis_runfolder_path/`;
  `cp t/data/Recipes/TileLayout.xml $analysis_runfolder_path/Config/`;
  `touch $recalibrated_path/touch_file`;
  if ($args->{with_latest_summary}) {
    `ln -s Data/Intensities/Bustard1.3.4_09-07-2009_auto/PB_cal $analysis_runfolder_path/Latest_Summary`;
  }
  return 1;
}

sub set_rta_staging_analysis_area {
  my ($self, $indexed, $id_run) = @_;
  $self->remove_staging();
  my $analysis_runfolder_path = $self->temp_directory() . $ANALYSIS_RUNFOLDER_PATH;
  if ( $id_run ) {
    $analysis_runfolder_path =~ s/1234/$id_run/gxms;
  }
  my $bustard_path = qq{$analysis_runfolder_path/Data/Intensities/Bustard1.5.1_09-07-2009_RTA};
  my $recalibrated_path = qq{$bustard_path/PB_cal};
  `mkdir -p $recalibrated_path`;
  `mkdir $analysis_runfolder_path/Config`;
  `mkdir $analysis_runfolder_path/t`;
  `touch $bustard_path/s_1_1_001_qval.txt.gz`;
  `touch $bustard_path/s_1_2_001_qval.txt.gz`;
  `touch $bustard_path/s_2_1_001_qval.txt.gz`;
  `touch $bustard_path/s_2_2_001_qval.txt.gz`;
  `cp t/data/runfolder/Data/Intensities/Bustard_RTA/config.xml $bustard_path/`;
  `cp t/data/summary_files/after_v7_mp_hack_Summary.xml $recalibrated_path/Summary.xml`;
  `cp t/data/summary_files/after_v7_mp_hack_Summary.htm $recalibrated_path/Summary.htm`;
  `cp t/data/summary_files/Summary.xsl $recalibrated_path/Summary.xsl`;
  `cp t/data/runfolder/Data/Intensities/Bustard_RTA/PB_cal/config.xml $recalibrated_path/`;
  if ($indexed) {
    `cp t/data/Recipes/Recipe_GA2-PEM_MP_2x76Cycle+8_v7.7.xml $analysis_runfolder_path/`;
  } else {
    `cp t/data/Recipes/Recipe_GA2_37Cycle_PE_v6.1.xml $analysis_runfolder_path/`;
    `cp t/data/runfolder/Data/Intensities/single_end_3cycle_1tile_config.xml $analysis_runfolder_path/Data/Intensities/config.xml`;
    foreach my $lane ( 1..8 ) {
      foreach my $cycle ( 1..3 ) {
        qx{mkdir -p $analysis_runfolder_path/Data/Intensities/L00$lane/C$cycle.1};
        qx{cp t/data/runfolder/Data/Intensities/demo_cif.cif $analysis_runfolder_path/Data/Intensities/L00$lane/C$cycle.1/s_${lane}_1.cif};
      }
    }
  }
  `cp t/data/Recipes/TileLayout.xml $analysis_runfolder_path/Config/`;
  `touch $recalibrated_path/touch_file`;
  return {bustard_path => $bustard_path, recalibrated_path => $recalibrated_path, runfolder_path => $analysis_runfolder_path};
}

sub remove_staging {
  my ($self) = @_;
  my $staging = $self->temp_directory() . $NFS_STAGING_DISK;
  `rm -rf $staging`;
  return 1;
}

# for dropping the generated temporary part from paths
# and also anything which has the cwd in it, will be stripped out
# since this will not be stable between test runs
sub drop_temp_part_from_paths {
  my ( $self, $path ) = @_;
  my $temp_dir = $self->temp_directory();
  my $cwd = $self->cwd();
  $path =~ s{\Q$temp_dir\E}{}gxms;
  $path =~ s{\Q$cwd/\E}{}gxms;
  $path =~ s{\Q$cwd\E}{}gxms;
  return $path;
}

# ensure that the environment variables do not get passed around and that extraneous files do not get left behind
sub DEMOLISH {
  $ENV{ npg::api::request->cache_dir_var_name() } = q{};
  unlink 'Latest_Summary';
}

1;
