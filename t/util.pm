package t::util;

use Moose;
use Carp;
use English qw{-no_match_vars};
use File::Temp qw{ tempdir };
use Readonly;
use Cwd qw(getcwd);
use File::Path qw(make_path);
use npg::api::request;
#TODO: purge all reference to Recipes - we're RunParameters.xml and RunInfo.xml now

Readonly::Scalar my $NFS_STAGING_DISK => q{/nfs/sf45};

has q{temp_directory} => (
  isa => q{Str},
  is => q{ro},
  lazy_build => 1,
);
sub _build_temp_directory {
  my $self = shift;
  my $clean = $self->clean_temp_directory ? 1 : 0;
  return tempdir(CLEANUP => $clean);
}

has q{clean_temp_directory} => (
  isa     => q{Bool},
  is      => q{ro},
  default => 1,
);

###############
# path setups

Readonly::Scalar our $DEFAULT_RUNFOLDER => q{123456_IL2_1234};
Readonly::Scalar our $ANALYSIS_RUNFOLDER_PATH => $NFS_STAGING_DISK . q{/IL2/analysis/} . $DEFAULT_RUNFOLDER;
Readonly::Scalar our $BUSTARD_PATH            => qq{$ANALYSIS_RUNFOLDER_PATH/Data/Intensities/Bustard1.3.4_09-07-2009_auto};
Readonly::Scalar our $RECALIBRATED_PATH       => qq{$BUSTARD_PATH/PB_cal};

sub analysis_runfolder_path {
  my ( $self ) = @_;
  return $self->temp_directory() . $ANALYSIS_RUNFOLDER_PATH;
}

sub standard_bam_basecall_path {
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
  `cp t/data/Recipes/TileLayout.xml $analysis_runfolder_path/Config/`;
  `mkdir $analysis_runfolder_path/Data/Intensities/archive`;
  `ln -s Data/Intensities/Bustard1.3.4_09-07-2009_auto/PB_cal $analysis_runfolder_path/Latest_Summary`;
  `mkdir -p $analysis_runfolder_path/InterOp`;
  `cp t/data/p4_stage1_analysis/TileMetricsOut.bin $analysis_runfolder_path/InterOp`;
  return 1;
}

sub create_multiplex_analysis {
  my ($self, $args) = @_;
  $args ||= {};

  $self->create_analysis($args);
  my $analysis_runfolder_path = $self->temp_directory() . $ANALYSIS_RUNFOLDER_PATH;
  my $recalibrated_path = $self->temp_directory() . $RECALIBRATED_PATH;
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
  `cp t/data/run_params/runParameters.miseq.xml $analysis_runfolder_path/runParameters.xml`;
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
  `cp t/data/run_params/runParameters.miseq.xml $analysis_runfolder_path/runParameters.xml`;
  `cp t/data/runfolder/Data/Intensities/Bustard_RTA/config.xml $bustard_path/`;
  `cp t/data/summary_files/after_v7_mp_hack_Summary.xml $recalibrated_path/Summary.xml`;
  `cp t/data/summary_files/after_v7_mp_hack_Summary.htm $recalibrated_path/Summary.htm`;
  `cp t/data/summary_files/Summary.xsl $recalibrated_path/Summary.xsl`;
  `cp t/data/runfolder/Data/Intensities/Bustard_RTA/PB_cal/config.xml $recalibrated_path/`;
  if ( ! $indexed) {
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
  return {recalibrated_path => $recalibrated_path, runfolder_path => $analysis_runfolder_path};
}

sub remove_staging {
  my ($self) = @_;
  my $staging = $self->temp_directory() . $NFS_STAGING_DISK;
  `rm -rf $staging`;
  return 1;
}

sub create_runfolder {
  my ($self, $dir, $names) = @_;

  $dir   ||= $self->temp_directory;
  $names ||= {};
  my $rf_name = $names->{'runfolder_name'} || q[180524_A00510_0008_BH3W7VDSXX];

  my $paths = {};
  $paths->{'runfolder_name'} = $rf_name;
  $paths->{'runfolder_path'} = join q[/], $dir, $rf_name;
  $paths->{'intensity_path'} = join q[/], $paths->{'runfolder_path'}, q[Data/Intensities];
  $paths->{'basecall_path'}  = join q[/], $paths->{'intensity_path'}, q[BaseCalls];
 
  if ($names->{'analysis_path'}) {
    $paths->{'analysis_path'}  = join q[/], $paths->{'intensity_path'}, $names->{'analysis_path'};
    $paths->{'nocal_path'}     = join q[/], $paths->{'analysis_path'}, q[no_cal];
    $paths->{'archive_path'}   = join q[/], $paths->{'nocal_path'}, q[archive];
  }

  make_path(values %{$paths});
  return $paths;
}

sub create_run_info {
  my ($self, $reads_wanted) = @_;

  my $default_reads_wanted = q[    <Read Number="1" NumCycles="76" IsIndexedRead="N" />];

  my $reads = ( defined $reads_wanted ) ? $reads_wanted : $default_reads_wanted;

  my $fh;
  my $runinfofile = $self->analysis_runfolder_path() . q[/RunInfo.xml];
  open($fh, '>', $runinfofile) or die "Could not open file '$runinfofile' $!";
  print $fh <<"ENDXML";
<?xml version="1.0"?>
<RunInfo xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" Version="3">
  <Run>
    <Reads>
$reads
    </Reads>
    <FlowcellLayout LaneCount="8" SurfaceCount="2" SwathCount="1" TileCount="60">
    </FlowcellLayout>
  </Run>
</RunInfo>
ENDXML
  close $fh;
}

# ensure that the environment variables do not get passed around
sub DEMOLISH {
  $ENV{ npg::api::request->cache_dir_var_name() } = q{};
}

1;
