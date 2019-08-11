package t::util;

use Moose;
use File::Temp qw{ tempdir };
use Readonly;
use File::Path qw(make_path);
use npg::api::request;

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
Readonly::Scalar our $BBCALLS_PATH            => qq{$ANALYSIS_RUNFOLDER_PATH/Data/Intensities/BAM_basecalls_09-07-2009};
Readonly::Scalar our $RECALIBRATED_PATH       => qq{$BBCALLS_PATH/no_cal};

sub analysis_runfolder_path {
  my ( $self ) = @_;
  return $self->temp_directory() . $ANALYSIS_RUNFOLDER_PATH;
}

sub standard_bam_basecall_path {
  my ( $self ) = @_;
  return $self->temp_directory() . $BBCALLS_PATH;
}

sub standard_analysis_recalibrated_path {
  my ( $self ) = @_;
  return $self->temp_directory() . $RECALIBRATED_PATH;
}

sub create_analysis {
  my ($self) = @_;
  $self->remove_staging();
  my $analysis_runfolder_path = $self->temp_directory() . $ANALYSIS_RUNFOLDER_PATH;
  my $recalibrated_path = $self->temp_directory() . $RECALIBRATED_PATH;
  `mkdir -p $recalibrated_path`;
  `ln -s Data/Intensities/BAM_basecalls_09-07-2009/no_cal $analysis_runfolder_path/Latest_Summary`;
  `mkdir -p $analysis_runfolder_path/InterOp`;
  `cp t/data/p4_stage1_analysis/TileMetricsOut.bin $analysis_runfolder_path/InterOp`;
  `cp t/data/run_params/runParameters.miseq.xml $analysis_runfolder_path/runParameters.xml`;
  return;
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
