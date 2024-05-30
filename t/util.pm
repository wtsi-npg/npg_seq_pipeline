package t::util;

use Moose;
use File::Temp qw(tempdir);
use Readonly;
use File::Path qw(make_path);

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

1;
