package npg_pipeline::runfolder_scaffold;

use Moose::Role;
use File::Path qw/make_path/;
use File::Spec;
use File::Slurp;
use Readonly;
use Carp;

our $VERSION = '0';

Readonly::Scalar my $OUTGOING_PATH_COMPONENT    => q[/outgoing/];
Readonly::Scalar my $ANALYSIS_PATH_COMPONENT    => q[/analysis/];
Readonly::Scalar my $LOG_DIR_NAME               => q[log];
Readonly::Scalar my $STATUS_FILES_DIR_NAME      => q[status];
Readonly::Scalar my $METADATA_CACHE_DIR_NAME    => q[metadata_cache_];
Readonly::Scalar my $TILEVIZ_INDEX_DIR_NAME     => q[tileviz];
Readonly::Scalar my $TILEVIZ_INDEX_FILE_NAME    => q[index.html];

sub create_product_level {
  my $self = shift;

  if (!$self->can('products')) {
    croak 'products attribute should be implemented';
  }

  my @dirs = ();
  # Create cache dir for short files and qc out directory for every product
  foreach my $p ( (map { @{$_} } values %{$self->products}) ) {
    push @dirs, ( map { $p->$_($self->archive_path()) }
                  qw/path qc_out_path short_files_cache_path/ );
  }
  # Create tileviz directory for lane products only
  push @dirs, ( map { $_->tileviz_path($self->archive_path()) }
                @{$self->products->{'lanes'}} );
  $self->_create_tileviz_index();

  my @errors = $self->make_dir(@dirs);
  my $m = join qq[\n], 'Created the following directories:', @dirs;
  return {'msgs' => [$m], 'errors' => \@errors};
}

sub create_top_level {
  my $self = shift;

  my @info = ();
  my @dirs = ();
  my $path;

  ######
  # The directory names for paths are hardcoded here. Primary definitions for
  # them are located in a number of tracking roles. These definitions should
  # be made available in tracking so the they can be used here.
  #
  if (!$self->has_intensity_path()) {
    $path = File::Spec->catdir($self->runfolder_path(), q{Data}, q{Intensities});
    if (!-e $path) {
      push @info, qq{Intensities path $path not found};
      $path = $self->runfolder_path();
    }
    $self->_set_intensity_path($path);
  }
  push @info, 'Intensities path: ', $self->intensity_path();

  if (!$self->has_basecall_path()) {
    $path = File::Spec->catdir($self->intensity_path() , q{BaseCalls});
    if (!-e $path) {
      push @info, qq{BaseCalls path $path not found};
      $path = $self->runfolder_path();
    }
    $self->_set_basecall_path($path);
  }
  push @info, 'BaseCalls path: ' . $self->basecall_path();

  if(!$self->has_bam_basecall_path()) {
    $path= File::Spec->catdir($self->intensity_path(), q{BAM_basecalls_} . $self->timestamp());
    push @dirs, $path;
    $self->set_bam_basecall_path($path);
  }
  push @info, 'BAM_basecall path: ' . $self->bam_basecall_path();

  if (!$self->has_recalibrated_path()) {
    $self->_set_recalibrated_path(File::Spec->catdir($self->bam_basecall_path(), 'no_cal'));
  }
  push @dirs, $self->recalibrated_path();
  push @info, 'no_cal path: ' . $self->recalibrated_path();

  my $metadata_cache_dir = $self->metadata_cache_dir_path();
  push @dirs, $metadata_cache_dir;
  push @info, "metadata cache path: $metadata_cache_dir";

  push @dirs, $self->archive_path(), $self->status_files_path();
  push @dirs, $self->_tileviz_index_dir_path();

  my @errors = $self->make_dir(@dirs);

  return {'msgs' => \@info, 'errors' => \@errors};
}

sub status_files_path {
  my $self = shift;
  my $apath = $self->analysis_path;
  if (!$apath) {
    croak 'Failed to retrieve analysis_path';
  }
  return File::Spec->catdir($apath, $STATUS_FILES_DIR_NAME);
}

sub metadata_cache_dir_path {
  my $self = shift;
  my $apath = $self->analysis_path;
  if (!$apath) {
    croak 'Failed to retrieve analysis_path';
  }
  return File::Spec->catdir($apath, $METADATA_CACHE_DIR_NAME . $self->id_run());
}

sub make_log_dir4names {
  my ($pkg, $analysis_path, @names) = @_;
  my @dirs = map { File::Spec->catdir(_log_path($analysis_path), $_) } @names;
  my @errors = __PACKAGE__->make_dir(@dirs);
  return {'dirs' => \@dirs, 'errors' => \@errors};
}

sub make_dir {
  my ($pkg, @dirs) = @_;

  my $err;
  make_path(@dirs, {error => \$err});
  my @errors = ();
  if (@{$err}) {
    for my $diag (@{$err}) {
      my ($d, $message) = %{$diag};
      if ($d eq q[]) {
        push @errors, "General error: $message";
      } else {
        push @errors, "Problem creating $d: $message";
      }
    }
  }
  return @errors;
}

sub path_in_outgoing {
  my ($pkg, $path) = @_;
  $path or croak 'Path required';
  $path =~ s{$ANALYSIS_PATH_COMPONENT}{$OUTGOING_PATH_COMPONENT}xms;
  return $path;
}

sub _tileviz_index_dir_path {
  my $self = shift;
  return File::Spec->catdir($self->archive_path, $TILEVIZ_INDEX_DIR_NAME);
}

sub _create_tileviz_index {
  my $self = shift;

  my %lanes =  map { $_->composition->get_component(0)->position,
                     $_->tileviz_path(q[..]) } @{$self->products->{'lanes'}};
  my $title = join q[ ], 'Run', $self->id_run(), 'Tileviz', 'Reports';
  my @content = ();
  push @content, "<html><head><title>$title</title></head>";
  push @content, "<h2>$title</h2>";
  foreach my $lane (sort keys %lanes) {
    my $ref = $lanes{$lane};
    push @content, qq[<div><a href="$ref">Lane $lane</a></div>];
  }
  push @content, '</html>';

  my $index = File::Spec->catfile($self->_tileviz_index_dir_path(),
                                  $TILEVIZ_INDEX_FILE_NAME);
  write_file($index, map { $_ . qq[\n] } @content);

  return;
}

sub _log_path {
  my $analysis_path = shift;
  $analysis_path or croak 'Analysis path is needed';
  return File::Spec->catdir($analysis_path, $LOG_DIR_NAME);
}

no Moose::Role;

1;

__END__

=head1 NAME

npg_pipeline::runfolder_scaffold

=head1 SYNOPSIS

=head1 DESCRIPTION

Analysis run folder scaffolding.

=head1 SUBROUTINES/METHODS

=head2 create_product_level

Creates product-level directories for all expected products, together with
short file cache directories, qc output and tileviz direcgtories if appropriate.

=head2 create_top_level

Sets all top level paths needed during the lifetime of the analysis runfolder,
starting from bam basecalls directory. Creates directories if they do not
exist.

Does not create product-level directories. Does not create top-level qc directory,
which was created by earlier versions of the pipeline. Presence of the top-level
qc directory will be used to distinguish between different directory structures.

=head2 status_files_path

A directory path to save status files to.

=head2 make_dir

Creates directories listed in the argiment list, creates intermwdiate directories
if they do not exist. Returns a list of errors, which, if all commands succeed,
is empty. Can be called both as an instance and a class method.

  my @errors = $scaffold->make_dir(qw/first second/);

=head2 metadata_cache_dir_path

=head2 make_log_dir4names

=head2 path_in_outgoing

Given a path in analysis directory changes it to outgoing directory.

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item namespace::autoclean

=item File::Path

=item File::Spec

=item File::Slurp

=item Readonly

=item Carp

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

=over

=item Andy Brown

=item Marina Gourtovaia

=back

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2018 Genome Research Ltd

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
