package npg_pipeline::function::runfolder_scaffold;

use Moose;
use namespace::autoclean;
use File::Path qw(make_path);
use File::Spec;
use Readonly;
use Carp;

use npg_pipeline::function::definition;

extends q{npg_pipeline::base};

our $VERSION = '0';

Readonly::Scalar my $OUTGOING_PATH_COMPONENT => q[/outgoing/];
Readonly::Scalar my $ANALYSIS_PATH_COMPONENT => q[/analysis/];
Readonly::Scalar my $LOG_DIR_NAME            => q[log];
Readonly::Scalar my $TILEVIZ_DIR_NAME        => q[tileviz];

sub create {
  my $self = shift;

  my @dirs = (
               $self->archive_path(),
               $self->status_files_path(),
               $self->qc_path(),
               File::Spec->catdir($self->qc_path(), $TILEVIZ_DIR_NAME),
             );

  if ($self->is_indexed()) {
    foreach my $position ($self->positions()) {
      if ($self->is_multiplexed_lane($position)) {
        ###########
        # Lane directories for primary analysis output
        #
        push @dirs, File::Spec->catdir($self->recalibrated_path(), q{lane} . $position);

        push @dirs, $self->lane_archive_path($position);
        push @dirs, $self->lane_qc_path($position);
      }
    }
  }

  my @errors = __PACKAGE__->make_dir(@dirs);
  if (@errors) {
    $self->logcroak(join qq[\n], @errors);
  } else {
    $self->info(q[Created the following directories: ], join q[, ], @dirs);
  }

  my $d = npg_pipeline::function::definition->new(
    created_by     => __PACKAGE__,
    created_on     => $self->timestamp(),
    identifier     => $self->id_run(),
    immediate_mode => 1
  );

  return [$d];
}

sub create_top_level {
  my ($pkg, $obj) = @_;

  my @info = ();
  my @dirs = ();
  my $path;

  if (!$obj->has_intensity_path()) {
    $path = File::Spec->catdir($obj->runfolder_path(), q{Data}, q{Intensities});
    if (!-e $path) {
      push @info, qq{Intensities path $path not found};
      $path = $obj->runfolder_path();
    }
    $obj->_set_intensity_path($path);
  }
  push @info, 'Intensities path: ', $obj->intensity_path();

  if (!$obj->has_basecall_path()) {
    $path = File::Spec->catdir($obj->intensity_path() , q{BaseCalls});
    if (!-e $path) {
      push @info, qq{BaseCalls path $path not found};
      $path = $obj->runfolder_path();
    }
    $obj->_set_basecall_path($path);
  }
  push @info, 'BaseCalls path: ' . $obj->basecall_path();

  if(!$obj->has_bam_basecall_path()) {
    $path= File::Spec->catdir($obj->intensity_path(), q{BAM_basecalls_} . $obj->timestamp());
    push @dirs, $path;
    $obj->set_bam_basecall_path($path);
  }
  push @info, 'BAM_basecall path: ' . $obj->bam_basecall_path();

  if (!$obj->has_recalibrated_path()) {
    $obj->_set_recalibrated_path(File::Spec->catdir($obj->bam_basecall_path(), 'no_cal'));
  }
  push @dirs, $obj->recalibrated_path();
  push @info, 'no_cal path: ' . $obj->recalibrated_path();

  my @errors = __PACKAGE__->make_dir(@dirs);

  return {'msgs' => \@info, 'errors' => \@errors};
}

sub make_log_dir4name {
  my ($pkg, $analysis_path, $name) = @_;
  my $dir = File::Spec->catdir(_log_path($analysis_path), $name);
  my @errors = __PACKAGE__->make_dir($dir);
  return ($dir, @errors);
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
  $path =~ s{$ANALYSIS_PATH_COMPONENT}{$OUTGOING_PATH_COMPONENT}xms;
  return $path;
}

sub future_path {
  my ($pkg, $d, $path) = @_;

  ($d && $path) or croak 'Definition and path arguments required' ;
  (ref($d) eq 'npg_pipeline::function::definition')
      or croak 'First argument should be a definition object';

  #####
  # The jobs that should be executed after the run folder is moved to
  # the outgoing directory have a preexec expression that check that
  # the path has changed to the outgoing directory. This fact is used
  # here to flag cases where the log directory pathe should change
  # from analysis to outgoing.
  #
  if ($d->has_command_preexec() &&
      $d->command_preexec() =~ /$OUTGOING_PATH_COMPONENT/smx) {
    $path = __PACKAGE__->path_in_outgoing($path);
  }

  return $path;
}

sub _log_path {
  my $analysis_path = shift;
  $analysis_path or croak 'Analysis path is needed';
  return File::Spec->catdir($analysis_path, $LOG_DIR_NAME);
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

npg_pipeline::function::runfolder_scaffold

=head1 SYNOPSIS

  my $afg = npg_pipeline::function::runfolder_scaffold->new(
    run_folder => $sRunFolder,
  );

=head1 DESCRIPTION

Analysis run folder scaffolding.

=head1 SUBROUTINES/METHODS

=head2 create

Scaffolds the analysis directory.
Returns an array with a single npg_pipeline::function::definition
object, which has immediate_mode attribute set to true.

=head2 create_top_level

Sets all paths needed during the lifetime of the analysis runfolder.
Creates any of the paths that do not exist.

=head2 make_dir

Creates directories listed in the argiment list, creates intermwdiate directories
if they do not exist. Returns a list of errors, which, if all commands succeed,
is empty. Can be called both as an instance and a class method.

  my @errors = $scaffold->make_dir(qw/first second/);

=head2 make_log_dir4name

=head2 path_in_outgoing

Given a path in analysis directory changes it to outgoing directory.

=head2 future_path

If the job will run in the outgoing directory, a path in analysis directory
is changed to a path in outgoing directory.

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item namespace::autoclean

=item File::Path

=item File::Spec

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
