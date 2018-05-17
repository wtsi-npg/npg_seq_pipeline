package npg_pipeline::function::runfolder_scaffold;

use Moose;
use namespace::autoclean;
use English qw{-no_match_vars};

use npg_pipeline::function::definition;

extends q{npg_pipeline::base};

our $VERSION = '0';

sub create_dir {
  my ($self, $owning_group) = @_;

  $owning_group ||= $ENV{OWNING_GROUP};
  $owning_group ||= $self->general_values_conf()->{group};

  my @dirs = ( $self->archive_path(),
               $self->archive_path() . q{/.npg_cache_10000},
               $self->qc_path(),
               $self->qc_path() . q{/tileviz}, );
  my @all_dirs = @dirs;

  for my $dir (@dirs) {
    $self->info("Creating $dir and it's log directory");
    push @all_dirs, $self->make_log_dir($dir);
  }

  if ( $self->is_indexed() ){
    foreach my $position ($self->positions()) {
      if ( ! $self->is_multiplexed_lane( $position ) ) {
        next;
      }
      ###########
      # Lane directories for primary analysis output
      #
      my $lane_dir = $self->recalibrated_path() . q{/lane} . $position;
      push @all_dirs, $lane_dir;
      push @all_dirs, $self->make_log_dir( $lane_dir );
      ###########
      # Lane directories for secondary analysis output
      #
      $lane_dir = $self->archive_path() . q{/lane} . $position;
      push @dirs, $lane_dir;
      push @all_dirs, $self->make_log_dir( $lane_dir );
      ###########
      # Lane directories for qc output
      #
      my $lane_qc_dir = $lane_dir . q{/qc};
      push @all_dirs, $lane_qc_dir;
      push @all_dirs, $self->make_log_dir( $lane_qc_dir );
      ###########
      # cache directories for subsampled fastq output
      #
      my $lane_cache_dir = $lane_dir . q{/.npg_cache_10000};
      push @all_dirs, $lane_cache_dir;
      push @all_dirs, $self->make_log_dir( $lane_cache_dir );
    }
  }

  foreach my $dir (@all_dirs) {
    if (-d $dir) {
      ###########
      # Set owning group
      #
      if ($owning_group) {
        $self->info("chgrp $owning_group $dir");
        my $rc = `chgrp $owning_group $dir`;
        if ( $CHILD_ERROR ) {
          $self->warn("could not chgrp $dir\n\t$rc"); # not fatal
        }
      }
      ###########
      # Set correct permissions
      #
      $self->info("chmod u=rwx,g=srxw,o=rx $dir");
      my $rc = `chmod u=rwx,g=srxw,o=rx $dir`;
      if ( $CHILD_ERROR ) {
        $self->warn("could not chmod $dir\n\t$rc");   # not fatal
      }
    }
  }

  my $d = npg_pipeline::function::definition->new(
    created_by     => __PACKAGE__,
    created_on     => $self->timestamp(),
    identifier     => $self->id_run(),
    immediate_mode => 1
  );

  return [$d];
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

Object module which knows how to construct the path and creates the archival directory.

=head1 SUBROUTINES/METHODS

=head2 create_dir

Creates the archive directory if is doesn't exist and sets the correct group
and permissions on it. Returns an array with a single npg_pipeline::function::definition
object, which has immediate_mode attribute set to true.

  try {
    $afg->create_dir($s<OptionalOwningGroup);
  } catch {
    ...error handling...
  };

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item namespace::autoclean

=item English -no_match_vars

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown

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
