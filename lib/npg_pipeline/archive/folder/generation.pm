package npg_pipeline::archive::folder::generation;

use Moose;
use Carp;
use English qw{-no_match_vars};

our $VERSION = '0';;

extends q{npg_pipeline::base};

sub create_dir {
  my ($self, $owning_group) = @_;

  $owning_group ||= $ENV{OWNING_GROUP};
  $owning_group ||= $self->general_values_conf()->{group};

  my $archive_dir = $self->archive_path();
  my $archive_log_dir = $archive_dir . q{/log};
  my $qc_dir = $self->qc_path();
  my $qc_log_dir = $qc_dir . q{/log};
  my $tileviz_dir = $qc_dir . q{/tileviz};
  my $rna_seqc_dir = $qc_dir . q{/rna_seqc};

  #############
  # check existence of archive directory
  # create if it doesn't

  if ( ! -d $archive_dir) {
    $self->make_log_dir( $self->archive_path() );
  } else {
    $self->log("$archive_log_dir already exists");
  }

  #############
  # check existence of qc directory
  # create if it doesn't

  if ( ! -d $qc_dir) {
    $self->make_log_dir( $qc_dir );
  } else {
    $self->log("$qc_log_dir already exists");
  }

  #############
  # check existence of tileviz directory
  # create if it doesn't

  if ( ! -d $tileviz_dir) {
    my $mk_tileviz_dir_cmd = qq{mkdir -p $tileviz_dir};
    $self->log( $mk_tileviz_dir_cmd );
    my $return = qx{$mk_tileviz_dir_cmd};
    if ( $CHILD_ERROR ) {
      croak $tileviz_dir . qq{ does not exist and unable to create: $CHILD_ERROR\n$return};
    }
  }

  #############
  # check existence of rna_seqc directory
  # create if it doesn't

  if ( ! -d $rna_seqc_dir) {
    my $mk_rna_seqc_dir_cmd = qq{mkdir -p $rna_seqc_dir};
    $self->log( $mk_rna_seqc_dir_cmd );
    my $return = qx{$mk_rna_seqc_dir_cmd};
    if ( $CHILD_ERROR ) {
      croak $tileviz_dir . qq{ does not exist and unable to create: $CHILD_ERROR\n$return};
    }
  }

  #############
  # check existence of multiplex lane and qc directory
  # create if they doesn't
  if( $self->is_indexed() ){

      my @positions = $self->positions();

      foreach my $position ( @positions ){

          if ( ! $self->is_multiplexed_lane( $position ) ) {
             next;
          }

          my $lane_dir = $archive_dir . q{/lane} . $position;

          if ( ! -d $lane_dir ) {
             $self->make_log_dir( $lane_dir );
          }

          my $lane_qc_dir = $lane_dir . q{/qc};

          if( ! -d $lane_qc_dir ){
              my $mk_lane_qc_dir_cmd = qq{mkdir -p $lane_qc_dir};
              $self->log( $mk_lane_qc_dir_cmd );
              my $return = qx{$mk_lane_qc_dir_cmd};
              if ( $CHILD_ERROR ) {
                    croak $lane_qc_dir . qq{ does not exist and unable to create: $CHILD_ERROR\n$return};
              }
          }
      }
  }

  if ($owning_group) {
    ############
    # ensure that the owning group is what we expect

    $self->log("chgrp $owning_group $archive_dir");
    my $rc = `chgrp $owning_group $archive_dir`;
    if ( $CHILD_ERROR ) {
      $self->log("could not chgrp $archive_dir\n\t$rc");                # not fatal
    }

    $self->log("chgrp $owning_group $qc_dir");
    $rc = `chgrp $owning_group $qc_dir`;
    if ( $CHILD_ERROR ) {
      $self->log("could not chgrp $qc_dir\n\t$rc");                # not fatal
    }

    $self->log("chgrp $owning_group $tileviz_dir");
    $rc = `chgrp $owning_group $tileviz_dir`;
    if ( $CHILD_ERROR ) {
      $self->log("could not chgrp $tileviz_dir\n\t$rc");                # not fatal
    }

    $self->log("chgrp $owning_group $rna_seqc_dir");
    $rc = `chgrp $owning_group $rna_seqc_dir`;
    if ( $CHILD_ERROR ) {
      $self->log("could not chgrp $rna_seqc_dir\n\t$rc");                # not fatal
    }
    ############
    # ensure that the owning group is what we expect

    $self->log("chgrp $owning_group $archive_log_dir");
    $rc = `chgrp $owning_group $archive_log_dir`;
    if ( $CHILD_ERROR ) {
      $self->log("could not chgrp $archive_log_dir\n\t$rc");                # not fatal
    }

    $self->log("chgrp $owning_group $qc_log_dir");
    $rc = `chgrp $owning_group $qc_log_dir`;
    if ( $CHILD_ERROR ) {
      $self->log("could not chgrp $qc_log_dir\n\t$rc");                # not fatal
    }
  }

  ###########
  # set correct permissions on the archive directory

  $self->log("chmod u=rwx,g=srxw,o=rx $archive_dir");
  my $rc = `chmod u=rwx,g=srxw,o=rx $archive_dir`;
  if ( $CHILD_ERROR ) {
    $self->log("could not chmod $archive_dir\n\t$rc");                # not fatal
  }

  $self->log("chmod u=rwx,g=srxw,o=rx $qc_dir");
  $rc = `chmod u=rwx,g=srxw,o=rx $qc_dir`;
  if ( $CHILD_ERROR ) {
    $self->log("could not chmod $qc_dir\n\t$rc");                # not fatal
  }

  $self->log("chmod u=rwx,g=srxw,o=rx $tileviz_dir");
  $rc = `chmod u=rwx,g=srxw,o=rx $tileviz_dir`;
  if ( $CHILD_ERROR ) {
    $self->log("could not chmod $tileviz_dir\n\t$rc");                # not fatal
  }

  $self->log("chmod u=rwx,g=srxw,o=rx $rna_seqc_dir");
  $rc = `chmod u=rwx,g=srxw,o=rx $rna_seqc_dir`;
  if ( $CHILD_ERROR ) {
    $self->log("could not chmod $rna_seqc_dir\n\t$rc");                # not fatal
  }

  $self->log("chmod u=rwx,g=srxw,o=rx $archive_log_dir");
  $rc = `chmod u=rwx,g=srxw,o=rx $archive_log_dir`;
  if ( $CHILD_ERROR ) {
    $self->log("could not chmod $archive_log_dir\n\t$rc");                # not fatal
  }

  $self->log("chmod u=rwx,g=srxw,o=rx $qc_log_dir");
  $rc = `chmod u=rwx,g=srxw,o=rx $qc_log_dir`;
  if ( $CHILD_ERROR ) {
    $self->log("could not chmod $qc_log_dir\n\t$rc");                # not fatal
  }


  return 1;
}

no Moose;
__PACKAGE__->meta->make_immutable;
1;
__END__

=head1 NAME

npg_pipeline::archive::folder::generation

=head1 SYNOPSIS

  my $afg = npg_pipeline::archive::folder::generation->new(
    run_folder => $sRunFolder,
  );

=head1 DESCRIPTION

Object module which knows how to construct the path and creates the archival directory.

=head1 SUBROUTINES/METHODS

=head2 create_dir - creates the archive directory if is doesn't exist and sets the correct group and permissions on it

  eval {
    $afg->create_dir($s<OptionalOwningGroup);
  } or do {
    ...error handling...
  };

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item Carp

=item English -no_match_vars

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2014 Genome Research Ltd

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
