package npg_pipeline::daemon::archival;

use Moose;
use namespace::autoclean;
use Readonly;
use Try::Tiny;

extends qw{npg_pipeline::daemon};

our $VERSION = '0';

Readonly::Scalar our $POST_QC_REVIEW_SCRIPT => q{npg_pipeline_post_qc_review};
Readonly::Scalar our $ARCHIVAL_PENDING      => q{archival pending};
Readonly::Scalar  my $ARCHIVAL_IN_PROGRESS  => q{archival in progress};
Readonly::Scalar  my $MAX_NUMBER_NEW_RUNS_IN_ARCHIVAL  => 5;
Readonly::Scalar  my $NUM_HOURS_LOOK_BACK_FOR_NEW_RUNS => 1;

sub build_pipeline_script_name {
  return $POST_QC_REVIEW_SCRIPT;
}

sub run {
  my $self = shift;

  my $submitted = 0;
  foreach my $run ($self->runs_with_status($ARCHIVAL_PENDING)) {
    $submitted && last; # Return after submitting one run, give
                        # this run a chance to change status to
                        # 'archival in progress'.
    my $id_run = $run->id_run();
    try {
      $self->info();
      $self->info(qq{Considering run $id_run});
      if ($self->seen->{$id_run}) {
        $self->info(qq{Already seen run $id_run, skipping...});
      } else {
        if ( $self->staging_host_match($run->folder_path_glob) &&
             $self->_can_start_archival() ) {
          if ($self->run_command($id_run, $self->_generate_command($id_run))) {
            $self->info();
            $self->info(qq{Submitted run $id_run for archival});
            $submitted += 1;
          }
        }
      }
    } catch {
      $self->error("Error processing run ${id_run}: $_");
    };
  }

  return $submitted;
}

sub _generate_command {
  my ($self, $id_run) = @_;

  my $cmd = $self->pipeline_script_name();
  $cmd = $cmd . q{ --verbose --runfolder_path } . $self->runfolder_path4run($id_run);
  my $path = join q[:], $self->local_path(), $ENV{PATH};

  return qq{export PATH=$path; $cmd};
}

sub _can_start_archival {
  my $self = shift;

  my $time = DateTime->now()
             ->subtract(hours => $NUM_HOURS_LOOK_BACK_FOR_NEW_RUNS);
  my $num_runs = $self->runs_with_status($ARCHIVAL_IN_PROGRESS, $time);

  return ($num_runs < $MAX_NUMBER_NEW_RUNS_IN_ARCHIVAL);
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

npg_pipeline::daemon::archival

=head1 SYNOPSIS

  my $runner = npg_pipeline::daemon::archival->new();
  $runner->loop();

=head1 DESCRIPTION

A daemon for invoking the archival pipeline.
Inherits most of functionality, including the loop() method,
from the npg_pipeline::daemon class.

=head1 SUBROUTINES/METHODS

=head2 run

Invokes the archival pipeline for runs with the current status of
'archival pending'. Returns after successfully invoking the archival
pipeline for one run regardless of whether there are other runs that
could have been considered for archival.

The number of runs that can be archived concurrently is throttled, no
more that five runs can be moved to archival in an hour between all
archival daemons.

=head2 build_pipeline_script_name

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item namespace::autoclean

=item Try::Tiny

=item Readonly

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

=over

=item Andy Brown

=item Marina Gourtovaia

=back

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2016,2017,2018,2020,2021 Genome Research Ltd.

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
