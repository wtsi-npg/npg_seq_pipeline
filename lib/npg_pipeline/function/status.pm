package npg_pipeline::function::status;

use Moose;
use Carp;
use Readonly;

extends q{npg_pipeline::base};

our $VERSION = '0';

Readonly::Scalar our $STATUS_SCRIPT  => q{npg_status2file};

has q{status}           => (isa      => q{Str},
                            is       => q{ro},
                            required => 1,
);

has q{lane_status_flag} => (isa      => q{Bool},
                            is       => q{ro},
                            required => 0,
                            default  => 0,
);

sub _command {
  my $self = shift;

  my $command = sprintf '%s --id_run %i --status "%s" --dir_out %s',
                  $STATUS_SCRIPT,
                  $self->id_run,
                  $self->status,
                  $self->status_files_path;

  if ($self->lane_status_flag) {
    my @lanes = @{$self->lanes};
    if (!@lanes) {
      @lanes = $self->positions;
    }
    foreach my $lane ( @lanes ) {
      $command .= " --lanes $lane";
    }
  }

  return $command;
}

sub submit {
  my ($self) = @_;
  return ($self->submit_bsub_command($self->_generate_bsub_command()));
}

###############
# responsible for generating the bsub command to be executed
sub _generate_bsub_command {
  my ($self) = @_;

  my $timestamp = $self->timestamp();
  my $run_folder = $self->run_folder();
  my $status     = $self->status();
  my $id_run     = $self->id_run();
  my $status_with_underscores = $self->status();
  $status_with_underscores =~ s/[ ]/_/gxms;

  my $job_name = $self->lane_status_flag ? q{save_lane_status_} : q{save_run_status_};
  $job_name .= join q{_}, $id_run, $status_with_underscores, $timestamp;

  my $bsub_command = qq{bsub -J $job_name -q } . $self->small_lsf_queue() . q{ };
  $bsub_command   .=  q{-o } . $self->status_files_path . q{/log/} . qq{$job_name.out };
  $bsub_command   .=  q{'}   . $self->_command . q{'};

  $self->debug($bsub_command);

  return $bsub_command;
}

no Moose;
__PACKAGE__->meta->make_immutable;
1;
__END__

=head1 NAME

npg_pipeline::function::status

=head1 SYNOPSIS

=head1 DESCRIPTION

Launches a job for saving run and lane statuses to a file.

=head1 SUBROUTINES/METHODS

=head2 status - status description

=head2 lane_status_flag

  A boolean flag; if true, a lane status will be set; false by default

=head2 submit - handles generating and submitting an LSF job

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item Readonly

=item Carp

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

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
