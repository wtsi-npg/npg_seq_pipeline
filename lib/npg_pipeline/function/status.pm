package npg_pipeline::function::status;

use Moose;
use namespace::autoclean;
use Readonly;

extends q{npg_pipeline::base_resource};
with q{npg_pipeline::runfolder_scaffold};

our $VERSION = '0';

Readonly::Scalar my $STATUS_SCRIPT => q{npg_status2file};

has q{status}           => (isa      => q{Str},
                            is       => q{ro},
                            required => 1,);

has q{lane_status_flag} => (isa      => q{Bool},
                            is       => q{ro},
                            required => 0,
                            default  => 0,);

sub create {
  my $self = shift;

  my $status_with_underscores = $self->status();
  $status_with_underscores =~ s/[ ]/_/gxms;
  my $status_files_path = $self->status_files_path();
  my $job_name = join q{_},
    $self->lane_status_flag ? q{save_lane_status} : q{save_run_status},
    $self->id_run(),
    $status_with_underscores,
    $self->timestamp();

  my $d = $self->create_definition({
    job_name      => $job_name,
    command       => $self->_command($status_files_path),
  });

  return [$d];
}

sub _command {
  my ($self, $status_files_path) = @_;

  my $command = sprintf '%s --id_run %i --status "%s" --dir_out %s',
                  $STATUS_SCRIPT,
                  $self->id_run,
                  $self->status,
                  $status_files_path;

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

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

npg_pipeline::function::status

=head1 SYNOPSIS

=head1 DESCRIPTION

Launches a job for saving run and lane statuses to a file.

=head1 SUBROUTINES/METHODS

=head2 status

Npg tracking status description

=head2 lane_status_flag

A boolean flag, false by default.
If true, a lane status will be set.

=head2 create

Creates and returns a single function definition as an array.
Function definition is created as a npg_pipeline::function::definition
type object.

  my $definitions = $obj->create();

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item namespace::autoclean

=item Readonly

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
