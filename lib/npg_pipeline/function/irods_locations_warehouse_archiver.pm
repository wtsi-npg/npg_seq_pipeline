package npg_pipeline::function::irods_locations_warehouse_archiver;

use Moose;
use namespace::autoclean;
use Readonly;

extends q{npg_pipeline::base_resource};
with q{npg_pipeline::runfolder_scaffold};

our $VERSION = '0';

Readonly::Scalar my $SCRIPT_NAME => q{npg_irods_locations2ml_warehouse};

sub create {
  my $self = shift;
  my $ref;
  if ($self->no_irods_archival) {
    $ref = {'excluded' => 1};
    $self->info(q{Archival to iRODS is switched off, } .
                q{iRODS locations loader will not run.});
  } else {
    my $locations_dir = $self->irods_locations_dir_path();
    ####
    # This directory might be absent for older run folders or when only
    # a subset of archival functions is run. Creating it here guarantees
    # that the job will not fail due to an invalid input path.
    $self->make_dir($locations_dir);
    $ref = {
      command  => qq{$SCRIPT_NAME --path $locations_dir --verbose},
      job_name => join q{_}, $SCRIPT_NAME, $self->label, $self->timestamp()
    };
  }
  return [$self->create_definition($ref)];
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

npg_pipeline::function::irods_locations_warehouse_archiver

=head1 SYNOPSIS

  my $archiver = npg_pipeline::function::irods_locations_warehouse_archiver
                 ->new(runfolder_path => '/some/path/');

=head1 DESCRIPTION

Defines a job for loading iRODS locations from json files in the
runfolder into ml_warehouse

=head1 SUBROUTINES/METHODS

=head2 create

Creates command definition to load ml_warehouse product_irods_locations
table from json files in the runfolder

=cut

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item Readonly

=item namespace::autoclean

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

=over

=item Michael Kubiak

=back

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2022 Genome Research Ltd.

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


