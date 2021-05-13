package npg_pipeline::function::remove_intermediate_data;

use namespace::autoclean;

use Moose;
use MooseX::StrictConstructor;
use Readonly;

extends 'npg_pipeline::base_resource';

our $VERSION = '0';

=head1 NAME

npg_pipeline::function::remove_intermediate_data

=head1 SYNOPSIS

  my $o = npg_pipeline::function::remove_intermediate_data->new(
    id_run              => 1234,
  );
  my $definitions= $o->create();

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 create

  Arg [1]    : None

  Example    : my $defs = $obj->create
  Description: Create per-product function definitions.

  Returntype : ArrayRef[npg_pipeline::function::definition]

=cut

sub create {
  my ($self) = @_;

  if ($self->has_product_rpt_list) {
    $self->logcroak(q{Not implemented for individual products});
  }

  my $id_run = $self->id_run;
  my $job_name = join q[_], q[remove_intermediate_data], $id_run, $self->timestamp;

  my $recal_path = $self->recalibrated_path;
  if(not $recal_path) { $self->logcroak('unable to determine recalibrated path for intermediate data deletion'); }

  my $command = sprintf q[rm -fv %s/*.cram], $recal_path;

  return [
    $self->create_definition({
      identifier   => $id_run,
      job_name     => $job_name,
      command      => $command,
    })
  ];
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

npg_pipeline::function::remove_intermediate_data

=head1 SYNOPSIS

  my $obj = npg_pipeline::function::remove_intermediate_data->new(
    id_run => $id_run);

=head1 DESCRIPTION


=head1 SUBROUTINES/METHODS

=head1 BUGS AND LIMITATIONS

=head1 INCOMPATIBILITIES

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item namespace::autoclean

=item Moose

=item MooseX::StrictConstructor

=item Readonly

=back

=head1 AUTHOR

Kevin Lewis

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2019 Genome Research Ltd.

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
