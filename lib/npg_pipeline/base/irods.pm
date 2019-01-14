package npg_pipeline::base::irods;

use Moose::Role;
use Readonly;

requires qw/id_run platform_NovaSeq/;

our $VERSION = '0';

Readonly::Scalar my $IRODS_ROOT_NON_NOVASEQ_RUNS => q[/seq];
Readonly::Scalar my $IRODS_ROOT_NOVASEQ_RUNS     => q[/seq/illumina/runs];

has 'irods_destination_collection' => (
  isa           => 'Str',
  is            => 'ro',
  required      => 0,
  lazy_build    => 1,
  documentation => 'iRODS destination collection for run product data',
);
sub _build_irods_destination_collection {
  my $self = shift;
  return join q[/],
    $self->platform_NovaSeq() ? $IRODS_ROOT_NOVASEQ_RUNS : $IRODS_ROOT_NON_NOVASEQ_RUNS,
    $self->id_run;
}

no Moose::Role;

1;

__END__

=head1 NAME

npg_pipeline::base::irods

=head1 SYNOPSIS

=head1 DESCRIPTION

Moose role providing utility methods for function modules.

=head1 SUBROUTINES/METHODS

=head2 irods_destination_collection

Returns iRODS destination collection for this run.

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose::Role

=item Readonly

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

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
