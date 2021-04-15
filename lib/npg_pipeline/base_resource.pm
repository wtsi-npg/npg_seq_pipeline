package npg_pipeline::base_resource;

use Moose;

our $VERSION = '0';

extends 'npg_pipeline::base';

=head1 NAME

npg_pipeline::base_resource

=head1 SYNOPSIS

=head1 DESCRIPTION

Inheritable class that understands resources as defined in the pipeline JGF
format. It provides methods for accessing default resources, as well as merged
resource defintions for special sub-tasks in a pipeline function, e.g.
particular memory requirements for a certain type of alignment, or a specific
set of properties for a Portable Pipeline

=head1 SUBROUTINES/METHODS
=cut

has default_defaults => (
  isa => 'HashRef',
  is => 'ro',
  default => sub {{
    low_cpu => 1,
    high_cpu => 1,
    memory => 2
  }},
  documentation => 'Basic resources that all jobs might need',
);


=head2 get_resources

Args:        String, Optional. A name of a special job type within the function
Description: Provides the combination of defaults and specific resource
             requirements as applicable to the function. Global defaults are
             overridden by function-specific defaults, which are in turn
             overridden by individual resource settings
Returntype:  HashRef of resources needed. Memory is in gigabytes
Example:     $resources = $self->get_resources('bigmem');

=cut

has resource => (
  isa => 'HashRef',
  is => 'ro',
  lazy => 1,
  default => sub {{}},
  documentation => 'Function-specific resource spec',
);

sub get_resources {
  my ($self, $special) = @_;

  my $resource = $self->resource->{default};
  if ($special && !exists $self->resource->{$special}) {
    $self->logcroak(
      sprintf 'Tried to get resource spec "%s" in %s but have %s',
      $special,
      __PACKAGE__,
      join ', ', keys %{$self->resource}
    );
  }
  return {
    %{$self->default_defaults},
    %{$self->resource->{default}},
    (defined $special) ? %{$self->resource->{$special}} : ()
  }
}


__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item namespace::autoclean

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2021 Genome Research Ltd.

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
