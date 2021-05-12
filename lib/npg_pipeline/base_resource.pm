package npg_pipeline::base_resource;

use Moose;
use MooseX::Getopt::Meta::Attribute::Trait::NoGetopt;
use Readonly;
use npg_pipeline::function::definition;

Readonly::Scalar my $GB_TO_MB_CONVERSION => 1000; # 1024? Not in original code

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

=head2 resource

HashRef of resource requests for the function, e.g.
{
  default => {
    minimum_cpu => 4,
    maximum_cpu => 8,
    memory => 10,
    db => ['mlwh']
  }
}

=cut

has resource => (
  isa => 'HashRef',
  is => 'ro',
  default => sub {{}},
  documentation => 'Function-specific resource spec',
  metaclass  => 'NoGetopt',
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

sub get_resources {
  my ($self, $special) = @_;

  my $resource = $self->resource->{default};
  if ($special && !exists $self->resource->{$special}) {
    $self->logcroak(
      sprintf 'Tried to get resource spec "%s" in %s but have %s',
      $special,
      ref $self,
      join ', ', keys %{$self->resource}
    );
  }
  return {
    (exists $self->resource->{default}) ? %{$self->resource->{default}} : (),
    (defined $special) ? %{$self->resource->{$special}} : ()
  }
}


=head2 create_definition

Args [1]:    Hashref of specific requirements for this Function
             NOTE: If setting cpus, set in arrayref form: [1,2]
Args [2]:    String, optional. The name of a special resource spec in the graph
Description: Takes custom properties and integrates resources defined for this
             function and instantiates a definition object.
             Some translation is made between resource spec and expectation
             of the definition.
Returntype:  npg_pipeline::function::definition instance
Example:     my $definition = $self->create_definition({preexec => 'sleep 10'});

=cut

sub create_definition {
  my ($self, $custom_args, $special_resource) = @_;

  # Load combined resource requirements
  my $resources = $self->_get_massaged_resources($special_resource);
  $custom_args //= {};
  # and combine with any custom arguments
  $resources = { %{$resources}, %{$custom_args} };
  # Scale up memory numbers to MB expected by definition
  $resources->{memory} *= $GB_TO_MB_CONVERSION;
  # Delete any resource properties that are not accepted by the definition
  # for my $for_show (qw//) {
  #   delete $resources->{$for_show};
  # }

  return npg_pipeline::function::definition->new(
    created_by => ref $self,
    created_on => $self->timestamp(),
    %{$resources}
  );
}

=head2 _get_massaged_resources

Args [1]:    String, optional. The name of a special resource spec in the graph
Description: Converts a minimum and maximum cpu specification and returns
             the resource definition with the change
Returntype:  HashRef containing a compute resource specification
=cut

sub _get_massaged_resources {
  my ($self, $special_resource) = @_;

  my $resources = $self->get_resources($special_resource);
  if (! exists $resources->{minimum_cpu}) {
    $self->logcroak(sprintf 'Resource spec for %s must specify minimum_cpu', ref $self);
  }
  my @num_cpus = ($resources->{minimum_cpu});
  if (exists $resources->{maximum_cpu}) {
    push @num_cpus, $resources->{maximum_cpu};
  }
  delete $resources->{minimum_cpu};
  delete $resources->{maximum_cpu};
  $resources->{num_cpus} = \@num_cpus;

  # Ensure any and all multithreading automatically is constrained to a
  # single host
  if ($num_cpus[-1] > 1) {
    $resources->{num_hosts} = 1;
  }


  return $resources;
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

=item npg_pipeline::function::definition

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
