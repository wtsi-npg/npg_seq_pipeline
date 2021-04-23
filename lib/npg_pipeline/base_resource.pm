package npg_pipeline::base_resource;

use Moose;
use npg_pipeline::function::definition;

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
    minimum_cpu => 1,
    memory => 2
  }},
  documentation => 'Basic resources that all jobs might need',
);

=head2 resource

HashRef of resource requests for the function, e.g.
{
  minimum_cpu => 4,
  maximum_cpu => 8,
  memory => 10,
  db => ['mlwh']
}

=cut

has resource => (
  isa => 'HashRef',
  is => 'ro',
  lazy => 1,
  default => sub {{}},
  documentation => 'Function-specific resource spec',
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
      __PACKAGE__,
      join ', ', keys %{$self->resource}
    );
  }
  return {
    %{$self->default_defaults},
    (exists $self->resource->{default}) ? %{$self->resource->{default}} : (),
    (defined $special) ? %{$self->resource->{$special}} : ()
  }
}


=head2 create_definition

Args [1]:    Hashref of specific requirements for this Function
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

  # Load combined resource requirements, and combine with any custom arguments
  my $resources = $self->get_resources($special_resource);
  $resources = { %{$resources}, %{$custom_args} };
  my $num_cpus;
  if (exists $resources->{maximum_cpu} && $resources->{minimum_cpu} != $resources->{maximum_cpu}) {
    # Format discrete CPU values for definition ArrayRef
    $num_cpus = [
      delete $resources->{minimum_cpu},
      delete $resources->{maximum_cpu}
    ];
  } else {
    $num_cpus = [delete $resources->{minimum_cpu}];
    delete $resources->{maximum_cpu} if exists $resources->{maximum_cpu};
  }
  # Scale up memory numbers to MB expected by definition
  $resources->{memory} *= 1000;

  # Delete any resource properties that are not accepted by the definition
  # for my $for_show (qw//) {
  #   delete $resources->{$for_show};
  # }

  return npg_pipeline::function::definition->new(
    created_by => __PACKAGE__,
    created_on => $self->timestamp(),
    num_cpus => $num_cpus,
    %{$resources}
  );
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
