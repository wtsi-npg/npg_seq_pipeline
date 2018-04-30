package npg_pipeline::executor;

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;
use Graph::Directed;
use Readonly;

use npg_tracking::util::types;

with qw{ WTSI::DNAP::Utilities::Loggable };

our $VERSION = '0';

Readonly::Scalar my $VERTEX_NUM_DEFINITIONS_ATTR_NAME => q{num_definitions};

=head1 NAME

npg_pipeline::executor

=head1 SYNOPSIS

  package npg_pipeline::executor::exotic;
  use Moose;
  extends 'pg_pipeline::executor';
  
  override 'execute' => sub {
    my $self = shift;
    $self->info('Child implementation');
  };
  1;

  package main;
  use Graph::Directed;
  use npg_pipeline::function::definition;

  my $g =  Graph::Directed->new();
  $g->add_edge('node_one', 'node_two');

  my $d = npg_pipeline::function::definition->new(
    created_by   => 'module',
    created_on   => 'June 25th',
    job_name     => 'name',
    identifier   => 2345,
    command      => '/bin/true',
    log_file_dir => '/tmp/dir'
  );

  my $e1 = npg_pipeline::executor::exotic->new(
    function_graph          => $g,
    function_definitions    => {node_one => [$d]},
    commands4jobs_file_path => '/tmp/path'
  );
  $e1->execute();

  my $e2 = npg_pipeline::executor::exotic->new(
    function_graph       => $g,
    function_definitions => {node_one => [$d],
    analysis_path        => '/tmp/analysis'
  );
  print $e2->commands4jobs_file_path();
  $e2->execute(); 

=head1 DESCRIPTION

Submission of function definition for execution - parent object.
Child classes should implement 'execute' method.

=cut

=head1 SUBROUTINES/METHODS

=cut

##################################################################
################## Public attributes #############################
##################################################################

=head2 analysis_path

=cut

has 'analysis_path' => (
  isa       => 'NpgTrackingDirectory',
  is        => 'ro',
  required  => 0,
  predicate => 'has_analysis_path',
);

=head2 function_graph

=cut

has 'function_graph' => (
  is       => 'ro',
  isa      => 'Graph::Directed',
  required => 1,
);

=head2 function_definitions

=cut

has 'function_definitions' => (
  is       => 'ro',
  isa      => 'HashRef',
  required => 1,
);

=head2 commands4jobs

=cut

has 'commands4jobs' => (
  is      => 'ro',
  isa     => 'HashRef',
  default => sub {return {};},
);

=head2 commands4jobs_file_path

=cut

has 'commands4jobs_file_path' => (
  is         => 'ro',
  isa        => 'Str',
  lazy_build => 1,
);
sub _build_commands4jobs_file_path {
  my $self = shift;

  if (!$self->has_analysis_path()) {
    $self->logcroak(q{analysis_path attribute is not set});
  }
  my @functions = keys %{$self->function_definitions()};
  if (!@functions) {
    $self->logcroak(q{Definition hash is empty});
  }
  my $d = $self->function_definitions()->{$functions[0]}->[0];
  if (!$d) {
    $self->logcroak(q{Empty definition array for } . $functions[0]);
  }
  my $name = join q[_], 'commands4jobs', $d->identifier(), $d->created_on();
  return join q[/], $self->analysis_path(), $name;
}

=head2 function_graph4jobs

The graph of functions that have to be executed. The same graph as in
the 'function_graph' attribute, but the functions that have to be skipped
or have definitions that are immediately executed are excluded.

Each node of this graph has 'num_definitions' attribute set.

=cut

has 'function_graph4jobs' => (
  is         => 'ro',
  isa        => 'Graph::Directed',
  init_arg   => undef,
  required   => 0,
  lazy_build => 1,
);
sub _build_function_graph4jobs {
  my $self = shift;

  my $graph = Graph::Directed->new();

  my $g = $self->function_graph();
  my @nodes = $g->topological_sort();
  if (!@nodes) {
    $self->logcroak('Empty function graph');
  }

  foreach my $function (@nodes) {

    if (!exists $self->function_definitions()->{$function}) {
      $self->logcroak(qq{Function $function is not defined});
    }
    my $definitions = $self->function_definitions()->{$function};
    if (!$definitions) {
      $self->logcroak(qq{No definition array for function $function});
    }
    if(!@{$definitions}) {
      $self->logcroak(qq{Definition array for function $function is empty});
    }

    my $num_definitions = scalar @{$definitions};

    if ($num_definitions == 1) {
      my $d = $definitions->[0];
      if ($d->immediate_mode) {
        $self->info(qq{***** Function $function has been already run});
        next;
      }
      if ($d->excluded) {
        $self->info(qq{***** Function $function is excluded});
        next;
      }
    }

    #####
    # Find all closest ancestors that represent functions that will be
    # submitted for execution, bypassing the skipped functions and functions
    # that have been executed in the immediate mode.
    #
    # For each returned predecessor create an edge from the redecessor function
    # to this function. Adding an edge implicitly add its vertices. Adding
    # a vertex is by default idempotent. Setting a vertex attribute creates
    # a vertex if it does not already exists.
    #
    my @predecessors = predecessors($g, $function, $VERTEX_NUM_DEFINITIONS_ATTR_NAME);

    if (@predecessors || $g->is_source_vertex($function)) {
      foreach my $gr (($g, $graph)) {
        $gr->set_vertex_attribute($function,
                                  $VERTEX_NUM_DEFINITIONS_ATTR_NAME,
                                  $num_definitions);
      }
      foreach my $p (@predecessors) {
        $graph->add_edge($p, $function);
      }
    }
  }

  if (!$graph->vertices()) {
    $self->logcroak('New function graph is empty');
  }

  return $graph;
}

##################################################################
############## Public methods ####################################
##################################################################

=head2 execute

Basic implementation that does not do anything. The method should be
implemented by a child class.

=cut

sub execute { return; }

=head2 predecessors

Recursive function. The recursion ends when we either
reach the start point - the vertext that has no predesessors -
or a vertex whose all predesessors have the attribute, the name of
which is given as an argument, set.

Should not be called as a class or instance method.

Returns a list of found predecessors.

  my @predecessor_functions = predecessors($graph,
                                           'qc_insert_size',
                                           'num_definitions');
=cut

sub predecessors {
  my ($g, $function_name, $attr_name) = @_;

  my @predecessors = ();
  foreach my $p (sort $g->predecessors($function_name)) {
    if ($g->has_vertex_attribute($p, $attr_name)) {
      push @predecessors, $p;
    } else {
      push @predecessors, predecessors($g, $p, $attr_name);
    }
  }
  return @predecessors;
}

=head2 dependencies

Returns a list of function's (jobs') dependencies that are saved
in graph nodes' attributes given as the second argument;

  my @dependencies = $e->dependencies('qc_insert_size', 'lsf_job_ids');

=cut

sub dependencies {
  my ($self, $function_name, $attr_name) = @_;

  my $g = $self->function_graph4jobs();
  my @dependencies = ();
  foreach my $p ($g->predecessors($function_name)) {
    if (!$g->has_vertex_attribute($p, $attr_name)) {
      $self->logcroak(qq{$attr_name attribute does not exist for $p})
    }
    my $attr_value = $g->get_vertex_attribute($p, $attr_name);
    if (!$attr_value) {
      $self->logcroak(qq{Value of the $attr_name is not defined for $p});
    }
    push @dependencies, $attr_value;
  }

  return @dependencies;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item MooseX::StrictConstructor

=item namespace::autoclean

=item Graph::Directed

=item Readonly

=item WTSI::DNAP::Utilities::Loggable

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

=over

=item Marina Gourtovaia

=back

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2018 Genome Research Ltd.

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
