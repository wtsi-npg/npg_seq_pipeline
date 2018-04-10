package npg_pipeline::executor;

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;

with qw( WTSI::DNAP::Utilities::Loggable );

our $VERSION = '0';

=head1 NAME

npg_pipeline::executor

=head1 SYNOPSIS

=head1 DESCRIPTION

Submission of function definition for execution - parent object.

Child classes should implement 'executor4function' method that
performs executor-specific processesing of definitions for a single
function. The name of the function is passed to this method as an
argument. 

=cut

=head1 SUBROUTINES/METHODS

=cut

##################################################################
################## Public attributes #############################
##################################################################

=head2 analysis_path

=cut

has 'analysis_path' => (
  isa      => 'Str',
  is       => 'ro',
  required => 0,
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
  my $key = (keys %{$self->function_definitions()})[0];
  my $d = $self->function_definitions()->{$key}->[0];
  my $name = join q[_], 'commands4jobs', $d->identifier(), $d->created_on();
  return join q[/], $self->analysis_path(), $name;
}

##################################################################
############## Public methods ####################################
##################################################################

=head2 execute

Basic implementation. Calls function_loop method and exits on its
return.

=cut

sub execute {
  my $self = shift;
  $self->function_loop();
  return;
}

=head2 function_loop

Implementation of a function loop which relies on presence of a
method that performs executor-specific processesing of definitions .
The method is called as instance method and given one argument, the name
of the function. 

=cut

sub function_loop {
  my $self = shift;

  my @nodes = $self->function_graph()->topological_sort();
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

    $self->info();
    $self->info(qq{***** Processing $function *****});
    if (@{$definitions} == 1) {
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

    $self->executor4function($function);
  }

  return;
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
