#############
# $Id: dispatch_tree.pm 14773 2011-12-12 10:52:07Z mg8 $
# Created By: ajb
# Last Maintained By: $Author: mg8 $
# Created On: 2009-08-07
# Last Changed On: $Date: 2011-12-12 10:52:07 +0000 (Mon, 12 Dec 2011) $
# $HeadURL: svn+ssh://svn.internal.sanger.ac.uk/repos/svn/new-pipeline-dev/npg-pipeline/trunk/lib/npg_pipeline/dispatch_tree.pm $

package npg_pipeline::dispatch_tree;

use Moose;
use JSON;
use Readonly; Readonly::Scalar our $VERSION => do { my ($r) = q$LastChangedRevision: 14773 $ =~ /(\d+)/mxs; $r; };

has q{json_structure} => (isa => q{Str},
                          is  => 'ro',
                          predicate => q{has_json_structure},
                          writer => q{_set_json_structure},);
has q{functions}      => (isa => q{ArrayRef},
                          is  => 'ro',
                          default => sub {return [];},
                          writer => q{_set_functions},);

sub BUILD {
  my ($self) = @_;
  if ($self->has_json_structure()) {
    $self->_populate_with_json();
  }
  return 1;
}

sub append_to_functions {
  my ($self, $function_info) = @_;
  push @{$self->functions()}, $function_info;
  return 1;
}

sub tree_as_json {
  my ($self) = @_;
  my $href = $self->_data_structure();
  $self->_set_json_structure(to_json($href));
  return $self->json_structure();
}

sub num_functions {
  my $self = shift;
  my $num = @{$self->functions};
  return $num;
}

sub ordered_job_ids {
  my $self = shift;
  my @ids = ();
  foreach my $f (@{$self->functions}) {
    if ($f->{job_ids_launched} && @{$f->{job_ids_launched}}) {
      push @ids, @{$f->{job_ids_launched}};
    }
  }
  return @ids;
}

sub first_function_name {
  my $self = shift;
  my @functions = @{$self->functions};
  if (@functions) {
    return $functions[0]->{function};
  }
  return q[];
}

sub _populate_with_json {
  my ($self) = @_;
  my $href = from_json($self->json_structure());
  foreach my $key (keys %{$href}) {
    my $writer_method = q{_set_}.$key;
    $self->$writer_method($href->{$key});
  }
  return $self;
}

sub _data_structure {
  my ($self) = @_;
  my $href = {};
  $href->{functions} = $self->functions();

  return $href;
}

no Moose;
__PACKAGE__->meta->make_immutable;
1;
__END__

=head1 NAME

npg_pipeline::dispatch_tree

=head1 VERSION

$LastChangedRevision: 14773 $

=head1 SYNOPSIS

  my $oTree = npg_pipeline::dispatch_tree->new();

The object can also be given the json structure it would create, and populate itself with that

  my $oTree = npg_pipeline::dispatch_tree->new(json_structure => $json_string);

=head1 DESCRIPTION

Object model for storing, creating and reading a dispatch tree from a pipeline.
Writes a json string of all processes in the pipeline to the log file (via finish method) so that it can be read and utilised.

=head1 SUBROUTINES/METHODS

=head2 BUILD - sets up the functions array on new construction

=head2 append_to_functions - appends into the data structure an element which needs to be stored for the functions called in order

  $oTree->append_to_functions({
    function => <function_name>,
    job_ids_launched => [job_id_1,job_id_2,...],
    job_dependencies => q{-w'done(1) && done(2) && done(3)'},
  });

=head2 tree_as_json - outputs the stored data tree/structure as a JSON string

  my $sJsonStructure = $oTree->tree_as_json();

=head2 functions - returns arrayref of the functions or function information stored in the order appended

  my $aFunctions = $oTree->functions();

=head2 first_function_name

=head2 num_functions

=head2 ordered_job_ids - iob ids in order they were submitted

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item JSON

=item Readonly

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

$Author: mg8 $

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2010 GRL, by Andy Brown (ajb@sanger.ac.uk)

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
