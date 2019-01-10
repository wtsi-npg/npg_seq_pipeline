package npg_pipeline::validation::entity;

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;
use File::Spec;
use Carp;

our $VERSION = '0';

=head1 NAME

npg_pipeline::validation::entity

=head1 SYNOPSIS
 
=head1 DESCRIPTION

A wrapper for npg_pipeline product objects for the same entity.

=head1 SUBROUTINES/METHODS

=cut

=head2 target_product

An npg_pipeline::product object  with no subset defined.

=cut

has 'target_product' => (
  isa      => 'npg_pipeline::product',
  is       => 'ro',
  required => 1,
);

=head2 staging_archive_root

Top level directory where files are located.
Normally, the archive directory of the run folder.

=cut

has 'staging_archive_root' => (
  isa      => 'Str',
  is       => 'ro',
  required => 1,
);

=head2 per_product_archive

A boolean attribute indicating whether a per-product staging archive
is being used, true by default.

=cut

has 'per_product_archive' => (
  isa      => 'Bool',
  is       => 'ro',
  required => 0,
  default  => 1,
);

=head2 subsets

An array of subsets which should be available for the
target product.

=cut

has 'subsets' => (
  isa      => 'ArrayRef[Str]',
  is       => 'ro',
  required => 0,
  default  => sub {[]},
);

=head2 description

A description of the target product.

=cut

has 'description' => (
  isa        => 'Str',
  is         => 'ro',
  required   => 0,
  lazy_build => 1,
);
sub _build_description {
  my $self = shift;
  return $self->target_product->composition->freeze;
}

=head2 related_products

An array of npg_pipeline::product object, which correspond
to the 

=cut

has 'related_products' => (
  isa        => 'ArrayRef',
  is         => 'ro',
  required   => 0,
  lazy_build => 1,
);
sub _build_related_products {
  my $self = shift;
  my @products = map {$self->target_product->subset_as_product($_)}
                     @{$self->subsets};
  return \@products;
}

=head2 entity_relative_path

=cut

has 'entity_relative_path' => (
  isa        => 'Str',
  is         => 'ro',
  required   => 0,
  lazy_build => 1,
);
sub _build_entity_relative_path {
  my $self = shift;

  my $rel_path;
  if ($self->per_product_archive) {
    $rel_path = $self->target_product->dir_path;
  } else {
    my $composition = $self->target_product->composition;
    my $component = $composition->get_component(0);
    $rel_path = defined $component->tag_index ?
                'lane' . $component->position : q[];
  }

  return $rel_path;
}

=head2 entity_staging_path

=cut

has 'entity_staging_path' => (
  isa        => 'Str',
  is         => 'ro',
  required   => 0,
  lazy_build => 1,
);
sub _build_entity_staging_path {
  my $self = shift;
  my $path = $self->staging_archive_root;
  if ($self->entity_relative_path) {
    $path = File::Spec->catdir($path, $self->entity_relative_path);
  }
  return $path;
}

=head2 staging_files

Returns a list of files (full paths) which are expected to be on staging
for this entity.

=cut

sub staging_files {
  my ($self, $extension) = @_;
  $extension or croak 'Extension required';
  return map { join q[.], $_, $extension } @{$self->_staging_files_no_ext};
}

has '_staging_files_no_ext' => (
  isa        => 'ArrayRef[Str]',
  is         => 'ro',
  required   => 0,
  lazy_build => 1,
);
sub _build__staging_files_no_ext {
  my $self = shift;
  my @files =
    map { File::Spec->catfile($self->entity_staging_path, $_->file_name_root) }
    $self->target_product, @{$self->related_products};
  return \@files;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item namespace::autoclean

=item Moose

=item MooseX::StrictConstructor

=item File::Spec

=item Carp

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

=cut
