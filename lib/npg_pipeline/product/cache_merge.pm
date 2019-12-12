package npg_pipeline::product::cache_merge;

use namespace::autoclean;

use Data::Dump qw[pp];
use Moose::Role;
use File::Spec::Functions qw{catdir};

with qw{npg_pipeline::product::release};

our $VERSION = '0';

=head1 NAME

npg_pipeline::product::cache_merge

=head1 SYNOPSIS

=head1 DESCRIPTION

A role providing locations of directories in which to cache data
products ready for a merge with top-up data.

=head1 SUBROUTINES/METHODS

=head2 merge_component_study_cache_dir

  Arg [1]    : npg_pipeline::product

  Example    : $obj->merge_component_cache_dir($product)
  Description: Returns a study-specific directory in which to cache data
               products ready for a merge with top-up data or an undefined
               value if not configured. The directory might not exist.

  Returntype : Str

=cut

sub merge_component_study_cache_dir {
  my ($self, $product) = @_;

  my $dir = $self->find_study_config($product)->{merge}->{component_cache_dir};
  if (ref $dir) {
    $self->logconfess('Invalid directory in configuration file: ', pp($dir));
  }

  return $dir;
}

=head2 merge_component_cache_dir

  Arg [1]    : npg_pipeline::product

  Example    : $obj->merge_component_cache_dir($product)
  Description: Returns a product-specific directory in which to cache data
               products ready for a merge with top-up data or an undefined
               value if not configured. The directory might not exist.

  Returntype : Str

=cut

sub merge_component_cache_dir {
  my ($self, $product) = @_;
  my $study_dir =  $self->merge_component_study_cache_dir($product);
  my $dir;
  if ($study_dir) {
    my $digest = $product->composition()->digest();
    $dir = catdir($study_dir, substr($digest,0,2), substr($digest,2,2), $digest);
  }
  return $dir;
}

1;

__END__

=head1 BUGS AND LIMITATIONS

=head1 INCOMPATIBILITIES

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Data::Dump

=item Moose::Role

=item File::Spec::Functions

=back

=head1 AUTHOR

David K. Jackson

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
