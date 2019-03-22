package npg_pipeline::validation::s3;

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;

with qw/ npg_pipeline::validation::common
         npg_pipeline::product::release
         WTSI::DNAP::Utilities::Loggable /;

our $VERSION = '0';

=head1 NAME

npg_pipeline::validation::s3

=head1 SYNOPSIS

=head1 DESCRIPTION

Validation of files present in a pre-configures s3 location
against files present on staging. Only files that belong
to products that should have been arcived to s3 are
considered. Currently validation fails if any files for this
run should have been archived to s3.

=head1 SUBROUTINES/METHODS

=head2 product_entities

=head2 eligible_product_entities

=head2 build_eligible_product_entities

Builder method for the eligible_product_entities attribute.

=cut

sub build_eligible_product_entities {
  my $self = shift;
  @{$self->product_entities}
    or $self->logcroak('product_entities array cannot be empty');
  my @p =
    grep { $self->is_release_data($_->target_product) &&
           $self->is_for_s3_release($_->target_product) }
    @{$self->product_entities};
  return \@p;
}

=head2 fully_archived

Currently returns false if any files for this
run should have been archived to s3.

=cut

sub fully_archived {
  my $self = shift;
  #####
  # Proper assessment is pending. For now we do not want
  # to delete any runs that have data that should have
  # gone to s3.
  return !@{$self->eligible_product_entities()};
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

=item npg_pipeline::product::release

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2019 GRL

This file is part of NPG.

NPG is free software: you can redistribute it and/or modify
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
