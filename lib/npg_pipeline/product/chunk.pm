package npg_pipeline::product::chunk;

use Moose::Role;
use Readonly;
use npg_tracking::util::types;

our $VERSION = '0';

Readonly::Scalar my $CHUNK_DELIM => q[.];

=head1 NAME

npg_pipeline::product::chunk

=head1 SYNOPSIS

=head1 DESCRIPTION

Sequence chunk definition.
Anticipated use:
  - Integer denoting chunk number starting from 1. 

=head1 SUBROUTINES/METHODS

=head2 chunk

=head2 has_chunk

=cut

has 'chunk' =>  ( isa       => 'Maybe[NpgTrackingPositiveInt]',
                  is        => 'ro',
                  predicate => 'has_chunk',
                  required  => 0,
);

=head2 chunk_label

=cut

sub chunk_label {
  my $self = shift;
  return $self->has_chunk() ? $CHUNK_DELIM . $self->chunk() : q[];
}

no Moose::Role;

1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose::Role

=item Readonly

=item npg_tracking::util::types

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Martin Pollard

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2019 Genome Research Ltd.

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
