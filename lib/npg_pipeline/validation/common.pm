package npg_pipeline::validation::common;

use Moose::Role;

with 'WTSI::DNAP::Utilities::Loggable';

our $VERSION = '0';

=head1 NAME

npg_pipeline::validation::common

=head2 logger

=head2 staging_files

=cut

has 'staging_files'  => (isa      => 'HashRef',
                         is       => 'ro',
                         required => 1,
                        );

=head2 file_extension

File extension for the sequence file format, required.

=cut

has 'file_extension' => (isa      => 'Str',
                         is       => 'ro',
                         required => 0,
                        );

=head2 index_file_extension

File extension for the sequence file index, cannot be set, inferred.

=cut

has 'index_file_extension' => (isa        => 'Str',
                               is         => 'ro',
                               required   => 0,
                               lazy_build => 1,
                              );
sub _build_index_file_extension {
  my $self = shift;
  my $e = $self->file_extension;
  $e =~ s/m\Z/i/xms;
  return $e;
}

=head2 composition_file_extension

File extension for the sequence file format, required.

=cut

has 'composition_file_extension' => (isa      => 'Str',
                                     is       => 'ro',
                                     required => 0,
                                     default  => 'composition.json',
                                    );

no Moose::Role;

1;

__END__

=head1 NAME

npg_pipeline::validation::common

=head1 SYNOPSIS

=head1 DESCRIPTION

Moose role. Common functionality for modules of run_is_deletable script.

=head1 SUBROUTINES/METHODS

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose::Role

=item WTSI::DNAP::Utilities::Loggable

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

