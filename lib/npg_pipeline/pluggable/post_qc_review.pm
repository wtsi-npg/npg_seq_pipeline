package npg_pipeline::pluggable::post_qc_review;

use Moose;

extends qw{npg_pipeline::pluggable};

our $VERSION = '0';

=head1 NAME

npg_pipeline::pluggable::post_qc_review

=head1 SYNOPSIS

  my $p = npg_pipeline::pluggable::post_qc_review->new();

=head1 DESCRIPTION

Pipeline module for the post_qc_review pipeline

=head1 SUBROUTINES/METHODS

=cut

no Moose;
__PACKAGE__->meta->make_immutable;
1;
__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown
Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2018 Genome Research Ltd

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
