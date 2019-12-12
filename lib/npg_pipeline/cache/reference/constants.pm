package npg_pipeline::cache::reference::constants;

use strict;
use warnings;
use Carp;
use Exporter qw( import );

Readonly::Scalar our $TARGET_REGIONS_DIR           => q{target};
Readonly::Scalar our $TARGET_AUTOSOME_REGIONS_DIR  => q{target_autosome};
Readonly::Scalar our $REFERENCE_ABSENT             => q{REFERENCE_NOT_AVAILABLE};

our $VERSION = '0';

our @EXPORT_OK = qw/ $TARGET_REGIONS_DIR $TARGET_AUTOSOME_REGIONS_DIR $REFERENCE_ABSENT /;

1;

=head1 NAME

 npg_pipeline::cache::reference::constants

=head1 SYNOPSIS

 use npg_pipeline::cache::reference::constants qw( $TARGET_REGIONS_DIR );

=head1 DESCRIPTION

 Contains constants used by npg_pipeline::cache::reference.

=head1 SUBROUTINES/METHODS

=head1 BUGS AND LIMITATIONS

=head1 INCOMPATIBILITIES

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Carp

=item Exporter

=back

=head1 AUTHOR

 Martin Pollard

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
