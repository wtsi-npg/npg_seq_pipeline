package npg_pipeline::product::heron::upload::library;

use strict;

use Carp;
use Data::Dump qw(pp);
use List::MoreUtils qw(one);
use Moose;
use MooseX::StrictConstructor;

our $VERSION = '0';

# Sequencing kits
our $NEB_ULTRA = q(NEB Ultra II);
our @SEQUENCING_KITS = ($NEB_ULTRA);

# Sequencing protocols
our $LIGATION  = q(LIGATION);
our @SEQUENCING_PROTOCOLS = ($LIGATION);

# Sequencing layout configs
our @SEQUENCING_LAYOUT_CONFIGS = qw(SINGLE PAIRED);

# Library selections
our @LIBRARY_SELECTIONS = qw(RANDOM PCR RANDOM_PCR OTHER);

# Library sources
our @LIBRARY_SOURCES = qw(GENOMIC TRANSCRIPTOMIC METAGENOMIC METATRANSCRIPTOMIC VIRAL_RNA OTHER);

# Library strategies
our @LIBRARY_STRATEGIES = qw(WGA WGS AMPLICON TARGETED_CAPTURE OTHER);

has 'name' =>
    (isa           => 'Str',
     is            => 'ro',
     required      => 1,
     documentation => 'The COG-UK compliant library name',);

has 'selection' =>
    (isa           => 'Str',
     is            => 'ro',
     required      => 1,
     documentation => 'The COG-UK compliant library selection',);

has 'source' =>
    (isa           => 'Str',
     is            => 'ro',
     required      => 1,
     documentation => 'The COG-UK compliant library source',);

has 'strategy' =>
    (isa           => 'Str',
     is            => 'ro',
     required      => 1,
     documentation => 'The COG-UK compliant library strategy',);

has 'artic_protocol' =>
    (isa           => 'Str',
     is            => 'ro',
     required      => 0,
     predicate     => 'has_artic_protocol',
     documentation => 'The COG-UK compliant ARTIC protocol name. ' .
                      'This is not validated by the server and is not necessary',);

has 'artic_primers_version' =>
    (isa           => 'Str',
     is            => 'ro',
     required      => 0,
     predicate     => 'has_artic_primers_version',
     documentation => 'The version of the ARTIC primers used',);

has 'sequencing_kit' =>
    (isa           => 'Str',
     is            => 'ro',
     required      => 1,
     documentation => 'The COG-UK compliant sequencing kit name',);

has 'sequencing_protocol' =>
    (isa           => 'Str',
     is            => 'ro',
     required      => 1,
     documentation => 'The COG-UK compliant sequencing protocol name',);

has 'sequencing_layout_config' =>
    (isa           => 'Str',
     is            => 'ro',
     required      => 1,
     documentation => 'The COG-UK compliant sequencing layout config',);

=head2 BUILD

Validates constructor arguments for correctness. See
https://docs.covid19.climb.ac.uk/metadata

=cut

sub BUILD {
  my ($self, $args) = @_;

  # Zero is not a valid value for any of these
  $args->{name}                  or croak 'An empty name was supplied';

  my $sel = $args->{selection};
  one { $sel eq $_ } @LIBRARY_SELECTIONS or croak "Invalid library selection '$sel'";

  my $src = $args->{source};
  one { $src eq $_ } @LIBRARY_SOURCES or croak "Invalid library source '$src'";

  my $str = $args->{strategy};
  one { $str eq $_ } @LIBRARY_STRATEGIES or croak "Invalid library strategy '$str'";

  my $kit = $args->{sequencing_kit};
  one { $kit eq $_ } @SEQUENCING_KITS or croak "Invalid sequencing kit '$kit'";

  my $pro = $args->{sequencing_protocol};
  one { $pro eq $_ } @SEQUENCING_PROTOCOLS or croak "Invalid sequencing protocol '$pro'";

  my $lay = $args->{sequencing_layout_config};
  one { $lay eq $_ } @SEQUENCING_LAYOUT_CONFIGS or croak "Invalid sequencing layout config '$lay'";

  return 1;
}

=head2 str

Return a string summary of the library.

=cut

sub str {
  my ($self) = @_;

  return sprintf q[{library: name: %s, artic primers version: %s, selection: %s, ] .
                 q[source: %s, strategy: %s, kit: %s, protocol: %s, layout: %s}],
      $self->name,
      $self->has_artic_primers_version ? $self->artic_primers_version : q(),
      $self->selection, $self->source, $self->strategy, $self->sequencing_kit,
      $self->sequencing_protocol, $self->sequencing_layout_config;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

npg_pipeline::product::heron::upload::library

=head1 VERSION

=head1 SYNOPSIS

=head1 DESCRIPTION

Represents an instrument run to be uploaded to the COG-UK endpoint
described at at https://docs.covid19.climb.ac.uk/metadata.

Instances will validate their constructor arguments and raise an error
if any are invalid according to the description above.

=head1 SUBROUTINES/METHODS

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2020, Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
