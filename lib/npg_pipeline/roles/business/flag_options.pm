package npg_pipeline::roles::business::flag_options;

use Moose::Role;

our $VERSION = '0';

=head1 NAME

npg_pipeline::roles::business::flag_options

=head1 SYNOPSIS

  package MyPackage;
  use Moose;
  ...
  with qw{npg_pipeline::roles::business::flag_options};

=head1 DESCRIPTION

This role gives some boolean flag options which can be set
on construction (or via the command line if using MooseX::Getopt)
so that you can turn off global features/functions.

=head1 SUBROUTINES/METHODS

=head2 verbose

Boolean option to switch on verbose mode

=cut

has q{verbose} => (
  isa           => q{Bool},
  is            => q{ro},
  documentation => q{Boolean decision to switch on verbose mode},
);

=head2 no_summary_link

Do not create a summary link

=cut

has q{no_summary_link} => (
  isa           => q{Bool},
  is            => q{ro},
  lazy          => 1,
  builder       => '_default_to_local',
  documentation => q{Turn off creating a Latest_Summary link},
);

=head2 no_irods_archival

Switches off archival to iRODS repository.

=cut

has q{no_irods_archival} => (
  isa           => q{Bool},
  is            => q{ro},
  lazy          => 1,
  builder       => '_default_to_local',
  documentation => q{Switches off archival to iRODS repository.},
);

sub _default_to_local {
  my $self = shift;
  return $self->local;
}
## use critic

=head2 no_warehouse_update

Switches off updating the NPG warehouse.

=cut

has q{no_warehouse_update} => (
  isa           => q{Bool},
  is            => q{ro},
  lazy          => 1,
  builder       => '_default_to_local',
  documentation => q{Switches off updating the NPG warehouse.},
);

=head2 local

Sets the default for no_irods_archival, no_warehouse_update and
no_summary_link to true.
Defaults to the value of no_bsub flag if no_bsub flag is available.

=cut

has q{local} => (
  isa           => q{Bool},
  is            => q{ro},
  lazy_build    => 1,
  documentation => q{Turn off lots of archiving and updating flags},
);
sub _build_local {
  my $self = shift;
  return $self->can('no_bsub') && $self->no_bsub ? 1 : 0;
}

=head2 no_adapterfind

Switches off adapter find/clip

=cut

has q{no_adapterfind} => (
  isa           => q{Bool},
  is            => q{ro},
  documentation => q{Switches off adapter finding/clipping.},
);

=head2 p4s2_aligner_intfile

Forces p4 stage2 to create an intermediate file when doing alignments

=cut

has q{p4s2_aligner_intfile} => (
  isa           => q{Bool},
  is            => q{ro},
  documentation => q{Forces p4 stage2 to create an intermediate file when doing alignments.},
);

1;
__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose::Role

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown

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
