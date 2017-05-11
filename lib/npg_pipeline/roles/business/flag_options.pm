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

This role gives some boolean flag options which should be set on construction (or via the command line if using MooseX::Getopt)
so that you can turn off global features/functions without having to necessarily specify them.

  --no_summary_link

These would globally stop anything being done should functions be requested which do these (either directly, or by job submission)

=head1 SUBROUTINES/METHODS

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

=head2 no_fix_config_files

flag option to request that config files are not checked and fixed

=cut

has q{no_fix_config_files} => (
  isa           => q{Bool},
  is            => q{ro},
  documentation => q{Request that config files are not checked and fixed (where fixing is appropriate)},
);

=head2 no_array_cpu_limit

flag option to allow job arrays to flood, if able, the farm

=cut

has q{no_array_cpu_limit} => (
  isa           => q{Bool},
  is            => q{ro},
  documentation => q{Allow job arrays to keep launching if cpus available},
);

=head2 array_cpu_limit

set the most number of cpus which each job array can use at a time, applied only if no_array_cpu_limit not set

=cut

has q{array_cpu_limit} => (
  isa           => q{Int},
  is            => q{ro},
  lazy_build    => 1,
  documentation => q{Set the most number of CPUs that a Job array can use at a time},
);
sub _build_array_cpu_limit {
  my ( $self ) = @_;
  return $self->general_values_conf()->{array_cpu_limit};
}

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

## no critic (ProhibitUnusedPrivateSubroutines)
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

=head2 no_sf_resource

do not use sf resource tokens; set if working outside the npg sequencing farm

=cut

has q{no_sf_resource} => (
  isa           => q{Bool},
  is            => q{ro},
  documentation =>
  q{Do not use sf resource tokens; set if working outside the npg sequencing farm},
);

=head2 no_bsub

disable submitting any jobs to bsub, so the pipeline can be run, and all cmds logged

=cut

has q{no_bsub} => (
  isa           => q{Bool},
  is            => q{ro},
  documentation =>
  q{Turn off submitting any jobs to lsf, just logging them instead},
);

=head2 local

sets the default for no_irods_archival, no_warehouse_update and no_summary_link to true;
defaults to the value of no_bsub flag

=cut

has q{local} => (
  isa           => q{Bool},
  is            => q{ro},
  lazy_build    => 1,
  documentation => q{Turn off lots of archiving and updating flags},
);
sub _build_local {
  my $self = shift;
  return $self->no_bsub ? 1 : 0;
}

=head2 spatial_filter

Do we want to use the spatial_filter program?

=cut

has q{spatial_filter} => (
  isa           => q{Bool},
  is            => q{ro},
  default       => 1,
  documentation => q{Use the spatial_filter program},
);

=head2 spider

Toggles spider (creating/reusing cached LIMs data), true by default

=cut

has q{spider} => (
  isa           => q{Bool},
  is            => q{ro},
  default       => 1,
  documentation => q{Toggles spider (creating/reusing cached LIMs data), true by default},
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

Copyright (C) 2017 Genome Research Ltd

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
