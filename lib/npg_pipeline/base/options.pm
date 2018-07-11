package npg_pipeline::base::options;

use Moose::Role;
use npg_pipeline::cache;

our $VERSION = '0';

=head1 NAME

npg_pipeline::base::options

=head1 SYNOPSIS

=head1 DESCRIPTION

This role defines options which can be set on object construction
(or via the command line if using MooseX::Getopt).

=head1 SUBROUTINES/METHODS

=head2 verbose

Boolean flag to switch on verbose mode

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

=head2 lanes

An array of lanes (positions) to run the pipeline on.

=cut

has q{lanes} => (
  isa           => q{ArrayRef[Int]},
  is            => q{ro},
  documentation => q{An array of lanes (positions) to run the pipeline on},
  default       => sub { [] },
);

=head2 adapterfind

Toggles adapter finding in stage1 analysis

=cut

has q{adapterfind} => (
  isa           => q{Bool},
  is            => q{ro},
  lazy_build    => 1,
  documentation => q{Toggles adapter finding in stage1 analysis.},
);
sub _build_adapterfind {
  my $self = shift;

  return $self->platform_NovaSeq? 0: 1;
}

=head2 p4s1_alignment_method

set the PhiX alignment method for p4 stage1

=cut

has q{p4s1_phix_alignment_method} => (
  isa           => q{Str},
  is            => q{ro},
  lazy_build    => 1,
  documentation => q{set the PhiX alignment method for p4 stage1},
);
sub _build_p4s1_phix_alignment_method {
  my $self = shift;

  my $alignment_method = $self->platform_NovaSeq? q[minimap2]: q[bwa_aln];

  if($alignment_method eq q[bwa_aln] and not $self->is_paired_read) {
      $alignment_method = q[bwa_aln_se];
  }

  return $alignment_method;
}


=head2 p4s2_aligner_intfile

Forces p4 stage2 to create an intermediate file when doing alignments

=cut

has q{p4s2_aligner_intfile} => (
  isa           => q{Bool},
  is            => q{ro},
  documentation => q{Forces p4 stage2 to create an intermediate file when doing alignments.},
);

=head2 align_tag0

Toggles alignment of tag#0 in secondary analysis

=cut

has q{align_tag0} => (
  isa           => q{Bool},
  is            => q{ro},
  default       => 0,
  documentation => q{Do target alignment for tag#0 in stage2 analysis.},
);

=head2 repository

A custom repository root directory.

=cut

has q{repository} => (
  isa           => q{Str},
  is            => q{ro},
  required      => 0,
  predicate     => q{has_repository},
  documentation => q{A custom repository root directory},
);

=head2 id_flowcell_lims

Optional LIMs identifier for flowcell.

=cut

has q{id_flowcell_lims} => (
  isa      => q{Int},
  is       => q{ro},
  required => 0,
);

=head2 qc_run

Boolean flag indicating whether this run is a qc run,
will be built if not supplied;

=cut

has q{qc_run} => (
 isa           => q{Bool},
 is            => q{ro},
 lazy_build    => 1,
 documentation => q{Boolean flag indicating whether the run is QC run, }.
                  q{will be built if not supplied},);
sub _build_qc_run {
  my $self = shift;
  return $self->is_qc_run();
}

=head2 is_qc_run

Examines id_flowcell_lims attribute. If it consists of 13 digits, ie is a tube barcode,
returns true, otherwise returns false.

=cut

sub is_qc_run {
  my ($self, $lims_id) = @_;
  $lims_id ||= $self->id_flowcell_lims;
  return $lims_id && $lims_id =~ /\A\d{13}\z/smx; # it's a tube barcode
}

=head2 lims_driver_type

Optional lims driver type name

=cut

has q{lims_driver_type} => (
  isa           => q{Str},
  is            => q{ro},
  lazy_build    => 1,
  documentation => q{Optional lims driver type name},
);
sub _build_lims_driver_type {
  my $self = shift;
  return $self->qc_run ?
    ($self->is_qc_run($self->id_flowcell_lims) ?
       npg_pipeline::cache->warehouse_driver_name :
       npg_pipeline::cache->mlwarehouse_driver_name
    ) : npg_pipeline::cache->mlwarehouse_driver_name;
}

no Moose::Role;

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
