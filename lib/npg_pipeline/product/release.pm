package npg_pipeline::product::release;

use namespace::autoclean;

use Data::Dump qw{pp};
use Moose::Role;
use List::Util qw{all any};
use Readonly;

with qw{WTSI::DNAP::Utilities::Loggable
        npg_tracking::util::pipeline_config};

our $VERSION = '0';

Readonly::Scalar our $IRODS_RELEASE                   => q{irods};
Readonly::Scalar our $IRODS_PP_RELEASE                => q{irods_pp};

=head1 SUBROUTINES/METHODS

=head2 expected_files

  Arg [1]    : Data product whose files to list, npg_pipeline::product.

  Example    : my @files = $obj->expected_files($product)
  Description: Return a sorted list of the files expected to be present for
               archiving in the runfolder.

  Returntype : Array

=cut

sub expected_files {
  my ($self, $product) = @_;

  $product or $self->logconfess('A product argument is required');

  my @expected_files;
  my $lims = $product->lims or
    $self->logcroak('Product requires lims attribute to determine alignment');
  my $aligned = $lims->study_alignments_in_bam;

  my $dir_path = $product->existing_path($self->archive_path());
  my @extensions = qw{cram cram.md5 seqchksum sha512primesums512.seqchksum};
  if ( $aligned ) { push @extensions, qw{cram.crai bcfstats}; }
  push @expected_files,
    map { $product->file_path($dir_path, ext => $_) } @extensions;

  my @suffixes = qw{F0x900 F0xB00};
  if  ( $aligned ) { push @suffixes, qw{F0xF04_target F0xF04_target_autosome}; }
  push @expected_files,
    map { $product->file_path($dir_path, suffix => $_, ext => 'stats') }
    @suffixes;

  if ($aligned){
    my $qc_path = $product->existing_qc_out_path($self->archive_path());
    my @qc_extensions = qw{verify_bam_id.json};
    push @expected_files,
      map { $product->file_path($qc_path, ext => $_) } @qc_extensions;
  }

  @expected_files = sort @expected_files;

  return @expected_files;
}

=head2 is_release_data

  Arg [1]    : npg_pipeline::product

  Example    : $obj->is_release_data($product)
  Description: Return true if the product is data for release i.e.
                - is not spiked-in control data
                - is not data from tag zero, ie leftover data
                  after deplexing

  Returntype : Bool

=cut

sub is_release_data {
  my ($self, $product) = @_;

  $product or $self->logconfess('A product argument is required');

  my $rpt = $product->rpt_list();
  my $name = $product->file_name_root();
  if ($product->is_tag_zero_product) {
    $self->debug("Product $name, $rpt is NOT for release (is tag zero)");
    return 0;
  }

  if ($product->lims->is_control) {
    $self->debug("Product $name, $rpt is NOT for release (is control)");
    return 0;
  }

  $self->debug("Product $name, $rpt is for release ",
              '(is not tag zero or control)');

  return 1;
}

=head2 is_for_release

  Arg [1]    : npg_pipeline::product or st::api::lims or similar
  Arg [2]    : Str, type of release

  Example    : $obj->is_for_release($product, 'irods');
  Description: Return true if the product is to be released via the
               mechanism defined by the second argument.

  Returntype : Bool

=cut

sub is_for_release {
  my ($self, $product, $type_of_release) = @_;

  my @rtypes = ($IRODS_RELEASE, $IRODS_PP_RELEASE);

  $type_of_release or
      $self->logcroak(q[A defined type_of_release argument is required, ],
                      q[expected one of: ], pp(\@rtypes));

  any { $type_of_release eq $_ } @rtypes or
      $self->logcroak("Unknown release type '$type_of_release', ",
                      q[expected one of: ], pp(\@rtypes));

  my $study_config = (ref $product eq 'npg_pipeline::product')
                   ? $self->find_study_config($product)
                   : $self->study_config($product); # the last one is for lims objects
  if ($study_config and $study_config->{$type_of_release}) {
    return $study_config->{$type_of_release}->{enable};
  }
  return;
}

=head2 haplotype_caller_enable
 
 Arg [1]    : npg_pipeline::product
 
 Example    : $obj->haplotype_caller_enable($product)
 Description: Return true if HaplotypeCaller is to be run on the product.
 
 Returntype : Bool
 
=cut

sub haplotype_caller_enable {
  my ($self, $product) = @_;

  my $rpt          = $product->rpt_list();
  my $name         = $product->file_name_root();

  if ($self->find_tertiary_config($product)->{haplotype_caller}->{enable}) {
    $self->info("Product $name, $rpt is for HaplotypeCaller processing");
    return 1;
  }

  $self->info("Product $name, $rpt is NOT for HaplotypeCaller processing");

  return 0;
}

=head2 haplotype_caller_chunking
 
 Arg [1]    : npg_pipeline::product
 
 Example    : $obj->haplotype_caller_chunking($product)
 Description: Returns base name of chunking file for product.

 Returntype : Str
 
=cut

sub haplotype_caller_chunking {
  my ($self, $product) = @_;

  return $self->find_tertiary_config($product)->{haplotype_caller}->{sample_chunking};
}

=head2 haplotype_caller_chunking_number
 
 Arg [1]    : npg_pipeline::product
 
 Example    : $obj->haplotype_caller_chunking_number($product)
 Description: Returns number of chunks for product.
 
 Returntype : Str
 
=cut

sub haplotype_caller_chunking_number {
  my ($self, $product) = @_;

  return $self->find_tertiary_config($product)->{haplotype_caller}->{sample_chunking_number};
}

=head2 bqsr_enable
 
 Arg [1]    : npg_pipeline::product
 
 Example    : $obj->bqsr_enable($product)
 Description: Return true if BQSR is to be run on the product.
 
 Returntype : Bool
 
=cut

sub bqsr_enable {
  my ($self, $product) = @_;

  my $rpt          = $product->rpt_list();
  my $name         = $product->file_name_root();

  if ($self->find_tertiary_config($product)->{bqsr}->{enable}) {
    $self->info("Product $name, $rpt is for BQSR processing");
    return 1;
  }

  $self->info("Product $name, $rpt is NOT for BQSR processing");

  return 0;
}

=head2 bqsr_apply_enable
 
 Arg [1]    : npg_pipeline::product
 
 Example    : $obj->bqsr_enable($product)
 Description: Return true if BQSR is to be applied to the product.
 
 Returntype : Bool
 
=cut

sub bqsr_apply_enable {
  my ($self, $product) = @_;

  my $rpt          = $product->rpt_list();
  my $name         = $product->file_name_root();

  if ($self->find_tertiary_config($product)->{bqsr}->{apply}) {
    $self->info("Product $name, $rpt is for BQSR application");
    return 1;
  }

  $self->info("Product $name, $rpt is NOT for BQSR application");

  return 0;
}


=head2 bqsr_known_sites
 
 Arg [1]    : npg_pipeline::product
 
 Example    : $obj->bqsr_known_sites($product)
 Description: Returns array of known sites for product.
 
 Returntype : Array[Str]
 
=cut

sub bqsr_known_sites {
  my ($self, $product) = @_;
  my @known_sites = @{$self->find_tertiary_config($product)->{bqsr}->{'known-sites'}};
  return @known_sites;
}

=head2 bwakit_enable
 
 Arg [1]    : npg_pipeline::product
 
 Example    : $obj->bwakit_enable($product)
 Description: Return true if bwakit's postalt processing is to be run on the product.
 
 Returntype : Bool
 
=cut

sub bwakit_enable {
  my ($self, $product) = @_;

  my $rpt          = $product->rpt_list();
  my $name         = $product->file_name_root();

  if ($self->find_study_config($product)->{bwakit}->{enable}) {
    $self->info("Product $name, $rpt is for bwakit postalt processing");
    return 1;
  }

  $self->info("Product $name, $rpt is NOT for bwakit postalt processing");

  return 0;
}

=head2 markdup_method

  Arg [1]    : npg_pipeline::product

  Example    : $obj->markdup_method($product);
  Description: Return mark duplicate method,
               the value might be undefined.

  Returntype : Str

=cut

sub markdup_method {
  my ($self, $product) = @_;
  return $self->find_study_config($product)->{markdup_method};
}

=head2 staging_deletion_delay
 
 Arg [1]    : npg_pipeline::product
 
 Example    : $obj->staging_deletion_delay($product)
 Description: If the study has staging deletion delay configured,
              returns this value, otherwise returns an undefined value.
 
 Returntype : Int
 
=cut

sub staging_deletion_delay {
  my ($self, $product) = @_;
  return $self->find_study_config($product)->{'data_deletion'}->{'staging_deletion_delay'};
}

=head2 can_run_gbs
 
 Arg [1]    : npg_pipeline::product
 
 Example    : $obj->can_run_gbs($product)
 Description: Return true if the product is allowed to be diverted down the gbs pipeline
 
 Returntype : Bool
 
=cut

sub can_run_gbs {
  my ($self, $product) = @_;

  my $study_config = (ref $product eq 'npg_pipeline::product')
                   ? $self->find_study_config($product)
                   : $self->study_config($product);

  return $study_config->{gbs_pipeline}->{allowed};
}

1;

__END__

=head1 NAME

npg_pipeline::product::release

=head1 SYNOPSIS

  foreach my $product (@products) {
    if ($self->is_release_data($product)    and
        $self->has_qc_for_release($product)) {
      $self->do_release($product);
    }
  }

=head1 DESCRIPTION

A role providing configuration and methods for decision-making during
product release.

The configuration file gives per-study settings and a default to be
used for any study without a specific configuration.

 irods:
    enable: <boolean> iRODS release enabled if true.

e.g.

---
default:
  irods:
    enable: true

study:
  - study_id: "5290"
    irods:
      enable: false

  - study_id: "1000"
    irods:
      enable: true

=head1 BUGS AND LIMITATIONS

=head1 INCOMPATIBILITIES

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Data::Dump

=item Moose::Role

=item Readonly

=item WTSI::DNAP::Utilities::Loggable

=item npg_tracking::util::pipeline_config

=back

=head1 AUTHOR

=over

=item Keith James

=item Fred Dodd

=back

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2018,2019,2020,2021,2022 Genome Research Ltd.

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
