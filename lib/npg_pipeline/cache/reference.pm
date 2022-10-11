package npg_pipeline::cache::reference;

use namespace::autoclean;

use MooseX::Singleton;
use Carp;
use Class::Load qw{load_class};
use List::Util qw(any);
use Readonly;
use Try::Tiny;

use npg_tracking::util::types;
use npg_tracking::data::reference;
use npg_tracking::data::primer_panel;
use npg_tracking::data::gbs_plex;
use npg_pipeline::function::util;
use npg_pipeline::cache::reference::constants qw( $TARGET_REGIONS_DIR $TARGET_AUTOSOME_REGIONS_DIR $REFERENCE_ABSENT );

with 'WTSI::DNAP::Utilities::Loggable';

our $VERSION = '0';

has [qw/ _ref_cache
         _resources_cache
         _calling_intervals_cache 
         _primer_panel_cache
         _gbs_plex_cache /] => (
  isa      => 'HashRef',
  is       => 'ro',
  required => 0,
  default  => sub {return {};},
);

=head2 get_path
 
 Arg [1]    : $dp
 Arg [2]    : $aligner
 
 Example    : my $path = npg_pipeline::cache::reference->instance->get_path($dp, 'fasta')
 Description: Get ref path from cache.
 
 Returntype : String
 
=cut

sub get_path {
  my ($self, $dp, $aligner, $repository, $do_gbs_plex_analysis) = @_;

  if (!$aligner) {
    $self->logcroak('Aligner missing');
  }

  my $dplims = $dp->lims;
  my $rpt_list = $dp->rpt_list;
  my $is_tag_zero_product = $dp->is_tag_zero_product;
  my $ref_name = $do_gbs_plex_analysis ? $dplims->gbs_plex_name : $dplims->reference_genome();

  my $ref = $ref_name ? $self->_ref_cache->{$ref_name}->{$aligner} : undef;
  if ($ref) {
    if ($ref eq $REFERENCE_ABSENT) {
      $ref = undef;
    }
  } else {
    my $href = { 'aligner' => $aligner, 'lims' => $dplims, };
    if ($repository) {
      $href->{'repository'} = $repository;
    }

    my $class = q[npg_tracking::data::] . ($do_gbs_plex_analysis ? 'gbs_plex' : 'reference');
    load_class($class);
    my $ruser = $class->new($href);
    my @refs = ();
    try {
      @refs = @{$ruser->refs};
    } catch {
      my $e = $_;
      # Either exist with an error or just log an error and carry on
      # with reference undefined.
      (any { /$aligner/smx } qq( $TARGET_REGIONS_DIR $TARGET_AUTOSOME_REGIONS_DIR ))
         ? $self->error($e) : $self->logcroak($e);
    };

    if (!@refs) {
      $self->warn(qq[No reference genome retrieved for $rpt_list]);
    } elsif (scalar @refs > 1) {
      my $m = qq{Multiple references for $rpt_list};
      # Either exist with an error or log it and carry on with
      # reference undefined.
      $is_tag_zero_product ? $self->logwarn($m) : $self->logcroak($m);
    } else {
      $ref = $refs[0];
    }

    # Cache the reference or the fact that it's not available.
    if ($ref_name) {
      $self->_ref_cache->{$ref_name}->{$aligner} = $ref ? $ref : $REFERENCE_ABSENT;
    }
  }

  if ($ref) {
    $self->info(qq{Reference found for $rpt_list: $ref});
  }
  return $ref;
}

=head2 get_known_sites_dir
 
 Arg [1]    : $dp
 Arg [2]    : $repository, optional
 
 Example    : my $dir = npg_pipeline::cache::reference->instance
                        ->get_known_sites_dir($dp);
              my $dir = npg_pipeline::cache::reference->instance
                        ->get_known_sites_dir($dp, $ref_repository_root);
 Description: Get directory path for known sites for this product.
              If the reference repository root argument is given, this
              custom repository is used, otherwise a default reference
              repository is used.
 
 Returntype : String
 
=cut

sub get_known_sites_dir {
  my ($self, $product, $repository) = @_;
  return $self->_get_vc_dir('resources', $product, $repository);
}

=head2 get_interval_lists_dir
 
 Arg [1]    : $dp
 Arg [2]    : $repository, optional
 
 Example    : my $dir = npg_pipeline::cache::reference->instance
                        ->get_interval_lists_dir($dp);
              my $dir = npg_pipeline::cache::reference->instance
                        ->get_interval_lists_dir($dp, $ref_repository_root);
 Description: Get directory path for interval lists for this product.
              If the reference repository root argument is given, this
              custom repository is used, otherwise a default reference
              repository is used.
 
 Returntype : String
 
=cut

sub get_interval_lists_dir {
  my ($self, $product, $repository) = @_;
  return $self->_get_vc_dir('calling_intervals', $product, $repository);
}

sub _get_vc_dir {
  my ($self, $rep_dir_name, $product, $repository) = @_;

  $product or croak 'Product argument required';
  $product->lims or croak 'Product should have lims attribute set';
  my $ref_genome = $product->lims->reference_genome();
  $ref_genome or croak 'reference_genome is not defined';

  my $r = npg_tracking::data::reference->new(
            $repository ? {repository => $repository} : {}
          );
  $repository = $r->repository;

  my $attr_name = q[_] . $rep_dir_name . q[_cache];

  my $dir = $self->$attr_name()->{$repository}->{$ref_genome};
  if (!$dir) {
    my ($species, $ref) = $r->parse_reference_genome($ref_genome);
    ($species && $ref) or croak "Failed to parse ref. genome $ref_genome";
    $dir = sprintf '%s/%s/%s/%s', $repository, $rep_dir_name, $species, $ref;
    #TODO: when this code is moved to tracking,
    #      check that this directory exists
    $self->$attr_name()->{$repository}->{$ref_genome} = $dir;
  }

  return $dir;
}

=head2 get_primer_panel_bed_file
 
 Arg [1]    : $dp
 Arg [2]    : $repository, optional
 
 Example    : my $file = npg_pipeline::cache::reference->instance
                        ->get_primer_panel_bed_file($dp);
              my $file = npg_pipeline::cache::reference->instance
                         ->get_primer_panel_bed_file($dp, $ref_repository_root);
 Description: Get primer panel bed file path for this product.
              If the reference repository root argument is given, this
              custom repository is used, otherwise a default reference
              repository is used. If the primer_panel LIMs value is not
              defined for this product, an undefined value is returned;
 
 Returntype : String
 
=cut

sub get_primer_panel_bed_file {
  my ($self, $product, $repository) = @_;
  $product or croak 'Product argument required';
  $product->lims or croak 'Product should have lims attribute set';

  my $init = { lims => $product->lims };
  if ($repository) {
    $init->{repository} = $repository;
  }
  my $pp = npg_tracking::data::primer_panel->new($init);

  $repository = $pp->repository;
  my $primer_panel = $pp->primer_panel;
  my $bed_file;

  if ($primer_panel) {
    my $reference_genome = $pp->lims->reference_genome;
    $reference_genome or croak 'reference_genome is not defined';
    $bed_file = $self->_primer_panel_cache()
                ->{$repository}->{$primer_panel}->{$reference_genome};
    if (!$bed_file) {
      $bed_file = $pp->primer_panel_bed_file();
      $self->_primer_panel_cache()
        ->{$repository}->{$primer_panel}->{$reference_genome} = $bed_file;
    }
  }

  return $bed_file;
}

=head2 get_gbs_plex_bed_file
 
 Arg [1]    : $dp
 Arg [2]    : $repository, optional
 
 Example    : my $file = npg_pipeline::cache::reference->instance
                        ->get_gbs_plex_bed_file($dp);
              my $file = npg_pipeline::cache::reference->instance
                         ->get_gbs_plex_bed_file($dp, $ref_repository_root);
 Description: Get gbs plex bed file path for this product.
              If the reference repository root argument is given, this
              custom repository is used, otherwise a default reference
              repository is used. If the primer_panel LIMs value is not
              defined for this product, an undefined value is returned;
 
 Returntype : String
 
=cut

sub get_gbs_plex_bed_file {
  my ($self, $product, $repository) = @_;
  $product or croak 'Product argument required';
  $product->lims or croak 'Product should have lims attribute set';

  my $init = { lims => $product->lims };
  if ($repository) {
    $init->{repository} = $repository;
  }
  my $gb = npg_tracking::data::gbs_plex->new($init);

  $repository = $gb->repository;
  my $gbs_plex = $gb->gbs_plex_name;
  my $bed_file;

  if ($gbs_plex) {
    my $reference_genome = $gb->lims->reference_genome;
    $reference_genome or croak 'reference_genome is not defined';
    $bed_file = $self->_gbs_plex_cache()
                ->{$repository}->{$gbs_plex}->{$reference_genome};
    if (!$bed_file) {
      $bed_file = $gb->gbs_plex_bed_path();
      $self->_gbs_plex_cache()
        ->{$repository}->{$gbs_plex}->{$reference_genome} = $bed_file;
    }
  }

  return $bed_file;
}



__PACKAGE__->meta->make_immutable;

1;

=head1 NAME

 npg_pipeline::cache::reference

=head1 SYNOPSIS


  npg_pipeline::cache::reference->instance->get_path($data_product, $aligner)

=head1 DESCRIPTION

 Maps reference names to paths

=head1 SUBROUTINES/METHODS

=head1 BUGS AND LIMITATIONS

=head1 INCOMPATIBILITIES

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES
 
=over

=item MooseX::Singleton

=item Readonly

=item Carp

=item Class::Load

=item Try::Tiny

=item npg_tracking::util::types

=item npg_tracking::data::reference

=item npg_tracking::data::primer_panel

=back

=head1 AUTHOR

 Martin Pollard

=head1 LICENSE AND COPYRIGHT

 Copyright (C) 2019,2020 Genome Research Ltd.

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
