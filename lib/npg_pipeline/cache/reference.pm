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
use npg_pipeline::function::util;
use npg_pipeline::cache::reference::constants qw( $TARGET_REGIONS_DIR $TARGET_AUTOSOME_REGIONS_DIR $REFERENCE_ABSENT );

with 'WTSI::DNAP::Utilities::Loggable';

our $VERSION = '0';

has [qw/ _ref_cache _resources_cache _calling_intervals_cache /] => (
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
  my ($self, $dp, $aligner, $repository, $_do_gbs_plex_analysis) = @_;

  if (!$aligner) {
    $self->logcroak('Aligner missing');
  }

  my $dplims = $dp->lims;
  my $rpt_list = $dp->rpt_list;
  my $is_tag_zero_product = $dp->is_tag_zero_product;
  my $ref_name = $_do_gbs_plex_analysis ? $dplims->gbs_plex_name : $dplims->reference_genome();

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

    my $class = q[npg_tracking::data::] . ($_do_gbs_plex_analysis ? 'gbs_plex' : 'reference');
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
