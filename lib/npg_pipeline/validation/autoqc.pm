package npg_pipeline::validation::autoqc;

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;
use Readonly;
use List::MoreUtils qw/any none uniq/;

use npg_tracking::glossary::composition;
use npg_tracking::glossary::composition::component::illumina;
use npg_qc::autoqc::role::result;
use npg_pipeline::product;

with qw/ npg_pipeline::validation::common /;

our $VERSION = '0';

Readonly::Array  my @COMMON_CHECKS         => qw/ qX_yield
                                                  adapter
                                                  gc_fraction
                                                  insert_size
                                                  ref_match
                                                  sequence_error
                                                /;

Readonly::Array  my @LANE_LEVELCHECKS      => qw/ spatial_filter
                                                /;

Readonly::Array  my @LANE_LEVELCHECKS4POOL => qw/ tag_metrics
                                                /;

Readonly::Array  my @WITH_SUBSET_CHECKS    => qw/ bam_flagstats
                                                  samtools_stats
                                                  sequence_summary
                                                /;

has 'skip_checks'    => (
  isa      => 'ArrayRef',
  is       => 'ro',
  required => 0,
  default  => sub { [] },
);

has 'is_paired_read' => (
  isa      => 'Bool',
  is       => 'ro',
  required => 1,
);

has 'qc_schema' => (
  isa        => 'npg_qc::Schema',
  is         => 'ro',
  required   => 1,
);

=head2 BUILD

=cut

sub BUILD {
  my $self = shift;
  @{$self->product_entities}
    or $self->logcroak('product_entities array cannot be empty');
  return;
}

sub fully_archived {
  my $self = shift;

  my @compositions = ();
  my @compositions_with_subsets = ();
  my @compositions_with_available_subsets = ();
  my @positions = ();
  my @non_pools = ();

  foreach my $entity (@{$self->product_entities}) {
    my $composition = $entity->target_product->composition;
    push @compositions, $composition;
    if (@{$entity->related_products}) {
      push @compositions_with_available_subsets, $entity->target_product->composition;
      push @compositions_with_subsets,
           (map { $_->composition } @{$entity->related_products});
    }

    push @positions, map {$_->position} $composition->components_list;

    if ($composition->num_components == 1) {
      my $component = $composition->get_component(0);
      if (!defined  $component->tag_index) {
        push  @non_pools, $component->position;
      }
    }
  }

  @positions = sort {$a <=> $b} uniq @positions;
  @non_pools = sort {$a <=> $b} uniq @non_pools;
  $self->debug('All lanes: ' . join q[, ], @positions);
  $self->debug(@non_pools ? 'Non-indexed ' . join q[, ], @non_pools :
                            'All lanes processed as indexed');

  my @common = grep {($_ ne 'insert_size') || $self->is_paired_read} @COMMON_CHECKS;

  my @checks = grep { !exists $self->_skip_checks_wsubsets->{$_} ||
                      scalar @{$self->_skip_checks_wsubsets->{$_}} }
               (@common, @WITH_SUBSET_CHECKS);
  my $context = 'Expected product level checks';
  $self->debug($context . q[: ] . join q[, ], @checks);
  my @flags = $self->_results_exist(\@compositions, \@checks, $context);

  my $per_check_map = $self->_prune_by_subset(\@compositions_with_subsets);
  @checks = keys  %{$per_check_map};
  $context = 'Expected checks for products with subsets';
  $self->debug($context . q[: ] . join q[, ], @checks);
  foreach my $check_name (@checks) {
    push @flags, $self->_results_exist($per_check_map->{$check_name}, [$check_name], $context);
  }

  $self->debug('Checking alignment_filter results');
  push @flags, $self->_results_exist(
    \@compositions_with_available_subsets, ['alignment_filter_metrics']);

  # Lane-level checks

  my $compositions4lanes = $self->_compositions4lanes(\@compositions);

  @checks = grep {$_ ne 'adapter'} @common;
  push @checks, @LANE_LEVELCHECKS;
  @checks = grep { !exists $self->_skip_checks_wsubsets->{$_} } @checks;
  $context = 'Expected lane level checks';
  $self->debug($context . q[: ] . join q[, ], @checks);
  push @flags, $self->_results_exist($compositions4lanes, \@checks, $context);

  if (@non_pools != @positions) {
    my @pools = ();
    foreach my $c (@{$compositions4lanes}) {
      my $p = $c->get_component(0)->position;
      next if any { $p == $_ } @non_pools;
      push @pools, $c;
    }
    @checks = grep { !exists $self->_skip_checks_wsubsets->{$_} } @LANE_LEVELCHECKS4POOL;
    $context = 'Expected lane level checks for a pool';
    $self->debug($context . q[: ] . join q[, ], @checks);
    push @flags, $self->_results_exist(\@pools, \@checks, $context);
  }

  return none { $_ == 0 } @flags;
}

has '_skip_checks_wsubsets' => (
  isa        => 'HashRef',
  is         => 'ro',
  required   => 0,
  lazy_build => 1,
);
sub _build__skip_checks_wsubsets {
  my $self = shift;
  my $skip_checks = {};

  foreach my $check ( @{$self->skip_checks} ) {
    my @parsed = split /[+]/smx, $check;
    my $name = shift @parsed;
    $skip_checks->{$name} = \@parsed;
  }

  return $skip_checks;
}

sub _results_exist {
  my ($self, $compositions, $checks, $context) = @_;

  my $exists = 1;
  my $expected = scalar @{$compositions};
  if ($expected) {
    $context = $context ? $context . q[. ] : q[];
    foreach my $check_name (@{$checks}) {
      my ($name, $class_name) = npg_qc::autoqc::role::result->class_names($check_name);
      my $rs = $self->qc_schema->resultset($class_name)
                    ->search_via_composition($compositions);
      my $count = $rs->count;

      if ($check_name eq 'samtools_stats') {
        # We expect at least two types of these files per composition
        my $e = $expected * 2;
        if ($count < $e) {
          $self->warn($context . qq[Expected at least $expected results got $count for $check_name]);
          $exists = 0;
        }
      } elsif ($check_name eq 'sequence_summary') {
        $rs = $rs->search({'me.iscurrent' => 1});
        $count = $rs->count;
        # Unfortunately, iscurrent filter is not set correctly, so we cannot
        # yet check for equality.
        if ($count < $expected) {
          $self->warn($context . qq[Expected at least $expected results got $count for $check_name]);
          $exists = 0;
        }
      } else {

        my $local_expected     = $expected;
        my $local_count        = $count;
        my $local_compositions = $compositions;

        if ($check_name eq 'adapter') {
          # Adapter check for tag zero might or might not be present.
          # We will not check for it.
          my @non_tag_zero_compositions = grep
            { (!defined $_->get_component(0)->tag_index()) || $_->get_component(0)->tag_index() }
            @{$compositions};
          @non_tag_zero_compositions or next;
          if (@non_tag_zero_compositions != @{$compositions}) {
            $local_compositions = \@non_tag_zero_compositions;
            $local_expected = scalar @non_tag_zero_compositions;
            $local_count = $rs->search_via_composition(\@non_tag_zero_compositions)->count;
          }
        }

        if ($local_count != $local_expected) {
          $self->warn($context . qq[Expected $local_expected results got $local_count for $check_name]);
          $exists = 0;
          $self->_report_missing_results($local_compositions, $class_name, $check_name);
        }
      }
    }
  }

  return $exists;
}

sub _report_missing_results {
  my ($self, $compositions, $class_name, $check) = @_;

  my $rs = $self->qc_schema->resultset($class_name);
  foreach my $c (@{$compositions}) {
    if ($rs->search_via_composition([$c])->count == 0) {
      $self->debug("$check result is missing for " . $c->freeze);
    }
  }
  return;
}

sub _compositions4lanes {
  my ($self, $id_run, @positions) = @_;

  my @compositions =
    map { npg_tracking::glossary::composition->new(components => [$_]) }
    map { npg_tracking::glossary::composition::component::illumina
          ->new(id_run => $id_run, position => $_) }
    @positions;

  return \@compositions;
}

sub _prune_by_subset {
  my ($self, $compositions) = @_;

  my $map = {};

  for my $check_name (@WITH_SUBSET_CHECKS) {
    if (!exists $self->_skip_checks_wsubsets->{$check_name}) {
      $map->{$check_name} = $compositions;
    } else {
      my @subsets = @{$self->_skip_checks_wsubsets->{$check_name}};
      # If no subsets are defined for the check, we skip the check.
      next if !@subsets;
      foreach my $c (@{$compositions}) {
        my $subset = $c->get_component(0)->subset;
        # If this subset for this check is defined,
        # we skip this autoqc check for this composition.
        next if any { $_ eq $subset} @subsets;
        push @{$map->{$check_name}}, $c;
      }
    }
  }

  return $map;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

npg_pipeline::validation::autoqc

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 qc_schema

  Attribute, required, DBIx schema object for the QC database.

=head2 product_entities

 Attribute, required, inherited from npg_pipeline::validation::common

=head2 is_paired_read

  A flag defining whether there are reverse reads.

=head2 skip_checks

  An optional array of autoqc check names to disregard..

  Setting this array to [qw/adaptor samtools_stats-phix/] ensure that absence of
  all adaptor results and absence of samtools_stats results for phix subsets will be
  disregarded.

=head2 fully_archived

  Returns true if all expected autoqc data are found, otherwise returns false.

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item MooseX::StrictConstructor

=item namespace::autoclean

=item Readonly

=item List::MoreUtils

=item npg_tracking::glossary::composition::component::illumina

=item npg_tracking::glossary::composition

=item npg_qc::autoqc::role::result

=item npg_pipeline::product

=item WTSI::DNAP::Utilities::Loggable

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia E<lt>mg8@sanger.ac.ukE<gt>

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2019,2021,2022 GRL

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
