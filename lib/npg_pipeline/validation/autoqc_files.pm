package npg_pipeline::validation::autoqc_files;

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;
use Readonly;
use Try::Tiny;
use List::MoreUtils qw/none uniq/;
#use File::Basename;

use npg_tracking::glossary::rpt;
use npg_tracking::glossary::composition;
use npg_qc::Schema;
use npg_qc::autoqc::role::result;
use npg_pipeline::product;

with 'npg_pipeline::validation::common';

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
                                                  upstream_tags
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

sub fully_archived {
  my $self = shift;

  my $compositions = {};
  my $compositions_with_subsets = {};
  my @positions = ();
  my @non_pools = ();

  foreach my $f (@{$self->staging_files->{'composition_files'}}) {

    my $composition = npg_tracking::glossary::composition->thaw(slurp($f));
    my $digest = $composition->digest;
    my $component = $composition->get_component(0);

    if ($component->subset) {
      $compositions->{$digest} = $composition;
    } else {
      $compositions_with_subsets->{$digest} = $composition;
    }

    @positions = map {$_->position} $composition->components_list;

    if ($composition->num_components == 1 && !defined $component->tag_index) {
      push  @non_pools, $component->position;
    }
  }

  @positions = uniq @positions;
  @non_pools = uniq @non_pools;

  my @common = grep {($_ ne 'insert_size') || $self->is_paired_read} @COMMON_CHECKS;

  my @checks = grep { !exists $self->_skip_checks_wsubsets->{$_} }
               (@common, @WITH_SUBSET_CHECKS);
  my @flags = $self->_results_exist($compositions, @checks);

  my $per_check_map = $self->_prune_by_subset($compositions_with_subsets);
  foreach my $check_name (keys  %{$per_check_map}) {
    push @flags, $self->_results_exist($per_check_map->{$check_name}, $check_name);
  }

  try {
    push @flags, $self->_results_exist(
      $self->_compositions4compositions_with_subsets($compositions, $compositions_with_subsets),
      'alignment_filter_metrics');
  } catch {
    $self->logwarn($_);
    push @flags, 0;
  };

  # Lane-level checks

  my $compositions4lanes = $self->_compositions4lanes($compositions);

  @checks = grep {$_ ne 'adapter'} @common;
  push @checks, @LANE_LEVELCHECKS;
  @checks = grep { !exists $self->_skip_checks_wsubsets->{$_} } @checks;
  push @flags, $self->_results_exist($compositions4lanes, @checks);

  if (@non_pools != @positions) {
    my $pools = {};
    while (my ($d, $c) = each %{$compositions4lanes}) {
      my $p = $c->get_component(0)->position;
      if (none { $p == $_ } @non_pools) {
        $pools->{$d} = $c;
      }
    }
    @checks = grep { !exists $self->_skip_checks_wsubsets->{$_} } @LANE_LEVELCHECKS4POOL;
    push @flags, $self->_results_exist($pools, @checks);
  }

  return none { $_ == 0 } @flags;
}

has '_qc_schema' => (
  isa        => 'npg_qc::Schema',
  is         => 'ro',
  required   => 0,
  lazy_build => 1,
);
sub _build__qc_schema {
  return npg_qc::Schema->connect();
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

sub _results_exists {
  my ($self, $compositions, @checks) = @_;

  my $exists = 1;
  my $expected = scalar keys %{$compositions};
  if ($expected) {
    foreach my $check_name (@checks) {
      my ($name, $class_name) = npg_qc::autoqc::role::result->class_names($check_name);
      my $count = $self->_qc_schema->resultset($class_name)
                       ->search_via_composition([values %{$compositions}])->count;
      if ($count != $expected) {
        $self->info(qq[Expected $expected results got $count for $check_name]);
        $exists = 0;
      }
    }
  }

  return $exists;
}

sub _compositions4compositions_with_subsets {
  my ($self, $compositions, $compositions_with_subsets) = @_;

  my $map = {};

  if (keys %{$compositions_with_subsets}) {

    my %rpt2composition_map = map { $_->freeze2rpt => $_} values %{$compositions};

    foreach my $composition (values %{$compositions_with_subsets}) {
      my $rpt_list = $composition->freeze2rpt;
      next if exists $map->{$rpt_list};
      exists $rpt2composition_map{$rpt_list}
        or $self->logcroak('No target entity for ' . $composition->freeze);
      $map->{$rpt_list} = $rpt2composition_map{$rpt_list};
    }
  }

  return $map;
}

sub _compositions4lanes {
  my ($self, $compositions) = @_;

  my $map = {};

  foreach my $composition (values %{$compositions}) {
    ##no critic (BuiltinFunctions::ProhibitComplexMappings)
    my @rpt_lists =
      map { delete $_->{'tag_index'}; $_ }
      map { npg_tracking::glossary::rpt->inflate_rpt($_) }
      map { $_->freeze2rpt() }
      $self->composition->components_list();
    ##use critic
    foreach my $rptl (@rpt_lists) {
      next if exists $map->{$rptl};
      $map->{$rptl} = npg_pipeline::product->new(rpt_list =>  $rptl)->composition();
    }
  }

  return $map;
}

sub _prune_by_subset {
  my ($self, $compositions) = @_;

  my $map = {};

  for my $check_name (@WITH_SUBSET_CHECKS) {
    if (!exists $self->_skip_checks_wsubsets->{$check_name}) {
      $map->{$check_name} = $compositions;
    } else {
      my @subsets = @{$self->_skip_checks_wsubsets->{$check_name}};
      # If no subsets are defined, we skip the check
      next if !@subsets;
      while (my ($key, $c) = each %{$compositions}) {
        my $entity_subset = $c->get_component(0)->subset;
        if ( none { $_ eq $entity_subset} ) {
          $map->{$check_name}->{$key} = $c;
	}
      }
    }
  }

  return $map;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

npg_pipeline::validation::autoqc_files

=head1 SYNOPSIS

=head1 DESCRIPTION

  Compares a set of archived bam files against a set of autoqc results for
  a run and decides whether all relevant autoqc results have been archived.
  Autoqc results that can easily be produced again from bam files are omitted.
  Presence of fastqcheck files in the archive is checked.
  
  A full comparison is performed. If at least one autoqc result is missing,
  the outcome is false, otherwise true is returned. If the verbose attribute
  is set, a path to each considered bam file is printed to STDERR and a
  representation of each query to find the autoqc result is printed to STDERR.
  In non-verbose mode (default) only the queries for missing results are printed.

=head1 SUBROUTINES/METHODS

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

=item Try::Tiny

=item List::MoreUtils

=item File::Basename

=item npg_tracking::glossary::rpt

=item npg_tracking::glossary::composition

=item npg_qc::Schema

=item npg_qc::autoqc::role::result

=item npg_pipeline::product

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia E<lt>mg8@sanger.ac.ukE<gt>

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2019 GRL

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
