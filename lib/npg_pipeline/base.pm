package npg_pipeline::base;

use Moose;
use namespace::autoclean;
use MooseX::Getopt::Meta::Attribute::Trait::NoGetopt;
use POSIX qw(strftime);
use Math::Random::Secure qw{irand};
use List::MoreUtils qw{any uniq};
use File::Basename;
use Readonly;

use npg_tracking::glossary::rpt;
use npg_tracking::glossary::composition::factory::rpt_list;
use st::api::lims;
use npg_pipeline::product;

our $VERSION = '0';

extends 'npg_tracking::illumina::runfolder';

with qw{
        MooseX::Getopt
        WTSI::DNAP::Utilities::Loggable
        npg_tracking::util::pipeline_config
        npg_pipeline::base::options
       };

Readonly::Array my @NO_SCRIPT_ARG_ATTRS  => qw/
                                               subpath
                                               tilelayout_rows
                                               tile_count
                                               lane_tilecount
                                               tilelayout_columns
                                               npg_tracking_schema
                                               tracking_run
                                               experiment_name
                                               logger
                                               lane_count
                                               expected_cycle_count
                                               run_flowcell
                                               qc_path
                                              /;

=head1 NAME

npg_pipeline::base

=head1 SYNOPSIS

=head1 DESCRIPTION

A parent class providing basic functionality to derived objects
within npg_pipeline package

=head1 SUBROUTINES/METHODS

=head2 npg_tracking_schema

=head2 tracking_run

=head2 logger

Logger instance.
Also all direct (ie invoked directly on $self) logging methods inherited from
WTSI::DNAP::Utilities::Loggable.

=cut

#####
# Amend inherited attributes which we do not want to show up as scripts' arguments.
#
has [map {q[+] . $_ }  @NO_SCRIPT_ARG_ATTRS] => (metaclass => 'NoGetopt',);

=head2 id_run

Run id, an optional attribute.

=cut

has q{+id_run} => (required => 0,);

=head2 product_rpt_list

An rpt list for a single product, an optional attribute.
Should be set if the pipeline deals with products that do
not belong to a single run or if the pipeline (most likely,
the archival pipeline) has to deal with a single product only.

=cut

has q{product_rpt_list} => (
  isa       => q{Str},
  is        => q{ro},
  predicate => q{has_product_rpt_list},
  required  => 0,
);

=head2 label

A custom label associated with invoking a particular pipeline on
particular input. It is used in log and other similar file names,
job names, etc. If not set and product_rpt_list is not set,
defaults to the value of the id_run attribute.

=cut

has q{label} => (
  isa           => q{Str},
  is            => q{ro},
  predicate     => q{has_label},
  required      => 1,
  lazy_build    => 1,
  documentation => 'A custom label which will be used in log ' .
                   'file names, job names, etc. instead of run id',
);
sub _build_label {
  my $self = shift;
  $self->product_rpt_list and $self->logcroak(
    q['product_rpt_list' attribute is set, cannot build ] .
    q['label' attribute, it should be pre-set]);
  return $self->id_run;
}

=head2 timestamp

A timestring YYYY-MM-DD HH:MM:SS, an attribute with a default
value of current local time.

  my $sTimeStamp = $obj->timestamp();

=cut

has q{timestamp} => (
  isa        => q{Str},
  is         => q{ro},
  default    => sub {return strftime '%Y%m%d-%H%M%S', localtime time;},
  metaclass  => q{NoGetopt},
);

=head2 random_string

Returns a random string, a random 32-bit integer between 0 and 2^32,
prepended with a value of the timestamp attribute it the latter is available.

  my $rs = $obj->random_string();

=cut

sub random_string {
  my $self = shift;
  return ($self->can('timestamp') ? $self->timestamp() . q[-] : q[]) . irand();
}

=head2 positions

A sorted list of lanes (positions) this pipeline will analyse.
This list is set from the values supplied by the C<lanes> attribute. If
lanes are not set explicitly, defaults to positions specified in LIMS.

=cut

sub positions {
  my $self = shift;
  my @positions = @{$self->lanes()} ? @{$self->lanes()} :
                  map {$_->position()} $self->lims->children();
  return (sort @positions);
}

=head2 general_values_conf

Returns a hashref of configuration details from the relevant configuration file

=cut

has q{general_values_conf} => (
  metaclass  => q{NoGetopt},
  isa        => q{HashRef},
  is         => q{ro},
  lazy_build => 1,
  init_arg   => undef,
);
sub _build_general_values_conf {
  my $self = shift;
  return $self->read_config( $self->conf_file_path(q{general_values.ini}) );
}

=head2 merge_lanes

Tells p4 stage2 (seq_alignment) to merge all lanes (at their plex level
if plexed, except spiked PhiX and tag_zero).

If not set, this attribute is build lazily. It is set to true for NovaSeq runs,
which use a Standard flowcell.

=cut

has q{merge_lanes} => (
  isa           => q{Bool},
  is            => q{ro},
  lazy          => 1,
  predicate     => q{has_merge_lanes},
  builder       => q{_build_merge_lanes},
  documentation => q{Tells p4 stage2 (seq_alignment) to merge all lanes } .
                   q{(at their plex level if plexed) and to run its } .
                   q{downstream tasks using corresponding compositions},
);
sub _build_merge_lanes {
  my $self = shift;
  return $self->all_lanes_mergeable;
}

=head2 merge_by_library

Tells p4 stage2 (seq_alignment) to merge all plexes that belong to the same
library, except spiked PhiX and tag_zero.

If not set, this attribute is build lazily. It is set to true for indexed
NovaSeqX runs.

=cut

has q{merge_by_library} => (
  isa           => q{Bool},
  is            => q{ro},
  lazy_build    => 1,
  documentation => q{Tells p4 stage2 (seq_alignment) to merge all plexes } .
                   q{that belong to the same library, except spiked PhiX and }.
                   q{tag zero},
);
sub _build_merge_by_library {
  my $self = shift;
  return $self->is_indexed && $self->platform_NovaSeqX();
}

=head2 process_separately_lanes

An array of lane (position) numbers, which should not be merged with any other
lanes. To be used in conjunction with C<merge_lanes> or C<merge_by_library>
attributes. Does not have any impact if both of these attributes are false.

Defaults to an empty array value, meaning that all possible entities will be
merged. 

=cut

has q{process_separately_lanes} => (
  isa           => q{ArrayRef},
  is            => q{ro},
  default       => sub { return []; },
  documentation => q{Array of lane numbers, which have to be excluded from } .
                   q{a merge},
);

=head2 lims

st::api::lims run-level or product-specific object

=cut

has q{lims} => (isa        => q{st::api::lims},
                is         => q{ro},
                metaclass  => q{NoGetopt},
                lazy_build => 1,);
sub _build_lims {
  my $self = shift;
  return $self->has_product_rpt_list ?
         st::api::lims->new(rpt_list => $self->product_rpt_list) :
         st::api::lims->new(id_run   => $self->id_run);
}

=head2 multiplexed_lanes

An array of positions that correspond to, if the run is indexed, pooled lanes.
Empty array for a not indexed run.

=cut

has q{multiplexed_lanes} => (isa        => q{ArrayRef},
                             is         => q{ro},
                             metaclass  => q{NoGetopt},
                             lazy_build => 1,);
sub _build_multiplexed_lanes {
  my ($self) = @_;
  if (!$self->is_indexed) {
    return [];
  }
  my @lanes = map {$_->position} grep {$_->is_pool} $self->lims->children;
  return \@lanes;
}

=head2 is_multiplexed_lane

Boolean flag, true if the run is indexed and the lane is a pool.

=cut

sub is_multiplexed_lane {
  my ($self, $position) = @_;
  if (!$position) {
    $self->logcroak('Position not given');
  }
  return any {$_ == $position} @{$self->multiplexed_lanes};
}

=head2 lims4lane
 
Return lane -level st::api::lims object for the argument position.
Error if the given lane (position) does not exist in LIMs.

  my $lane4_lims = $self->lims4lane(4);

=cut

sub lims4lane {
  my ($self, $position) = @_;
  if (!$position) {
    $self->logcroak('Position not given');
  }
  my $lane = $self->lims->children_ia->{$position};
  if (!$lane) {
    $self->logcroak("Failed to get lims data for lane $position");
  }
  return $lane;
}

=head2 get_tag_index_list

Returns an array of sorted tag indices for a lane, including tag zero.

=cut

sub get_tag_index_list {
  my ($self, $position) = @_;
  if (!$self->is_multiplexed_lane($position)) {
    return [];
  }
  my @tags = sort keys %{$self->lims4lane($position)->tags()};
  unshift @tags, 0;
  return \@tags;
}

=head2 products

Two arrays of npg_pipeline::product objects, one for lanes, hashed under
the 'lanes' key, another for end products, including, where relevant, tag
zero products, hashed under the 'data_products' key.

If product_rpt_list attribute is set, the 'lanes' key maps to an empty
array.

While computing the lists of data products, we examine whether data in any
of the lanes can be merged across lanes. Some of the lanes might be explicitly
excluded from the merge by setting the `process_separately_lanes` attribute
from the command line. This is likely to be done when the analysis pipeline
is run manually. Then the same lanes have to be excluded from the merge by
the archival pipeline and by the script that evaluates whether the run folder
can be deleted. To enable this, the value of the `process_separately_lanes`
attribute is saved to the metadate_cache_<ID_RUN> directory immediately after
the pipeline establishes the location of the samplesheet file or generates a
new samplesheet.

This method looks at the `process_separately_lanes` attribute first. If the
`process_separately_lanes` array is empty, an attempts to retrieve the cached
value is made.

=cut

has q{products} => (
  isa        => q{HashRef},
  is         => q{ro},
  metaclass  => q{NoGetopt},
  lazy_build => 1,
);
sub _build_products {
  my $self = shift;

  my (@lane_lims, @data_lims);

  if ($self->has_product_rpt_list) {
    @data_lims = ($self->lims);
  } else {
    @lane_lims = map { $self->lims4lane($_) } $self->positions;

    my %tag0_lims = ();
    if ($self->is_indexed) {
      %tag0_lims = map { $_->position => $_->create_tag_zero_object() }
                   grep { $_->is_pool } @lane_lims;
    }

    if ($self->merge_lanes || $self->merge_by_library) {

      my @separate_lanes = @{$self->process_separately_lanes};
      @separate_lanes |= $self->_get_cached_separate_lanes_info();

      my $all_lims = $self->lims->aggregate_libraries(
        \@lane_lims, @separate_lanes);
      @data_lims = @{$all_lims->{'singles'}}; # Might be empty.

      # merge_lanes option implies a merge across all lanes.
      if ($self->merge_lanes && (@lane_lims > 1)) {
        $self->_check_lane_merge_is_viable(
          \@lane_lims, $all_lims->{'singles'}, $all_lims->{'merges'});
      }

      # Tag zero LIMS objects for all pooled lanes, merged or unmerged.
      push @data_lims, map { $tag0_lims{$_} } (sort keys %tag0_lims);

      if ( @{$all_lims->{'merges'}} ) {
        # If the libraries are merged across a subset of lanes under analysis,
        # the 'selected_lanes' flag needs to be flipped to true.
        if (!$self->_selected_lanes) {
          my $rpt_list = $all_lims->{'merges'}->[0]->rpt_list;;
          my $num_components =
            npg_tracking::glossary::composition::factory::rpt_list
              ->new(rpt_list => $rpt_list)
              ->create_composition()->num_components();
          if ($num_components != scalar @lane_lims) {
            $self->_set_selected_lanes(1);
          }
        }
        push @data_lims, @{$all_lims->{'merges'}};
      }

    } else {
      # To keep backward-compatible order of pipeline invocations, add
      # tag zero LIMS object at the end of other objects for the lane.
      @data_lims = map {
        exists $tag0_lims{$_->position} ?
            ($_->children, $tag0_lims{$_->position}) : $_
      } @lane_lims;
    }
  }

  return {
    'data_products' => [map { $self->_lims_object2product($_) } @data_lims],
    'lanes'         => [map { $self->_lims_object2product($_) } @lane_lims]
  };
}

#####
# The boolean flag below defines whether lane numbers are explicitly
# listed in directory and file names for merged products. It is set
# to true whenever a subset of all available lanes is analysed.
# If it is set to false by the builder method, it can be reset to true
# when a full collection of products is constructed.
has q{_selected_lanes} => (
  isa           => q{Bool},
  is            => q{ro},
  writer        => q{_set_selected_lanes},
  lazy_build    => 1,
);
sub _build__selected_lanes {
  my $self = shift;
  if (!$self->has_product_rpt_list) {
    return ((join q[], $self->positions) ne
            (join q[], map {$_->position} $self->lims->children()))
  }
  return;
}

sub _lims_object2product {
  my ($self, $lims) = @_;

  return npg_pipeline::product->new(
    rpt_list       => $lims->rpt_list ? $lims->rpt_list :
                        npg_tracking::glossary::rpt->deflate_rpt($lims),
    lims           => $lims,
    selected_lanes => $self->_selected_lanes
  );
}

sub _check_lane_merge_is_viable {
  my ($self, $lane_lims, $singles, $merges) = @_;

  my %no_merge_lanes = map { $_ => 1 } @{$self->process_separately_lanes};
  my @num_plexes = uniq
                   map  { scalar @{$_} }
                   map  { [grep { !$_->is_control } @{$_}] }
                   map  { [$_->children()] }
                   grep { ! exists $no_merge_lanes{$_->position} }
                   @{$lane_lims};

  my $m = 'merge_lane option is not viable: ';
  if (@num_plexes > 1) {
    $self->logcroak($m . 'different number of samples in lanes');
  }

  my @unmerged_unexpected = grep { ! exists $no_merge_lanes{$_->position} }
                            grep { !$_->is_control }
                            @{$singles};
  if (@unmerged_unexpected) {
    $self->logcroak(
      $m . 'unexpected unmerged samples are present after aggregation');
  }

  if (@{$merges} != $num_plexes[0]) {
    $self->logcroak($m . 'number of merged samples after aggregation ' .
      'differs from the number of samples in a lane');
  }

  return 1;
}

sub _get_cached_separate_lanes_info {
  my $self = shift;
  # Read from a file in metadata_cache_<IDRUN> directory if the file exists.
  return;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item namespace::autoclean

=item MooseX::Getopt

=item Math::Random::Secure

=item POSIX

=item List::MoreUtils

=item File::Basename

=item Readonly

=item npg_tracking::glossary::rpt

=item npg_tracking::glossary::composition::factory::rpt_list

=item st::api::lims

=item WTSI::DNAP::Utilities::Loggable

=item npg_tracking::illumina::runfolder

=item npg_tracking::util::pipeline_config

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown
Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2014,2015,2016,2017,2018,2019,2020,2023 Genome Research Ltd.

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
