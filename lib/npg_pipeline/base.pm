package npg_pipeline::base;

use Moose;
use namespace::autoclean;
use MooseX::Getopt::Meta::Attribute::Trait::NoGetopt;
use POSIX qw(strftime);
use Math::Random::Secure qw{irand};
use List::MoreUtils qw{any};
use File::Basename;
use Readonly;

use npg_tracking::glossary::rpt;
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
                                               slot
                                               instrument_string
                                               subpath
                                               tilelayout_rows
                                               tile_count
                                               lane_tilecount
                                               tilelayout_columns
                                               npg_tracking_schema
                                               flowcell_id
                                               name
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

=head2 qc_schema

An attribute caching a connection to a QC database.
The attribute is allowed to be undefined and is implicitly undefined
since no default or build method is provided. This is done in order
to prevent the automatic connection to a database in child classes.

=cut

has 'qc_schema' => (
  metaclass  => 'NoGetopt',
  isa        => 'Maybe[npg_qc::Schema]',
  is         => 'ro',
  required   => 0,
);

=head2 flowcell_id

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
if plexed) and to run its downstream tasks using corresponding compositions.

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

=cut

has q{merge_by_library} => (
  isa           => q{Bool},
  is            => q{ro},
  lazy_build    => 1,
  documentation => q{Tells p4 stage2 (seq_alignment) to merge all plexes } .
                   q{that belong to the same library, except spiked PhiX and }.
                   q{tag zero)},
);
sub _build_merge_by_library {
  my $self = shift;
  return $self->is_indexed && $self->platform_NovaSeqX();
}

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

=cut

has q{products} => (
  isa        => q{HashRef},
  is         => q{ro},
  metaclass  => q{NoGetopt},
  lazy_build => 1,
);
sub _build_products {
  my $self = shift;

  my @lane_lims = ();
  my @data_lims = ();
  my @libmerged_data_lims = ();
  my $selected_lanes4libmerge = 0;

  if ($self->has_product_rpt_list) {
    @data_lims = ($self->lims);
  } else {
    my @positions = $self->positions;
    @lane_lims = map { $self->lims4lane($_) } @positions;

    if ($self->merge_lanes) {
      @data_lims = $self->lims->aggregate_xlanes(@positions);
    } else {

      my %tag0_lims = ();
      if ($self->is_indexed) {
        %tag0_lims = map { $_->position => $_->create_tag_zero_object() }
                     grep { $_->is_pool } @lane_lims;
      }

      if ($self->merge_by_library) {

        my $all_lims = $self->lims->aggregate_libraries(\@lane_lims);
        # Unmerged data, if any, including individual objects for
        # spiked controls.
        push @data_lims, @{$all_lims->{'single'}};
        # Tag zero LIMS objects for all lanes, merged or unmerged.
        push @data_lims, map { $tag0_lims{$_} } (sort keys %tag0_lims);
        # Merged data.
        @libmerged_data_lims =
          map { @{$all_lims->{'merges'}->{$_}} }
          ( sort keys %{$all_lims->{'merges'}} );
        # We might be analysing a full set of lanes, but the libraries
        # might be merged across some lanes only.
        $selected_lanes4libmerge = $self->_selected_lanes ||
          (!exists $all_lims->{'merges'}->{join q[,], @positions});

      } else {
        # To keep backward-compatible order of pipeline invocations, add
        # tag zero LIMS object at the end of other objects for the lane.
        @data_lims = map {
            exists $tag0_lims{$_->position} ?
            ($_->children, $tag0_lims{$_->position}) : $_
          } @lane_lims
      }
    }
  }

  my @data_products = map { $self->_lims_object2product($_) } @data_lims;
  if (@libmerged_data_lims) {
    push @data_products,
      map { $self->_lims_object2product($_, $selected_lanes4libmerge) }
      @libmerged_data_lims;
  }

  return {
    'data_products' => \@data_products,
    'lanes'         => [map { $self->_lims_object2product($_) } @lane_lims]
  };
}

#####
# The boolean flag below defines whether lane numbers are explicitly
# listed in directory and file names for merged products. It is set
# to true whenever a subset of all available lanes is analysed.
has q{_selected_lanes} => (
  isa           => q{Bool},
  is            => q{ro},
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
  my ($self, $lims, $selected_lanes) = @_;

  return npg_pipeline::product->new(
    rpt_list       => $lims->rpt_list ? $lims->rpt_list :
                        npg_tracking::glossary::rpt->deflate_rpt($lims),
    lims           => $lims,
    selected_lanes => defined $selected_lanes ?
                        $selected_lanes : $self->_selected_lanes
  );
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
