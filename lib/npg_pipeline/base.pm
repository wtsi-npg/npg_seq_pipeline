package npg_pipeline::base;

use Moose;
use namespace::autoclean;
use POSIX qw(strftime);
use Math::Random::Secure qw{irand};
use List::MoreUtils qw{any};
use File::Basename;

use npg_tracking::glossary::rpt;
use npg_tracking::glossary::composition::factory::rpt_list;
use st::api::lims;
use npg_pipeline::product;

our $VERSION = '0';

extends 'npg_tracking::illumina::runfolder';

with qw{
        MooseX::Getopt
        WTSI::DNAP::Utilities::Loggable
        npg_pipeline::base::config
        npg_pipeline::base::options
       };

=head1 NAME

npg_pipeline::base

=head1 SYNOPSIS

=head1 DESCRIPTION

A parent class providing basic functionality to derived objects
within npg_pipeline package

=head1 SUBROUTINES/METHODS

=cut

has [qw/ +npg_tracking_schema
         +slot
         +flowcell_id
         +instrument_string
         +reports_path
         +name
         +tracking_run /] => (metaclass => 'NoGetopt',);

has q{+id_run} => (required => 0,);

=head2 timestamp

A timestring YYYY-MM-DD HH:MM:SS, an attribute with a default
value of current local time.

  my $sTimeStamp = $obj->timestamp();

=cut

has q{timestamp} => (
  isa        => q{Str},
  is         => q{ro},
  default    => sub {return strftime '%Y%m%d-%H%M%S', localtime time;},
  metaclass  => 'NoGetopt',
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

A sorted array of lanes (positions) this pipeline will be run on.
Defaults to positions specified in LIMs.

=cut

sub positions {
  my $self = shift;
  my @positions = @{$self->lanes()} ? @{$self->lanes()} :
                  map {$_->position()} $self->lims->children();
  return (sort @positions);
}

=head2 merge_lanes

Tells p4 stage2 (seq_alignment) to merge lanes (at their plex level if plexed)
and to run its downstream tasks as corresponding compositions.

=cut

has q{merge_lanes} => (
  isa           => q{Bool},
  is            => q{ro},
  lazy          => 1,
  builder       => q{_build_merge_lanes},
  documentation => q{Tells p4 stage2 (seq_alignment) to merge lanes } .
                   q{(at their plex level if plexed) and to run its } .
                   q{downstream tasks as corresponding compositions},
);
sub _build_merge_lanes {
  my $self = shift;
  return $self->all_lanes_mergeable && !$self->is_rapid_run();
}

=head2 lims

st::api::lims run-level object

=cut

has q{lims} => (isa        => q{st::api::lims},
                is         => q{ro},
                metaclass  => q{NoGetopt},
                lazy_build => 1,);
sub _build_lims {
  my ($self) = @_;
  return st::api::lims->new(id_run => $self->id_run);
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

=head2 create_composition

Returns a one-component composition representing an input
object or hash.
 
  my $l = st::api::lims->new(id_run => 1, position => 2);
  my $composition = $base->create_composition($l);

  my $h = {id_run => 1, position => 2};
  $composition = $base->create_composition($h);

This method might be removed in the next round of development.

=cut

sub create_composition {
  my ($self, $l) = @_;
  return npg_tracking::glossary::composition::factory::rpt_list
      ->new(rpt_list => npg_tracking::glossary::rpt->deflate_rpt($l))
      ->create_composition();
}

has q{products} => (
  isa        => q{HashRef},
  is         => q{ro},
  metaclass  => q{NoGetopt},
  lazy_build => 1,
);
sub _build_products {
  my $self = shift;

  my $selected_lanes = (join q[], $self->positions) ne
                       (join q[], map {$_->position} $self->lims->children());

  my $lims2product = sub {
    my $lims = shift;
    return npg_pipeline::product->new(
      rpt_list       => npg_tracking::glossary::rpt->deflate_rpt($lims),
      lims           => $lims,
      selected_lanes => $selected_lanes);
  };

  my @lanes = map { $self->lims4lane($_) } $self->positions;

  my @data_products;
  if ($self->merge_lanes) {
    @data_products =
      map {
        npg_pipeline::product->new(lims           => $_,
                                   rpt_list       => $_->rpt_list,
                                   selected_lanes => $selected_lanes)
          }
      $self->lims->aggregate_xlanes($self->positions);
  } else {
    my @lims = ();
    foreach my $lane (@lanes) {
      if ($self->is_indexed && $lane->is_pool) {
        push @lims, $lane->children;
        push @lims, $lane->create_tag_zero_object();
      } else {
        push @lims, $lane;
      }
    }

    @data_products = map { $lims2product->($_) } @lims;
  }

  return { 'data_products' => \@data_products,
           'lanes'         => [map { $lims2product->($_) } @lanes] };
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

=item npg_tracking::glossary::rpt

=item npg_tracking::glossary::composition::factory::rpt_list

=item st::api::lims

=item WTSI::DNAP::Utilities::Loggable

=item npg_tracking::illumina::runfolder

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown
Marina Gourtovaia

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
