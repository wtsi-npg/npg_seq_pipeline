package npg_pipeline::roles::business::base;

use Moose::Role;
use Carp;
use List::MoreUtils qw{any};
use File::Basename;

use npg_tracking::util::abs_path qw{abs_path};
use npg_tracking::glossary::rpt;
use npg_tracking::glossary::composition::factory::rpt_list;
use npg::api::run;
use st::api::lims;
use npg_tracking::data::reference::find;
use npg_pipeline::cache;

our $VERSION = '0';

=head1 NAME

npg_pipeline::roles::business::base

=head1 SYNOPSIS

  package MyPackage;
  use Moose;
  ...
  with qw{npg_pipeline::roles::business::base};

=head1 DESCRIPTION

This role provides some base attributes and methods which are business logic related
and likely to be applied over the whole of the pipeline.

=head1 SUBROUTINES/METHODS

=head2 id_flowcell_lims

Optional LIMs identifier for flowcell

=cut

has q{id_flowcell_lims} => ( isa      => q{Int},
                             is       => q{ro},
                             required => 0,);

=head2 run

Run npg::api::run object, an id_run method is required for this.

=cut

has q{run} => (isa        => q{npg::api::run},
               is         => q{ro},
               metaclass  => q{NoGetopt},
               lazy_build => 1,);
sub _build_run {
  my ($self) = @_;
  return npg::api::run->new({id_run => $self->id_run(),});
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

=head2 qc_run

Boolean flag indicating whether this run is a qc run,
will be built if not supplied;

=cut

has q{qc_run} => (isa        => q{Bool},
                  is         => q{ro},
                  lazy_build => 1,
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

has q{lims_driver_type} => (isa           => q{Str},
                            is            => q{ro},
                            lazy_build    => 1,
                            documentation => q{Optional lims driver type name},);
sub _build_lims_driver_type {
  my $self = shift;
  return $self->qc_run ?
    ($self->is_qc_run($self->id_flowcell_lims) ?
       npg_pipeline::cache->warehouse_driver_name :
       npg_pipeline::cache->mlwarehouse_driver_name
    ) : npg_pipeline::cache->mlwarehouse_driver_name;
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

sub _lims4lane {
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

=head2 create_composition

Returns a one-component composition representing an input
object or hash.
 
  my $l = st::api::lims->new(id_run => 1, position => 2);
  my $composition = $base->create_composition($l);

  my $h = {id_run => 1, position => 2};
  $composition = $base->create_composition($h);

=cut

sub create_composition {
  my ($self, $l) = @_;
  return npg_tracking::glossary::composition::factory::rpt_list
      ->new(rpt_list => npg_tracking::glossary::rpt->deflate_rpt($l))
      ->create_composition();
}

=head2 get_tag_index_list

Returns an array of sorted tag indices for a lane, including tag zero.

=cut

sub get_tag_index_list {
  my ($self, $position) = @_;
  if (!$self->is_multiplexed_lane($position)) {
    return [];
  }
  my @tags = sort keys %{$self->_lims4lane($position)->tags()};
  unshift @tags, 0;
  return \@tags;
}


=head2 is_hiseqx_run

A boolean flag

=cut

has q{is_hiseqx_run} => (isa           => q{Bool},
                         is            => q{ro},
                         metaclass     => q{NoGetopt},
                         lazy_build    => 1,
                         documentation => q{modified to also identify HiSeq 4000 runs which start with HF},);
sub _build_is_hiseqx_run {
  my ($self) = @_;
  return $self->run->instrument->name =~ /\AH[XF]/xms;
}

=head2 positions

An array of lane positions for this submission.

=cut

sub positions {
  my $self = shift;
  my @positions = !$self->no_lanes() ? $self->all_lanes() : $self->all_positions();
  @positions = sort @positions;
  return @positions;
}

=head2 all_positions
 
Return all lanes available in the flowcell.

=cut

sub all_positions {
  my $self = shift;
  my @position = sort map {$_->position()} $self->lims->children;
  return @position;
}

=head2 repository

A custom reference repository root directory.

=cut

has q{repository} => ( isa       => q{Str},
                       is        => q{ro},
                       required  => 0,
                       predicate => q{has_repository},);

=head2 get_study_library_sample_names

Given a position and a tag_index, return a hash with study, library and sample names. 

=cut

sub get_study_library_sample_names {
  my ($self, $elims) = @_;

  my $sample_names = [];
  my %study_names_hash = ();

  my @alims = $elims->is_pool ? grep {not $_->is_control} $elims->children : ($elims);
  foreach my $al (@alims) {

     my $sample_name = $al->sample_publishable_name();
     if($sample_name){
        push @{$sample_names}, $al->sample_publishable_name;
     }

     my $study_name = $al->study_publishable_name();
     my $study_description = $al->is_control ? 'SPIKED_CONTROL' : $al->study_description;
     if( $study_name ){
        if( $study_description ){
           $study_description =~ s/\r//gmxs;
           $study_description =~ s/\n/\ /gmxs;
           $study_description =~ s/\t/\ /gmxs;
           $study_name .= q{: }.$study_description;
        }
        $study_names_hash{$study_name}++;
     }
  }

  my $library_aref = $elims->library_id ? [$elims->library_id] : [];
  my $href = {
          study    => [keys %study_names_hash],
          library  => $library_aref,
          sample   => $sample_names,
         };
  return $href;
}

=head2 ref_adapter_pre_exec_string

Pre-exec string to test the availability of the reference repository.

=cut

sub ref_adapter_pre_exec_string {
  my ( $self ) = @_;

  my $string = q{npg_pipeline_preexec_references};
  if ( $self->can( q{has_repository} ) && $self->has_repository() ) {
    $string .= q{ --repository } . $self->repository();
  }
  return $string;
}

=head2 metadata_cache_dir

Returns an absolute path of the metadata cache directory if in can be
inferred from the environment variables set up during caching.

=cut

sub metadata_cache_dir {
  my $dirs = {};
  foreach my $var (npg_pipeline::cache->env_vars()) {
    my $path = $ENV{$var};
    if (!$path) {
      next;
    }
    if (-f $path) {
      $path = dirname $path;
    } else {
      if (!-d $path) {
        $path = q[];
      }
    }
    if ($path) {
      $dirs->{abs_path $path} = 1;
    }
  }

  my @ds = keys %{$dirs};
  if (!@ds) {
    croak q{Cannot infer location of cache directory};
  }
  if (scalar @ds > 1) {
    croak q{Multiple possible locations for metadata cache directory: } . join q[ ], @ds;
  }

  return $ds[0];
}

=head2 num_cpus2array

=cut

sub num_cpus2array {
  my ($self, $num_cpus_as_string) = @_;
  my @numbers = grep  { $_ > 0 }
                map   { int }    # zero if conversion fails
                split /,/xms, $num_cpus_as_string;
  if (!@numbers || @numbers > 2) {
    $self->logcroak('Non-empty array of up to two numbers is expected');
  }
  return [sort {$a <=> $b} @numbers];
}

1;
__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose::Role

=item Carp

=item List::MoreUtils

=item File::Basename

=item npg_tracking::util::abs_path

=item st::api::lims

=item npg::api::run

=item npg_tracking::data::reference::find

=item npg_tracking::glossary::rpt

=item npg_tracking::glossary::composition::factory::rpt_list

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown
Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2018 Genome Research Limited

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
