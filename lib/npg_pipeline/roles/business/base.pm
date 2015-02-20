package npg_pipeline::roles::business::base;

use Moose::Role;
use Carp;
use List::MoreUtils qw{any};
use File::Basename;
use Cwd qw{abs_path};

use npg::api::run;
use st::api::lims;
use st::api::lims::warehouse;
use st::api::lims::ml_warehouse;
use npg_tracking::data::reference::find;
use npg_warehouse::Schema;
use WTSI::DNAP::Warehouse::Schema;
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

Boolean flag indicating whether thi srun is a qc run.

=cut

has q{qc_run} => (isa        => q{Bool},
                  is         => q{ro},
                  lazy_build => 1,);
sub _build_qc_run {
  my $self = shift;
  my $lims_id = $self->id_flowcell_lims;
  return $lims_id && $lims_id =~ /\A\d{13}\z/smx; # it's a tube barcode
}

has q{_mlwh_schema} => (
                isa        => q{WTSI::DNAP::Warehouse::Schema},
                is         => q{ro},
                required   => 0,
                lazy_build => 1,);
sub _build__mlwh_schema {
  return WTSI::DNAP::Warehouse::Schema->connect();
}

has q{_wh_schema} => (
                isa        => q{npg_warehouse::Schema},
                is         => q{ro},
                required   => 0,
                lazy_build => 1,);
sub _build__wh_schema {
  return npg_warehouse::Schema->connect();
}


=head2 samplesheet_source_lims

 Sometimes alternative sources of LIMs data have to be used.
 In these cases, return children of the relevant lims object, otherwise
 return undefined.

=cut

sub samplesheet_source_lims {
  my $self = shift;

  my $clims;
  my $lims_id = $self->id_flowcell_lims;

  if (!$lims_id) {

    my $driver = st::api::lims::ml_warehouse->new(
         mlwh_schema      => $self->_mlwh_schema,
         id_flowcell_lims => $lims_id,
         flowcell_barcode => $self->flowcell_id );

    my $lims = st::api::lims->new(
        id_flowcell_lims => $lims_id,
        flowcell_barcode => $self->flowcell_id,
        driver           => $driver,
        driver_type      => 'ml_warehouse' );
    $clims = [$lims->children];

  } elsif ($self->qc_run) {

    my $position = 1; # MiSeq runs only
    my $driver =  st::api::lims::warehouse->new(
        npg_warehouse_schema => $self->_wh_schema,
        position             => $position,
        tube_ean13_barcode   => $lims_id );

    my $lims = st::api::lims->new(
        position    => $position,
        driver      => $driver,
        driver_type => 'warehouse' );
    $clims = [$lims];
  }

  return $clims;
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
    croak 'Position not given';
  }
  return any {$_ == $position} @{$self->multiplexed_lanes};
}

=head2 get_tag_index_list

Returns an array of sorted tag indices for a lane, including tag zero.

=cut

sub get_tag_index_list {
  my ($self, $position) = @_;
  if (!$self->is_multiplexed_lane($position)) {
    return [];
  }
  my $lane = $self->lims->children_ia->{$position};
  if (!$lane) {
    croak "Failed to get lims data for lane $position";
  }
  my @tags = sort keys $lane->tags();
  unshift @tags, 0;
  return \@tags;
}


=head2 is_hiseqx_run

A boolean flag

=cut

has q{is_hiseqx_run} => (isa        => q{Bool},
                         is         => q{ro},
                         metaclass  => q{NoGetopt},
                         lazy_build => 1,);
sub _build_is_hiseqx_run {
  my ($self) = @_;
  return $self->run->instrument->name =~ /\AHX/xms;
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

=head2 tile_list

A string of wildcards for tiles for OLB, defaults to an empty string

=cut

has q{tile_list} => (isa => q{Str},
                     is => q{ro},
                     default => q{},
                     documentation => q{string of wildcards for tiles for OLB, defaults to an empty string},);

=head2 override_all_bustard_options

Overrides all bustard options (including any given via other options) as a string - it is up to the user to ensure all are correct and given

=head2 has_override_all_bustard_options

predicate to ensure that options are available

=cut

has q{override_all_bustard_options} => (
  isa => q{Str},
  is => q{ro},
  predicate => q{has_override_all_bustard_options},
  documentation => q{Overrides all bustard options (including any given via other options) as a string - it is up to the user to ensure all are correct and given - i.e. only use if you know what you are doing.},
);

=head2 repository

A custom reference repository root directory.

=cut

has q{repository} => ( isa       => q{Str},
                       is        => q{ro},
                       required  => 0,
                       predicate => q{has_repository},);

=head2 control_ref

 Path to a default control reference for a default aligner

=cut

has q{control_ref} => (isa           => q{Str},
                       is            => q{ro},
                       lazy_build    => 1,
                       documentation => q{path to a default control reference for a default aligner},);

sub _build_control_ref {
  my ( $self ) = @_;
  return $self->get_control_ref();
}

=head2 get_control_ref

Path to a default control reference for an aligner given by the argument or, if no argument is given, fo a default aligner

=cut

sub get_control_ref {
  my ($self, $aligner) = @_;

  $aligner ||= $self->pb_cal_pipeline_conf()->{default_aligner};
  my $arg_refs = {
    aligner => $aligner,
    species => $self->general_values_conf()->{spiked_species},
  };
  if ( $self->repository() ) {
    $arg_refs->{repository} = $self->repository();
  }

  return Moose::Meta::Class->create_anon_class(
    roles => [qw/npg_tracking::data::reference::find/])->new_object($arg_refs)->refs->[0];
}

=head2 control_snp_file

Path to a default control reference snp file.

=cut

sub control_snp_file {
  my $self = shift;

  my $path = $self->get_control_ref(q[snps]);
  if (!$path) {
    croak 'Failed to retrieve control snp file';
  }
  $path .= q[.rod];
  if (!-e $path) {
    croak "Snip file $path does not exists";
  }
  return $path;
}

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

  my $string = q{-E '} . q{npg_pipeline_preexec_references};
  if ( $self->can( q{has_repository} ) && $self->has_repository() ) {
    $string .= q{ --repository } . $self->repository();
  }
  $string .= q{'};
  return $string;
}

=head2 metadata_cache_dir

Returns an absolute path of teh metadata cache directory if in can be
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
    croak q{Multiple possible locations for metadata cache directory: }  . join q[ ], @ds;
  }

  return $ds[0];
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

=item Cwd

=item WTSI::DNAP::Warehouse::Schema

=item st::api::lims

=item st::api::lims::warehouse

=item st::api::lims::ml_warehouse

=item npg::api::run

=item npg_tracking::data::reference::find

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2015 Genome Research Limited

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
