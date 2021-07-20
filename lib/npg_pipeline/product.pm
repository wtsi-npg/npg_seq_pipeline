package npg_pipeline::product;

use Moose;
use namespace::autoclean;
use Carp;
use File::Spec;
use List::MoreUtils qw/all/;
use Readonly;

use npg_tracking::glossary::rpt;
use npg_tracking::glossary::composition::component::illumina;
use npg_tracking::glossary::composition;

extends 'npg_tracking::glossary::composition::factory::rpt_list';
with    'npg_pipeline::product::chunk';

our $VERSION = '0';

Readonly::Scalar my $QC_DIR_NAME                => q[qc];
Readonly::Scalar my $CHUNK_DIR_NAME             => q[chunk];
Readonly::Scalar my $SHORT_FILES_CACHE_DIR_NAME => q[.npg_cache_10000];
Readonly::Scalar my $TILEVIZ_DIR_NAME_PREFIX    => q[tileviz];
Readonly::Scalar my $STAGE1_OUT_DIR_NAME        => q[stage1];

=head1 NAME

npg_pipeline::product

=head1 SYNOPSIS

Either composition (an instance of npg_tracking::glossary::composition
class) or rpt_list attribute should be set in the constructor. The rpt_list
attribute is restricted to be a string. It should conform to the format
defined in npg_tracking::glossary::rpt.

An optional 'lims' attribute should be an instance of st::api::lims class,
which should represent the same entity as the rpt_list attribute. If both
rpt_list and composition are set in the constructor, they should be mutually
compatible.

It is expected that all the components in the composition are instances of
npg_tracking::glossary::composition::component::illumina class.

A number of different compositions can correspond to the same rpt_list value
since the latter does not take the 'subset' attribute of the components into
account.

An optional boolean 'selected_lanes' attribute can be set to true to
indicate that file and directory naming scheme should treat merges as being
done across a selection of lanes rather than across all lanes. By default
this attribute evaluates to false. 
  
  my $p;
  $p = npg_pipeline::product->new(
         rpt_list => '1234:2',
         lims     => st::api:::lims->new(id_run => 1234, position => 2));
  $p = npg_pipeline::product->new(
         rpt_list => '1234:1:1,1234:2:1',
         lims     => st::api:::lims->new(rpt_list => '1234:1:1,1234:2:1'));
  print $p->composition->num_components;   # 2
  print $p->composition->get_component(0)
          ->subset || 'undef'              # undef

Factory methods to generate related product instances:

  my @p_components = $p->components_as_products();
  print scalar @p_components;              # 2
  print ref $p_components[0];              # npg_pipeline::product
  print $p_components[0]->composition
                        ->num_components;  # 1
  print $p_components[1]->composition
                        ->num_components;  # 1
  print $p_components[0]->composition
        ->get_component(0)->tag_index;     # 1
  print $p_components[1]->composition
        ->get_component(0)->tag_index;     # 1
  print $p_components[0]->composition
        ->get_component(0)->position;      # 1
  print $p_components[1]->composition
        ->get_component(0)->position;      # 2

  my $ph = $p->subset_as_product('human');
  print ref $ph;                           # npg_pipeline::product
  print $ph->rpt_list eq $p->rpt_list;     # 1
  print $ph->composition->num_components;  # 2
  print $ph->composition->get_component(0)
           ->subset || 'undef'             # human
  print $ph->composition->get_component(1)
           ->subset || 'undef'             # human

  my @p_lanes = $p->lanes_as_products();
  print scalar @p_lanes;                   # 2
  print ref $p_lanes[0];                   # npg_pipeline::product
  print $p_lanes[0]->composition
                   ->num_components;       # 1
  print $p_lanes[1]->composition
                   ->num_components;       # 1
  print $p_lanes[0]->composition->get_component(0)
        ->tag_index || 'undef';            # 1
  print $p_lanes[1]->composition->get_component(0)
        ->tag_index || 'undef';            # 1
  print $p_lanes[0]->composition
        ->get_component(0)->position;      # 1
  print $p_lanes[1]->composition
        ->get_component(0)->position;      # 2

  @p_lanes = $ph->lanes_as_products();
  print $p_lanes[0]->composition->get_component(0)
           ->subset || 'undef'             # undef
  print $p_lanes[2]->composition->get_component(0)
           ->subset || 'undef'             # undef
  

=head1 DESCRIPTION

A wrapper object combining different attributes of a pipeline product,
such as composition, LIMs data, file naming schema, relative directory path.

=head1 SUBROUTINES/METHODS

=head2 composition

An optional attribute, an npg_tracking::glossary::composition object
corersponding to this product. If not set, is built using the value
of the rpt_list attribute.

=head2 has_composition

Predicate method for the 'composition' attribute.

=cut

#####
# 'composition' accessor is required by the npg_tracking::glossary::moniker
# role, so its definition should precede consuming the role.
#
has 'composition' => (
  isa        => q[npg_tracking::glossary::composition],
  is         => q[ro],
  required   => 0,
  predicate  => 'has_composition',
  lazy_build => 1,
);
sub _build_composition {
  my $self = shift;
  if (!$self->has_rpt_list()) {
    croak 'rpt_list attribute is not set, cannot build composition attribute';
  }
  return $self->create_composition();
}

#####
# 'composition' accessor defined, can consume the role
#
with 'npg_tracking::glossary::moniker' => {
       -alias    => { file_name => '_file_name_root' },
       -excludes => 'file_name',
     };

=head2 rpt_list

An optional string attribute. Inherited from npg_tracking::glossary::moniker,
extended to be lazy. If not set, is built using the value of the composition
attribute.

=head2 has_rpt_list

Predicate method for the 'rpt_list' attribute.

=cut

has  '+rpt_list' => (
  required   => 0,
  predicate  => 'has_rpt_list',
  lazy_build => 1,
);
sub _build_rpt_list {
  my $self = shift;
  if (!$self->has_composition()) {
    croak 'composition attribute is not set, cannot build rpt_list attribute';
  }
  return $self->composition->freeze2rpt();
}

=head2 selected_lanes

Boolean flag, defaults to false, is meaningful only for a product with multiple
components. If true, indicates that the conposition does not span all lanes
of the run.

=cut

has  'selected_lanes' => (
  isa        => 'Bool',
  is         => 'ro',
  required   => 0,
);

=head2 lims

An optional attribute, an st::api::lims object corresponding to this product.

=head2 has_lims

Predicate method for 'lims' attribute.

=cut

has  'lims' => (
  isa       => q[st::api::lims],
  is        => 'ro',
  predicate => 'has_lims',
  required  => 0,
);

=head2 file_name_root
 
=cut

has 'file_name_root' => (
  is         => q[ro],
  required   => 0,
  lazy_build => 1,
);
sub _build_file_name_root {
  my $self = shift;

  return sprintf '%s%s', $self->_file_name_root($self->selected_lanes), $self->chunk_label();
}

=head2 file_name

Generates and returns a file name for this product. Without arguments returns
the value of the file_name_root attribute. 

  $p->file_name_root();                                  # 26219_1#0
  $p->file_name();                                       # 26219_1#0
  $p->file_name(ext => 'bam');                           # 26219_1#0.bam
  $p->file_name(ext => 'stats', suffix => 'F0xB00');     # 26219_1#0_F0xB00.stats 
  $p->file_name(ext => 'stats.md5', suffix => 'F0xB00'); # 26219_1#0_F0xB00.stats.md5 

=cut

sub file_name {
  my ($self, @options) = @_;

  return $self->file_name_full($self->file_name_root(), @options);
}

=head2 dir_path

A relative path for the product, method is inherited from
npg_tracking::glossary::moniker.

=head2 path

Returns the directory path for data files for this product
taking argument directory path as a base. Uses dir_path to
generate the path.
 
=cut

sub path {
  my ($self, $dir) = @_;
  $dir or croak 'Directory argument is needed';
  return File::Spec->catdir($dir, $self->dir_path($self->selected_lanes));
}

=head2 existing_path

Returns an existing directory path for data files for this product
taking argument directory path as a base. First checks if the path
returned by the path method of this class exists. If not, assumes
that the name of the directory is the long digest of the product's
composition. Returns an existing path or raises an error.
 
=cut

sub existing_path {
  my ($self, $dir) = @_;

  $dir or croak 'Directory argument is needed';
  (-e $dir) or croak "Directory argument $dir does not exist";

  my $path = File::Spec->catdir($dir, $self->dir_path($self->selected_lanes));
  my $orig = $path;
  if (!-e $path) {
    $path = File::Spec->catdir($dir, $self->generic_name());
    ($path ne $orig) or croak "$path does not exist";
    (-e $path) or croak "Neither $orig nor $path exists";
  }

  return $path;
}

=head2 qc_out_path

Returns path for qc output directory for this product taking
argument directory path as a base.
 
=cut

sub qc_out_path {
  my ($self, $dir) = @_;
  return File::Spec->catdir($self->path($dir), $QC_DIR_NAME);
}

=head2 chunk_out_path
 
 Returns path for chunk output directory for this product taking
 argument directory path as a base.
 
=cut

sub chunk_out_path {
  my ($self, $dir) = @_;
  return File::Spec->catdir($self->path($dir), $CHUNK_DIR_NAME);
}

=head2 stage1_out_path
 
 Returns path for an unaligned *.cram for this product taking
 argument directory path as a base.
 
=cut

sub stage1_out_path {
  my ($self, $dir) = @_;
  return File::Spec->catdir($self->path($dir), $STAGE1_OUT_DIR_NAME);
}

=head2 existing_qc_out_path

Returns path for qc output directory for this product taking
argument directory path as a base. Uses existing_path method
of this object to find the product path. If either the product
path or the qc_path does not exist, an error is raised.
 
=cut

sub existing_qc_out_path {
  my ($self, $dir) = @_;
  my $path = File::Spec->catdir($self->existing_path($dir), $QC_DIR_NAME);
  (-e $path) or croak "QC path $path does not exist";
  return $path;
}

=head2 short_files_cache_path

Returns path for short files cache directory for this product
taking argument directory path as a base.
 
=cut

sub short_files_cache_path {
  my ($self, $dir) = @_;
  return File::Spec->catdir($self->path($dir), $SHORT_FILES_CACHE_DIR_NAME);
}

=head2 tileviz_path

Returns path for tileviz output directory for this (lane) product taking
argument directory path as a base.
 
=cut

sub tileviz_path {
  my ($self, $dir) = @_;
  # Follow convention used by bambi, ie append _lane<POSITION>
  return join q[_], $self->tileviz_path_prefix($dir),
                    q[lane] . $self->composition->get_component(0)->position;
}

=head2 tileviz_path_prefix

Returns path parefix for tileviz output directory for this (lane) product
taking argument directory path as a base.
 
=cut

sub tileviz_path_prefix {
  my ($self, $dir) = @_;
  return File::Spec->catdir($self->path($dir), $TILEVIZ_DIR_NAME_PREFIX);
}

=head2 file_path

Given a directory (first argument, required), returns a full file path.
Takes the same optional arguments as the file_name method. Uses the
file_name method to generate the file name

  $p->file_path('/tmp/files');                # /tmp/files/26219_1#0
  $p->file_path('/tmp/files', ext => 'bam');  # /tmp/files/26219_1#0.bam
  $p->file_path('/tmp/files', ext => 'stats', suffix => 'F0xB00');
                                              # /tmp/files/26219_1#0_F0xB00.stats        
 
=cut

sub file_path {
  my ($self, $dir, %options) = @_;
  $dir or croak 'Directory argument is needed';
  return File::Spec->catfile($dir, $self->file_name(%options));
}

=head2 has_multiple_components

Returns true if the product contains multiple components (is a result
of a merge), false otherwise.
 
=cut

sub has_multiple_components {
  my $self = shift;
  return $self->composition->num_components() > 1;
}

=head2 is_tag_zero_product

Returns true if all components of this product are for tag zero.
 
=cut

sub is_tag_zero_product {
  my $self = shift;
  return all {defined $_->tag_index && $_->tag_index == 0 }
         $self->composition->components_list();
}

=head2 components_as_products

For a product with any number of components, returns a list of
objects of this class representing individual components.

The subset information is not retained in the returned objects.
The lims attribute of the returned objects is not set.

  my $product = npg_pipeline::product->new(
                rpt_list => '3:4:5;3:3:5', lims_obj => $some);
  my @component_products = $product->components_as_products();

  $product->file_name_root();               # 3_3-4#5
  $component_products[0]->file_name_root(); # 3_3#5
  $component_products[1]->file_name_root(); # 3_4#5

=cut

sub components_as_products {
  my $self = shift;
  my @components = ();
  foreach my $c ($self->composition->components_list()) {
    push  @components, __PACKAGE__->new(selected_lanes => $self->selected_lanes,
                                        rpt_list       => $c->freeze2rpt());
  }
  return @components;
}

=head2 lanes_as_products

Returns a list of product lane-level objects, representing the lanes the
components of this product came from. For a single-component product returns
a list of one lane object. For a product that is itself a lane or a
composition of lanes, returns objects corresponding to component lanes.

The subset information is not retained in the returned objects.
The lims attribute of the returned objects is not set unless a true flag
is passed to the method as a single argument.

 my @lane_products = $product->lanes_as_products();
 defined $lane_products[0]->lims; # false
 my $with_lims = 1;
 @lane_products = $product->lanes_as_products($with_lims);
 defined $lane_products[0]->lims; # true

=cut

sub lanes_as_products {
  my ($self, $with_lims) = @_;

  if ($with_lims && !$self->has_lims) {
    croak 'In order to use with_lims option this product should have ' .
	  'lims attribute set';
  }
  ##no critic (BuiltinFunctions::ProhibitComplexMappings)
  my @lane_hashes =
    map { delete $_->{'subset'}; delete $_->{'tag_index'}; $_ }
    map { npg_tracking::glossary::rpt->inflate_rpt($_) }
    map { $_->freeze2rpt() }
    $self->composition->components_list();

  my @lane_products = ();
  foreach my $lane_hash (@lane_hashes) {
    my $list = npg_tracking::glossary::rpt->deflate_rpt($lane_hash);
    my $ref = {selected_lanes => $self->selected_lanes, rpt_list => $list};
    if ($with_lims) {
      $ref->{lims} = $self->lims->create_lane_object(
                     $lane_hash->{id_run}, $lane_hash->{position});
    }
    push @lane_products, __PACKAGE__->new($ref);
  }

  return  @lane_products;
}

=head2 subset_as_product

Interprets the argument string (required) as the name of a subset (phix,
human, etc.). Returns an object of this class for the a composition
identical to the composition object of this object with one exception -
the subset value in all components is set to the value of the argument string.

The lims attribute of the returned object is not set.

  my $subset_p = $p->subset_as_product('phix');
  $p->file_name_root();         # 123_6#4
  $subset_p->file_name_root();  # 123_6#4_phix

=cut

sub subset_as_product {
  my ($self, $subset) = @_;
  $subset or croak 'Subset argument should be given';
  ##no critic (BuiltinFunctions::ProhibitComplexMappings)
  my @components =
    map { npg_tracking::glossary::composition::component::illumina->new($_) }
    map { $_->{'subset'} = $subset; $_ }
    map { npg_tracking::glossary::rpt->inflate_rpt($_) }
    map { $_->freeze2rpt() }
    $self->composition->components_list();

  return __PACKAGE__->new(
    selected_lanes => $self->selected_lanes,
    composition => npg_tracking::glossary::composition->new(components => \@components));
}

=head2 chunks_as_product
 
 Interprets the argument integer (required) as the number of chunks to subset
 each product into. Returns a list of product objects with the chunk value
 in each set to one of the values in range 1 .. NUMBER_OF_GIVEN_CHUNKS. See
 chunk_as_product method for details of a product object with the chunk
 attribute defined.

 The products in the list are sorted in the accending chunk value order.
 
 my @chunks_p = $p->chunks_as_product(24);
 $p->file_name_root();         # 123_6#4
 $chunks_p[0]->file_name_root();  # 123_6#4.1
 $chunks_p[1]->file_name_root();  # 123_6#4.2
 
=cut

sub chunks_as_product {
  my ($self, $num_chunks) = @_;
  $num_chunks or croak 'Number of chunks argument should be given';

  my @chunks = ();
  # Let the chunk attribute take care of validating the $num_chunck
  # variable and fail early and cleanly.
  my $last_chunk = $self->chunk_as_product($num_chunks);
  my $max_current = $num_chunks - 1;
  if ($max_current) { # ie more than one chunk is required
    @chunks = map { $self->chunk_as_product($_) } (1 .. $max_current);
  }
  push @chunks, $last_chunk;

  return @chunks;
}

=head2 chunk_as_product
 
 Interprets the argument integer (required) as the chunk for this product
 to create a new product object for. Returns an object of this class for the chunk with
 a composition identical to the composition object of this object with one exception -
 the chunk value in the object is set to the value of the argument string.
 
 The lims attribute of the returned object is set if the lims attribute of
 this object is set.
 
 my @chunks_p = $p->product_chunk(2);
 $p->file_name_root();         # 123_6#4
 $chunk_p->file_name_root();  # 123_6#4.2
 
=cut

sub chunk_as_product {
  my ($self, $chunk) = @_;

  $self->chunk and croak
   'Cannot create a chunked product from a product with the chunk attribute set';
  defined $chunk or croak 'Chunk argument must be given.';

  # Note that here we allow for chuncking of products with subset defined.
  my $ref = { selected_lanes => $self->selected_lanes,
              composition    => $self->composition,
              rpt_list       => $self->rpt_list,
              chunk          => $chunk };
  $self->has_lims() and $ref->{lims} = $self->lims();
  return __PACKAGE__->new($ref);
}


=head2 final_seqqc_objs

  Returns a list of  DBIx row objects representing a sequencing QC outcomes
  for component lanes of this product. If not all lanes have a final outcome,
  an empty list is returned.

  npg_qc::Schema object argument is required.

    use List::MoreUtils qw/all any/;
    my @seq_qc_objs = $p->final_seqqc_objs($schema);
    my $passed = @seq_qc_objs && (all { $_->is_accepted } @seq_qc_objs);
    my $failed = !@seq_qc_objs || (any { $_->is_rejected } @seq_qc_objs);
=cut

sub final_seqqc_objs {
  my ($self, $schema) = @_;

  $schema or croak 'qc schema argument is required';

  my @lp = $self->lanes_as_products;
  my @seqqc = grep { $_->has_final_outcome }
              $schema->resultset('MqcOutcomeEnt')
              ->search_via_composition([map{$_->composition}@lp])->all;
  if (@lp != @seqqc) {
    return;
  }

  return @seqqc;
}

=head2 seqqc_objs

  Returns a list of  DBIx row objects representing a sequencing QC outcomes
  for component lanes of this product, even if not all outcomes are final.

  npg_qc::Schema object argument is required.

    use List::MoreUtils qw/all any/;
    my @seq_qc_objs = $p->seqqc_objs($schema);
    #these may or may not be final outcomes
    my $passed = @seq_qc_objs && (all { $_->is_accepted } @seq_qc_objs);
    my $failed = !@seq_qc_objs || (any { $_->is_rejected } @seq_qc_objs);

=cut

sub seqqc_objs {
  my ($self, $schema) = @_;

  $schema or croak 'qc schema argument is required';

  my @lp = $self->lanes_as_products;
  my @seqqc = $schema->resultset('MqcOutcomeEnt')
              ->search_via_composition([map{$_->composition}@lp])->all;
  if (@lp != @seqqc) {
    return;
  }
  return @seqqc;
}

=head2 final_libqc_obj

  Returns a DBIx row object representing a final library QC outcome.
  Returns an undefined value if the final library QC outcome is
  not available for this product.

  npg_qc::Schema object argument is required.

    my $lib_qc_obj = $p->final_libqc_obj($schema);
    print $lib_qc_obj->is_accepted;

=cut

sub final_libqc_obj {
  my ($self, $schema) = @_;

  $schema or croak 'qc schema argument is required';

  my $libqc = $schema->resultset('MqcLibraryOutcomeEnt')
                     ->search_via_composition([$self->composition])->next;
  if ($libqc && $libqc->has_final_outcome) {
    return $libqc;
  }

  return;
}

=head2 libqc_obj

  Returns a DBIx row object representing the library QC outcome.
  Returns an undefined value if the library QC outcome is
  not available for this product.

  npg_qc::Schema object argument is required.

    my $lib_qc_obj = $p->libqc_obj($schema);

=cut

sub libqc_obj {
  my ($self, $schema) = @_;

  $schema or croak 'qc schema argument is required';

  return $schema->resultset('MqcLibraryOutcomeEnt')
                ->search_via_composition([$self->composition])->next;
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

=item Carp

=item List::MoreUtils

=item File::Spec

=item npg_tracking::glossary::rpt

=item npg_tracking::glossary::composition::factory::rpt_list

=item npg_tracking::glossary::moniker

=item npg_tracking::glossary::composition::component::illumina

=item npg_tracking::glossary::composition

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

=over

=item Marina Gourtovaia

=item Fred Dodd

=back

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2018,2019,2020,2021 Genome Research Ltd.

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
