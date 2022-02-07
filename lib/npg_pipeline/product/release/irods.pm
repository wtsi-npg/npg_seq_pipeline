package npg_pipeline::product::release::irods;

use Moose::Role;
use Readonly;
use List::MoreUtils qw/uniq/;
use Carp;
use Try::Tiny;

with 'npg_pipeline::product::release' => {
       -alias    => { is_for_release => '_is_for_release' },
     };

our $VERSION = '0';

Readonly::Scalar my $THOUSAND                    => 1000;
Readonly::Scalar my $PRODUCTION_IRODS_ROOT       => q[/seq];
Readonly::Scalar my $IRODS_REL_ROOT_NOVASEQ_RUNS => q[illumina/runs];
Readonly::Scalar my $IRODS_PP_CONF_KEY           =>
  $npg_pipeline::product::release::IRODS_PP_RELEASE;

=head1 NAME

npg_pipeline::product::release::irods

=head1 SYNOPSIS

=head1 DESCRIPTION

Moose role providing utility methods for iRODS context.

=head1 SUBROUTINES/METHODS

=head2 irods_root_collection_ns

Configurable iRODS root collection path for NovaSeq data.
Defaults to C<illumina/runs> .

=cut

has 'irods_root_collection_ns' => (
  isa           => 'Str',
  is            => 'ro',
  required      => 0,
  default       => $IRODS_REL_ROOT_NOVASEQ_RUNS,
);

=head2 irods_destination_collection

Returns iRODS destination collection for the run.
This attribute will be built if not supplied by the caller.
C</seq> is used as the root of all collections.

Examples of values: C</seq/425>, C</seq/illumina/runs/34/34567>.

=cut

has 'irods_destination_collection' => (
  isa           => 'Str',
  is            => 'ro',
  required      => 0,
  lazy_build    => 1,
);
sub _build_irods_destination_collection {
  my $self = shift;
  my $c;
  try {
    $c = $self->irods_collection4run_rel(
      $self->id_run, $self->platform_NovaSeq());
  } catch {
    $self->logcroak($_);
  };
  return join q[/], $PRODUCTION_IRODS_ROOT, $c;
}

=head2 irods_collection4run_rel

Returns a relative path the run's destination collection. For the production
iRODS this path does not have the root c</seq> component. This methos can
be used as an instance method and a class (package) level method. If used
as a class (package) level method, a hardcoded common iRODS path
C<illumina/runs> is used for NovaSeq platform runs. In the instance method
this path can be customised by setting the C<irods_root_collection_ns>
attribute of the object. For objects it might be more convenient to use the
C<irods_destination_collection> attribute.

If the second argument is not present, the platform is not considered as
NovaSeq.

  my $rc = $obj->irods_collection4run_rel($id_run);
  my $platform_is_novaseq = 1;
  $rc = $obj->$obj->irods_collection4run_rel($id_run, $platform_is_novaseq);
  
  $rc = npg_pipeline::product::release::irods->
    irods_collection4run_rel(45666, 0);
  print $rc; # prints 45666

  $rc = npg_pipeline::product::release::irods->
    irods_collection4run_rel(45666, 1);
  print $rc; # prints illumina/runs/45/45666

=cut

sub irods_collection4run_rel {
  my ($self, $id_run, $platform_is_novaseq) = @_;

  $id_run or croak 'Run id should be given.';
  my @path = ($id_run);
  if ($platform_is_novaseq) {
    unshift @path, int $id_run/$THOUSAND;
    # Is this method called as a class/package method or an instance method?
    # For an instance method we need to retain the ability to configure
    # the relative path for NovaSeq runs. 
    unshift @path, ref $self
                   ? $self->irods_root_collection_ns
                   : $IRODS_REL_ROOT_NOVASEQ_RUNS;
  }

  return join q[/], @path;
}

=head2 irods_product_destination_collection

Returns iRODS destination collection for the argument product.

  my $pc = $obj->irods_product_destination_collection(
                 $run_collection, $product_obj);

=cut

sub irods_product_destination_collection {
  my ($self, $run_collection, $product) = @_;
  my $dc;
  try {
    $dc = $self->irods_product_destination_collection_norf(
          $run_collection, $product, $self->platform_NovaSeq());
  } catch {
    $self->logcroak($_);
  };
  return $dc;
}

=head2 irods_product_destination_collection_norf

Returns iRODS destination collection for the argument product.
Can be use as a package-level or class method since all its inputs are
given as arguments. If the third argument is not present, the platform
is not considered as NovaSeq.

  my $product = npg_pipeline::product(rpt_list => '34567:2:1');
  my $pc = $obj->irods_product_destination_collection_norf(
                 $run_collection, $product);
  my $platform_is_novaseq = 0;
  $pc = $obj->irods_product_destination_collection_norf(
    'some/irods/path/34/34567', $product, $platform_is_novaseq);
  print $pc; # prints some/irods/path/34/34567 
 
  $platform_is_novaseq = 1;
  my $product = npg_pipeline::product(rpt_list => '34567:2:1');
  $pc = npg_pipeline::product::release::irods->
    irods_product_destination_collection_norf(
    'some/irods/path/34/34567', $product, $platform_is_novaseq);
  print $pc; # prints some/irods/path/34/34567/lane2/plex1
  
=cut

sub irods_product_destination_collection_norf {
  my ($self, $run_collection, $product, $platform_is_novaseq) = @_;

  $run_collection or croak('Run collection iRODS path is required');
  $product or croak('Product object is required');
  return $platform_is_novaseq
         ? join q[/], $run_collection, $product->dir_path()
         : $run_collection;
}

=head2 is_for_irods_release

Return true if the product is to be released via iRODS, false otherwise.

  $obj->is_for_irods_release($product)

=cut

sub is_for_irods_release {
  my ($self, $product) = @_;

  my $enable = !$self->is_release_data($product)
                ? $self->_siblings_are_for_irods_release($product)
                : $self->_is_for_release($product, 'irods');

  $self->info(sprintf 'Product %s, %s is %sfor iRODS release',
                      $product->file_name_root(),
                      $product->composition->freeze(),
                      $enable ? q[] : q[NOT ]);

  return $enable;
}

=head2 is_for_pp_irods_release

Return true if the portable pipeline product is to be released via iRODS,
false otherwise.

  $obj->is_for_pp_irods_release($product)

=cut

sub is_for_pp_irods_release {
  my ($self, $product) = @_;
  return $self->_is_for_release($product, $IRODS_PP_CONF_KEY);
}

sub _siblings_are_for_irods_release {
  my ($self, $product) = @_;

  $product->lims or croak 'Need lims object';

  my @lims = ();
  my $with_lims = 1;
  foreach my $p ($product->lanes_as_products($with_lims)) {
    my $l = $p->lims;
    if ($l->is_pool) {
      push @lims, (grep { !$_->is_phix_spike } $l->children);
    } else {
      push @lims, $l;
    }
  }

  my @flags = uniq map { $self->_is_for_release($_, 'irods') ? 1 : 0 } @lims;

  return (@flags == 1) && $flags[0];
}


no Moose::Role;

1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose::Role

=item Readonly

=item List::MoreUtils

=item Carp

=item Try::Tiny

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2019,2020,2021 Genome Research Ltd.

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
