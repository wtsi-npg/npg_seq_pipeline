package npg_pipeline::function::cache_merge_component;

use namespace::autoclean;

use File::Basename;
use File::Spec::Functions qw{catdir catfile};
use Moose;
use MooseX::StrictConstructor;
use Readonly;
use List::Util qw(all);

use npg_pipeline::function::definition;

extends 'npg_pipeline::base';

with qw{npg_pipeline::product::release};

Readonly::Scalar my $LINK_EXECUTABLE => 'ln';

our $VERSION = '0';

=head2 create

  Arg [1]    : None

  Example    : my $defs = $obj->create
  Description: Create per-product data file function definitions.

  Returntype : ArrayRef[npg_pipeline::function::definition]

=cut

sub create {
  my ($self) = @_;

  my $id_run = $self->id_run();
  my $job_name = sprintf q{%s_%d}, $LINK_EXECUTABLE, $id_run;

  my @products = $self->no_cache_merge_component ? () :
                 grep { $self->is_cacheable($_) }
                 @{$self->products->{data_products}};
  my @definitions = ();

  foreach my $product (@products) {
    my $digest = $product->composition()->digest();
    my $destdir = catdir($self->merge_component_study_cache_dir($product),
      substr($digest,0,2), substr($digest,2,2), $digest );

    my @file_paths = sort $self->expected_files($product);
    $self->_check_files(@file_paths);;

    my @commands;
    foreach my $file_path (@file_paths) {
      my $filename   = basename($file_path);

      push @commands, sprintf q{%s %s %s},
        $LINK_EXECUTABLE, $file_path, $destdir;
    }

    my $command = join q{ && }, qq(mkdir -p $destdir), reverse @commands;
    $self->debug("Adding command '$command'");

    push @definitions,
      npg_pipeline::function::definition->new
        ('created_by'  => __PACKAGE__,
         'created_on'  => $self->timestamp(),
         'identifier'  => $id_run,
         'job_name'    => $job_name,
         'command'     => $command,
         'composition' => $product->composition());
  }

  if (not @definitions) {
    push @definitions, npg_pipeline::function::definition->new
      ('created_by' => __PACKAGE__,
       'created_on' => $self->timestamp(),
       'identifier' => $id_run,
       'excluded'   => 1);
  }

  return \@definitions;
}

sub _check_files {
  my ($self, @file_paths) = @_;

  my @missing;
  foreach my $file_path (@file_paths) {
    if (not -e $file_path) {
      push @missing, $file_path;
    }
  }

  if (@missing) {
    $self->logcroak('Failed to cache files; the following files ',
                    'are missing: ', join q{ }, @missing);
  }

  return;
}

=head2 merge_component_study_cache_dir

  Arg [1]    : npg_pipeline::product

  Example    : $obj->merge_component_cache_dir($product)
  Description: Returns a directory in which to cache data products
               ready for a merge with top-up data.

  Returntype : Str

=cut

sub merge_component_study_cache_dir {
  my ($self, $product) = @_;

  my $rpt          = $product->rpt_list();
  my $name         = $product->file_name_root();
  my $study_config = $self->_find_study_config($product);

  my $dir;

  if ($study_config) {
    $dir = $study_config->{merge}->{component_cache_dir};
    if (ref $dir) {
      $self->logconfess('Invalid directory in configuration file: ', pp($dir));
    }
  }

  return $dir;
}

=head2 is_cacheable

  Arg [1]    : npg_pipeline::product

  Example    : $obj->is_cacheable($product)
  Description: Return true if the product should be cached for a later
               top-up or merge - seq QC Pass, lib QC undecided

  Returntype : Bool

=cut

sub is_cacheable {
  my ($self, $product) = @_;

  my $rpt          = $product->rpt_list();
  my $name         = $product->file_name_root();

  if( $self->merge_component_study_cache_dir( $product ) ) {
    my @seqqc = $self->qc_schema->resultset('MqcOutcomeEnt')->search_via_composition([map{$_->composition}$product->lanes_as_products])->all;
    if(not @seqqc) {
      $self->info("Product $name, $rpt has no seq QC value(s) and so is NOT eligible for caching");
      return 0;
    }
    if(not all { $_->has_final_outcome and $_->is_accepted }  @seqqc) {
      $self->info("Product $name, $rpt are not all Accepted Final seq QC values and so is NOT eligible for caching");
      return 0;
    }
    my @libqc = $self->qc_schema->resultset('MqcLibraryOutcomeEnt')->search_via_composition([$product->composition])->all;
    if(1 != @libqc) {
      $self->info("Product $name, $rpt has no, or different, lib QC value(s) and so is NOT eligible for caching");
      return 0;
    }
    if( $libqc[0]->has_final_outcome and not $libqc[0]->is_undecided) {
      $self->info("Product $name, $rpt has Final lib QC value which is not undecided and so is NOT eligible for caching");
      return 0;
    }

    $self->info("Product $name, $rpt is eligible for caching");
    return 1;
  }

  $self->info("Study for product $name, $rpt is NOT configured for caching");
  return 0;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

npg_pipeline::function::cache_merge_component

=head1 SYNOPSIS

  my $obj = npg_pipeline::function::cache_merge_component->new
    (runfolder_path => $runfolder_path);

=head1 DESCRIPTION

Caches a data product ready for merging with top-up data.

Caching is configured per-study using the configuration file
product_release.yml, see npg_pipeline::product::release.


=head1 SUBROUTINES/METHODS

=head1 BUGS AND LIMITATIONS

=head1 INCOMPATIBILITIES

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item JSON

=item Moose

=item Readonly

=back

=head1 AUTHOR

David K. Jackson

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
