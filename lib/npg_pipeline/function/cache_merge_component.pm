package npg_pipeline::function::cache_merge_component;

use namespace::autoclean;

use Data::Dump qw{pp};
use File::Basename;
use File::Spec::Functions qw{catdir catfile};
use Moose;
use MooseX::StrictConstructor;
use Readonly;
use Try::Tiny;

use npg_pipeline::function::definition;
use npg_qc::mqc::outcomes;

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

=item Data::Dump

=item JSON

=item Moose

=item Readonly

=item Try::Tiny

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
