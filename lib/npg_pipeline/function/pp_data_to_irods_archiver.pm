package npg_pipeline::function::pp_data_to_irods_archiver;

use Moose;
use Data::Dump qw[pp];
use JSON;
use File::Slurp;
use namespace::autoclean;
use Readonly;

extends 'npg_pipeline::function::seq_to_irods_archiver';

our $VERSION = '0';

Readonly::Scalar my $IRODS_PP_ROOT       => q{illumina/pp/runs};
Readonly::Scalar my $PUBLISH_SCRIPT_NAME => q{npg_publish_tree.pl};

has '+irods_root_collection_ns' => (
  default => $IRODS_PP_ROOT,
);

sub create {
  my $self = shift;

  my $ref = $self->basic_definition_init_hash();
  my @definitions = ();

  if (not $ref->{'excluded'}) {

    $self->ensure_restart_dir_exists();

    # Not using script name in the job name since the tree publisher
    # script is generic and can be used in many different functions.
    my $job_name = __PACKAGE__;
    ($job_name) = $job_name =~ /::(\w+)\Z/smx;
    $job_name = join q[_], $job_name, $self->label;

    my @products = grep { $self->is_release_data($_) }
                   @{$self->products->{data_products}};

    foreach my $product (@products) {
      my $config = $self->find_study_config($product);
      $config or next;
      my $enable = $config->{irods_pp}->{enable};
      $self->info(sprintf 'Pp data for product %s will%s be archived to iRODS',
                  $product->composition->freeze,
                  $enable ? q() : q( not));
      $enable or next;

      my $metadata_file = $self->_create_metadata_file($product, $job_name);

      my @args = ( q{--collection}, $product->path($self->irods_destination_collection()),
                   q{--source},     $product->path($self->pp_archive_path()),
                   q{--group},      q('ss_).$product->lims->study_id().q(#seq'), #TODO use npg_irods code?
                   q{--metadata},   $metadata_file, );

      for my $filter_type (qw/include exclude/) {
        if (defined $config->{irods_pp}->{filters}->{$filter_type}) {
          my $filters = $config->{irods_pp}->{filters}->{$filter_type};
          (ref $filters eq 'ARRAY') or
            $self->logcroak(qq(Malformed configuration for filter '${filter_type}'; ),
                             q(expected a list, but found: ), pp($filters));
          foreach my $val (@{$filters}) {
            push @args, qq(--${filter_type}), qq('${val}');
          }
        }
      }

      my %dref = %{$ref};
      $dref{'composition'} = $product->composition;
      $dref{'command'}     = join q[ ], $PUBLISH_SCRIPT_NAME, @args;
      $self->assign_common_definition_attrs(\%dref, $job_name);
      push @definitions, $self->create_definition(\%dref);
    }

    if (not @definitions) {
      $self->info(q{No pp products to archive to iRODS});
      $ref->{'excluded'} = 1;
    }
  }

  return @definitions
         ? \@definitions
         : [$self->create_definition($ref)];
};

sub _create_metadata_file {
  my ($self, $product, $job_name) = @_;

  my %meta_hash = (
    composition => $product->composition->freeze,
    id_product  => $product->composition->digest,
    target      => q{pp},
  );
  if(my$ssn=$product->lims->sample_supplier_name){
    $meta_hash{sample_supplier_name} = $ssn;
  }

  # Convert to baton format.
  my @meta_list = ();
  foreach my $aname (sort keys %meta_hash) {
    push @meta_list,
      {attribute => $aname, value => $meta_hash{$aname}};
  }

  my $file = join q[_], $job_name, $self->random_string(),
                        $product->composition->digest();
  $file .= q{.metadata.json};
  $file = join q[/], $self->irods_publisher_rstart_dir_path(), $file;

  write_file($file, to_json(\@meta_list));

  return $file;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

npg_pipeline::function::pp_data_to_irods_archiver

=head1 SYNOPSIS



=head1 DESCRIPTION



=head1 SUBROUTINES/METHODS

=head2 irods_root_collection_ns

=head2 create

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item Readonly

=item namespace::autoclean

=item Data::Dump

=item JSON

=item File::Slurp

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Keith James
Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2020 Genome Research Ltd.

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
