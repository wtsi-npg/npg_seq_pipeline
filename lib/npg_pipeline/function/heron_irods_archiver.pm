package npg_pipeline::function::heron_irods_archiver;

use Data::Dump qw[pp];
use JSON;
use Moose;
use namespace::autoclean;
use Readonly;

use npg_pipeline::function::definition;

extends 'npg_pipeline::base';

with qw{npg_pipeline::product::release
        npg_pipeline::product::release::portable_pipeline};

our $VERSION = '0';

Readonly::Scalar my $IRODS_PP_ROOT       => q{/seq/illumina/pp/run};
Readonly::Scalar my $PUBLISH_SCRIPT_NAME => q{npg_publish_tree.pl};
Readonly::Scalar my $THOUSAND            => 1000;

has 'irods_destination_collection' => (
    isa      => 'Str',
    is       => 'ro',
    required => 1,
    lazy     => 1,
    builder  => '_build_irods_destination_collection',
);


sub create {
  my ($self) = @_;

  my $job_name = join q[_], $PUBLISH_SCRIPT_NAME, $self->label;

  my @products;

  # Translate the double negative
  my $do_archival = not $self->no_irods_archival();
  if ($do_archival) {
    @products = grep {
      $self->is_for_release($_, $npg_pipeline::product::release::IRODS_PP_RELEASE)
    } grep {
      $self->is_release_data($_)
    } @{$self->products->{data_products}}
  }

  my @definitions;
  foreach my $product (@products) {
    my $config = $self->find_study_config($product);
    if (not $config->{irods_pp}->{enable}) {
      next;
    }

    # TODO: add this to the metadata
    my $composition = encode_json($product->composition->freeze);

    # TODO: other metadata, example:
    # [{"attribute": "id_run", "value": "34576"},
    #  {"attribute": "position", "value": "1"},
    #  {"attribute": "tag_index", "value": "3"},
    #  {"attribute": "supplier_sample_name", "value": "XXYYZZ"},
    #  {"attribute": "pp_name", "value": "ncov2019_artic_nf"},
    #  {"attribute": "pp_version", "value": "v0.8.0"},
    #  {"attribute": "target", "value": "pp"}]

    # lims source for most of these. Are some of these in the config?
    # Guaranteed to be there?

    my $source = $self->pp_archive4product($product,
                                           $self->pps_config4product($product),
                                           # Where is this defined?
                                           $self->pp_archive_path);

    my @args = ($PUBLISH_SCRIPT_NAME,
                q{--collection}, $self->irods_destination_collection(),
                q{--source},     $source);


    if (defined $config->{irods_pp}->{filters}->{include}) {
      my $inc = $config->{irods_pp}->{filters}->{include};
      ref $inc eq 'ARRAY' or
          $self->logcroak(q{Malformed configuration; 'include' },
                          q{expected a list, but found: }, pp($inc));
      foreach my $val (@{$inc}) {
        push @args, q{--include}, "'$val'";
      }
    }

    # TODO: create the metadata and write to a tempfile
    # push @args, q{--metadata}, $self->metadata_json();

    my $command = join q[ ], @args;
    $self->error(qq[iRODS loader command "$command"]);

    push @definitions,
         npg_pipeline::function::definition->new
             ('created_by'  => __PACKAGE__,
              'created_on'  => $self->timestamp(),
              'identifier'  => $self->label,
              'job_name'    => $job_name,
              'command'     => $command,
              'composition' => $product->composition());
  }

  if (not @definitions) {
    push @definitions, npg_pipeline::function::definition->new
        ('created_by' => __PACKAGE__,
         'created_on' => $self->timestamp(),
         'identifier' => $self->label,
         'excluded'   => 1);
  }

  return \@definitions;
};


sub _build_irods_destination_collection {
  my ($self) = @_;
  return join q{/}, ($IRODS_PP_ROOT, int $self->id_run/$THOUSAND, $self->id_run);
}


__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

npg_pipeline::function::heron_run_irods_archiver;

=head1 SYNOPSIS



=head1 DESCRIPTION



=head1 SUBROUTINES/METHODS

=head2 create

Creates and returns a single function definition as an array.
Function definition is created as a npg_pipeline::function::definition
type object.

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item Readonly

=item namespace::autoclean

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR



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
