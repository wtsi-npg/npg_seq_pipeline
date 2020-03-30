package npg_pipeline::function::stage2pp;

use namespace::autoclean;
use Moose;
use MooseX::StrictConstructor;
use Readonly;

use npg_pipeline::function::definition;
use npg_pipeline::cache::reference;

extends 'npg_pipeline::base';
with qw{ npg_pipeline::function::util
         npg_pipeline::product::release };
with 'npg_common::roles::software_location' => { tools => [qw/nextflow/] };

Readonly::Scalar my $FUNCTION_NAME => 'stage2pp';
Readonly::Scalar my $MEMORY        => q{5000}; # memory in megabytes
Readonly::Scalar my $CPUS          => 4;
Readonly::Scalar my $CONFIG_FILE_KEY => join q[_], $FUNCTION_NAME, q[nf];

our $VERSION = '0';

=head2 nextflow_cmd

=head2 create

  Arg [1]    : None

  Example    : my $defs = $obj->create
  Description: Create per-product function definitions objects.

  Returntype : ArrayRef[npg_pipeline::function::definition]

=cut

sub create {
  my ($self) = @_;

  my @products = grep { $self->is_release_data($_) }
                 @{$self->products->{data_products}};

  my @definitions = ();
  my $nf;
  my %attrs = ('created_by' => __PACKAGE__,
               'created_on' => $self->timestamp(),
               'identifier' => $self->label,);
  my %job_attrs = %attrs;
  if (@products) {
    $job_attrs{'job_name'} = join q[_], $FUNCTION_NAME, $self->label();
    $job_attrs{'num_cpus'} = [$CPUS];
    $job_attrs{'memory'}   = $MEMORY;
    $nf = $self->general_values_conf()->{$CONFIG_FILE_KEY};
    $nf or $self->fatal("$CONFIG_FILE_KEY is not defined in the general config file");
  }

  my $ref_cache_instance   = npg_pipeline::cache::reference->instance();
  my $do_gbs_plex_analysis = 0;

  foreach my $product (@products) {

    my $in_dir_path  = $product->stage1_out_path($self->no_archive_path());
    my $out_dir_path = $product->path($self->archive_path());
    my $ref_path = $ref_cache_instance
                   ->get_path($product, 'bwa0_6', $self->repository, $do_gbs_plex_analysis);
    $ref_path or $self->logcroak(
      'bwa reference is not found for ' . $product->composition->freeze());
    my $bed_file = $ref_cache_instance
                   ->get_primer_panel_bed_file($product, $self->repository);
    $bed_file or $self->logcroak(
      'Bed file is not found for ' . $product->composition->freeze());
    # And yes, it's -profile, not --profile!
    my $command = join q[ ], $self->nextflow_cmd(), "run $nf",
                             '-profile singularity,sanger',
                             '--illumina --cram --prefix ' . $self->label,
                             "--ref $ref_path",
                             "--bed $bed_file",
                             "--directory $in_dir_path",
                             "--outdir $out_dir_path";
    $job_attrs{'command'}     = $command;
    $job_attrs{'composition'} = $product->composition();
    push @definitions, npg_pipeline::function::definition->new(\%job_attrs);
  }

  if (!@definitions) {
    $self->debug('no stage2pp enabled data products, skipping');
    $attrs{'excluded'} = 1;
    push @definitions, npg_pipeline::function::definition->new(\%attrs);
  }

  return \@definitions;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

npg_pipeline::function::stage2pp

=head1 SYNOPSIS

  my $obj = npg_pipeline::function::stage2pp->new(runfolder_path => $path);

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head1 BUGS AND LIMITATIONS

=head1 INCOMPATIBILITIES

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item namespace::autoclean

=item Moose

=item MooseX::StrictConstructor

=item Readonly

=item npg_common::roles::software_location

=back

=head1 AUTHOR

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
