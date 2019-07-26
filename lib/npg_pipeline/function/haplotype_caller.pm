package npg_pipeline::function::haplotype_caller;

use namespace::autoclean;

use File::Spec;
use Moose;
use MooseX::StrictConstructor;
use Readonly;

use npg_pipeline::function::definition;
use npg_pipeline::cache::reference;

extends 'npg_pipeline::base';
with    q{npg_pipeline::function::util};

with qw{npg_pipeline::product::release};

Readonly::Scalar my $FUNCTION_NAME => 'haplotype_caller';

Readonly::Scalar my $GATK_EXECUTABLE => 'gatk';
Readonly::Scalar my $GATK_TOOL_NAME => 'HaplotypeCaller';

Readonly::Scalar my $FS_NUM_SLOTS                 => 2;
Readonly::Scalar my $MEMORY                       => q{3600}; # memory in megabytes
Readonly::Scalar my $CPUS                         => 4;
Readonly::Scalar my $NUM_HOSTS                    => 1;


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
  my $job_name = sprintf q{%s_%d}, $FUNCTION_NAME, $id_run;

  my @products = $self->no_haplotype_caller ? () :
                 grep { $self->haplotype_caller_enable($_) }
                 @{$self->products->{data_products}};

  if ($self->no_haplotype_caller) {
    $self->debug('no_haplotype_caller set, skipping');
  }
  if (scalar @products == 0) {
    $self->debug('no haplotype_caller enabled data products, skipping');
  }
  my @definitions = ();

  #FIXME: real path to this
  #my $dbsnp = "dbsnp.vcf.gz";

  foreach my $super_product (@products) {
    # Check required metadata
    # This is required for our initial customer, but we should arrange
    # an alternative for when supplier_name is not provided
    #my $supplier_name = $product->lims->sample_supplier_name();
    #$supplier_name or
    #  $self->logcroak(sprintf q{Missing supplier name for product %s, %s},
    #                  $product->file_name_root(), $product->rpt_list());

    # TODO: Check required files
    my $dir_path = File::Spec->catdir($self->archive_path(), $super_product->dir_path());
    my $out_dir_path = $super_product->chunk_out_path($self->archive_path());
    my $input_path = $super_product->file_path($dir_path, ext => 'cram');

    my $ref_name = $super_product->lims->reference_genome || $self->debug(sprintf q{Missing reference genome for product %s, %s},
                  $super_product->file_name_root(), $super_product->rpt_list()) && next;
    if ($super_product->is_tag_zero_product || $super_product->lims->is_control) { next; }
    my $ref_path = npg_pipeline::cache::reference->instance->get_path($super_product, q(fasta), $self->repository());
    my $indel_model = ($super_product->lims->library_type && ($super_product->lims->library_type =~ /PCR free/smx)) ? 'NONE' : 'CONSERVATIVE';

    my $gatk_args = "--emit-ref-confidence GVCF -R $ref_path --pcr-indel-model $indel_model"; # --dbsnp $dbsnp
    my $lister = npg_tracking::data::reference->new(rpt_list => $super_product->rpt_list(), repository => $self->repository());

    my @chunk_products = $super_product->chunks_as_product($self->haplotype_caller_chunking_number($super_product));
    foreach my $product (@chunk_products) {
      my ($species, $ref, undef, undef) = $lister->parse_reference_genome($super_product->lims->reference_genome());
      my $region = sprintf '%s/calling_intervals/%s/%s/%s/%s.%d.interval_list',
        $self->repository,
        $species,
        $ref,
        $self->haplotype_caller_chunking($super_product),
        $self->haplotype_caller_chunking($super_product),
        $product->chunk;
      my $output_path = $product->file_path($out_dir_path, ext => 'g.vcf.gz');
      my $command = sprintf q{%s %s %s -I %s -O %s -L %s},
        $GATK_EXECUTABLE, $GATK_TOOL_NAME, $gatk_args, $input_path, $output_path, $region;

      $self->debug("Adding command '$command'");

      push @definitions,
        npg_pipeline::function::definition->new
          ('created_by'   => __PACKAGE__,
           'created_on'   => $self->timestamp(),
           'identifier'   => $id_run,
           'job_name'     => $job_name,
           'command'      => $command,
           'fs_slots_num' => $FS_NUM_SLOTS,
           'num_hosts'    => $NUM_HOSTS,
           'num_cpus'     => [$CPUS],
           'memory'       => $MEMORY,
           'composition'  => $product->composition());
    }
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

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

npg_pipeline::function::haplotype_caller

=head1 SYNOPSIS

  my $obj = npg_pipeline::function::haplotype_caller->new
    (runfolder_path => $runfolder_path);

=head1 DESCRIPTION


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

Martin Pollard

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
