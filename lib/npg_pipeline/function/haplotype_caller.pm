package npg_pipeline::function::haplotype_caller;

use namespace::autoclean;
use Moose;
use MooseX::StrictConstructor;
use Readonly;

use npg_tracking::data::reference::find;
use npg_pipeline::function::definition;
use npg_pipeline::cache::reference;
use npg_pipeline::runfolder_scaffold;

extends 'npg_pipeline::base';
with qw{ npg_pipeline::function::util
         npg_pipeline::product::release };
with 'npg_common::roles::software_location' => { tools => [qw/gatk/] };

Readonly::Scalar my $FUNCTION_NAME   => 'haplotype_caller';

Readonly::Scalar my $GATK_TOOL_NAME  => 'HaplotypeCaller';
Readonly::Scalar my $GATK_BQSR_TOOL_NAME  => 'ApplyBQSR';


Readonly::Scalar my $FS_NUM_SLOTS                 => 2;
Readonly::Scalar my $MEMORY                       => q{3600}; # memory in megabytes
Readonly::Scalar my $CPUS                         => 4;
Readonly::Scalar my $NUM_HOSTS                    => 1;


our $VERSION = '0';

=head2 gatk_cmd

=head2 create

  Arg [1]    : None

  Example    : my $defs = $obj->create
  Description: Create per-product function definitions objects.

  Returntype : ArrayRef[npg_pipeline::function::definition]

=cut

sub create {
  my ($self) = @_;

  my $job_name = sprintf q{%s_%d}, $FUNCTION_NAME, $self->label();

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

  my @out_dirs = ();

  foreach my $super_product (@products) {

    $self->is_release_data($super_product) or next;

    # TODO: Check required files
    my $dir_path     = $super_product->path($self->archive_path());
    my $out_dir_path = $super_product->chunk_out_path($self->archive_path());
    push @out_dirs, $out_dir_path;
    my $input_path   = $super_product->file_path($dir_path, ext => 'cram');

    my $ref_name = $super_product->lims->reference_genome || $self->debug(sprintf q{Missing reference genome for product %s, %s},
                  $super_product->file_name_root(), $super_product->rpt_list()) && next;

    my $ref_path = npg_pipeline::cache::reference->instance->get_path($super_product, q(fasta), $self->repository());
    my $indel_model = ($super_product->lims->library_type && ($super_product->lims->library_type =~ /PCR free/smx)) ? 'NONE' : 'CONSERVATIVE';

    my $gatk_args = "--emit-ref-confidence GVCF -R $ref_path --pcr-indel-model $indel_model"; # --dbsnp $dbsnp

    my @chunk_products = $super_product->chunks_as_product($self->haplotype_caller_chunking_number($super_product));
    foreach my $product (@chunk_products) {

      my ($species, $ref, undef, undef) = npg_tracking::data::reference::find
        ->parse_reference_genome($super_product->lims->reference_genome());
      my $chuncking_base_name = $self->haplotype_caller_chunking($super_product);
      my $region = sprintf '%s/calling_intervals/%s/%s/%s/%s.%d.interval_list',
        $self->repository,
        $species,
        $ref,
        $chuncking_base_name,
        $chuncking_base_name,
        $product->chunk;
      my $output_path = $product->file_path($out_dir_path, ext => 'g.vcf.gz');
      my $command;

      if ($self->bqsr_enable($product) && $self->bqsr_apply_enable($product)) {
        ##no critic (ValuesAndExpressions::RequireInterpolationOfMetachars)
        # critic complaines about not interpolating $TMPDIR
        my $make_temp = 'TMPDIR=`mktemp -d -t bqsr-XXXXXXXXXX`';
        my $rm_cmd = 'trap "(rm -r $TMPDIR || :)" EXIT';
        my $debug_cmd = 'echo "BQSR tempdir: $TMPDIR"';
        ##use critic
        my $bqsr_table   = $super_product->file_path($dir_path, ext => 'bqsr_table');
        my $temp_path = $product->file_name(ext => 'cram', suffix => 'bqsr');

        my $bqsr_args = "-R $ref_path --preserve-qscores-less-than 6 --static-quantized-quals 10 --static-quantized-quals 20 --static-quantized-quals 30";
        ##no critic (ValuesAndExpressions::RequireInterpolationOfMetachars)
        my $bqsr_cmd = sprintf q(%s %s %s --bqsr-recal-file %s -I %s -O $TMPDIR/%s -L %s),
          $self->gatk_cmd, $GATK_BQSR_TOOL_NAME, $bqsr_args, $bqsr_table, $input_path, $temp_path, $region;
        my $gatk_cmd = sprintf q{%s %s %s -I $TMPDIR/%s -O %s -L %s},
          $self->gatk_cmd, $GATK_TOOL_NAME, $gatk_args, $temp_path, $output_path, $region;
        ##use critic

        $command = join ' && ', ($make_temp, $rm_cmd, $debug_cmd, $bqsr_cmd, $gatk_cmd);
      } else {
        $command = sprintf q{%s %s %s -I %s -O %s -L %s},
          $self->gatk_cmd, $GATK_TOOL_NAME, $gatk_args, $input_path, $output_path, $region;
      }

      $self->debug("Adding command '$command'");

      push @definitions,
        npg_pipeline::function::definition->new
          ('created_by'   => __PACKAGE__,
           'created_on'   => $self->timestamp(),
           'identifier'   => $self->label,
           'job_name'     => $job_name,
           'command'      => $command,
           'fs_slots_num' => $FS_NUM_SLOTS,
           'num_hosts'    => $NUM_HOSTS,
           'num_cpus'     => [$CPUS],
           'memory'       => $MEMORY,
           'composition'  => $product->composition());
    }
  }

  if (@definitions) {
    my @errors = npg_pipeline::runfolder_scaffold->make_dir(@out_dirs);
    @errors and $self->logcroak(join qq[\n], @errors);
  } else {
    push @definitions, npg_pipeline::function::definition->new
      ('created_by' => __PACKAGE__,
       'created_on' => $self->timestamp(),
       'identifier' => $self->label,
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

=item namespace::autoclean

=item Moose

=item MooseX::StrictConstructor

=item Readonly

=item npg_tracking::data::reference::find

=item npg_common::roles::software_location

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
