package npg_pipeline::function::merge_recompress;

use namespace::autoclean;

use Moose;
use MooseX::StrictConstructor;
use Readonly;

use npg_pipeline::function::definition;

extends 'npg_pipeline::base';
with qw{ npg_pipeline::function::util
         npg_pipeline::product::release };
with 'npg_common::roles::software_location' => { tools => [qw/bcftools/] };

Readonly::Scalar my $FUNCTION_NAME => 'merge_recompress';

Readonly::Scalar my $BCFTOOLS_TOOL_NAME => 'concat';
Readonly::Scalar my $BCFTOOLS_INDEX_NAME => 'tabix';

Readonly::Scalar my $FS_NUM_SLOTS                 => 2;
Readonly::Scalar my $MEMORY                       => q{2000}; # memory in megabytes
Readonly::Scalar my $CPUS                         => 1;
Readonly::Scalar my $NUM_HOSTS                    => 1;


our $VERSION = '0';

=head2 bcftools_cmd

=head2 create

  Arg [1]    : None

  Example    : my $defs = $obj->create
  Description: Create per-product function definitions.

  Returntype : ArrayRef[npg_pipeline::function::definition]

=cut

sub create {
  my ($self) = @_;

  my $label = $self->label();
  my $job_name = sprintf q{%s_%d}, $FUNCTION_NAME, $label;

  # If haplotype_caller is disabled we should not run.
  my @products = $self->no_haplotype_caller ? () :
                 grep { $self->haplotype_caller_enable($_) }
                 grep { $self->is_release_data($_) }
                 @{$self->products->{data_products}};

  if ($self->no_haplotype_caller) {
    $self->debug('no_haplotype_caller set, skipping');
  }
  if (scalar @products == 0) {
    $self->debug('no haplotype_caller enabled data products, skipping');
  }
  my @definitions = ();


  foreach my $unchunked_product (@products) {
    my $dir_path = $unchunked_product->path($self->archive_path());
    my $in_dir_path = $unchunked_product->chunk_out_path($self->no_archive_path());

    my @chunk_products = map { $_->file_path($in_dir_path, ext => 'g.vcf.gz') }
      $unchunked_product->chunks_as_product($self->haplotype_caller_chunking_number($unchunked_product));
    my $input_path = join q{ }, @chunk_products;
    my $output_path = $unchunked_product->file_path($dir_path, ext => 'g.vcf.gz');
    my $command = sprintf q{%s %s -O z -o %s %s && %s %s -p vcf %s},
      $self->bcftools_cmd, $BCFTOOLS_TOOL_NAME, $output_path, $input_path, $self->bcftools_cmd, $BCFTOOLS_INDEX_NAME, $output_path;

    $self->debug("Adding command '$command'");

    push @definitions,
      npg_pipeline::function::definition->new
        ('created_by'   => __PACKAGE__,
         'created_on'   => $self->timestamp(),
         'identifier'   => $label,
         'job_name'     => $job_name,
         'command'      => $command,
         'fs_slots_num' => $FS_NUM_SLOTS,
         'num_hosts'    => $NUM_HOSTS,
         'num_cpus'     => [$CPUS],
         'memory'       => $MEMORY,
         'composition'  => $unchunked_product->composition());
  }

  if (not @definitions) {
    push @definitions, npg_pipeline::function::definition->new
      ('created_by' => __PACKAGE__,
       'created_on' => $self->timestamp(),
       'identifier' => $label,
       'excluded'   => 1);
  }

  return \@definitions;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

npg_pipeline::function::merge_recompress

=head1 SYNOPSIS

  my $obj = npg_pipeline::function::merge_recompress->new
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
