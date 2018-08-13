package npg_pipeline::function::s3_archiver;

use namespace::autoclean;

use File::Basename;
use File::Spec::Functions qw{catdir catfile};
use Moose;
use Readonly;
use Try::Tiny;

use npg_pipeline::function::definition;

extends 'npg_pipeline::base';

with 'npg_pipeline::base::config';

Readonly::Scalar my $ARCHIVE_EXECUTABLE => 'aws';
Readonly::Scalar my $ARCHIVE_CONFIG     => 's3_archive.json';

Readonly::Scalar my $CONFIG_ITEMS_KEY   => 's3_archive';
Readonly::Scalar my $CONFIG_STUDY_KEY   => 'study_id';
Readonly::Scalar my $CONFIG_URL_KEY     => 'url';

our $VERSION = '0';


=head2 expected_files

  Arg [1]    : Data product whose files to list,npg_pipeline::product.

  Example    : my @files = $obj->expected_files($product)
  Description: Return a list of the files expected to to present for
               archiving in the runfolder.

  Returntype : Array

=cut

sub expected_files {
  my ($self, $product) = @_;

  $product or $self->logconfess('A product argument is required');

  my @expected_files;

  my $dir_path = catdir($self->archive_path(), $product->dir_path());
  my @extensions = qw{cram cram.md5 cram.crai
                      seqchksum sha512primesums512.seqchksum
                      bcfstats};
  push @expected_files,
    map { $product->file_path($dir_path, ext => $_) } @extensions;

  my @suffixes = qw{F0x900 F0xB00 F0xF04_target};
  push @expected_files,
    map { $product->file_path($dir_path, suffix => $_, ext => 'stats') }
    @suffixes;

  my $qc_path = $product->qc_out_path($self->archive_path());

  my @qc_extensions = qw{verify_bam_id.json};
  push @expected_files,
    map { $product->file_path($qc_path, ext => $_) } @qc_extensions;

  @expected_files = sort @expected_files;

  return @expected_files;
}

=head2 create

  Arg [1]    : None

  Example    : my $defs = $obj->create
  Description: Create per-product data file function definitions.

  Returntype : ArrayRef[npg_pipeline::function::definition]

=cut

sub create {
  my ($self) = @_;

  my $id_run         = $self->id_run();
  my $archive_config = $self->_read_config();

  my @definitions;

  my $i = 0;
  foreach my $product (@{$self->products->{data_products}}) {
    if ($product->is_tag_zero_product) {
      $self->info('Skipping archiving for tag zero product ',
                  $product->file_name_root);
      next;
    }
    if ($product->lims->is_control) {
      $self->info('Skipping archiving for control product ',
                  $product->file_name_root);
      next;
    }

    my $sample     = $product->lims->sample_supplier_name;
    $sample or $self->logcroak('Failed to get a supplier sample name',
                               'for product ', $product->file_name_root);

    my $study_id   = $product->lims->study_id;
    $study_id or $self->logcroak('Failed to get a study_id',
                                 'for product ', $product->file_name_root);

    if (exists $archive_config->{$study_id}) {
      $self->info(sprintf q{S3 archiving %s in study %s to bucket URL %s},
                  $product->file_name_root, $study_id,
                  $archive_config->{$study_id});
    } else {
      $self->info(sprintf q{Skipping S3 archiving %s in study %s},
                  $product->file_name_root, $study_id);
      next;
    }

    my $job_name = sprintf q{%s_%d_%d}, $ARCHIVE_EXECUTABLE, $id_run, $i;
    my $base_url = $archive_config->{$study_id};
    $self->debug(sprintf q{Using base URL '%s' for study %s},
                 $base_url, $study_id);

    my @file_paths = sort _cram_last $self->expected_files($product);
    $self->_check_files(@file_paths);;

    my @aws_args = qw{--cli-connect-timeout 300
                      --acl bucket-owner-full-control};
    my @commands;
    foreach my $file_path (@file_paths) {
      my $filename   = basename($file_path);
      my $file_url   = "$base_url/$sample/$filename";

      push @commands, sprintf q{%s s3 cp %s %s %s},
        $ARCHIVE_EXECUTABLE, join(q{ }, @aws_args), $file_path, $file_url;

      $self->info(sprintf q{S3 archiving %s in study %s to %s},
                  $file_path, $study_id, $file_url);
    }

    my $command = join q{ && }, reverse @commands;
    $self->debug("Adding command '$command'");

    push @definitions,
      npg_pipeline::function::definition->new
        ('created_by'  => __PACKAGE__,
         'created_on'  => $self->timestamp(),
         'identifier'  => $id_run,
         'job_name'    => $job_name,
         'command'     => $command,
         'composition' => $product->composition);
    $i++;
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
    $self->logcroak('Failed to send files to S3; the following files ',
                    'are missing: ', join q{ }, @missing);
  }

  return;
}

sub _read_config {
  my ($self) = @_;

  my $config = $self->read_config($self->conf_file_path($ARCHIVE_CONFIG));

  my $archive_config;
  if (exists $config->{$CONFIG_ITEMS_KEY}) {
    my $items = $config->{$CONFIG_ITEMS_KEY};
    if (not ref $items eq 'ARRAY') {
      $self->error('Failed to load archiving configuration: ', pp($items));
    } else {
      foreach my $item (@{$items}) {
        $archive_config->{$item->{$CONFIG_STUDY_KEY}} =
          $item->{$CONFIG_URL_KEY};
      }
    }
  }

  return $archive_config;
}

sub _cram_last {
   return $a =~ /[.]cram$/ ? 1 : -1;
}


__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

npg_pipeline::function::s3_archiver

=head1 SYNOPSIS

  my $obj = npg_pipeline::function::s3_archiver->new
    (runfolder_path => $runfolder_path);

=head1 DESCRIPTION

Uploads files for a data product to S3.

Upload is configured per-study using the configuration file
s3_archive.json which must contain an entry for the study_id of any
study whose data products are to be included in notification:

{
    "s3_archive": [
        {"study_id": "5290", "url": "s3://product_bucket"}
    ]
}

An empty array here will result skipping S3 upload for all studies.

The "url" value indicates the S3 bucket destination for the files. The
files will be copied to the bucket with a prefix of the supplier's
sample name. i.e.

<file> => s3://product_bucket/<sample name>/<file>


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

Keith James

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2018 Genome Research Ltd.

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
