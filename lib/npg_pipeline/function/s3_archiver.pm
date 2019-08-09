package npg_pipeline::function::s3_archiver;

use namespace::autoclean;

use File::Basename;
use Moose;
use MooseX::StrictConstructor;
use Readonly;

use npg_pipeline::function::definition;

extends 'npg_pipeline::base';

with qw{npg_pipeline::product::release};

# gsutil
Readonly::Scalar my $ARCHIVE_EXECUTABLE => 'gsutil';
# gsutil

our $VERSION = '0';

=head2 create

  Arg [1]    : None

  Example    : my $defs = $obj->create
  Description: Create per-product data file function definitions.

  Returntype : ArrayRef[npg_pipeline::function::definition]

=cut

sub create {
  my ($self) = @_;

  my $job_name = join q[_], $ARCHIVE_EXECUTABLE, $self->label;

  my @products = $self->no_s3_archival ? () :
                 grep { $self->is_s3_releasable($_) }
                 @{$self->products->{data_products}};
  my @definitions = ();

  foreach my $product (@products) {

    # This is required for our initial customer, but we should arrange
    # an alternative for when supplier_name is not provided
    my $supplier_name = $product->lims->sample_supplier_name();
    $supplier_name or
      $self->logcroak(sprintf q{Missing supplier name for product %s, %s},
                      $product->file_name_root(), $product->rpt_list());

    my @file_paths = sort _cram_last $self->expected_files($product);
    $self->_check_files(@file_paths);

    # gsutil
    #
    # TODO: Add support for using the -h Content-MD5:<my file's MD5> argument
    # to do checksum validation on upload
    my @aws_args = qw{cp};

    my $base_url = $self->s3_url($product);
    $self->info("Using base S3 URL '$base_url'");

    my $url = $base_url;
    if ($self->s3_date_binning($product)) {
      my ($date, $time) = split /-/msx, $self->timestamp();
      $url = "$url/$date";
    }

    my $env = q{};
    my $profile = $self->s3_profile($product);
    if ($profile) {
      $self->info(q{Using S3 client profile 'boto-}, $profile, q{'});

      ## no critic (ValuesAndExpressions::RequireInterpolationOfMetachars)
      $env = 'export BOTO_CONFIG=$HOME/.gcp/boto-' . $profile . q{;};
      ## use critic
    }
    else {
      $self->info('Using the default S3 client profile');
    }

    my @commands;
    foreach my $file_path (@file_paths) {
      my $filename   = basename($file_path);
      my $file_url   = "$url/$supplier_name/$filename";

      push @commands, sprintf q{%s %s %s %s},
        $ARCHIVE_EXECUTABLE, join(q{ }, @aws_args), $file_path, $file_url;
    }

    my $command = join q{ && }, reverse @commands;
    if ($env) {
      $command = "$env $command";
    }
    $self->debug("Adding command '$command'");

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

## no critic (ValuesAndExpressions::ProhibitMagicNumbers)
sub _cram_last {
  return $a =~ /[.]cram$/msx ? 1 : -1;
}
## use critic

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

Keith James

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2018, 2019 Genome Research Ltd.

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
