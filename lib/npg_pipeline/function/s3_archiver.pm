package npg_pipeline::function::s3_archiver;

use namespace::autoclean;

use File::Basename;
use File::Slurp;
use MIME::Base64 qw( encode_base64 );
use Moose;
use MooseX::StrictConstructor;
use Readonly;

use npg_qc::Schema;

extends 'npg_pipeline::base_resource';

with qw{npg_pipeline::product::release};

# gsutil
Readonly::Scalar my $ARCHIVE_EXECUTABLE => 'gsutil';
# gsutil

our $VERSION = '0';

has '+qc_schema' => (
  lazy       => 1,
  builder    => '_build_qc_schema',
);
sub _build_qc_schema {
  return npg_qc::Schema->connect();
}

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

      push @commands, join q{ },
        $ARCHIVE_EXECUTABLE,
        $self->_base64_encoded_md5_gsutil_arg($file_path),
        @aws_args, $file_path, $file_url;
    }

    my $command = join q{ && }, reverse @commands;
    if ($env) {
      $command = "$env $command";
    }
    $self->debug("Adding command '$command'");

    push @definitions, $self->create_definition({
      job_name    => $job_name,
      command     => $command,
      composition => $product->composition()
    });
  }

  if (not @definitions) {
    push @definitions, $self->create_excluded_definition();
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

sub _base64_encoded_md5_gsutil_arg {
# if there is a corresponding .md5 file for the given path, use its contents to
# add an MD5 header for the data being uploaded.
  my ($self, $path) = @_;
  $path .= q(.md5);
  if (not -e $path){ return; }
  my ($md5) = read_file($path) =~ m/^(\S{32})(?!\S)/smx;
  if (not $md5) { $self->logcroak("Found md5 file for $path without valid md5 value"); }
  return q(-h Content-MD5:).(encode_base64((pack q(H*),$md5),q()))
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

=item npg_qc::Schema

=back

=head1 AUTHOR

Keith James

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2018,2019,2020 Genome Research Ltd.

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
