package npg_pipeline::product::release;

use namespace::autoclean;

use Data::Dump qw[pp];
use Moose::Role;
use File::Spec::Functions qw{catdir};
use List::Util qw{all};
use npg_qc::Schema;

with qw{WTSI::DNAP::Utilities::Loggable
        npg_tracking::util::pipeline_config};

our $VERSION = '0';

=head1 SUBROUTINES/METHODS

=head2 qc_schema

Lazy-build attribute. The builder method in this role returns a
DBIx database connection object. The attribute is allowed to be
undefined in order to prevent, if necessary, the automatic connection
to a database in consuming classes, which can be achieved by
supplying a custom builder method.

=cut

has 'qc_schema' =>
  (isa        => 'Maybe[npg_qc::Schema]',
   is         => 'ro',
   required   => 1,
   builder    => '_build_qc_schema',
   lazy       => 1,);

=head2 expected_files

  Arg [1]    : Data product whose files to list, npg_pipeline::product.

  Example    : my @files = $obj->expected_files($product)
  Description: Return a sorted list of the files expected to be present for
               archiving in the runfolder.

  Returntype : Array

=cut

sub expected_files {
  my ($self, $product) = @_;

  $product or $self->logconfess('A product argument is required');

  my @expected_files;

  my $dir_path = $product->existing_path($self->archive_path());
  my @extensions = qw{cram cram.md5 cram.crai
                      seqchksum sha512primesums512.seqchksum
                      bcfstats};
  push @expected_files,
    map { $product->file_path($dir_path, ext => $_) } @extensions;

  my @suffixes = qw{F0x900 F0xB00 F0xF04_target F0xF04_target_autosome};
  push @expected_files,
    map { $product->file_path($dir_path, suffix => $_, ext => 'stats') }
    @suffixes;

  my $qc_path = $product->existing_qc_out_path($self->archive_path());

  my @qc_extensions = qw{verify_bam_id.json};
  push @expected_files,
    map { $product->file_path($qc_path, ext => $_) } @qc_extensions;

  @expected_files = sort @expected_files;

  return @expected_files;
}

=head2 is_release_data

  Arg [1]    : npg_pipeline::product

  Example    : $obj->is_release_data($product)
  Description: Return true if the product is data for release i.e.

                - is not control data
                - is not data from tag zero that could not be deplexed

  Returntype : Bool

=cut

sub is_release_data {
  my ($self, $product) = @_;

  $product or $self->logconfess('A product argument is required');

  my $rpt = $product->rpt_list();
  my $name = $product->file_name_root();
  if ($product->is_tag_zero_product) {
    $self->info("Product $name, $rpt is NOT for release (is tag zero)");
    return 0;
  }

  if ($product->lims->is_control) {
    $self->info("Product $name, $rpt is NOT for release (is control)");
    return 0;
  }

  $self->info("Product $name, $rpt is for release ",
              '(is not tag zero or control)');

  return 1;
}

=head2 has_qc_for_release

  Arg [1]    : npg_pipeline::product

  Example    : $obj->has_qc_for_release($product)
  Description: Return true if the product has passed all QC necessary
               to be released.

  Returntype : Bool

=cut

sub has_qc_for_release {
  my ($self, $product) = @_;

  $product or $self->logconfess('A product argument is required');

  my $rpt  = $product->rpt_list();
  my $name = $product->file_name_root();

  my @seqqc = $product->final_seqqc_objs($self->qc_schema);
  @seqqc or $self->logcroak("Product $name, $rpt are not all Final seq QC values");

  if(not all { $_->is_accepted }  @seqqc) {
    $self->info("Product $name, $rpt are not all Final Accepted seq QC values");
    return 0;
  }
  my $libqc_obj = $product->final_libqc_obj($self->qc_schema);
  # Lib outcomes are not available for full lane libraries, so the code below
  # might give an error when absence of QC outcome is legitimate.
  $libqc_obj or $self->logcroak("Product $name, $rpt is not Final lib QC value");
  if ($libqc_obj->is_accepted) {
    $self->info("Product $name, $rpt is for release (passed manual QC)");
    return 1;
  }

  $self->info("Product $name, $rpt is NOT for release (did not pass manual QC)");

  return 0;
}

=head2 customer_name

  Arg [1]    : npg_pipeline::product

  Example    : $obj->customer_name($product)
  Description: Return a name for the customer to whom data are being
               released.

  Returntype : Str

=cut

sub customer_name {
  my ($self, $product) = @_;

  my $customer_name = $self->find_study_config($product)->{s3}->{customer_name};
  $customer_name or
    $self->logcroak(
      q{Missing s3 archival customer name in configuration file for product } .
      $product->composition->freeze());

  if (ref $customer_name) {
    $self->logconfess('Invalid customer name in configuration file: ',
                      pp($customer_name));
  }

  return $customer_name;
}

=head2 receipts_location

  Arg [1]    : npg_pipeline::product

  Example    : $obj->receipts_location($product);
  Description: Return location of the receipts for S3 submission,
               the value might be undefined.

  Returntype : Str

=cut

sub receipts_location {
  my ($self, $product) = @_;
  return $self->find_study_config($product)->{s3}->{receipts};
}

=head2 is_for_release

  Arg [1]    : npg_pipeline::product
  Arg [2]    : Str, type of release

  Example    : $obj->is_for_release($product, 'irods');
               $obj->is_for_release($product, 's3');
  Description: Return true if the product is to be released via the
               mechanism defined by the second argument.

  Returntype : Bool

=cut

sub is_for_release {
  my ($self, $product, $type_of_release) = @_;
  return $self->find_study_config($product)->{$type_of_release}->{enable};
}

=head2 is_for_s3_release

  Arg [1]    : npg_pipeline::product

  Example    : $obj->is_for_s3_release($product)
  Description: Return true if the product is to be released via S3.
               Raise an error if no S3 URL has been configured.

  Returntype : Bool

=cut

sub is_for_s3_release {
  my ($self, $product) = @_;

  $product or $self->logconfess('A product argument is required');

  my $name        = $product->file_name_root();
  my $description = $product->composition->freeze();

  my $enable = $self->is_for_release($product, 's3');

  if ($enable and not $self->s3_url($product)) {
    $self->logconfess("Configuration error for product $name, $description: " ,
                      'S3 release is enabled but no URL was provided');
  }

  $self->info(sprintf 'Product %s, %s is %sfor S3 release',
                      $name, $description, $enable ? q[] : q[NOT ]);

  return $enable;
}

=head2 s3_url

  Arg [1]    : npg_pipeline::product

  Example    : $obj->s3_url($product)
  Description: Return an S3 URL for release of the product or undef
               if there is no URL.

  Returntype : Str

=cut

sub s3_url {
  my ($self, $product) = @_;

  my $url = $self->find_study_config($product)->{s3}->{url};
  if (ref $url) {
    $self->logconfess('Invalid S3 URL in configuration file: ', pp($url));
  }

  return $url;
}

=head2 s3_profile

  Arg [1]    : npg_pipeline::product

  Example    : $obj->s3_profile($product)
  Description: Return an S3 profile name for release of the product or
               undef if there is no profile. A profile is a named set of
               credentials used by some S3 client software.

  Returntype : Str

=cut

sub s3_profile {
  my ($self, $product) = @_;

  my $profile = $self->find_study_config($product)->{s3}->{profile};
  if (ref $profile) {
    $self->logconfess('Invalid S3 profile in configuration file: ',
                      pp($profile));
  }

  return $profile;
}

=head2 s3_date_binning

  Arg [1]    : npg_pipeline::product

  Example    : $obj->s3_date_binning($product)
  Description: Return true if a date of processing element is to be added
               as the root of the object prefix the S3 bucket. e.g.

               ./2019-01-31/...

  Returntype : Bool

=cut

sub s3_date_binning {
  my ($self, $product) = @_;

  return $self->find_study_config($product)->{s3}->{date_binning};
}

=head2 is_s3_releasable

  Arg [1]    : npg_pipeline::product

  Example    : $obj->is_for_s3_release($product)
  Description: Return true if the product is to be released via S3
               and has QC outcome compatible with being released.

  Returntype : Bool

=cut

sub is_s3_releasable {
  my ($self, $product) = @_;

  return $self->is_release_data($product)   &&
         $self->is_for_s3_release($product) &&
         $self->has_qc_for_release($product);
}

=head2 is_for_s3_release_notification

  Arg [1]    : npg_pipeline::product

  Example    : $obj->is_for_s3_release_notification($product)
  Description: Return true if a notification is to be sent on release
               for the product.

  Returntype : Bool

=cut

sub is_for_s3_release_notification {
  my ($self, $product) = @_;

  my $rpt          = $product->rpt_list();
  my $name         = $product->file_name_root();

  if ($self->find_study_config($product)->{s3}->{notify}) {
    $self->info("Product $name, $rpt is for S3 release notification");
    return 1;
  }

  $self->info("Product $name, $rpt is NOT for S3 release notification");

  return 0;
}

sub _build_qc_schema {
  my ($self) = @_;

  return npg_qc::Schema->connect();
}

1;

__END__

=head1 NAME

npg_pipeline::product::release

=head1 SYNOPSIS

  foreach my $product (@products) {
    if ($self->is_release_data($product)    and
        $self->has_qc_for_release($product)) {
      $self->do_release($product);
    }
  }

=head1 DESCRIPTION

A role providing configuration and methods for decision-making during
product release.

The configuration file gives per-study settings and a default to be
used for any study without a specific configuration.

 S3:
    enable: <boolean> S3 release enabled if true.
    url:    <URL>     The S3 bucket URL to send to.
    notify: <boolean> A notificastion message will be sent if true.

 irods:
    enable: <boolean> iRODS release enabled if true.
    notify: <boolean> A notification message will be sent if true.

e.g.

---
default:
  s3:
    enable: false
    url: null
    notify: false
  irods:
    enable: true
    notify: false

study:
  - study_id: "5290"
    s3:
      enable: true
      url: "s3://product_bucket"
      notify: true
    irods:
      enable: false
      notify: false

  - study_id: "1000"
    s3:
      enable: false
      url: null
      notify: false
    irods:
      enable: true
      notify: false

=head1 BUGS AND LIMITATIONS

=head1 INCOMPATIBILITIES

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Data::Dump

=item Moose::Role

=item Readonly

=item WTSI::DNAP::Utilities::Loggable

=item npg_tracking::util::pipeline_config

=item npg_qc::Schema

=back

=head1 AUTHOR

Keith James

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
