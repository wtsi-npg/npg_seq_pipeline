package npg_pipeline::product::release;

use namespace::autoclean;

use Data::Dump qw[pp];
use Moose::Role;

use npg_qc::Schema;
use npg_qc::mqc::outcomes;

with qw{WTSI::DNAP::Utilities::Loggable
        npg_pipeline::base::config};

our $VERSION = '0';

Readonly::Scalar my $RELEASE_CONFIG_FILE => 'product_release.yml';

has 'qc_schema' =>
  (isa        => 'npg_qc::Schema',
   is         => 'ro',
   required   => 1,
   builder    => '_build_qc_schema',
   lazy       => 1,);

has 'release_config' =>
  (isa        => 'HashRef',
   is         => 'rw',
   required   => 1,
   builder    => '_build_release_config',
   lazy       => 1,);

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
  my $outcomes = npg_qc::mqc::outcomes->new(qc_schema => $self->qc_schema);
  if ($outcomes->get_library_outcome($rpt)) {
    $self->info("Product $name, $rpt is for release (passed manual QC)");
    return 1;
  }

  $self->info("Product $name, $rpt is NOT for release (failed manual QC)");

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

  my $rpt          = $product->rpt_list();
  my $name         = $product->file_name_root();
  my $study_config = $self->_find_study_config($product);

  my $customer_name;

  if ($study_config) {
    $customer_name = $study_config->{s3}->{customer_name};
    $customer_name or
      $self->logconfess(sprintf q{Missing customer name in } .
                                q{configuration file: %s for study %s'},
                        $self->conf_file_path($RELEASE_CONFIG_FILE),
                        $study_config->{study_id});

    if (ref $customer_name) {
      $self->logconfess('Invalid customer name in configuration file: ',
                        pp($customer_name));
    }
  }

  return $customer_name;
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
  my $study_config = $self->_find_study_config($product);
  return ($study_config and $study_config->{$type_of_release}->{enable});
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

  my $rpt          = $product->rpt_list();
  my $name         = $product->file_name_root();
  my $study_config = $self->_find_study_config($product);

  my $url;

  if ($study_config) {
    $url = $study_config->{s3}->{url};
    if (ref $url) {
      $self->logconfess('Invalid S3 URL in configuration file: ', pp($url));
    }
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

  my $rpt          = $product->rpt_list();
  my $name         = $product->file_name_root();
  my $study_config = $self->_find_study_config($product);

  my $profile;

  if ($study_config) {
    $profile = $study_config->{s3}->{profile};
    if (ref $profile) {
      $self->logconfess('Invalid S3 profile in configuration file: ',
                        pp($profile));
    }
  }

  return $profile;
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
  my $study_config = $self->_find_study_config($product);

  if ($study_config and $study_config->{s3}->{notify}) {
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

sub _build_release_config {
  my ($self) = @_;

  my $file = $self->conf_file_path($RELEASE_CONFIG_FILE);
  $self->info("Reading product release configuration from '$file'");
  my $config = $self->read_config($file);
  $self->debug('Loaded product release configuration: ', pp($config));

  return $config;
}

sub _find_study_config {
  my ($self, $product) = @_;

  my $rpt      = $product->rpt_list();
  my $name     = $product->file_name_root();
  my $study_id = $product->lims->study_id();

  $study_id or
    $self->logconfess("Failed to get a study_id for product $name, $rpt");

  my ($study_config) = grep { $_->{study_id} eq $study_id }
    @{$self->release_config->{study}};

  if (not defined $study_config) {
    my $default_config = $self->release_config->{default};
    if (not defined $default_config) {
      $self->logcroak(sprintf q{No release configuration was defined } .
                              q{for study %s and no default was defined in %s},
                      $study_id, $self->conf_file_path($RELEASE_CONFIG_FILE));
    }

    $self->info(sprintf q{Using the default release configuration for } .
                        q{study %s defined in %s},
                $study_id, $self->conf_file_path($RELEASE_CONFIG_FILE));
    $study_config = $default_config;
  }

  return $study_config;
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
    notify: <boolean> A notificastion message will be sent if true.

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



=head1 SUBROUTINES/METHODS

=head1 BUGS AND LIMITATIONS

=head1 INCOMPATIBILITIES

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Data::Dump

=item Moose::Role

=item Readonly

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
