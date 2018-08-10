package npg_pipeline::function::product_delivery_notifier;

use namespace::autoclean;

use Data::Dump qw{pp};
use English qw{-no_match_vars};
use File::Path qw{make_path};
use File::Spec::Functions;
use JSON;
use Moose;
use Readonly;
use Try::Tiny;
use UUID qw{uuid};

use npg_pipeline::function::definition;

extends 'npg_pipeline::base';

with 'npg_pipeline::base::config';

Readonly::Scalar my $EVENT_TYPE            => 'npg.events.sample.completed';
Readonly::Scalar my $EVENT_ROLE_TYPE       => 'sample';
Readonly::Scalar my $EVENT_SUBJECT_TYPE    => 'data';
Readonly::Scalar my $EVENT_LIMS_ID         => 'npg';
Readonly::Scalar my $EVENT_USER_ID         => 'npg_pipeline';
Readonly::Scalar my $EVENT_MESSAGE_DIRNAME => 'messages';

Readonly::Scalar my $SEND_MESSAGE_SCRIPT   => 'npg_pipeline_notify_delivery';
Readonly::Scalar my $SEND_MESSAGE_CONFIG   => 'notify_delivery.json';
Readonly::Scalar my $CONFIG_ITEMS_KEY      => 'notify_delivery';
Readonly::Scalar my $CONFIG_STUDY_KEY      => 'study_id';

Readonly::Scalar my $MD5SUM_LENGTH         => 32;


our $VERSION = '0';

has 'customer_name' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The name of the customer (for message metadata)',);

has 'message_host' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The FQDN of the messaging host',);

has 'message_port' =>
  (isa           => 'Int',
   is            => 'ro',
   required      => 1,
   documentation => 'The port number of the messaging service',);

has 'message_vhost' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The virtual host of the messaging service, without ' .
                    'any leading slash i.e. "production", not "/production"',);

has 'message_exchange' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The name of the messaging exchange to contact',);

has 'message_routing_key' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   default       => q{},
   documentation => 'The messaging routing key, or an empty string',);

has 'message_dir' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   builder       => '_build_message_dir',
   lazy          => 1,
   documentation => 'The directory where message JSON files will be saved',);


=head2 make_message

  Arg [1]    : Data product which the message will describe,
               npg_pipeline::product.

  Example    : my $msg = $obj->make_message($product)
  Description: Create a product delivery message as a Perl data structure.
               Encoding the returned value as JSON will yield a string
               suitable to be send as a message body.
  Returntype : HashRef

=cut

sub make_message {
  my ($self, $product) = @_;

  my $dir_path       = catdir($self->archive_path(), $product->dir_path());
  my $cram_file      = $product->file_path($dir_path, ext => 'cram');
  my $cram_md5_file  = $product->file_path($dir_path, ext => 'cram.md5');
  my $cram_md5       = $self->_read_md5_file($cram_md5_file);

  my $lims             = $product->lims();
  my $flowcell_barcode = $lims->flowcell_barcode();

  my @names          = $lims->sample_names();
  my @ids            = $lims->sample_ids();
  my @supplier_names = $lims->sample_supplier_names();

  $self->_check_count("$cram_file sample_name",          1, @names);
  $self->_check_count("$cram_file sample_id",            1, @ids);
  $self->_check_count("$cram_file sample_supplier_name", 1, @supplier_names);

  my $message_uuid = uuid();
  my $subject_uuid = uuid(); # This is a placeholder until we can
                             # obtain the SScape UUID of the sample

  my $message_body =
    {
     'lims'  => $EVENT_LIMS_ID,
     'event' =>
     {'event_type'      => $EVENT_TYPE,
      'uuid'            => $message_uuid,
      'occurred_at'     => undef,
      'user_identifier' => $EVENT_USER_ID,
      'subjects'        =>[{
                            'role_type'     => $EVENT_ROLE_TYPE,
                            'subject_type'  => $EVENT_SUBJECT_TYPE,
                            'friendly_name' => $supplier_names[0],
                            'uuid'          => $subject_uuid,
                           }],
      'metadata' =>
      {
       'customer_name'        => $self->customer_name,
       'sample_name'          => $names[0],
       'sample_supplier_name' => $supplier_names[0],
       'flowcell_barcode'     => $flowcell_barcode,
       'file_path'            => $cram_file,
       'file_md5'             => $cram_md5,
       'rpt_list'             => $product->rpt_list,
      }
     }
    };

  return $message_body;
}

=head2 create

  Arg [1]    : None

  Example    : my $defs = $obj->create
  Description: Create per-product function definitions.

  Returntype : ArrayRef[npg_pipeline::function::definition]

=cut

sub create {
  my ($self) = @_;

  my $id_run = $self->id_run();

  if (! -d $self->message_dir) {
    make_path($self->message_dir);
  }

  my $studies_config = $self->_read_config();

  my @definitions;

  my $i = 0;
  foreach my $product (@{$self->products->{data_products}}) {
    if ($product->is_tag_zero_product) {
      $self->info('Skipping delivery notification for tag zero product ',
                  $product->file_name_root);
      next;
    }
    if ($product->lims->is_control) {
      $self->info('Skipping delivery notification for control product ',
                  $product->file_name_root);
      next;
    }

    my $study_id = $product->lims->study_id;
    if (exists $studies_config->{$study_id}) {
      $self->info(sprintf q{Sending delivery notification for %s in study %s},
                  $product->file_name_root, $study_id);
    } else {
      $self->info(sprintf q{Skipping delivery notification for %s in study %s},
                  $product->file_name_root, $study_id);
      next;
    }

    my $msg_file = $product->file_path($self->message_dir, ext => 'msg.json');
    $self->_write_message_file($msg_file, $self->make_message($product));

    my $job_name = sprintf q{%s_%d_%d}, $SEND_MESSAGE_SCRIPT, $id_run, $i;
    my $command =
      sprintf q{%s --host %s --port %d --vhost %s --exchange %s},
      $SEND_MESSAGE_SCRIPT, $self->message_host(), $self->message_port(),
      $self->message_vhost(), $self->message_exchange();

    if ($self->message_routing_key()) {
      $command .= sprintf q{ --routing-key %s}, $self->message_routing_key;
    }

    $command .= sprintf q{ %s}, $msg_file;

    push @definitions,
      npg_pipeline::function::definition->new
        ('created_by' => __PACKAGE__,
         'created_on' => $self->timestamp(),
         'identifier' => $id_run,
         'job_name'   => $job_name,
         'command'    => $command);
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

sub _build_message_dir {
  my ($self) = @_;

  return catdir($self->runfolder_path, $EVENT_MESSAGE_DIRNAME);
}

sub _read_config {
  my ($self) = @_;

  my $config = {};
  try {
    $config = $self->read_config($self->conf_file_path($SEND_MESSAGE_CONFIG));
  } catch {
    $self->error('Failed to load messaging configuration: ', $_);
  };

  my $studies_config;
  if (exists $config->{$CONFIG_ITEMS_KEY}) {
    my $items = $config->{$CONFIG_ITEMS_KEY};
    if (not ref $items eq 'ARRAY') {
      $self->error('Failed to load messaging configuration: ', pp($items));
    } else {
      foreach my $item (@{$items}) {
        $studies_config->{$item->{$CONFIG_STUDY_KEY}} = 1;
      }
    }
  }

  return $studies_config;
}

# Raise an error unless there are num_expected values.
sub _check_count {
  my ($self, $name, $num_expected, @values) = @_;

  my $num_values = scalar @values;
  if ($num_values != $num_expected) {
    $self->logcroak("Expected $num_expected '$name' ",
                    "but found $num_values: ", pp(\@values));
  }

  $self->debug("Found $num_expected '$name' as expected: ", pp(\@values));

  return;
}

sub _read_md5_file {
  my ($self, $md5_file) = @_;

  my $md5;
  open my $fh, '<', $md5_file or
    $self->logcroak("Failed to open '$md5_file' for reading: $ERRNO");
  $md5 = <$fh>;
  if ($md5) {
    chomp $md5;
  }
  close $fh or $self->logcroak("Failed to close '$md5_file': $ERRNO");

  if (not defined $md5 or length $md5 != $MD5SUM_LENGTH) {
    $self->logcroak("Corrupt MD5 '$md5' found in '$md5_file'");
  }

  return $md5;
}

sub _write_message_file {
  my ($self, $file_path, $message) = @_;

  $self->debug('Writing ', pp($message), " to '$file_path'");
  open my $fh, '>', $file_path or
    $self->logcroak("Failed to open '$file_path' for writing: $ERRNO");

  print {$fh} encode_json($message), "\n" or
    $self->logcroak("Failed to write to '$file_path': $ERRNO");

  close $fh or $self->logcroak("Failed to close '$file_path': $ERRNO");

  return;
}

__PACKAGE__->meta->make_immutable;

1;


__END__

=head1 NAME

npg_pipeline::function::product_delivery_notifier

=head1 SYNOPSIS

  my $obj = npg_pipeline::function::product_delivery_notifier
    (customer_name    => $customer,
     message_host     => $msg_host,
     message_port     => $msg_port,
     message_vhost    => $msg_vhost,
     message_exchange => $msg_exchange,
     runfolder_path   => $runfolder_path);

=head1 DESCRIPTION

Notifies a message queue that a set of data products from a sequencing
run have been delivered to a customer.

Notification is configured per-study using the configuration file
notify_delivery.json which must contain an entry for the study_id of
any study whose data products are to be included in notification:

{
  "notify_delivery": [
    { "study_id": "5290" }
  ]
}

An empty array here will result skipping notification for all studies.


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

=item UUID

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
