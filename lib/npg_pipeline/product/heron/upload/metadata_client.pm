package npg_pipeline::product::heron::upload::metadata_client;

use strict;
use warnings;

use Data::Dump qw(pp);
use JSON;
use LWP::UserAgent;
use Moose;
use MooseX::StrictConstructor;
use URI;

with qw[
    WTSI::DNAP::Utilities::Loggable
    WTSI::DNAP::Utilities::JSONCodec
];

our $VERSION = '0';

our $JSON_CONTENT_TYPE = 'application/json';

has 'username' =>
    (isa           => 'Str',
     is            => 'ro',
     required      => 1,
     documentation => 'The registered user name for the COG-Uk API account',);

has 'token' =>
    (isa           => 'Str',
     is            => 'ro',
     required      => 1,
     documentation => 'The registered access token for the COG-UK API account',);

has 'api_uri' =>
  (isa           => 'URI',
   is            => 'ro',
   required      => 1,
   default       => sub { return URI->new('https://localhost') },
   documentation => 'COG-UK API endpoint URI to receive sequencing metadata',);

=head2 send_library_metadata

  Arg[1]     : Library, npg_pipeline::product::heron::upload::library.
  Arg[2]     : Sample ID, ArrayRef[Str]. These must be the identifiers supplied by the
               originator of the sample material.
  Arg[3]     : Force biosample creation, Bool. Force the creation of new biosamples if the
               samples in this library are not already present. Optional, defaults to false.

  Example    : my $response_content = $client->send_library_metadata($library_name, \@ids)
  Description: Return the (decoded) response data on successful upload of metadata
               to the COG-UK API endpoint, or raise an error if some or all of the
               metadata were rejected.
  Returntype : HashRef.

=cut

sub send_library_metadata {
   my ($self, $library, $sample_ids, $force_biosamples) = @_;

  my $document = $self->_make_library_metadata($library, $sample_ids,
                                               $force_biosamples);

  # Add credentials
  $document->{username} = $self->username;
  $document->{token}    = $self->token;

  my $req_content = $self->encode($document);

  my $ua = LWP::UserAgent->new;
  $ua->env_proxy();
  $ua->default_header('Content-Type' => $JSON_CONTENT_TYPE);

  my $uri = $self->_lib_add_uri;
  $self->debug(sprintf q(POST to '%s', content %s ),
                       $uri, $req_content);

  my $response = $ua->post($uri, Content => $req_content);

  return $self->_handle_response($response);
}

=head2 send_run_metadata

  Arg[1]     : Library, npg_pipeline::product::heron::upload::library.
  Arg[n]     : Runs, ArrayRef[npg_pipeline::product::heron::upload::run].

  Example    : my $response_content = $client->send_run_metadata($library_name, \@runs)
  Description: Return the (decoded) response data on successful upload of metadata
               to the COG-UK API endpoint, or raise an error if some or all of the
               metadata were rejected.
  Returntype : HashRef.

=cut

sub send_run_metadata {
  my ($self, $library, $runs) = @_;

  my $document = $self->_make_run_metadata($library, $runs);

  # Add credentials
  $document->{username} = $self->username;
  $document->{token}    = $self->token;

  my $req_content = $self->encode($document);

  my $ua = LWP::UserAgent->new;
  $ua->env_proxy();
  $ua->default_header('Content-Type' => $JSON_CONTENT_TYPE);

  my $uri = $self->_seq_add_uri;
  $self->debug(sprintf q(POST to '%s', content %s ),
                       $uri, $req_content);

  my $response = $ua->post($uri, Content => $req_content);

  return $self->_handle_response($response);
}

sub _handle_response {
  my ($self, $response) = @_;

  my $uri = $response->request->uri;
  $self->debug(sprintf q(POST to %s returned code %d, %s), $uri,
                       $response->code, $response->message);

  my $content;
  if ($response->is_success) {
    my $res_json = $response->content;
    $self->debug('Response JSON ', $res_json);
    $content = $self->decode($res_json);
  }
  else {
    $self->logcroak(sprintf q(Failed to get results from URI '%s': %s),
                            $uri, $response->message);
  }

  foreach my $add (@{$content->{new}}) {
    $self->info('Added ', pp($add));
  }
  foreach my $update (@{$content->{updated}}) {
    $self->info('Updated ', pp($update));
  }
  foreach my $ignore (@{$content->{ignored}}) {
    $self->info('Ignored ', $ignore);
  }

  my $summary = sprintf q(POST to %s failed; %d errors, %d warnings, ) .
                            q(%d updated, %d ignored),
                        $uri, $content->{errors}, $content->{warnings},
                        scalar @{$content->{updated}},
                        scalar @{$content->{ignored}};

  if ($content->{success}) {
    foreach my $message (@{$content->{messages}}) {
      $self->info('Endpoint message: ', pp($message));
    }
    $self->info($summary);
  }
  else {
    foreach my $message (@{$content->{messages}}) {
      $self->error('Endpoint message: ', pp($message));
    }
    $self->logcroak($summary);
  }

  return $content;
}

sub _lib_add_uri {
  my ($self) = @_;

  my $uri = $self->api_uri->clone;
  $uri->path('api/v2/artifact/library/add/');

  return $uri;
}

sub _seq_add_uri {
  my ($self) = @_;

  my $uri = $self->api_uri->clone;
  $uri->path('api/v2/process/sequencing/add/');

  return $uri;
}

sub _make_run_metadata {
  my ($self, $library, $runs) = @_;

  $library or $self->logconfess('No library was supplied');

  my @run_metadata;
  foreach my $run (@{$runs}) {
    push @run_metadata, {run_name         => $run->name,
                         instrument_make  => $run->instrument_make,
                         instrument_model => $run->instrument_model};
  }

  return {library_name => $library->name,
          runs         => \@run_metadata};
}

sub _make_library_metadata {
  my ($self, $library, $sample_ids, $force_biosamples) = @_;

  $library or $self->logconfess('No library was supplied');

  my @sample_metadata;
  foreach my $sample_id (@{$sample_ids}) {
    push @sample_metadata, {central_sample_id => $sample_id,
                            library_selection => $library->selection,
                            library_source    => $library->source,
                            library_strategy  => $library->strategy};
  }

  my $metadata = {
      library_name          => $library->name,
      library_seq_kit       => $library->sequencing_kit,
      library_seq_protocol  => $library->sequencing_protocol,
      library_layout_config => $library->sequencing_layout_config,
      metadata              => {
          artic => {
              primers => $library->artic_primers_version,
          },
      },
      biosamples            => \@sample_metadata,
      force_biosamples      => $force_biosamples,
  };

  if ($library->has_artic_protocol) {
    $metadata->{metadata}->{artic}->{protocol} = $library->artic_protocol;
  }

  return $metadata;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

npg_pipeline::product::heron::upload::metadata_client

=head1 VERSION

=head1 SYNOPSIS

=head1 DESCRIPTION

Client capable of uploading sequencing run metadata to the COG-UK API
endpoint described at https://docs.covid19.climb.ac.uk/metadata.

=head1 SUBROUTINES/METHODS

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2020, Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
