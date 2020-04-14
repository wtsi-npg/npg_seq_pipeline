use strict;
use warnings;

use Data::Compare;
use File::Temp qw(tempdir);
use JSON;
use Log::Log4perl qw[:levels];
use Test::HTTP::Server;
use Test::More tests => 20;
use Test::Exception;
use URI;

use_ok('npg_pipeline::product::heron::upload::library');
use_ok('npg_pipeline::product::heron::upload::run');
use_ok('npg_pipeline::product::heron::upload::metadata_client');

# my $logfile = join q[/], tempdir(CLEANUP => 1), 'logfile';
my $logfile = 'tests.log';
note "Log file: $logfile";
Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => $logfile,
                          utf8   => 1});

my $add_run_success_request = {
    library_name => 'test_library_name',
    token        => 'test_token',
    username     => 'test_username',
    runs         => [
        {
            run_name         => 'SANG-run1',
            instrument_make  => 'ILLUMINA',
            instrument_model => 'NovaSeq',
        },
        {
            run_name         => 'SANG-run2',
            instrument_make  => 'ILLUMINA',
            instrument_model => 'NovaSeq',
        },
        {
            run_name         => 'SANG-run3',
            instrument_make  => 'ILLUMINA',
            instrument_model => 'NovaSeq',
        },
    ]
};

my $add_library_success_response = {
    errors   => 0,
    warnings => 0,
    messages => [],
    new      => [
        ['object_type1', 'object_uuid1', 'object_id1'],
    ],
    updated  => [],
    ignored  => [],

    success  => 1
};

my $add_library_success_request = {
  biosamples => [
  {
      central_sample_id => 'sample1',
        library_selection => 'PCR',
        library_source    => 'VIRAL_RNA',
        library_strategy  => 'AMPLICON'
  },
  {
      central_sample_id => 'sample2',
      library_selection => 'PCR',
      library_source    => 'VIRAL_RNA',
      library_strategy  => 'AMPLICON'
  },
  {
      central_sample_id => 'sample3',
      library_selection => 'PCR',
      library_source    => 'VIRAL_RNA',
      library_strategy  => 'AMPLICON'
  }
  ],
  library_layout_config => 'PAIRED',
  library_name          => 'test_library_name',
  library_seq_kit       => 'NEB Ultra II',
  library_seq_protocol  => 'LIGATION',
  metadata => {
      artic => {
          primers  => '3',
      }
  },
  token    => 'test_token',
  username => 'test_username'
};

my $add_run_success_response = {
    errors   => 0,
    warnings => 0,
    messages => [],
    new      => [
        ['object_type1', 'object_uuid1', 'object_id1'],
        ['object_type1', 'object_uuid2', 'object_id2'],
        ['object_type1', 'object_uuid3', 'object_id3'],
    ],
    updated  => [],
    ignored  => [],

    success  => 1
};

my $fail_response = {
    errors   => 1,
    warnings => 0,
    messages => [],
    new      => [],
    updated  => [],
    ignored  => [],

    success  => 0};

my $FAILURE_RESPONSE    = 0;
my $ADD_LIBRARY_SUCCESS = 1;
my $ADD_RUN_SUCCESS     = 2;

my $response_type = $FAILURE_RESPONSE;
sub Test::HTTP::Server::Request::api {
  my ($request) = @_;
  my $body = decode_json($request->{body});

  # Example success responses
  if ($response_type == $ADD_LIBRARY_SUCCESS) {
    if (Compare($body, $add_library_success_request)) {
      return encode_json($add_library_success_response);
    }

    diag explain $body;

    return encode_json($fail_response);
  }
  elsif ($response_type == $ADD_RUN_SUCCESS) {
    if (Compare($body, $add_run_success_request)) {
      return encode_json($add_run_success_response);
    }

    diag explain $body;

    return encode_json($fail_response);
  }

  # Example of a failure response
  return encode_json($fail_response);
}

my $library_name             = 'test_library_name';
my $library_selection        = 'PCR';
my $library_source           = 'VIRAL_RNA';
my $library_strategy         = 'AMPLICON';
my $artic_protocol           = 'test_protocol';
my $artic_primers_version    = '3';
my $sequencing_kit           = 'NEB Ultra II';
my $sequencing_protocol      = 'LIGATION';
my $sequencing_layout_config = 'PAIRED';

my $make  = 'ILLUMINA';
my $model = 'NovaSeq';
my @instrument_args = (instrument_make  => $make,
                       instrument_model => $model);

# Library tests
my $libpkg = qw(npg_pipeline::product::heron::upload::library);
my %library_initargs = (
    name                     => $library_name,
    selection                => $library_selection,
    source                   => $library_source,
    strategy                 => $library_strategy,
    artic_primers_version    => $artic_primers_version,
    sequencing_kit           => $sequencing_kit,
    sequencing_protocol      => $sequencing_protocol,
    sequencing_layout_config => $sequencing_layout_config);

ok($libpkg->new(%library_initargs),  'can make a library');

foreach my $arg (keys %library_initargs) {
  next if $arg eq 'name';
  next if $arg eq 'artic_protocol';
  next if $arg eq 'artic_primers_version';

  my %initargs = %library_initargs;
  $initargs{$arg} = 'invalid_' . $initargs{$arg};

  dies_ok {
    $libpkg->new(%initargs);
  } "library will not accept an invalid $arg";
}

# Run tests
my $runpkg = qw(npg_pipeline::product::heron::upload::run);
foreach my $make (qw(ILLUMINA OXFORD_NANOPORE PACIFIC_BIOSCIENCES)) {
  ok($runpkg->new(name             => 'run1',
                  instrument_make  => $make,
                  instrument_model => 'SomeModel'), "$make make is OK");
}

dies_ok {
  $runpkg->new(name             => 'SANG-run1',
               instrument_make  => 'invalid_make',
               instrument_model => 'SomeModel');
} 'run will not accept an invalid make';


# Metadata sending tests
$response_type = $ADD_LIBRARY_SUCCESS;
my $server = Test::HTTP::Server->new;
my $server_uri = URI->new($server->uri);
my $client = npg_pipeline::product::heron::upload::metadata_client->new
    (username  => 'test_username',
     token     => 'test_token',
     api_uri   => $server_uri);

my $library = $libpkg->new(%library_initargs);

# We can send library requests and accept a success response
my @sample_ids = qw(sample1 sample2 sample3);

my $lib_response = $client->send_library_metadata($library, @sample_ids);
ok($lib_response, 'Send library response successful');
is_deeply($lib_response, $add_library_success_response) or
    diag explain $lib_response;

# We can send run requests and accept a success response
$response_type = $ADD_RUN_SUCCESS;
undef $server;
$server = Test::HTTP::Server->new;

is($client->api_uri, $server_uri, 'has expected URI') or diag explain
    "Expected $server_uri, but got " . $client->api_uri;

# We can send run requests and accept a success response
my @runs = ($runpkg->new(name => 'SANG-run1', @instrument_args),
            $runpkg->new(name => 'SANG-run2', @instrument_args),
            $runpkg->new(name => 'SANG-run3', @instrument_args));

my $run_response = $client->send_run_metadata($library, @runs);
ok($run_response, 'Send run response successful');
is_deeply($run_response, $add_run_success_response) or
    diag explain $run_response;

# We error on a failure response
$response_type = $FAILURE_RESPONSE;
undef $server;
$server = Test::HTTP::Server->new;

dies_ok {
  my $response = $client->send_run_metadata($library, @runs);
} 'Sending metadata dies on error response';

