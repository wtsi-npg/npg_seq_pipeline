package npg_pipeline::product::heron::majora;

use autodie;
use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;
use DateTime;
use HTTP::Request;
use JSON::XS;
use LWP::UserAgent;
use Log::Log4perl qw(:easy);
use List::MoreUtils qw(any uniq);
use Carp;

use npg_tracking::Schema;
use WTSI::DNAP::Warehouse::Schema;

with qw{MooseX::Getopt};

our $VERSION = '0';

has '_npg_tracking_schema'    => (
    isa        => q{npg_tracking::Schema},
    is         => q{ro},
    required   => 1,
    lazy_build => 1,
);

has '_mlwh_schema'    => (
    isa        => q{WTSI::DNAP::Warehouse::Schema},
    is         => q{ro},
    required   => 1,
    lazy_build => 1,
);

has '_id_runs_are_given' => (
    isa     => q{Bool},
    is      => q{rw},
    default => 0,
);

has '_majora_update_runs' => (
    isa        => q{HashRef},
    is         => q{ro},
    required   => 0,
    lazy_build => 1,
);

has 'verbose'  => (
    isa     => q{Bool},
    is      => q{ro},
    default => 1,
);

has 'dry_run'  => (
    isa     => q{Bool},
    is      => q{ro},
    default => 0,
);

has 'days' => (
    isa       => q{Int},
    is        => q{ro},
    predicate => q{_has_days},
);

has 'update'  => (
    isa     => q{Bool},
    is      => q{ro},
    default => 0,
);

has 'id_runs' => (
    isa        => q{ArrayRef[Int]},
    is         => q{ro},
    lazy_build => 1,
    predicate  => '_has_id_runs',
);

has 'logger' => (
    metaclass  => q{NoGetopt},
    isa        => q{Log::Log4perl::Logger},
    is         => q{ro},
    lazy_build => 1,
);

has 'user_agent' => (
    metaclass  => q{NoGetopt},
    is         => q{ro},
    lazy_build => 1,
);

sub _build__npg_tracking_schema {
  return npg_tracking::Schema->connect();
}

sub _build__mlwh_schema {
  return WTSI::DNAP::Warehouse::Schema->connect();
}

sub _build__majora_update_runs {
  my $self = shift;

  my @majora_update_runs = ();
  if ($self->update) {
    if ($self->_id_runs_are_given) {
      @majora_update_runs = @{$self->id_runs};
    } else {
      if ($self->days) {
        $self->logger->debug(
          'Selecting id_runs missing data from the last ' .
          $self->days.' days for Majora update');
        @majora_update_runs = $self->get_id_runs_missing_data_in_last_days([undef]);
      } else {
        $self->logger->debug('Getting id_runs with missing data for Majora update');
        @majora_update_runs = $self->get_id_runs_missing_data([undef]);
      }
    }
  }
  my %h = map { $_ => 1 } @majora_update_runs;

  return \%h;
}

sub _build_id_runs {
  my $self = shift;

  my @id_runs = ();
  if (not $self->days) {
    $self->logger->info('Getting id_runs missing COG metadata');
    @id_runs = $self->get_id_runs_missing_data();
  } else {
    @id_runs = $self->get_id_runs_missing_data_in_last_days();
    $self->logger->debug( join(q{, }, @id_runs) .
    ' = id_runs after getting missing data from the last ' .
    $self->days . ' days');
  }

  return \@id_runs;
}

sub _build_user_agent {
  return LWP::UserAgent->new();
}

sub _build_logger {
  my $self=shift;
  Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n'});
  my $logger = Log::Log4perl->get_logger();
  $logger->level($self->verbose ? $DEBUG : $INFO);
  return $logger;
}

sub BUILD {
  my $self = shift;

  if ($self->update and $self->dry_run){
    $self->logger->error_die(
      q{'update' and 'dry_run' attributes cannot be set at the same time});
  }
  if (($self->_has_days) and $self->days <= 0){
    $self->logger->error_die(
      q{'days' attribute value should be a positive number});
  }
  if ($self->_has_id_runs and $self->days) {
    $self->logger->error_die(
      q{'id_runs' and 'days' attributes cannot be set at the same time});
  }
  if ($self->_has_id_runs and (@{$self->id_runs} == 0)) {
    $self->logger->error_die(
      q{'id_runs' attribute cannot be set to an empty array});
  }

  $self->_has_id_runs and $self->_id_runs_are_given(1);

  if ($self->dry_run) {
    $self->logger->info(q{DRY RUN});
  }

  return;
}

sub run {
  my $self = shift;

  for my $id_run (@{$self->id_runs}){
    if ($self->_majora_update_runs->{$id_run} and (not $self->dry_run)){
      $self->logger->info("Updating Majora for $id_run");
      $self->update_majora($id_run);
    }
    $self->logger->info("Fetching npg_tracking and Warehouse DB info for $id_run");
    my ($fn,$rs) = $self->get_table_info_for_id_run($id_run);

    $self->logger->info("Fetching Majora data for $fn");
    my $json_string = $self->get_majora_data($fn);

    $self->logger->debug('Converting the json returned from Majora to perl structure');
    my %ds = $self->json_to_structure($json_string,$fn);

    if (not $self->dry_run){
      $self->logger->info("Updating Metadata for $id_run");
      $self->update_metadata($rs,\%ds);
    }
  }
  return;
}

sub get_table_info_for_id_run {
  my ($self, $id_run) = @_;
  if (!defined $id_run) {$self->logger->error_die('need an id_run');};

  my$rs=$self->_npg_tracking_schema->resultset(q(Run));
  my$fn=$rs->find($id_run)->folder_name;

  my $rs_iseq=$self->_mlwh_schema->resultset('IseqProductMetric')->search({'id_run' => $id_run},
                                     {join=>{'iseq_flowcell' => 'sample'}});
  my @table_info = ($fn,$rs_iseq);
  return (@table_info);
}

sub get_majora_data {
  my ($self,$fn) = @_;
  my $url =q(/api/v2/process/sequencing/get/);
  my $data_to_encode = {run_name=>["$fn"]};
  my $res = $self->_use_majora_api('POST',$url,$data_to_encode);
  return($res);
}

sub json_to_structure {
  my ($self,$json_string, $fn) = @_;
  my $data = decode_json($json_string);
  if (@{$data->{ignored}} != 0) {
    $self->logger->error('response from Majora ignored a folder : ' . $json_string);
  }
  my %data_structure= ();
  if ($data) {
    my $libref = $data->{get}{$fn}{libraries}|| [];
    my @libarray = @{$libref};
    foreach my $lib (@libarray) {
      my $lib_name = $lib->{library_name};
      my $bioref = $lib->{biosamples};
      my @biosamples = @{$bioref};
      foreach my $sample (@biosamples) {
        my $central_sample_id=$sample->{central_sample_id};
        $data_structure{$lib_name}->{$central_sample_id}=$sample;
      }
    }
  }
  return(%data_structure);
}

sub update_metadata {
  my ($self,$rs_iseq,$ds_ref) = @_;
  my %data_structure = %{$ds_ref};
  while (my $row=$rs_iseq->next) {
    my $fc = $row->iseq_flowcell;
    $fc or next; #no LIMS data
    my $sn = $fc->sample->supplier_name;
    $sn or next;# no Heron/COG-UK relevant id
    my $hm = $row->iseq_heron_product_metric;
    $hm or next; #if missing iseq_heron_product_metric row
    my $libdata = $data_structure{$fc->id_pool_lims};
    my $sample_meta;
    if ($libdata) {
      my $sample_data = $libdata->{$sn};
      if ($sample_data) {
        $sample_meta = defined $sample_data->{submission_org} ?1:0;
        $self->logger->info("setting $sample_meta for $sn");
      }
    }
    $hm->update({cog_sample_meta=>$sample_meta});
  };
  return;
}

sub _get_id_runs_missing_cog_metadata_rs{
  my ($self,$meta_search) = @_;
  $meta_search //= [undef,0]; # missing run -> library -> biosample connection, or missing biosample metadata
  return $self->_mlwh_schema->resultset('IseqHeronProductMetric')->search(
    {
      'study.name'         => 'Heron Project',
      'me.cog_sample_meta' => $meta_search,
      'me.climb_upload'    => {-not=>undef} # only consider for data uploaded
    },
    {
      join => {'iseq_product_metric' => {'iseq_flowcell' => ['study']}},
      columns => 'iseq_product_metric.id_run',
      distinct => 1
    }
  );
}

sub get_id_runs_missing_data{
  my ($self,$meta_search) = @_;
  my @ids = map { $_->iseq_product_metric->id_run } $self->_get_id_runs_missing_cog_metadata_rs($meta_search)->all();
  return @ids;
}

sub get_id_runs_missing_data_in_last_days{
  my ($self, $meta_search) = @_;
  my $dt = DateTime->now();
  $dt->subtract(days => $self->days);
  $dt = $self->_mlwh_schema->storage->datetime_parser->format_datetime($dt);
  my $rs = $self->_get_id_runs_missing_cog_metadata_rs($meta_search)->search(
    {
      'me.climb_upload'    =>{ q(>) =>$dt }
    }
  );
  my @ids = map { $_->iseq_product_metric->id_run } $rs->all();
  return @ids;
}

sub update_majora{
  my ($self,$id_run)= @_;

  my $libtypes = {
    LIGATION => ['PCR amplicon ligated adapters',
                 'PCR amplicon ligated adapters 384',
                 'Sanger_artic_V3_96',
                 'Sanger_artic_V4_96'],
    TAILING =>  ['PCR with TruSeq tails amplicon',
                 'PCR amplicon tailed adapters 384',
                 'Sanger_tailed_artic_v1_384',
                 'PCR with TruSeq tails amplicon 384',
                 'Sanger_tailed_artic_v1_96' ]
  };

  if (!defined $id_run) {
    $self->logger->error('need an id_run')
  };

  my $runfolder_name =
    $self->_npg_tracking_schema->resultset(q(Run))->find($id_run)->folder_name;
  my $rs=$self->_mlwh_schema->resultset(q(IseqProductMetric))->search_rs(
    {'me.id_run' => $id_run, 'me.tag_index' => {q(>) => 0}},
    {join     => [{iseq_flowcell=>q(sample)}, q(iseq_heron_product_metric)],
     prefetch => [{iseq_flowcell=>q(sample)}, q(iseq_heron_product_metric)]});
  my $rsu=$self->_mlwh_schema->resultset(q(Sample))->search(
    {q(iseq_heron_product_metric.climb_upload)=>{q(-not)=>undef}},
    {join     => {iseq_flowcells=>{iseq_product_metrics=>q(iseq_heron_product_metric)}}});

  my %l2bs; my %l2pp; my %l2lsp; my %r2l;

  while (my $r = $rs->next) {
      my $ifc = $r->iseq_flowcell or next;
      my $sample_name = $ifc->sample->supplier_name;
      my $lib = $ifc->id_pool_lims;

      # lookup by library and sample name - skip if no climb_uploads.
      $rsu->search({q(me.supplier_name)=>$sample_name, q(iseq_flowcells.id_pool_lims)=>$lib})->count() or next;
      # i.e. do not use exising $r record as same library might upload different samples
      # in differnt runs - Majora library must contain both

      my $primer_panel = $r->iseq_flowcell->primer_panel;
      ($primer_panel) = $primer_panel =~ m{nCoV-2019/V(\d)\b}smx?$1:q("");

      my $lt = $r->iseq_flowcell->pipeline_id_lims;
      $lt ||= q();
      $lt = uc $lt;
      my $lib_seq_protocol = q();
      for my $type (keys %{$libtypes}) {
        if (any { $lt eq $_ } map { uc } @{$libtypes->{$type}}) {
          $lib_seq_protocol = $type;
          last;
        }
      }
      if (not $lib_seq_protocol) {
        $self->logger->error_die("Do not know how to deal with library type: '$lt'");
      }

      my $pp_name;
      my $pp_version;
      my $heron_row = $r->iseq_heron_product_metric();
      if ($heron_row) {
        $pp_name    = $heron_row->pp_name;
        $pp_version = $heron_row->pp_version;
      }
      $pp_name    ||= q();
      $pp_version ||= q();

      push @{$r2l{$runfolder_name}{$lib}}, {$pp_name => $pp_version};
      $l2bs{$lib}{$sample_name}++;
      $l2pp{$lib}{$primer_panel}++;
      $l2lsp{$lib}{$lib_seq_protocol}++;
  }

  my $url = q(api/v2/artifact/library/add/);

  foreach my $lb (sort keys %l2bs) {

    (1 == keys %{$l2pp{$lb}})  or $self->logger->error_die("multiple primer panels in $lb");
    (1 == keys %{$l2lsp{$lb}}) or $self->logger->error_die("multiple library seq protocol in $lb");
    my ($primer_panel)     = keys %{$l2pp{$lb}};
    my ($lib_seq_protocol) = keys %{$l2lsp{$lb}};

    my @biosample_info = ();
    foreach my $key (keys%{$l2bs{$lb}}){
      push @biosample_info, {central_sample_id=> $key,
                             library_selection=> 'PCR',
                             library_source   => 'VIRAL_RNA',
                             library_strategy => 'AMPLICON',
                             library_protocol => q{},
                             library_primers  => $primer_panel
                            };
    }

    my $data_to_encode = {
                           library_name          => $lb,
                           library_layout_config => 'PAIRED',
                           library_seq_kit       => 'NEB ULTRA II',
                           library_seq_protocol  => $lib_seq_protocol,
                           force_biosamples      => \1, # JSON encode as true, Sanger-only Majora interaction
                           biosamples            => \@biosample_info
                         };
   $self->logger->debug("Sending call to update Majora for library $lb");
   $self->_use_majora_api('POST', $url, $data_to_encode);
  }

  # adding sequencing run
  $url = q(api/v2/process/sequencing/add/);

  foreach my $runfolder_name (sort keys %r2l) {
    foreach my $lb ( sort keys %{$r2l{$runfolder_name}} ) {
      my $instrument_model = ($runfolder_name =~ m{_MS}smx ? q(MiSeq) : q(NovaSeq));
      my @pipelines_info = @{$r2l{$runfolder_name}{$lb}};
      my @pp_names = uniq map { keys %{$_} } @pipelines_info;
      my $pp_name = q();
      if (@pp_names == 1) {
        $pp_name = $pp_names[0];
        $pp_name or $self->logger->warn('No pp_name value retrieved');
      } else {
        my $m = 'Different values found for pp_name and pp_version. ' .
                'Passing empty value';
        $self->logger->warn($m);
        carp $m;
      }
      my $pp_version = q();
      if ($pp_name) {
        my @pp_versions = uniq map { values %{$_} } @pipelines_info;
        if (@pp_versions == 1) {
          $pp_version = $pp_versions[0];
          $pp_version or $self->logger->warn('No pp_version value retrieved');
        } else {
          my $m = 'Different values found for pp_version. Passing empty value';
          $self->logger->warn($m);
          carp $m;
        }
      }
      my $data_to_encode = {
                            library_name => $lb,
                            runs=> [{
                                     run_name             => $runfolder_name,
                                     instrument_make      => 'ILLUMINA',
                                     instrument_model     => $instrument_model,
                                     bioinfo_pipe_version => $pp_version,
                                     bioinfo_pipe_name    => $pp_name
                                   }]
                           };
      $self->logger->debug("Sending call to update Majora for library $lb");
      $self->_use_majora_api('POST', $url, $data_to_encode);
    }
  }

  return;
}

sub _use_majora_api{
  my ($self, $method, $url_end, $data_to_encode) = @_;

  $data_to_encode = {%{$data_to_encode}};
  if (!defined $ENV{MAJORA_DOMAIN}){
    $self->logger->error('MAJORA_DOMAIN environment variable not set');
  }
  my $url = $ENV{MAJORA_DOMAIN}.$url_end;
  my $header;
  if (my $token = $ENV{MAJORA_OAUTH_TOKEN}){
    $header = [q(Authorization) => qq(Bearer $token) ,q(Content-Type) => q(application/json; charset=UTF-8)];
    $data_to_encode->{token} = 'DUMMYTOKENSTRING';
  }else{
    $header = [q(Content-Type) => q(application/json; charset=UTF-8)];
    $data_to_encode->{token} = $ENV{MAJORA_TOKEN};
  }
  $data_to_encode->{username} = $ENV{MAJORA_USER};
  my $encoded_data = encode_json($data_to_encode);
  my $ua =  $self->user_agent;

  my $r = HTTP::Request->new($method, $url, $header, $encoded_data);
  my $res = $ua->request($r);

  if ($res->is_error){
    $self->logger->error_die(q(Majora API returned a ).($res->code).qq( code. Content:\n).($res->decoded_content()).qq(\n));
  }

  return $res->decoded_content;
}

__PACKAGE__->meta->make_immutable;

1;

=head1 NAME

npg_pipeline::product::heron::majora

=head1 SYNOPSIS

=head1 DESCRIPTION

Updates Majora data and/or id_run cog_sample_meta data.

=head1 SUBROUTINES/METHODS

=head2 BUILD

Builds attributes and dies when options are contradicting 
e.g. dry_run and update are both set.

=head2 run

Takes the options (--dry_run, --verbose,--id_run,--update,--days)
from command line using Moose::GetOpt,to run the methods based on 
the options given.

=head2 get_table_info_for_id_run

Takes an id_run as argument.
Returns an List containing the id_run, corresponding foldername and
the resultset from IseqProductMetrics table in the database for the
given id_run.
 
=head2 get_majora_data

Takes the folder name (corresponding to the given id_run) as an 
argument.
Returns JSON data fetched from Majora as a string value.

=head2 json_to_structure

Takes two arguments.
First argument - the JSON output returned from get_majora_data
stored as a string.
Second argument - the foldername relating to the id_run.
Converts the JSON format to a perl data structure of the format:
Library name => Biosample name => central sample id => sample data
Returns hash reference to the data structure created.
 
=head2 update_metadata

Takes two arguments.
First argument - The result set of the ISeqProductMetrics table.
Second argument - hash reference to the datastructure returned by the
method json_to_structure.
Updates the mlwarehouse database with the new cog_sample_meta values
depending on the whether there is sample data for the given run in Majora.
If there IS NO sample data:
cog_sample_meta is set to NULL.
If there IS sample data AND there IS a value for submission_org:
cog_sample_meta is set to 1.
If there IS sample data AND there IS NO value for submission_org:
cog_sample_meta is set to 0.

=head2 get_id_runs_missing_data

Optionally takes an argument: array ref of cog_sample_meta to search 
for (default [undef,0]).
searches schema for Heron runs which are missing cog_sample_meta
values and returns as a list their id_runs.

=head2 get_id_runs_missing_data_in_last_days

Optionally takes an argument: array ref of cog_sample_meta to search 
for (default [undef,0]).
id_runs will be fetched.

=head2 update_majora

Takes id_run as argument, to then call api to update Majora.

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item autodie

=item Moose

=item MooseX::StrictConstructor

=item namespace::autoclean

=item DateTime

=item HTTP::Request

=item JSON::XS

=item LWP::UserAgent

=item Log::Log4perl

=item MooseX::Getopt

=item List::MoreUtils

=item Carp

=item npg_tracking::Schema

=item WTSI::DNAP::Warehouse::Schema

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Fred Dodd

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2020,2021 Genome Reserach Ltd.

This file is part of NPG.

NPG is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

=cut

