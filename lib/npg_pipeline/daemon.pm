package npg_pipeline::daemon;

use FindBin qw($Bin);
use Moose;
use namespace::autoclean;
use MooseX::StrictConstructor;
use English qw/-no_match_vars/;
use List::MoreUtils qw/none/;
use Readonly;
use Try::Tiny;

use npg_tracking::util::abs_path qw/abs_path/;
use npg_tracking::illumina::runfolder;
use npg_tracking::Schema;

with qw{ 
         MooseX::Getopt
         WTSI::DNAP::Utilities::Loggable
       };

our $VERSION = '0';

Readonly::Scalar my $GREEN_DATACENTRE  => q[green];
Readonly::Array  my @GREEN_STAGING     =>
   qw(sf18 sf19 sf20 sf21 sf22 sf23 sf24 sf25 sf26 sf27 sf28 sf29 sf30 sf31 sf46 sf47 sf49 sf50 sf51);

Readonly::Scalar my $SLEEPY_TIME => 900;

has 'pipeline_script_name' => (
  isa        => q{Str},
  is         => q{ro},
  metaclass  => 'NoGetopt',
  lazy_build => 1,
  builder    => 'build_pipeline_script_name',
);

has 'dry_run' => (
  isa        => q{Bool},
  is         => q{ro},
  required   => 0,
  default    => 0,
  documentation => 'dry run mode flag, false by default',
);

has 'seen' => (
  isa       => q{HashRef},
  is        => q{ro},
  init_arg  => undef,
  metaclass => 'NoGetopt',
  default   => sub { return {}; },
);

has 'green_host' => (
  isa        => q{Bool},
  is         => q{ro},
  metaclass  => 'NoGetopt',
  lazy_build => 1,
);
sub _build_green_host {
  my $self = shift;
  my $datacentre = `machine-location|grep datacentre`;
  if ($datacentre) {
    $self->info(qq{Running in $datacentre});
    return ($datacentre && $datacentre =~ /$GREEN_DATACENTRE/xms) ? 1 : 0;
  }
  $self->warn(q{Do not know what datacentre I am running in});
  return;
}

has 'npg_tracking_schema' => (
  isa        => q{npg_tracking::Schema},
  is         => q{ro},
  metaclass  => 'NoGetopt',
  lazy_build => 1,
);
sub _build_npg_tracking_schema {
  return npg_tracking::Schema->connect();
}

sub runs_with_status {
  my ($self, $status_name, $from_time) = @_;
  if (!$status_name) {
    $self->logcroak(q[Need status name]);
  }

  my $condition =  {
    q[me.iscurrent]                => 1,
    q[run_status_dict.description] => $status_name
  };
  if ($from_time) {
    my $time = $self->npg_tracking_schema()->storage
                    ->datetime_parser->format_datetime($from_time);
    $condition->{q[me.date]} = {q[>] => $time};
  }

  return
    map { $_->run() }
    $self->npg_tracking_schema()->resultset(q[RunStatus])->search(
      $condition,
      {prefetch=>q[run_status_dict], order_by => q[me.date],}
    )->all();
}

sub staging_host_match {
  my ($self, $folder_path_glob) = @_;

  my $match = 1;

  if (defined $self->green_host) {
    if (!$folder_path_glob) {
      $self->logcroak(
	q[Need folder_path_glob to decide whether the run folder ] .
        q[and the daemon host are co-located]);
    }
    $match =  $self->green_host ^ none { $folder_path_glob =~ m{/$_/}smx } @GREEN_STAGING;
  }

  return $match;
}

sub run_command {
  my ( $self, $id_run, $cmd ) = @_;

  $self->info(qq{COMMAND: $cmd});
  my ($output, $error);

  if (!$self->dry_run) {
    $output = `$cmd`;
    $error  = $CHILD_ERROR;
  }
  if ($error) {
    $self->warn(
      qq{Error $error occured. Will try $id_run again on next loop.});
  }else{
    $self->seen->{$id_run}++;
  }

  if ($output) {
    $self->info(qq{COMMAND OUTPUT: $output});
  }

  return $error ? 0 : 1;
}

sub local_path {
  my $self = shift;
  my $perl_path = "$EXECUTABLE_NAME";
  $perl_path =~ s/\/perl$//xms;
  return map { abs_path $_ } ($Bin, $perl_path);
}

sub runfolder_path4run {
  my ($self, $id_run) = @_;

  my $path = npg_tracking::illumina::runfolder->new(
    npg_tracking_schema => $self->npg_tracking_schema,
    id_run              => $id_run,
  )->runfolder_path;

  return abs_path($path);
}

sub run {
  return;
}

sub loop {
  my $self = shift;

  my $class = ref $self;
  while (1) {
    try {
      $self->info(qq{$class running});
      if ($self->dry_run) {
        $self->info(q{DRY RUN});
      }
      $self->run();
    } catch {
      $self->warn(qq{Error in $class : $_} );
    };
    $self->info(qq{Going to sleep for $SLEEPY_TIME secs});
    sleep $SLEEPY_TIME;
  }

  return;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

npg_pipeline::daemon

=head1 SYNOPSIS

  package npg_pipeline::daemons::my_pipeline;
  use Moose;
  extends 'npg_pipeline::daemon';

=head1 DESCRIPTION

A Moose parent class for npg_pipeline daemons.

=head1 SUBROUTINES/METHODS

=head2 dry_run

Dry run mode flag, false by default.

=head2 pipeline_script_name

An attribute

=head2 build_pipeline_script_name

Builder method for the pipeline_script_name attribute, should be
implemented by children.

=head2 seen

=head2 npg_tracking_schema

=head2 run_command

Runs the pipeline script. Returns 1 if successful, 0 in
case of error.

=head2 runs_with_status

With one argument, which should be a valid run status description,
returns a list of DBIx::Class::Row objects from the Run result set,
which correspond to runs with the current status descriptiongiven
by the argument.
  
  # find runs with current status 'archival pending'
  my @rows = $obj->runs_with_status('archival pending');

With two arguments, the first one a valid run status description,
the second one a DateTime object, returns a subset of the list
that is returned by this method with one argument. The additional
selection condition is that the time of timestamp of the run status
should be after the time given by the second argument.

  # find runs which have current status 'archival in progress' and
  # have reached this status within the last two hours
  my $date = DateTime-now()->subtract(hours => 2);
  my @rows = $obj->runs_with_status('archival in progress', $date);

In both cases a list of returned objects is sorted in the assending
run status timestamp order.

If no run satisfies the conditions given by the argument(s), an
empty list is returned.

=head2 staging_host_match

=head2 green_host

=head2 local_path

Returns a list with paths to bin the code is running from
and perl executable the code is running under

=head2 runfolder_path4run

Returns runfolder path for given id_run

=head2 run

Single pass through the eligible runs. An empty implementation in
this class. Should be implemented by children.

=head2 loop

An indefinite loop of calling run() method with 15 mins pauses
between the repetitions. Any errors in the run() method are
captured and printed to the log.

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item FindBin

=item Moose

=item namespace::autoclean

=item MooseX::StrictConstructor

=item MooseX::Getopt

=item English

=item List::MoreUtils

=item WTSI::DNAP::Utilities::Loggable

=item Readonly

=item Try::Tiny

=item npg_tracking::illumina::runfolder

=item use npg_tracking::util::abs_path

=item npg_tracking::Schema

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

=over

=item Marina Gourtovaia

=back

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2016,2017,2018,2019,2021 Genome Research Ltd.

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
