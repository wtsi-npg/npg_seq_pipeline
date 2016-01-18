package npg_pipeline::daemons::base;

use Moose;
use MooseX::ClassAttribute;
use Moose::Meta::Class;
use Carp;
use English qw/-no_match_vars/;
use List::MoreUtils  qw/none/;
use FindBin qw/$Bin/;
use Readonly;
use Log::Log4perl;

use npg_tracking::illumina::run::folder::location;
use npg_tracking::illumina::run::short_info;
use npg_tracking::util::abs_path qw/abs_path/;
use WTSI::DNAP::Warehouse::Schema;
use WTSI::DNAP::Warehouse::Schema::Query::IseqFlowcell;

extends qw{npg_pipeline::base};

our $VERSION = '0';

Readonly::Scalar my $GREEN_DATACENTRE  => q[green];
Readonly::Array  my @GREEN_STAGING     =>
   qw(sf18 sf19 sf20 sf21 sf22 sf23 sf24 sf25 sf26 sf27 sf28 sf29 sf30 sf31 sf46 sf47 sf49 sf50 sf51);

Readonly::Scalar my $NO_LIMS_LINK => -1;

class_has 'pipeline_script_name' => (
  isa        => q{Str},
  is         => q{ro},
  metaclass  => 'NoGetopt',
  lazy_build => 1,
);

has 'dry_run' => (
  isa        => q{Bool},
  is         => q{ro},
  required   => 0,
  default    => 0,
  documentation => 'Dry run mode flag, false by default',
);

has 'logger' => (
  isa        => q{Log::Log4perl::Logger},
  is         => q{ro},
  metaclass  => 'NoGetopt',
  default    => sub { Log::Log4perl->get_logger() },
);

around 'log' => sub {
   my $orig = shift;
   my $self = shift;
   $self->logger->info('"log" method is deprecated');
   return $self->logger->warn(@_);
};

has 'seen' => (
  isa       => q{HashRef},
  is        => q{ro},
  init_arg  => undef,
  metaclass => 'NoGetopt',
  default   => sub { return {}; },
);

has q{green_host} => (
  isa        => q{Bool},
  is         => q{ro},
  metaclass  => 'NoGetopt',
  lazy_build => 1,
);
sub _build_green_host {
  my $self = shift;
  my $datacentre = `machine-location|grep datacentre`;
  if ($datacentre) {
    $self->logger->info(qq{Running in $datacentre});
    return ($datacentre && $datacentre =~ /$GREEN_DATACENTRE/xms) ? 1 : 0;
  }
  $self->logger->warn(q{Do not know what datacentre I am running in});
  return;
}

has 'iseq_flowcell' => (
  isa        => q{DBIx::Class::ResultSet},
  is         => q{ro},
  metaclass  => 'NoGetopt',
  lazy_build => 1,
);
sub _build_iseq_flowcell {
  return WTSI::DNAP::Warehouse::Schema->connect()->resultset('IseqFlowcell');
}

has 'lims_query_class' => (
  isa        => q{Moose::Meta::Class},
  is         => q{ro},
  metaclass  => 'NoGetopt',
  default    => sub {
    my $package_name = 'npg_pipeline::mlwh_query';
    my $class=Moose::Meta::Class->create($package_name);
    $class->add_attribute('flowcell_barcode', {isa =>'Str', is=>'ro'});
    $class->add_attribute('id_flowcell_lims', {isa =>'Maybe[Str]', is=>'ro'});
    $class->add_attribute('iseq_flowcell',    {isa =>'DBIx::Class::ResultSet', is=>'ro'});
    return Moose::Meta::Class->create_anon_class(
      superclasses=> [$package_name],
      roles       => [qw/WTSI::DNAP::Warehouse::Schema::Query::IseqFlowcell/] );
  },
);

sub runs_with_status {
  my ($self, $status_name) = @_;
  if (!$status_name) {
    croak q[Need status name];
  }
  return map {$_->run() } $self->npg_tracking_schema()->resultset(q[RunStatus])->search(
     { q[me.iscurrent] => 1, q[run_status_dict.description] => $status_name},
     {prefetch=>q[run_status_dict], order_by => q[me.date],}
  )->all();
}

sub staging_host_match {
  my ($self, $folder_path_glob) = @_;

  my $match = 1;

  if (defined $self->green_host) {
    if (!$folder_path_glob) {
      croak q[Need folder_path_glob to decide whether the run folder ] .
            q[and the daemon host are co-located];
    }
    $match =  $self->green_host ^ none { $folder_path_glob =~ m{/$_/}smx } @GREEN_STAGING;
  }

  return $match;
}

sub check_lims_link {
  my ($self, $run) = @_;

  my $fc_barcode = $run->flowcell_id;
  if(!$fc_barcode) {
    croak q{No flowcell barcode};
  }

  my $batch_id = $run->batch_id();
  my $ref = { 'iseq_flowcell'  => $self->iseq_flowcell };
  $ref->{'flowcell_barcode'} = $fc_barcode;
  $ref->{'id_flowcell_lims'} = $batch_id;

  my $obj = $self->lims_query_class()->new_object($ref);
  my $fcell_row = $obj->query_resultset()->next;

  if ( !($batch_id || $fcell_row)  ) {
    croak q{No matching flowcell LIMs record is found};
  }

  my $lims = {};
  $lims->{'id'} = $batch_id;
  if ($fcell_row) {
    $lims->{'gclp'} = $fcell_row->from_gclp;
  } else {
    $lims->{'qc_run'} = $self->is_qc_run($lims->{'id'});
    if (!$lims->{'qc_run'}) {
      croak q{Not QC run and not in the ml warehouse};
    }
  }

  return $lims;
}

sub run_command {
  my ( $self, $id_run, $cmd ) = @_;

  $self->logger->info(qq{COMMAND: $cmd});
  my ($output, $error);

  if (!$self->dry_run) {
    $output = `$cmd`;
    $error  = $CHILD_ERROR;
  }
  if ($error) {
    $self->logger->warn(
      qq{Error $error occured. Will try $id_run again on next loop.});
  }else{
    $self->seen->{$id_run}++;
  }

  if ($output) {
    $self->logger->info(qq{COMMAND OUTPUT: $output});
  }

  return;
}

sub local_path {
  my $perl_path = "$EXECUTABLE_NAME";
  $perl_path =~ s/\/perl$//xms;
  my @paths = map { abs_path($_) } ($Bin, $perl_path);
  return @paths;
}

no Moose;
__PACKAGE__->meta->make_immutable;
1;

__END__

=head1 NAME

npg_pipeline::daemons::base

=head1 SYNOPSIS

  package npg_pipeline::daemons::my_pipeline;
  use Moose;
  extends 'npg_pipeline::daemons::base';

=head1 DESCRIPTION

A Moose parent class for npg_pipeline daemons.

=head1 SUBROUTINES/METHODS

=head2 dry_run

Dry run mode flag, false by default.

=head2 run_command

=head2 runs_with_status

=head2 staging_host_match

=head2 green_host

=head2 check_lims_link

=head2 local_path

Returns a list with paths to bin the code is running from
and perl executable the code is running under

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item MooseX::ClassAttribute

=item Carp

=item FindBin

=item English -no_match_vars

=item List::MoreUtils

=item Readonly

=item Moose::Meta::Class

=item Log::Log4perl

=item npg_tracking::illumina::run::folder::location

=item npg_tracking::illumina::run::short_info

=item use npg_tracking::util::abs_path

=item WTSI::DNAP::Warehouse::Schema

=item WTSI::DNAP::Warehouse::Schema::Query::IseqFlowcell

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2015 Genome Research Ltd.

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
