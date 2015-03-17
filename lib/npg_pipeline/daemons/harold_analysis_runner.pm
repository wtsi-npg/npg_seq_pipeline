package npg_pipeline::daemons::harold_analysis_runner;

use Moose;
use MooseX::ClassAttribute;
use Moose::Meta::Class;
use Carp;
use English qw{-no_match_vars};
use List::MoreUtils  qw/none/;
use Readonly;

use npg_tracking::illumina::run::folder::location;
use npg_tracking::illumina::run::short_info;
use WTSI::DNAP::Warehouse::Schema;
use WTSI::DNAP::Warehouse::Schema::Query::IseqFlowcell;

extends qw{npg_pipeline::base};

our $VERSION = '0';

Readonly::Scalar my $PIPELINE_SCRIPT        => q{npg_pipeline_central};

Readonly::Scalar my $DEFAULT_JOB_PRIORITY   => 50;
Readonly::Scalar my $RAPID_RUN_JOB_PRIORITY => 60;
Readonly::Scalar my $ANALYSIS_PENDING  => q{analysis pending};
Readonly::Scalar my $GREEN_DATACENTRE  => q[green];
Readonly::Array  my @GREEN_STAGING     =>
   qw(sf18 sf19 sf20 sf21 sf22 sf23 sf24 sf25 sf26 sf27 sf28 sf29 sf30 sf31 sf46 sf47 sf49 sf50 sf51);

Readonly::Scalar my $NO_LIMS_LINK => -1;

class_has 'pipeline_script_name' => (
  isa        => q{Str},
  is         => q{ro},
  lazy_build => 1,
);
sub _build_pipeline_script_name {
  return $PIPELINE_SCRIPT;
}

has 'seen' => (
  isa     => q{HashRef},
  is      => q{rw},
  default => sub { return {}; },
);

has q{green_host} => (
  isa        => q{Bool},
  is         => q{ro},
  lazy_build => 1,
);
sub _build_green_host {
  my $self = shift;
  my $datacentre = `machine-location|grep datacentre`;
  if ($datacentre) {
    $self->log(qq{Running in $datacentre});
  } else {
    $self->log(q{Do not know what datacentre I am running in});
  }
  return ($datacentre && $datacentre =~ /$GREEN_DATACENTRE/xms);
}

has 'iseq_flowcell' => (
  isa        => q{DBIx::Class::ResultSet},
  is         => q{ro},
  lazy_build => 1,
);
sub _build_iseq_flowcell {
  return WTSI::DNAP::Warehouse::Schema->connect()->resultset('IseqFlowcell');
}

has 'lims_query_class' => (
  isa        => q{Moose::Meta::Class},
  is         => q{ro},
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

sub run {
  my ($self) = @_;
  $self->log(q{Analysis daemon running...});
  foreach my $run ($self->runs_with_status($ANALYSIS_PENDING)) {
    eval{
      if ( $self->staging_host_match($run->folder_path_glob)) {
        $self->_process_one_run($run);
      }
      1;
    } or do {
      $self->log('Problems to process one run ' . $run->id_run() );
      $self->log($EVAL_ERROR);
      next;
    };
  }
  return 1;
}

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
  if ($folder_path_glob) {
    return $self->green_host ^ none { $folder_path_glob =~ m{/$_/}smx } @GREEN_STAGING;
  } else {
    croak q[Need folder_path_glob to decide whether the run folder and daemon host are co-located];
  }
  return;
}

sub _process_one_run {
  my ($self, $run) = @_;

  my $id_run = $run->id_run();
  $self->log(qq{Considering run $id_run});
  if ($self->seen->{$id_run}) {
    $self->log(qq{Already seen run $id_run, skipping...});
    return;
  }

  my $arg_refs = $self->_check_lims_link($run);
  if ($arg_refs->{'id'} == $NO_LIMS_LINK) {
    my $m = $arg_refs->{'message'};
    $self->log(qq{$m for run $id_run, will revisit later});
    return;
  }

  $arg_refs->{'script'} = $self->pipeline_script_name;

  $arg_refs->{'job_priority'} = $run->run_lanes->count <= 2 ?
    $RAPID_RUN_JOB_PRIORITY : $DEFAULT_JOB_PRIORITY;
  my $inherited_priority = $run->priority;
  if ($inherited_priority > 0) { #not sure we curate what we get from LIMs
    $arg_refs->{'job_priority'} += $inherited_priority;
  }

  $arg_refs->{'rf_path'} = $self->_runfolder_path($id_run);

  $self->run_command( $id_run, $self->_generate_command( $arg_refs ) );

  return;
}

sub _check_lims_link {
  my ($self, $run) = @_;

  my $lims = {'id' => $NO_LIMS_LINK};

  my $fc_barcode = $run->flowcell_id;
  if(!$fc_barcode) {
    $lims->{'message'} = q{No flowcell barcode};
  } else {
    my $batch_id = $run->batch_id();

    my $ref = { 'iseq_flowcell'  => $self->iseq_flowcell };
    $ref->{'flowcell_barcode'} = $fc_barcode;
    $ref->{'id_flowcell_lims'} = $batch_id;

    my $obj = $self->lims_query_class()->new_object($ref);
    my $fcell_row = $obj->query_resultset()->next;

    if ( !($batch_id || $fcell_row)  ) {
       $lims->{'message'} = q{No matching flowcell LIMs record is found};
    } else {
      $lims->{'id'}   = $batch_id || 0;
      $lims->{'gclp'} = $fcell_row ? $fcell_row->from_gclp : 0;
    }
  }

  return $lims;
}

sub _runfolder_path {
  my ($self, $id_run) = @_;

  my $class =  Moose::Meta::Class->create_anon_class(
    roles => [ qw/npg_tracking::illumina::run::folder::location
                  npg_tracking::illumina::run::short_info/ ]
  );
  $class->add_attribute(q(npg_tracking_schema),{isa => 'npg_tracking::Schema', is=>q(ro)});

  return $class->new_object(
    npg_tracking_schema => $self->npg_tracking_schema,
    id_run              => $id_run,
  )->runfolder_path;
}

sub _generate_command {
  my ( $self, $arg_refs ) = @_;

  my $cmd = sprintf '%s --verbose --job_priority %i --runfolder_path %s',
             $self->pipeline_script_name,
             $arg_refs->{'job_priority'},
             $arg_refs->{'rf_path'};

  if ( $arg_refs->{'gclp'} ) {
    $cmd .= ' --function_list gclp';
  } elsif ( $arg_refs->{'id'} ) {
    $cmd .= ' --id_flowcell_lims ' . $arg_refs->{'id'};
  }

  my $path = join q[:], $self->local_path(), $ENV{PATH};
  $cmd = qq{export PATH=$path;} . $cmd;
  return $cmd;
}

# run the command generated
sub run_command {
  my ( $self, $id_run, $cmd ) = @_;
  my $output = `$cmd`;
  if ( $CHILD_ERROR ) {
    $self->log( qq{Error $CHILD_ERROR occured. Will try $id_run again on next loop.});
  }else{
    $self->log(qq{Output:\n$output});
    $self->seen->{$id_run}++; # you have now seen this
  }
  return;
}

no Moose;
__PACKAGE__->meta->make_immutable;
1;
__END__

=head1 NAME

npg_pipeline::daemons::harold_analysis_runner

=head1 SYNOPSIS

  my $runner = npg_pipeline::daemons::harold_analysis_runner->new();
  $runner->run();

=head1 DESCRIPTION

This module interrogates the npg database for runs with a status of analysis pending
and then, if a link to LIMs data can be established,
starts the pipeline for each of them.

=head1 SUBROUTINES/METHODS

=head2 run - runner method for the daemon

=head2 run_command

=head2 runs_with_status

=head2 staging_host_match

=head2 green_host

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item MooseX::ClassAttribute

=item Carp

=item English -no_match_vars

=item List::MoreUtils

=item Readonly

=item Moose::Meta::Class

=item npg_tracking::illumina::run::folder::location

=item npg_tracking::illumina::run::short_info

=item WTSI::DNAP::Warehouse::Schema

item WTSI::DNAP::Warehouse::Schema::Query::IseqFlowcell

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown
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
