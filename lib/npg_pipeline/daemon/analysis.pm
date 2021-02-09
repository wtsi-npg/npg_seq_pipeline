package npg_pipeline::daemon::analysis;

use Moose;
use namespace::autoclean;
use Readonly;
use Try::Tiny;

extends qw{npg_pipeline::daemon};

our $VERSION = '0';

Readonly::Scalar my $PIPELINE_SCRIPT        => q{npg_pipeline_central};
Readonly::Scalar my $DEFAULT_JOB_PRIORITY   => 50;
Readonly::Scalar my $RAPID_RUN_JOB_PRIORITY => 60;
Readonly::Scalar my $ANALYSIS_PENDING       => q{analysis pending};
Readonly::Scalar my $PATH_DELIM             => q{:};

sub build_pipeline_script_name {
  return $PIPELINE_SCRIPT;
}

sub run {
  my $self = shift;

  foreach my $run ($self->runs_with_status($ANALYSIS_PENDING)) {
    try {
      if ( $self->staging_host_match($run->folder_path_glob)) {
        $self->_process_one_run($run);
      }
    } catch {
      $self->warn(
        sprintf 'Error processing run %i: %s', $run->id_run(), $_ );
    };
  }

  return;
}

sub _get_batch_id {
  my ($self, $run) = @_;
  my $batch_id = $run->batch_id();
  $batch_id or $self->logcroak(q{No batch id});
  return $batch_id;
}

sub _process_one_run {
  my ($self, $run) = @_;

  my $id_run = $run->id_run();
  $self->info(qq{Considering run $id_run});
  if ($self->seen->{$id_run}) {
    $self->info(qq{Already seen run $id_run, skipping...});
    return;
  }

  my $arg_refs = {
    batch_id     => $self->_get_batch_id($run),
    rf_path      => $self->runfolder_path4run($id_run),
    job_priority => $run->run_lanes->count <= 2 ?
                    $RAPID_RUN_JOB_PRIORITY : $DEFAULT_JOB_PRIORITY
  };

  my $inherited_priority = $run->priority;
  if ($inherited_priority > 0) { #not sure we curate what we get from LIMs
    $arg_refs->{'job_priority'} += $inherited_priority;
  }

  $self->run_command($id_run, $self->_generate_command($arg_refs));

  return;
}

sub _generate_command {
  my ($self, $arg_refs) = @_;

  my $cmd = sprintf
    '%s --verbose --job_priority %i --runfolder_path %s --id_flowcell_lims %s',
             $self->pipeline_script_name,
             $arg_refs->{'job_priority'},
             $arg_refs->{'rf_path'},
             $arg_refs->{'batch_id'};

  my $path = join $PATH_DELIM, $self->local_path(), $ENV{'PATH'};

  return qq{export PATH=$path; $cmd};
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

npg_pipeline::daemon::analysis

=head1 SYNOPSIS

  my $runner = npg_pipeline::daemon::analysis->new();
  $runner->loop();

=head1 DESCRIPTION

Runner for the analysis pipeline.
Inherits most of functionality, including the loop() method,
from npg_pipeline::base.

=head1 SUBROUTINES/METHODS

=head2 build_pipeline_script_name

=head2 run

Invokes the analysis pipeline script for runs with 'analysis pending'
status. Runs for which LIMS data are not available are skipped. 

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item namespace::autoclean

=item Try::Tiny

=item Readonly

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

=over

=item Andy Brown

=item Marina Gourtovaia

=back

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2016,2017,2018,2020,2021 Genome Research Ltd.

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
