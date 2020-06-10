package npg_pipeline::daemon::analysis;

use Moose;
use Carp;
use Readonly;
use Try::Tiny;
use List::MoreUtils qw/uniq/;

use npg_tracking::util::abs_path qw/abs_path/;

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

sub _process_one_run {
  my ($self, $run) = @_;

  my $id_run = $run->id_run();
  $self->info(qq{Considering run $id_run});
  if ($self->seen->{$id_run}) {
    $self->info(qq{Already seen run $id_run, skipping...});
    return;
  }

  my $arg_refs = $self->check_lims_link($run);
  $arg_refs->{'script'} = $self->pipeline_script_name;

  $arg_refs->{'job_priority'} = $run->run_lanes->count <= 2 ?
    $RAPID_RUN_JOB_PRIORITY : $DEFAULT_JOB_PRIORITY;
  my $inherited_priority = $run->priority;
  if ($inherited_priority > 0) { #not sure we curate what we get from LIMs
    $arg_refs->{'job_priority'} += $inherited_priority;
  }
  $arg_refs->{'rf_path'}  = $self->runfolder_path4run($id_run);

  $self->run_command( $id_run, $self->_generate_command( $arg_refs ));

  return;
}

sub _generate_command {
  my ( $self, $arg_refs ) = @_;

  my $cmd = sprintf '%s --verbose --job_priority %i --runfolder_path %s',
             $self->pipeline_script_name,
             $arg_refs->{'job_priority'},
             $arg_refs->{'rf_path'};

  if (!$arg_refs->{'id'}) {
    # Batch id is needed for MiSeq runs, including qc runs
    $self->logcroak(q{Lims flowcell id is missing});
  }
  if ($arg_refs->{'qc_run'}) {
    $cmd .= q{ --qc_run};
    $self->info('QC run');
  }
  $cmd .= q{ --id_flowcell_lims } . $arg_refs->{'id'};

  my $path = join $PATH_DELIM, $self->local_path(), $ENV{'PATH'};

  return qq{export PATH=$path; $cmd};
}

no Moose;
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

=head2 study_analysis_conf

Returns a hash ref of study analysis configuration details.
If the configuration file is not found or is not readable,
an empty hash is returned.

=head2 run

Invokes the analysis pipeline script for runs with 'analysis pending'
status. Runs for which LIMS data are not available are skipped. 

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item Try::Tiny

=item Readonly

=item Carp

=item List::MoreUtils

=item npg_tracking::util::abs_path

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown
Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2016, 2020 Genome Research Ltd.

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
