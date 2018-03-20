package npg_pipeline::function::log_files_archiver;

use Moose;
use Readonly;

extends qw{npg_pipeline::base};

our $VERSION = '0';

Readonly::Scalar my $LOG_PUBLISHER_SCRIPT_NAME => 'npg_publish_illumina_logs.pl';

sub submit_to_lsf {
  my ($self) = @_;
  if ($self->no_irods_archival) {
    $self->warn(q{Archival to iRODS is switched off.});
    return ();
  }
  my $job_id = $self->submit_bsub_command($self->_generate_bsub_command());
  return ($job_id);
}

sub _generate_bsub_command {
  my ($self) = @_;

  my $job_name = join q{_}, $LOG_PUBLISHER_SCRIPT_NAME, $self->id_run(), $self->timestamp();

  my $location_of_logs = $self->make_log_dir( $self->recalibrated_path() );
  $location_of_logs = $self->path_in_outgoing($location_of_logs);
  my $bsub_command = q{bsub -q } . $self->lowload_lsf_queue() . qq{ -J $job_name };

  $bsub_command .=  ( $self->fs_resource_string( {
    counter_slots_per_job => 1,
    seq_irods             => $self->general_values_conf()->{'default_lsf_irods_resource'},
  } ) ) . q{ };

  $bsub_command .=  q{-o } . $location_of_logs . qq{/$job_name.out };

  my $future_path = $self->path_in_outgoing($self->runfolder_path());

  $bsub_command .= qq{-E "[ -d '$future_path' ]" };
  $bsub_command .= q{'};
  $bsub_command .= $LOG_PUBLISHER_SCRIPT_NAME . q{ --runfolder_path } . $future_path . q{ --id_run } . $self->id_run();
  $bsub_command .= q{'};

  $self->debug($bsub_command);

  return $bsub_command;
}

no Moose;
__PACKAGE__->meta->make_immutable;
1;
__END__

=head1 NAME

npg_pipeline::function::log_files_archiver

=head1 SYNOPSIS

  my $fsa = npg_pipeline::function::log_files_archiver->new(
    run_folder => 'run_folder'
  );

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 submit_to_lsf

handler for submitting to LSF the log publishing job 
returns an array of lsf job ids

  my @job_ids = $fsa->submit_to_lsf({
    required_job_completion => q[string of lsf job dependencies],
  });

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item Readonly

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Jennifer Liddle

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
