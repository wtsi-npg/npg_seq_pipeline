package npg_pipeline::archive::qc::illumina_analysis;

use Moose;
use Carp;
use English qw{-no_match_vars};

extends qw{npg_pipeline::base};

our $VERSION = '0';

sub submit_to_lsf {
  my ($self, $arg_refs) = @_;
  my $job_sub = $self->_generate_bsub_command($arg_refs);
  my $job_id = $self->submit_bsub_command($job_sub);
  return ($job_id);
}

sub _generate_bsub_command {
  my ($self, $arg_refs) = @_;

  my $required_job_completion = $arg_refs->{required_job_completion};
  my $timestamp = $self->timestamp();
  my $job_name_prefix = q{illumina_analysis_loader};

  my $job_name = join q{_}, $job_name_prefix, $self->id_run() , $timestamp;

  my $location_of_logs = $self->make_log_dir( $self->recalibrated_path() );
  my $bsub_command = q{bsub -q } . $self->lowload_lsf_queue . qq{ $required_job_completion -J $job_name };
  $bsub_command .=  ( $self->fs_resource_string( {
    counter_slots_per_job => 1,
  } ) ) . q{ };
  $bsub_command .=  qq{-E 'script_must_be_unique_runner -job_name="$job_name_prefix" -own_job_name="$job_name"' };
  $bsub_command .=  q{-o } . $location_of_logs . q{/} . $job_name . q{.out };
  $bsub_command .=  q{'npg_qc_illumina_analysis_loader};
  $bsub_command .= q{  --id_run } . $self->id_run;
  $bsub_command .= q{  --run_folder } . $self->run_folder;
  $bsub_command .= q{  --runfolder_path } . $self->runfolder_path;
  if ($self->bam_basecall_path) {
    $bsub_command .= q{  --bam_basecall_path } . $self->bam_basecall_path;
  }
  $bsub_command .= q{  --basecall_path } . $self->basecall_path;

  if ($self->verbose()) {
    $bsub_command .= q{  --verbose'};
  } else {
    $bsub_command .= q{'};
  }
  $self->debug($bsub_command);

  return $bsub_command;
}


no Moose;
__PACKAGE__->meta->make_immutable;
1;
__END__

=head1 NAME

npg_pipeline::archive::qc::illumina_analysis

=head1 SYNOPSIS

  my $aia = npg_pipeline::archive::qc::illumina_analysis->new(
    run_folder => 'run_folder',
  );

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 submit_to_lsf - handles calling out to create the bsub command and submits it, returning the job ids

  my @job_ids = $aia->submit_to_lsf({
    required_job_completion => 'lsf job requirement string',
  });

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item Carp

=item English -no_match_vars

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2014 Genome Research Ltd

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
