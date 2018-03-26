package npg_pipeline::function::illumina_qc_archiver;

use Moose;
use namespace::autoclean;
use Readonly;

use npg_pipeline::function::definition;

extends qw{npg_pipeline::base};

our $VERSION = '0';

Readonly::Scalar my $SCRIPT_NAME => q{npg_qc_illumina_analysis_loader};

sub create {
  my $self = shift;

  my $job_name_prefix = q{illumina_analysis_loader};
  my $job_name = join q{_}, $job_name_prefix, $self->id_run(), $self->timestamp();
  my $location_of_logs = $self->make_log_dir( $self->recalibrated_path() );

  my $preexec =  qq{npg_pipeline_script_must_be_unique_runner -job_name="$job_name_prefix" -own_job_name="$job_name"};

  my $command =  $SCRIPT_NAME;
  $command .= q{ --id_run } . $self->id_run;
  $command .= q{ --run_folder } . $self->run_folder;
  $command .= q{ --runfolder_path } . $self->runfolder_path;
  if ($self->bam_basecall_path) {
    $command .= q{ --bam_basecall_path } . $self->bam_basecall_path;
  }
  $command .= q{ --basecall_path } . $self->basecall_path;
  if ($self->verbose()) {
    $command .= q{ --verbose};
  }

  my $d = npg_pipeline::function::definition->new(
    created_by      => __PACKAGE__,
    created_on      => $self->timestamp(),
    identifier      => $self->id_run(),
    job_name        => $job_name,
    command         => $command,
    command_preexec => $preexec,
    log_file_dir    => $location_of_logs,
    fs_slots_num    => 1,
    queue           =>
      $npg_pipeline::function::definition::LOWLOAD_QUEUE,
  );

  return [$d];
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

npg_pipeline::function::illumina_qc_archiver

=head1 SYNOPSIS

  my $aia = npg_pipeline::function::illumina_qc_archiver->new(
    run_folder => 'run_folder',
  );

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 create

Creates and returns a single function definition as an array.
Function definition is created as a npg_pipeline::function::definition
type object.

  my @job_ids = $aia->submit_to_lsf();

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item namespace::autoclean

=item Readonly

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown
Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2018 Genome Research Ltd

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
