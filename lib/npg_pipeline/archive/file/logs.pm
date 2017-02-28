package npg_pipeline::archive::file::logs;

use Moose;

extends qw{npg_pipeline::base};

our $VERSION = '0';

has 'irods_root' => ( isa => 'Str',
                      is  => 'rw',
                      lazy_build => 1,
                    );

sub _build_irods_root {
  my $self = shift;
  return $self->gclp ? q(/gseq/) : q(/seq/);
}

sub submit_to_lsf {
  my ($self, $arg_refs) = @_;
  my $job_sub = $self->_generate_bsub_command($arg_refs);
  my $job_id = $self->submit_bsub_command($job_sub);
  return ($job_id);
}

sub _generate_bsub_command {
  my ($self, $arg_refs) = @_;

  my $irodsinstance = $self->gclp ? q(gclp) : q();
  my $id_run = $self->id_run();

  my $required_job_completion = $arg_refs->{'required_job_completion'};
  my $timestamp = $self->timestamp();
  my $archive_script = q{npg_irods_log_loader.pl};
  my $job_name_prefix = $archive_script . q{_} . $self->id_run();
  my $job_name = $job_name_prefix . q{_} . $timestamp;

  my $location_of_logs = $self->make_log_dir( $self->recalibrated_path() );
  $location_of_logs = $self->path_in_outgoing($location_of_logs);
  my $bsub_command = q{bsub -q } . $self->lowload_lsf_queue() . qq{ $required_job_completion -J $job_name };

  $bsub_command .=  ( $self->fs_resource_string( {
    counter_slots_per_job => 1,
    seq_irods             => $self->general_values_conf()->{default_lsf_irods_resource},
  } ) ) . q{ };

  $bsub_command .=  q{-o } . $location_of_logs . qq{/$job_name.out };

  my $future_path = $self->path_in_outgoing($self->runfolder_path());
  $bsub_command .= qq{-E "[ -d '$future_path' ]" };

  $bsub_command .=  q{'};

  if ($irodsinstance) {
    $bsub_command .= q{irodsEnvFile=$}.q{HOME/.irods/.irodsEnv-} . $irodsinstance . q{-iseq-logs };
  }

  $bsub_command .=  $archive_script . q{ --runfolder_path } . $future_path . q{ --id_run } . $self->id_run();

  $bsub_command .= q{ --irods_root } . $self->irods_root();

  $bsub_command .=  q{'};

  $self->debug($bsub_command);

  return $bsub_command;
}

no Moose;
__PACKAGE__->meta->make_immutable;
1;
__END__

=head1 NAME

npg_pipeline::archive::file::logs

=head1 SYNOPSIS

  my $fsa = npg_pipeline::archive::file::logs->new(
    run_folder => 'run_folder',
    timestamp => $sTimeStamp,
  );

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 submit_to_lsf

handler for submitting to LSF the archival bam files to irods 
returns an array of lsf job ids

  my @job_ids = $fsa->submit_to_lsf({
    required_job_completion => q[string of lsf job dependencies],
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

Jennifer Liddle

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
