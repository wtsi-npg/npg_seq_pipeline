#############
# $Id: to_irods.pm 18739 2014-11-26 14:28:42Z mg8 $
# Created By: gq1
# Last Maintained By: $Author: mg8 $
# Created On: 2010-09-23
# Last Changed On: $Date: 2014-11-26 14:28:42 +0000 (Wed, 26 Nov 2014) $
# $HeadURL: svn+ssh://svn.internal.sanger.ac.uk/repos/svn/new-pipeline-dev/npg-pipeline/trunk/lib/npg_pipeline/archive/file/to_irods.pm $

package npg_pipeline::archive::file::to_irods;
use Moose;
use Carp;
use English qw{-no_match_vars};
use Readonly; Readonly::Scalar our $VERSION => do { my ($r) = q$LastChangedRevision: 18739 $ =~ /(\d+)/mxs; $r; };

extends qw{npg_pipeline::base};

sub submit_to_lsf {
  my ($self, $arg_refs) = @_;
  my $job_sub = $self->_generate_bsub_command($arg_refs);
  my $job_id = $self->submit_bsub_command($job_sub);;
  return ($job_id);
}

sub _generate_bsub_command {
  my ($self, $arg_refs) = @_;

  my $id_run = $self->id_run();
  my @positions = $self->positions();

  my $position_list = q{};
  if (scalar @positions < scalar $self->lims->children) {
    foreach my $p  (@positions){
      $position_list .= qq{ --positions $p};
    }
  }

  my $required_job_completion = $arg_refs->{'required_job_completion'};
  my $timestamp = $self->timestamp();
  my $archive_script = $self->external_script_names_conf()->{'archive_to_irods'};
  my $job_name_prefix = $archive_script . q{_} . $self->id_run();
  my $job_name = $job_name_prefix . q{_} . $timestamp;

  my $location_of_logs = $self->make_log_dir( $self->recalibrated_path() );
  my $bsub_command = q{bsub -q } . $self->lsf_queue() . qq{ $required_job_completion -J $job_name };

  $bsub_command .=  ( $self->fs_resource_string( {
    counter_slots_per_job => 1,
    seq_irods             => $self->general_values_conf()->{default_lsf_irods_resource},
  } ) ) . q{ };

  $bsub_command .=  qq{-E 'script_must_be_unique_runner -job_name="$job_name_prefix"' };
  $bsub_command .=  q{-o } . $location_of_logs . qq{/$job_name.out };
  $bsub_command .=  q{'} . $archive_script . q{ --archive_path } . $self->archive_path() . q{ --runfolder_path } . $self->runfolder_path() . q{ --id_run } . $self->id_run();

  if($position_list){
     $bsub_command .=  $position_list
  }

  $bsub_command .=  q{'};

  if ($self->verbose()) {
    $self->log($bsub_command);
  }

  return $bsub_command;
}

no Moose;
__PACKAGE__->meta->make_immutable;
1;
__END__

=head1 NAME

npg_pipeline::archive::file::to_irods

=head1 VERSION

$LastChangedRevision: 18739 $

=head1 SYNOPSIS

  my $fsa = npg_pipeline::archive::file::to_irods->new({
    run_folder => <run_folder>,
    timestamp => $sTimeStamp,
  });

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 submit_to_lsf

handler for submitting to LSF the archival bam files to irods 
returns an array of lsf job ids

  my @job_ids = $fsa->submit_to_lsf({
    required_job_completion => <string of lsf job dependencies>,
  });

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item Carp

=item English -no_match_vars

=item Readonly

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

$Author: mg8 $

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2010 GRL, by Guoying Qi (gq1@sanger.ac.uk)

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
