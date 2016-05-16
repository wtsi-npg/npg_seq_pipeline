package npg_pipeline::pluggable::harold::post_qc_review;

use Moose;
use Carp;
use English qw{-no_match_vars};
use File::Spec;

use npg_pipeline::cache;
extends qw{npg_pipeline::pluggable::harold};

our $VERSION = '0';

=head1 NAME

npg_pipeline::pluggable::harold::post_qc_review

=head1 SYNOPSIS

  my $oPostQCReview = npg_pipeline::pluggable::harold::post_qc_review->new();

=head1 DESCRIPTION

Pluggable pipeline module for the post_qc_review pipeline

=head1 SUBROUTINES/METHODS

=head2 archive_to_irods

upload all archival files to irods

=cut

sub archive_to_irods {
  my ($self, @args) = @_;
  if ($self->no_irods_archival) {
    $self->log(q{Archival to iRODS is switched off.});
    return ();
  }
  my $required_job_completion = shift @args;
  my $ats = $self->new_with_cloned_attributes(q{npg_pipeline::archive::file::to_irods});
  my @job_ids = $ats->submit_to_lsf({
    required_job_completion => $required_job_completion,
  });
  return @job_ids;
}

=head2 archive_logs

upload all log files to irods

=cut

sub archive_logs {
  my ($self, @args) = @_;
  if ($self->no_irods_archival) {
    $self->log(q{Archival to iRODS is switched off.});
    return ();
  }
  my $required_job_completion = shift @args;
  my $ats = $self->new_with_cloned_attributes(q{npg_pipeline::archive::file::logs});
  my @job_ids = $ats->submit_to_lsf({
    required_job_completion => $required_job_completion,
  });
  return @job_ids;
}

=head2 upload_illumina_analysis_to_qc_database

upload illumina analysis qc data 

=cut

sub upload_illumina_analysis_to_qc_database {
  my ($self, @args) = @_;
  my $required_job_completion = shift @args;
  my $aia = $self->new_with_cloned_attributes(q{npg_pipeline::archive::qc::illumina_analysis});
  my @job_ids = $aia->submit_to_lsf({
    required_job_completion => $required_job_completion,
  });
  return @job_ids;
}

=head2 upload_fastqcheck_to_qc_database

upload fastqcheck files to teh qc database

=cut

sub upload_fastqcheck_to_qc_database {
  my ($self, @args) = @_;
  my $required_job_completion = shift @args;
  my $aia = $self->new_with_cloned_attributes(q{npg_pipeline::archive::qc::fastqcheck_loader});
  my @job_ids = $aia->submit_to_lsf({
    required_job_completion => $required_job_completion,
  });
  return @job_ids;
}

=head2 upload_auto_qc_to_qc_database

upload internal auto_qc data

=cut

sub upload_auto_qc_to_qc_database {
  my ($self, @args) = @_;
  my $required_job_completion = shift @args;
  my $aaq = $self->new_with_cloned_attributes(q{npg_pipeline::archive::qc::auto_qc});
  my @job_ids = $aaq->submit_to_lsf({
    required_job_completion => $required_job_completion,
  });
  return @job_ids;
}

=head2 update_warehouse

Updates run data in the npg tables of the warehouse.

=cut
sub update_warehouse {
  my ($self, @args) = @_;
  if ($self->no_warehouse_update) {
    $self->log(q{Update to warehouse is switched off.});
    return ();
  }
  return $self->submit_bsub_command(
    $self->_update_warehouse_command('warehouse_loader', @args));
}

=head2 update_warehouse_post_qc_complete

Updates run data in the npg tables of the ml_warehouse.
Runs when the runfolder is moved to the outgoing directory.

=cut
sub update_warehouse_post_qc_complete {
  my ($self, @args) = @_;
  push @args, {'post_qc_complete' => 1};
  return $self->update_warehouse(@args);
}

=head2 update_ml_warehouse

Updates run data in the npg tables of the ml_warehouse.

=cut
sub update_ml_warehouse {
  my ($self, @args) = @_;
  if ($self->no_warehouse_update) {
    $self->log(q{Update to warehouse is switched off.});
    return ();
  }
  return $self->submit_bsub_command(
    $self->_update_warehouse_command('npg_runs2mlwarehouse', @args));
}

=head2 update_ml_warehouse_post_qc_complete

Updates run data in the npg tables of the ml_warehouse.
Runs when the runfolder is moved to the outgoing directory.

=cut
sub update_ml_warehouse_post_qc_complete {
  my ($self, @args) = @_;
  push @args, {'post_qc_complete' => 1};
  return $self->update_ml_warehouse(@args);
}

sub _update_warehouse_command {
  my ($self, $loader_name, @args) = @_;

  my $required_job_completion = shift @args;
  my $option = pop @args;
  my $post_qc_complete = $option and (ref $option eq 'HASH') and $option->{'post_qc_complete'} ? 1 : 0;
  my $id_run = $self->id_run;

  my $command = q[];
  if ($loader_name eq 'warehouse_loader') {
    # Currently, we need pool library name and link to plexes in SeqQC.
    # Therefore, we need to run live.
    $command = join q[], map {q[unset ] . $_ . q[;]} npg_pipeline::cache->env_vars;
  }

  $command .= qq{$loader_name --verbose --id_run $id_run};
  my $job_name = join q{_}, $loader_name, $id_run, $self->pipeline_name;
  my $path = $self->make_log_dir($self->recalibrated_path());
  my $prereq = q[];
  if ($post_qc_complete) {
    $path = $self->path_in_outgoing($path);
    $job_name .= '_postqccomplete';
    $prereq = qq(-E "[ -d '$path' ]");
  }
  my $out = join q{_}, $job_name, $self->timestamp . q{.out};
  $out =  File::Spec->catfile($path, $out);
  return q{bsub -q } . $self->lowload_lsf_queue() . qq{ $required_job_completion -J $job_name -o $out $prereq '$command'};
}

=head2 copy_interop_files_to_irods

Copy the copy_interop_files files to iRODS

=cut
sub copy_interop_files_to_irods
{
  my ($self, @args) = @_;
  my $required_job_completion = shift @args;
  my $command = $self->_interop_command($required_job_completion);
  return $self->submit_bsub_command($command);
}

sub _interop_command
{
  my ($self, $required_job_completion) = @_;
  my $id_run = $self->id_run;
  my $command = "irods_interop_loader.pl --id_run $id_run --runfolder_path ".$self->runfolder_path();
  my $job_name = 'interop_' . $id_run . '_' . $self->pipeline_name;
  my $out = join q{_}, $job_name, $self->timestamp . q{.out};
  $out =  File::Spec->catfile($self->make_log_dir( $self->runfolder_path()), $out );
  my $resources = $self->fs_resource_string( {
                   counter_slots_per_job => 1,
                   seq_irods             => $self->general_values_conf()->{default_lsf_irods_resource},
                                             } );
  return q{bsub -q } . $self->lowload_lsf_queue() . qq{ $required_job_completion -J $job_name $resources -o $out '$command'};
}

no Moose;
__PACKAGE__->meta->make_immutable;
1;
__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item Carp

=item English -no_match_vars

=item File::Spec

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
