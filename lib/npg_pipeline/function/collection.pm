package npg_pipeline::function::collection;

use Moose;
use Readonly;
use File::Spec;

use npg_pipeline::lsf_job;
extends q{npg_pipeline::base};

our $VERSION = '0';

=head1 NAME

npg_pipeline::function::collection

=head1 SYNOPSIS

  my $c = npg_pipeline::function::collection->new(
    id_run => 1234,
    run_folder => q{123456_IL2_1234},
  );

=head1 DESCRIPTION

A collection of pipeline functions' implementors

=head1 SUBROUTINES/METHODS

=head2 update_warehouse

Updates run data in the npg tables of the warehouse.

=cut

sub update_warehouse {
  my ($self, $flag) = @_;
  if ($self->no_warehouse_update) {
    $self->warn(q{Update to warehouse is switched off.});
    return ();
  }
  return $self->submit_bsub_command(
    $self->_update_warehouse_command('warehouse_loader', $flag));
}

=head2 update_warehouse_post_qc_complete

Updates run data in the npg tables of the ml_warehouse.
Runs when the runfolder is moved to the outgoing directory.

=cut

sub update_warehouse_post_qc_complete {
  my $self = shift;
  return $self->update_warehouse('post_qc_complete');
}

=head2 update_ml_warehouse

Updates run data in the npg tables of the ml_warehouse.

=cut

sub update_ml_warehouse {
  my ($self, $flag) = @_;
  if ($self->no_warehouse_update) {
    $self->warn(q{Update to warehouse is switched off.});
    return ();
  }
  return $self->submit_bsub_command(
    $self->_update_warehouse_command('npg_runs2mlwarehouse', $flag));
}

=head2 update_ml_warehouse_post_qc_complete

Updates run data in the npg tables of the ml_warehouse.
Runs when the runfolder is moved to the outgoing directory.

=cut

sub update_ml_warehouse_post_qc_complete {
  my $self = shift;
  return $self->update_ml_warehouse('post_qc_complete');
}

sub _update_warehouse_command {
  my ($self, $loader_name, $post_qc_complete) = @_;

  my $id_run = $self->id_run;
  my $command = qq{$loader_name --verbose --id_run $id_run};
  if ($loader_name eq 'warehouse_loader') {
    $command .= q{ --lims_driver_type };
    $command .= $post_qc_complete ? 'ml_warehouse_fc_cache' : 'samplesheet';
  }
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

  return q{bsub -q } . $self->lowload_lsf_queue() . qq{ -J $job_name -o $out $prereq '$command'};
}

=head2 bam2fastqcheck_and_cached_fastq

Creates and caches short fastq file for autoqc checks.
Takes the lane bam file as input.

=cut

sub bam2fastqcheck_and_cached_fastq {
  my $self = shift;
  my $id = $self->submit_bsub_command(
    $self->_bam2fastqcheck_and_cached_fastq_command() );
  return ($id);
}

sub _bam2fastqcheck_and_cached_fastq_command {
  my $self = shift;

  my $timestamp = $self->timestamp();
  my $id_run = $self->id_run();

  my $job_name = join q{_}, q{bam2fastqcheck_and_cached_fastq}, $id_run, $timestamp;
  my $out = $job_name . q{.%I.%J.out};
  $out =  File::Spec->catfile($self->make_log_dir($self->recalibrated_path), $out );

  $job_name = q{'} . $job_name . npg_pipeline::lsf_job->create_array_string( $self->positions()) . q{'};

  my $job_sub = q{bsub -q } . $self->lsf_queue() . q{ } .
                $self->fs_resource_string( {counter_slots_per_job => 1,} ) .
                qq{ -J $job_name -o $out };
  $job_sub .= q{'} .
              q{generate_cached_fastq --path } . $self->archive_path() .
              q{ --file } . $self->recalibrated_path() . q{/} . $id_run . q{_} . $self->lsb_jobindex() . q{.bam} .
              q{'};

  return $job_sub;
}

sub _token_job {
  my ($self, $function_name, $suspended) = @_;
  my $runfolder_path = $self->runfolder_path();
  my $job_name = join q{_}, $function_name, $self->id_run(), $self->pipeline_name();
  my $out = join q{_}, $function_name, $self->timestamp, q{%J.out};
  $out = join q{/}, $runfolder_path, $out;
  my $cmd = q{bsub };
  if ($suspended) {
    $cmd .= q{-H };
  }
  $cmd .= q{-q } . $self->small_lsf_queue() . qq{ -J $job_name -o $out '/bin/true'};
  my $job_id = $self->submit_bsub_command($cmd);
  ($job_id) = $job_id =~ m/(\d+)/ixms;
  return ($job_id);
}

=head2 pipeline_start

First function that might be called by the pipeline.
Submits a suspended token job to LSF. The user-defined functions that are run
as LSF jobs will depend on the successful complition of this job. Therefore,
the pipeline jobs will stay pending till the start job is resumed and gets
successfully completed.

=cut

sub pipeline_start {
  my $self = shift;
  return $self->_token_job('pipeline_start', 1);
}

=head2 pipeline_end

Last 'catch all' function that might be called by the pipeline.
Submits a token job to LSF. 

=cut

sub pipeline_end {
  my $self = shift;
  return $self->_token_job('pipeline_end');
}

no Moose;
1;
__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item Readonly

=item File::Spec

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

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
