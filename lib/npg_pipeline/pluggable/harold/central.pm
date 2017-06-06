package npg_pipeline::pluggable::harold::central;

use Moose;
use Carp;
use English qw{-no_match_vars};
use File::Spec;
use List::MoreUtils qw/any/;

use npg_pipeline::lsf_job;
extends qw{npg_pipeline::pluggable::harold::post_qc_review};

our $VERSION = '0';

=head1 NAME

npg_pipeline::pluggable::harold::central

=head1 SYNOPSIS

  npg_pipeline::pluggable::harold::central->new(id_run => 333)->main();

=head1 DESCRIPTION

Pluggable module runner for the main pipeline

=cut

=head1 SUBROUTINES/METHODS

=head2 prepare

 Sets all paths needed during the lifetime of the analysis runfolder.
 Creates any of the paths that do not exist.

=cut

override 'prepare' => sub {
  my $self = shift;
  $self->_set_paths();
  super(); # Correct order!
  return;
};

####
#
# Sets all paths needed during the lifetime of the analysis runfolder.
# Creates any of the paths that do not exist.
#

sub _set_paths {
  my $self = shift;

  my $sep = q[/];

  if ( ! $self->has_intensity_path() ) {
    my $ipath = join $sep, $self->runfolder_path(), q{Data}, q{Intensities};
    if (!-e $ipath) {
      $self->info(qq{Intensities path $ipath not found});
      $ipath = $self->runfolder_path();
    }
    $self->_set_intensity_path( $ipath );
  }
  $self->info('Intensities path: ', $self->intensity_path() );

  if ( ! $self->has_basecall_path() ) {
    my $bpath = join $sep, $self->intensity_path() , q{BaseCalls};
    if (!-e $bpath) {
      $self->warn(qq{BaseCalls path $bpath not found});
      $bpath = $self->runfolder_path();
    }
    $self->_set_basecall_path( $bpath);
  }
  $self->info('BaseCalls path: ' . $self->basecall_path() );

  if( ! $self->has_bam_basecall_path() ) {
    my $bam_basecalls_dir = join $sep, $self->intensity_path(), q{BAM_basecalls_} . $self->timestamp();
    $self->make_log_dir( $bam_basecalls_dir  );
    $self->set_bam_basecall_path( $bam_basecalls_dir );
  }
  $self->info('BAM_basecall path: ' . $self->bam_basecall_path());

  if (! $self->has_recalibrated_path()) {
    $self->_set_recalibrated_path(join $sep, $self->bam_basecall_path(), 'no_cal')
  }
  $self->make_log_dir($self->recalibrated_path());
  $self->info('PB_cal path: ' . $self->recalibrated_path());

  $self->make_log_dir( $self->status_files_path );

  return;
}

=head2 illumina_basecall_stats

Use Illumina tools to generate the (per run) BustardSummary and IVC reports (from on instrument RTA basecalling).

=cut

sub illumina_basecall_stats {
  my ($self, @args) = @_;

  if ( $self->is_hiseqx_run ) {
    $self->info(q{HiSeqX sequencing instrument, illumina_basecall_stats will not be run});
    return ();
  }
  my $required_job_completion = shift @args;
  return $self->new_with_cloned_attributes(q{npg_pipeline::analysis::illumina_basecall_stats})
    ->generate({required_job_completion => $required_job_completion,});
}

=head2 split_bam_by_tag

split lane bam file by indexing tag, marked by read group id

=cut

sub split_bam_by_tag {
  my ($self, @args) = @_;
  my $required_job_completion = shift @args;
  return $self->new_with_cloned_attributes( q{npg_pipeline::analysis::split_bam_by_tag} )
           ->generate({required_job_completion => $required_job_completion,});
}

=head2 create_archive_directory

creates the archive and qc directories,

and lane and lane qc directories if the lane is multiplexed.

=cut

sub create_archive_directory {
  my ($self, @args) = @_;
  $self->new_with_cloned_attributes(q{npg_pipeline::archive::folder::generation})->create_dir();
  return ();
}


=head2 p4_stage1_analysis

for stage 1 analysis using p4

=cut

sub p4_stage1_analysis {
  my ($self, @args) = @_;
  my $required_job_completion = shift @args;
  return $self->new_with_cloned_attributes(q{npg_pipeline::archive::file::generation::p4_stage1_analysis})
           ->generate({required_job_completion => $required_job_completion,});
}

=head2 seq_alignment

for each plex or a lane(non-indexed lane), do suitable alignment for data

=cut

sub seq_alignment {
  my ($self, @args) = @_;
  my $required_job_completion = shift @args;
  return $self->new_with_cloned_attributes(q{npg_pipeline::archive::file::generation::seq_alignment})
           ->generate({required_job_completion => $required_job_completion,});
}

=head2 bam_cluster_counter_check

For each lane, job submitted which checks that cluster counts are what they are expected to be in bam files

=cut

sub bam_cluster_counter_check {
  my ( $self, @args ) = @_;
  my $arg_refs = {required_job_completion => shift @args,};
  return $self->new_with_cloned_attributes( q{npg_pipeline::archive::file::BamClusterCounts} )->launch( $arg_refs );
}

=head2 bam2fastqcheck_and_cached_fastq

Creates and caches short fastq file for autoqc checks.
Takes the lane bam file as input.

=cut

sub bam2fastqcheck_and_cached_fastq {
  my ($self, @args) = @_;
  my $required_job_completion = shift @args;
  my $id = $self->submit_bsub_command(
    $self->_bam2fastqcheck_and_cached_fastq_command($required_job_completion) );
  return ($id);
}

sub _bam2fastqcheck_and_cached_fastq_command {
  my ($self, $required_job_completion) = @_;

  $required_job_completion ||= q{};
  my $timestamp = $self->timestamp();
  my $id_run = $self->id_run();

  my $job_name = join q{_}, q{bam2fastqcheck_and_cached_fastq}, $id_run, $timestamp;
  my $out = $job_name . q{.%I.%J.out};
  $out =  File::Spec->catfile($self->make_log_dir($self->recalibrated_path), $out );

  $job_name = q{'} . $job_name . npg_pipeline::lsf_job->create_array_string( $self->positions()) . q{'};

  my $job_sub = q{bsub -q } . $self->lsf_queue() . q{ } .
                $self->fs_resource_string( {counter_slots_per_job => 1,} ) .
                qq{ $required_job_completion -J $job_name -o $out };
  $job_sub .= q{'} .
              q{generate_cached_fastq --path } . $self->archive_path() .
              q{ --file } . $self->recalibrated_path() . q{/} . $id_run . q{_} . $self->lsb_jobindex() . q{.bam} .
              q{'};
  $self->debug($job_sub);

  return $job_sub;
}

=head2 seqchksum_comparator

Checks that the .seqchksum created in the illumin2bam step matches one created from all the plex/split product bam in archive directories

=cut

sub seqchksum_comparator {
  my ( $self, @args ) = @_;
  my $arg_refs = {required_job_completion => shift @args,};
  return $self->new_with_cloned_attributes( q{npg_pipeline::archive::file::generation::seqchksum_comparator} )->launch( $arg_refs );
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

=item List::MoreUtils

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Guoying Qi

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2017 Genome Research Limited

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
