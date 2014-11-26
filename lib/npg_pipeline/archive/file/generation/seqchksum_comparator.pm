###################
# $Id$
# Created By: Kate Taylor
# Created On: 6th August 2014
#

package npg_pipeline::archive::file::generation::seqchksum_comparator;

use Moose;
use Carp;
use English qw{-no_match_vars};
use File::Spec;
use Readonly; Readonly::Scalar our $VERSION => do { my ($r) = q$LastChangedRevision$ =~ /(\d+)/mxs; $r; };
use npg_pipeline::lsf_job;

extends qw{npg_pipeline::archive::file::generation};

Readonly::Scalar our $SEQCHKSUM_SCRIPT => q{npg_pipeline_seqchksum_comparator};

=head1 NAME

npg_pipeline::archive::file::generation::seqchksum_comparator

=head1 VERSION
  
$LastChangedRevision$
  
=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=cut

=head2 launch

Generates and submits the LSF job array to run the seqchksum comparator

=cut

sub launch {
  my ($self, $arg_refs) = @_;

  my @job_ids;

  my @positions = $self->positions( $arg_refs);

  if ( ! scalar @positions) {
    croak( 'No positions found, so not submitting any jobs');
  }

  $arg_refs->{array_string} = npg_pipeline::lsf_job->create_array_string( @positions );

  push @job_ids, $self->submit_bsub_command( $self->_generate_bsub_command ( $arg_refs) );
  return @job_ids;
}

sub _generate_bsub_command {
  my ($self, $arg_refs) = @_;

  my $array_string = $arg_refs->{array_string};
  my $required_job_completion = $arg_refs->{required_job_completion} || q{};
  my $timestamp = $self->timestamp();
  my $id_run = $self->id_run();

  my $job_name = $SEQCHKSUM_SCRIPT .q{_} . $id_run . q{_} .$timestamp;

  my $archive_out = $self->archive_path() . q{/log};
  my $out_subscript = q{.%I.%J.out};
  my $outfile = File::Spec->catfile($archive_out, $job_name . $out_subscript);

  $job_name = q{'} . $job_name . $array_string . q{'};

  my $job_sub = q{bsub -q } . $self->lsf_queue() . qq{ $required_job_completion -J $job_name -o $outfile '};
  $job_sub .= $SEQCHKSUM_SCRIPT;
  $job_sub .= q{ --id_run=} . $id_run;
  $job_sub .= q{ --lanes=}  . $self->lsb_jobindex();
  $job_sub .= q{ --archive_path=} . $self->archive_path();
  $job_sub .= q{ --bam_basecall_path=} . $self->bam_basecall_path();

  if ($self->verbose() ) {
    $job_sub .= q{ --verbose};
  }

  $job_sub .= q{'};

  if ($self->verbose() ) {
    $self->log($job_sub);
  }

  return $job_sub;
}

=head2 do_comparison

Bamcat any plex/split bamfiles back together to perform a bamseqchksum.
Compare it with the one produced by the illumina2bam step, or croak if that has not been done.
Use diff -u rather than cmp and store the file on disk to help work out what has gone wrong

=cut

sub do_comparison {
  my ($self) = @_;

  my $lanes = $self->lanes();

  if ( ! scalar @{$lanes}) {
    croak( 'No lanes found, so not performing any bamseqchksum comparison');
  }

   my $ret = 1;

  foreach my $position (@{$lanes}) {
    $self->log("About to build .all.seqchksum for lane $position");
    my $this = $self->_compare_lane($position);
    if ($this > 0 ) {
      $self->log ("Bamseqchksum comparisons for lane $position have FAILED");
      $ret = 1;
    }
  }
  return $ret;
}

sub _compare_lane {
  my ($self, $position) = @_;

  my $input_seqchksum_dir = $self->bam_basecall_path();
  #my $product_seqchksum_dir = $self->bam_basecall_path() .q{/no_cal/archive/};
  my $product_seqchksum_dir = $self->archive_path();
  my $compare_lane_seqchksum_file_name = q{};
  my $input_seqchksum_file_name = $self->id_run . '_' . $position . '.post_i2b.seqchksum';
  my $lane_seqchksum_file_name = $self->id_run . '_' . $position . '.all.seqchksum';

  my $input_lane_seqchksum_file_name = File::Spec->catfile($input_seqchksum_dir, $input_seqchksum_file_name);
  if ( ! -e $input_lane_seqchksum_file_name ) {
    croak "Cannot find $input_lane_seqchksum_file_name to compare: please check illumina2bam pipeline step";
  }

  my $cmd = q{};

  my $bam_file_name_glob = File::Spec->catfile ( $product_seqchksum_dir, qq({lane$position/,}). $self->id_run . '_' . $position . q{*.bam});
  my @bams = glob $bam_file_name_glob or croak "Cannot find any bam files using $bam_file_name_glob";
  $self->log("Building .all.seqchksum for lane $position from bam in $bam_file_name_glob ...");

  $compare_lane_seqchksum_file_name = File::Spec->catfile($product_seqchksum_dir, $lane_seqchksum_file_name);

  my $bam_count = scalar @bams;
  my $bam_plex_str = join q{ I=}, @bams;
  $cmd = 'bamcat level=0 I=' . $bam_plex_str . ' streaming=1 ';
  $cmd .= '| bamseqchksum > ' . $compare_lane_seqchksum_file_name;

  if ($cmd ne q{}) {
    $self->log("Running $cmd to generate $compare_lane_seqchksum_file_name");
    my $ret = system $cmd;
    if ( $ret  > 0 ) {
      croak "Failed to run command $cmd: $ret";
    }
  }

  my $compare_cmd = q{diff -u <(grep '.all' } . $input_lane_seqchksum_file_name . q{) <(grep '.all' } . $compare_lane_seqchksum_file_name . q{)};
  $self->log($compare_cmd);

  my $compare_ret = system qq[/bin/bash -c "$compare_cmd"];
  if ($compare_ret !=0) {
    croak "Found a difference in seqchksum for post_i2b and product running $compare_cmd: $compare_ret";
  } else {
    return $compare_ret;
  }

}

1;
__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item Carp

=item English -no_match_vars

=item Readonly

=item File::Spec

=item List::MoreUtils

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

$Author: kt6 $

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2014 GRL, by Kate Taylor (kt6@sanger.ac.uk)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

