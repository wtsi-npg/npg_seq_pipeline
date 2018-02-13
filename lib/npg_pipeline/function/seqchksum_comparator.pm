package npg_pipeline::function::seqchksum_comparator;

use Moose;
use Carp;
use English qw{-no_match_vars};
use File::Spec;
use Readonly;
use Cwd;

use npg_pipeline::lsf_job;
extends qw{npg_pipeline::base};

our $VERSION = '0';

Readonly::Scalar my  $SEQCHKSUM_SCRIPT => q{npg_pipeline_seqchksum_comparator};

=head1 NAME

npg_pipeline::function::seqchksum_comparator
  
=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=cut

=head2 launch

Generates and submits the LSF job array to run the seqchksum comparator

=cut

sub launch {
  my ($self) = @_;
  my $job_id = $self->submit_bsub_command($self->_generate_bsub_command());
  return ($job_id);
}

sub _generate_bsub_command {
  my ($self) = @_;

  my $array_string = npg_pipeline::lsf_job->create_array_string($self->positions());
  my $timestamp = $self->timestamp();
  my $id_run = $self->id_run();

  my $job_name = $SEQCHKSUM_SCRIPT .q{_} . $id_run . q{_} .$timestamp;

  my $archive_out = $self->archive_path() . q{/log};
  my $out_subscript = q{.%I.%J.out};
  my $outfile = File::Spec->catfile($archive_out, $job_name . $out_subscript);

  $job_name = q{'} . $job_name . $array_string . q{'};

  my $job_sub = q{bsub -q } . $self->lsf_queue() . qq{ -J $job_name -o $outfile '};
  $job_sub .= $SEQCHKSUM_SCRIPT;
  $job_sub .= q{ --id_run=} . $id_run;
  $job_sub .= q{ --lanes=}  . $self->lsb_jobindex();
  $job_sub .= q{ --archive_path=} . $self->archive_path();
  $job_sub .= q{ --bam_basecall_path=} . $self->bam_basecall_path();

  if ($self->verbose() ) {
    $job_sub .= q{ --verbose};
  }

  $job_sub .= q{'};

  $self->debug($job_sub);

  return $job_sub;
}

=head2 do_comparison

Bamcat any plex/split bamfiles back together to perform a bamseqchksum.
Compare it with the one for the whole lane or croak if that has not been done.
Use diff -u rather than cmp and store the file on disk to help work out what has gone wrong.

=cut

sub do_comparison {
  my ($self) = @_;

  my $lanes = $self->lanes();

  if ( ! scalar @{$lanes}) {
    $self->logcroak( 'No lanes found, so not performing any bamseqchksum comparison');
  }

   my $ret = 1;

  foreach my $position (@{$lanes}) {
    $self->info("About to build .all.seqchksum for lane $position");
    my $this = $self->_compare_lane($position);
    if ($this > 0 ) {
      $self->warn("Bamseqchksum comparisons for lane $position have FAILED");
      $ret = 1;
    }
  }
  return $ret;
}

sub _compare_lane {
  my ($self, $position) = @_;

  my $input_seqchksum_dir = $self->bam_basecall_path();
  #my $product_seqchksum_dir = $self->bam_basecall_path() .q{/no_cal/archive/};
  my $input_seqchksum_file_name = $self->id_run . '_' . $position . '.post_i2b.seqchksum';
  my $lane_seqchksum_file_name = $self->id_run . '_' . $position . '.all.seqchksum';

  my $input_lane_seqchksum_file_name = File::Spec->catfile($input_seqchksum_dir, $input_seqchksum_file_name);
  if ( ! -e $input_lane_seqchksum_file_name ) {
    $self->logcroak("Cannot find $input_lane_seqchksum_file_name to compare to");
  }

  my$wd = getcwd();
  $self->info('Changing to archive directory ', $self->archive_path());
  chdir $self->archive_path() or $self->logcroak('Failed to change directory');

  my $cram_file_name_glob = qq({lane$position/,}). $self->id_run . '_' . $position . q{*.cram};
  my @crams = glob $cram_file_name_glob or
    $self->logcroak("Cannot find any cram files using $cram_file_name_glob");
  $self->info("Building .all.seqchksum for lane $position from cram in $cram_file_name_glob ...");

  my $cmd = 'seqchksum_merge.pl ' . join(q{ }, @crams) . qq{> $lane_seqchksum_file_name};

  if ($cmd ne q{}) {
    $self->info("Running $cmd to generate $lane_seqchksum_file_name");
    my $ret = system qq[/bin/bash -c "set -o pipefail && $cmd"];
    if ( $ret  > 0 ) {
      $self->logcroak("Failed to run command $cmd: $ret");
    }
  }

  my $compare_cmd = q{diff -u <(grep '.all' } . $input_lane_seqchksum_file_name . q{ | sort) <(grep '.all' } . $lane_seqchksum_file_name . q{ | sort)};
  $self->info($compare_cmd);

  my $compare_ret = system qq[/bin/bash -c "$compare_cmd"];
  chdir $wd or $self->logcroak("Failed to change back to $wd");
  if ($compare_ret !=0) {
    $self->logcroak("Found a difference in seqchksum for post_i2b and product running $compare_cmd: $compare_ret");
  } else {
    return $compare_ret;
  }

  return;
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

=item Cwd

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Kate Taylor

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

