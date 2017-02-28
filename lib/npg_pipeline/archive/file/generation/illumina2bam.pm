package npg_pipeline::archive::file::generation::illumina2bam;

use Moose;
use Carp;
use English qw{-no_match_vars};
use Readonly;
use Perl6::Slurp;

use st::api::lims;
use npg_common::roles::software_location;
use npg_pipeline::lsf_job;
use npg_pipeline::analysis::create_lane_tag_file;

extends q{npg_pipeline::base};
with q{npg_tracking::illumina::run::long_info};

our $VERSION  = '0';

Readonly::Scalar our $DEFAULT_RESOURCES => npg_pipeline::lsf_job->new(memory => 2500)->memory_spec();
Readonly::Scalar our $JAVA_CMD          => q{java};

sub generate {
  my ( $self, $arg_refs ) = @_;

  $self->info(q{Creating Jobs to run illumina2bam for run} . $self->id_run );

  my $alims = $self->lims->children_ia;
  my @job_ids;
  for my $p ($self->positions()){
    my $tag_list_file;
    if ($self->is_multiplexed_lane($p)) {
      $self->info(qq{Lane $p is indexed, generating tag list});
      my $index_length = $self->_get_index_length( $alims->{$p} );
      $tag_list_file = npg_pipeline::analysis::create_lane_tag_file->new(
        location     => $self->metadata_cache_dir,
        lane_lims    => $alims->{$p},
        index_length => $index_length,
        hiseqx       => $self->is_hiseqx_run,
        verbose      => $self->verbose
      )->generate();
    }
    my $bsub_cmd = $self->_generate_bsub_commands( $arg_refs, $alims->{$p}, $tag_list_file);
    push @job_ids, $self->submit_bsub_command( $bsub_cmd );
  }

  return @job_ids;
}

foreach my $jar_name (qw/Illumina2bam BamAdapterFinder BamIndexDecoder/) {
  has q{_}.$jar_name.q{_jar} => (
                           isa        => q{NpgCommonResolvedPathJarFile},
                           is         => q{ro},
                           coerce     => 1,
                           default    => $jar_name.q{.jar},
                                );
}

has q{_illumina2bam_cmd} => (isa        => q{Str},
                             is         => q{ro},
                             lazy_build => 1,
                            );

sub _build__illumina2bam_cmd {
  my $self = shift;
  return $JAVA_CMD . q{ -Xmx1024m} . q{ -jar } . $self->_Illumina2bam_jar();
}

has q{_bam_adapter_detect_cmd} => (isa        => q{Str},
                                   is         => q{ro},
                                   lazy_build => 1,
                                  );
sub _build__bam_adapter_detect_cmd {
  return q(bamadapterfind);
}

has q{_bam_index_decode_cmd} => (isa        => q{Str},
                                 is         => q{ro},
                                 lazy_build => 1,
                                );
sub _build__bam_index_decode_cmd {
  my $self = shift;
  return $JAVA_CMD . q{ -Xmx1024m}
                   . q{ -jar } . $self->_BamIndexDecoder_jar()
                   . q{ VALIDATION_STRINGENCY=SILENT}
}

sub _get_index_length {
  my ( $self, $lane_lims ) = @_;

  my $index_length = $self->index_length;

  if ($lane_lims->inline_index_exists) {
    my $index_start = $lane_lims->inline_index_start;
    my $index_end = $lane_lims->inline_index_end;
    if ($index_start && $index_end) {
      $index_length = $index_end - $index_start + 1;
    }
  }

  return $index_length;
}

sub _generate_bsub_commands {
  my ( $self, $arg_refs, $lane_lims, $tag_list_file ) = @_;

  my $position = $lane_lims->position;
  my $required_job_completion = $arg_refs->{required_job_completion};

  my $id_run             = $self->id_run();
  my $intensity_path     = $self->intensity_path();
  my $bam_basecall_path  = $self->bam_basecall_path();

  my $full_bam_name  = $bam_basecall_path . q{/}. $id_run . q{_} .$position. q{.bam};

  my $job_name = q{illumina2bam_} . $id_run . q{_} . $position. q{_} . $self->timestamp();

  my $log_folder = $self->make_log_dir( $bam_basecall_path );
  my $outfile = $log_folder . q{/} . $job_name . q{.%J.out};

  $job_name = q{'} . $job_name . q{'};

  my $last_tool_picard_based = 1;
  my $job_command = $self->_illumina2bam_cmd()
                  . q{ I=} . $intensity_path
                  . q{ L=} . $position
                  . q{ B=} . $self->basecall_path()
                  . q{ RG=}. $id_run.q{_}.$position
                  . q{ PU=}. join q[_], $self->run_folder, $position;

  my $st_names = $self->_get_library_sample_study_names($lane_lims);

  if($st_names->{library}){
    $job_command .= q{ LIBRARY_NAME="} . $st_names->{library} . q{"};
  }
  if($st_names->{sample}){
    $job_command .= q{ SAMPLE_ALIAS="} . $st_names->{sample} . q{"};
  }
  if($st_names->{study}){
    my $study = $st_names->{study};
    $study =~ s/"/\\"/gmxs;
    $job_command .= q{ STUDY_NAME="} . $study . q{"};
  }
  if ($self->_extra_tradis_transposon_read) {
    $job_command .= ' SEC_BC_SEQ=BC SEC_BC_QUAL=QT BC_SEQ=tr BC_QUAL=tq';
  }

  if ($lane_lims->inline_index_exists) {
    my $index_start = $lane_lims->inline_index_start;
    my $index_end = $lane_lims->inline_index_end;
    my $index_read = $lane_lims->inline_index_read;

    if ($index_start && $index_end && $index_read) {
      my($first, $final) = $self->read1_cycle_range();
      if ($index_read == 1) {
        $index_start += ($first-1);
        $index_end += ($first-1);
        $job_command .= qq{ FIRST_INDEX=$index_start FINAL_INDEX=$index_end FIRST_INDEX=$first FINAL_INDEX=}.($index_start-1);
        $job_command .= q{ SEC_BC_SEQ=br SEC_BC_QUAL=qr BC_READ=1 SEC_BC_READ=1};
        $job_command .= q{ FIRST=}.($index_end+1).qq{ FINAL=$final};
        if ($self->is_paired_read()) {
          ($first, $final) = $self->read2_cycle_range();
          $job_command .= qq{ FIRST=$first FINAL=$final};
        }
      } elsif ($index_read == 2) {
        $self->is_paired_read() or $self->logcroak(q{Inline index read (2) does not exist});
        $job_command .= qq{ FIRST=$first FINAL=$final};
        ($first, $final) = $self->read2_cycle_range();
        $index_start += ($first-1);
        $index_end += ($first-1);
        $job_command .= qq{ FIRST_INDEX=$index_start FINAL_INDEX=$index_end FIRST_INDEX=$first FINAL_INDEX=}.($index_start-1);
        $job_command .= q{ SEC_BC_SEQ=br SEC_BC_QUAL=qr BC_READ=2 SEC_BC_READ=2};
        $job_command .= q{ FIRST=}.($index_end+1).qq{ FINAL=$final};
      } else {
        $self->logcroak("Invalid inline index read ($index_read)");
      }
    }
  }

  ###  TODO: can new bamadapterfind cope without these exclusions?
  if ( $self->is_paired_read() && !$lane_lims->inline_index_exists){
    # omit BamAdapterFinder for inline index
    my @range1 = $self->read1_cycle_range();
    my $read1_length = $range1[1] - $range1[0] + 1;
    my @range2 = $self->read2_cycle_range();
    my $read2_length = $range2[1] - $range2[0] + 1;
    # omit BamAdapterFinder if reads are different lengths
    if( $read1_length == $read2_length ){
      $job_command .= q{ OUTPUT=} . q{/dev/stdout} . q{ COMPRESSION_LEVEL=0};
      $job_command .= q{ | } . $self->_bam_adapter_detect_cmd();
      $last_tool_picard_based = 0;
    }
  }

  if( $self->is_multiplexed_lane($position) ){
    if (!$tag_list_file) {
      $self->logcroak('Tag list file path should be defined');
    }
    $job_command .= ($last_tool_picard_based
                 ?  q{ OUTPUT=} . q{/dev/stdout} . q{ COMPRESSION_LEVEL=0}
                 :  q{ level=0});
    $job_command .= q{ | }
                 . $self->_bam_index_decode_cmd()
                 . q{ I=/dev/stdin }
                 . q{ BARCODE_FILE=} . $tag_list_file
                 . q{ METRICS_FILE=} . $full_bam_name . q{.tag_decode.metrics};
    my $num_of_plexes_per_lane = $self->_get_number_of_plexes_excluding_control($lane_lims);
    if( $num_of_plexes_per_lane == 1 ){
      $job_command .= q{ MAX_NO_CALLS=} . $self->general_values_conf()->{single_plex_decode_max_no_calls};
      $job_command .= q{ CONVERT_LOW_QUALITY_TO_NO_CALL=true};
    }
    $last_tool_picard_based = 1;
  }

  $job_command .= ($last_tool_picard_based ? q{ CREATE_MD5_FILE=false OUTPUT=/dev/stdout} : q{ md5=1 md5filename=}.$full_bam_name.q{.md5} );
  #TODO - shift this seqchksum earlier before any compression....
  #TODO - shift this seqchksum as early as possible - immediately after illuina2bam? (but we need to stop altering read names at deplxing for that)

  my $full_bam_seqchksum_name = $full_bam_name;
  $full_bam_seqchksum_name =~ s/[.]bam$/.post_i2b.seqchksum/mxs;
  my $full_bam_md5_name = $full_bam_name;
  $full_bam_md5_name .= q{.md5};

  $job_command .= q{| tee >(bamseqchksum > } . $full_bam_seqchksum_name . q{)};
  if ($last_tool_picard_based) {
    $job_command .= q{ >(md5sum -b | tr -d '\\n *\\-' > } . $full_bam_md5_name . q{)};
  }
  $job_command .= q{ > } . $full_bam_name;

  my $resources = ( $self->fs_resource_string( {
      counter_slots_per_job => $self->general_values_conf()->{io_resource_slots},
      resource_string => $self->_default_resources(),
  } ) );

  $job_command =~ s/'/'"'"'/smxg;#for the bsub
  $job_command =~ s/'/'"'"'/smxg;#for the bash -c
  my $job_sub = q{bsub -q } . $self->lsf_queue() . qq{ $resources $required_job_completion -J $job_name -o $outfile /bin/bash -c 'set -o pipefail;$job_command'};

  $self->debug($job_sub);

  return $job_sub;
}

sub _default_resources {
  my ( $self ) = @_;
  my $mem = $self->general_values_conf()->{'illumina2bam_memory'};
  my $cpu = $self->general_values_conf()->{'illumina2bam_cpu'};
  my $hosts = 1;
  return (join q[ ], npg_pipeline::lsf_job->new(memory => $mem)->memory_spec(), "-R 'span[hosts=$hosts]'", "-n$cpu");
}

sub _get_library_sample_study_names {
  my ($self, $lane_lims) = @_;

  my $names = $self->get_study_library_sample_names($lane_lims);
  my ($study_names, $library_names, $sample_names);
  if($names->{study}){
    $study_names = join q{,}, @{$names->{study}};
  }
  if($names->{library}){
    $library_names = join q{,}, @{$names->{library}};
  }
  if($names->{sample}){
    $sample_names = join q{,}, @{$names->{sample}};
  }

  return {study=>$study_names, library=>$library_names, sample=>$sample_names};
}

sub _get_number_of_plexes_excluding_control {
  my ($self, $lane_lims) = @_;
  my $number = scalar keys %{$lane_lims->tags};
  if ($lane_lims->spiked_phix_tag_index) {
    $number--;
  }
  return $number;
}

has q{_extra_tradis_transposon_read} => (
                             isa        => q{Bool},
                             is         => q{rw},
                             lazy_build => 1,
                            );
sub _build__extra_tradis_transposon_read {
  my $self = shift;

  $self->is_indexed;
  my @i = $self->reads_indexed;
  my $reads_indexed = 0;
  ## no critic (ControlStructures::ProhibitPostfixControls)
  foreach (@i) { $reads_indexed++ if $_; }

  my $is_tradis = 0;
  foreach my $d ($self->lims->descendants()) {
    if ($d->library_type && $d->library_type =~ /^TraDIS/smx) {
      $is_tradis = 1;
      last;
    }
  }

  if ($is_tradis) {
    if ($self->run->is_multiplexed) {
      return 1 if ($reads_indexed > 1);
    } else {
      return 1 if ($reads_indexed > 0);
    }
  }

  return 0;
}


no Moose;

__PACKAGE__->meta->make_immutable;

1;
__END__

=head1 NAME

npg_pipeline::archive::file::generation::illumina2bam

=head1 SYNOPSIS

  my $oAfgfq = npg_pipeline::archive::file::generation::illumina2bam->new(
    run_folder => $sRunFolder,
  );

=head1 DESCRIPTION

Object module which knows how to construct and submits the command line to LSF for creating bam files from bcl files.

=head1 SUBROUTINES/METHODS

=head2 generate - generates the bsub jobs and submits them for creating the fastq files, returning an array of job_ids.

  my @job_ids = $oAfgfq->generate({
    required_job_completion} => q{-w (123 && 321)};
  });

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Carp

=item English -no_match_vars

=item Readonly

=item Moose

=item Perl6::Slurp

=item npg_common::roles::software_location

=item st::api::lims

=item npg_tracking::illumina::run::long_info

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Guoying Qi

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2014 Genome Research Limited

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
