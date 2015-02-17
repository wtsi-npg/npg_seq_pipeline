package npg_pipeline::archive::file::generation::seq_alignment;

use Moose;
use Carp;
use English qw{-no_match_vars};
use Readonly;
use Moose::Meta::Class;
use Try::Tiny;
use File::Slurp;
use JSON;

use npg_tracking::data::reference::find;
use npg_tracking::data::transcriptome;
use npg_pipeline::lsf_job;
use npg_common::roles::software_location;
use st::api::lims;
use List::Util qw(sum);
extends q{npg_pipeline::base};

our $VERSION  = '0';

Readonly::Scalar our $DNA_ALIGNMENT_SCRIPT  => q{bam_alignment.pl};
Readonly::Scalar our $NUM_THREADS  => q(12,16);
Readonly::Scalar our $MEMORY       => q{32000}; # memory in megabytes

=head2 phix_reference

A path to human reference for bwa alignment to split non-consented reads

=cut
has 'phix_reference' => (isa => 'Str',
                         is => 'rw',
                         required => 0,
                         lazy_build => 1,
);
sub _build_phix_reference {
   my $self = shift;

   my $ruser = Moose::Meta::Class->create_anon_class(
     roles => [qw/npg_tracking::data::reference::find/]
   )->new_object({
     species => q{PhiX},
     aligner => q{fasta},
     ($self->repository ? (q(repository)=>$self->repository) : ())
   });

   my $phix_ref;
   eval {
      $phix_ref = $ruser->refs->[0];
      1;
   } or do{
      carp $EVAL_ERROR;
   };

   return $phix_ref;
}

has q{_AlignmentFilter_jar} => (
                           isa        => q{NpgCommonResolvedPathJarFile},
                           is         => q{ro},
                           coerce     => 1,
                           default    => q{AlignmentFilter.jar},
                                );

has 'input_path'      => ( isa        => 'Str',
                           is         => 'ro',
                           lazy_build => 1,
                         );
sub _build_input_path {
  my $self = shift;
  return $self->recalibrated_path();
}

has 'job_name_root'  => ( isa        => 'Str',
                           is         => 'ro',
                           lazy_build => 1,
                         );
sub _build_job_name_root {
  my $self = shift;
  return join q{_}, q{seq_alignment},$self->id_run(),$self->timestamp();
}

has '_job_args'   => ( isa     => 'HashRef',
                       is      => 'ro',
                       default => sub { return {};},
                     );

sub generate {
  my ( $self, $arg_refs ) = @_;

  my (@lanes) = $self->positions($arg_refs);
  if ( ref $lanes[0] && ref $lanes[0] eq q{ARRAY} ) {   @lanes = @{ $lanes[0] }; }

  $self->_generate_command_arguments(\@lanes);

  my @job_indices = keys %{$self->_job_args};
  if (!@job_indices) {
    if ($self->verbose) {
      $self->log('Nothing to do');
    }
    return ();
  }

  my $job_id = $self->submit_bsub_command(
    $self->_command2submit($arg_refs->{required_job_completion})
  );
  $self->_save_arguments($job_id);

  return ($job_id);
}

sub _command2submit {
  my ($self, $required_job_completion) = @_;

  $required_job_completion ||= q{};
  my $outfile = join q{/} , $self->make_log_dir( $self->archive_path() ), $self->job_name_root . q{.%I.%J.out};
  my @job_indices = sort {$a <=> $b} keys %{$self->_job_args};
  my $job_name = q{'} . $self->job_name_root . npg_pipeline::lsf_job->create_array_string(@job_indices) . q{'};
  my $resources = ( $self->fs_resource_string( {
      counter_slots_per_job => 4,
      resource_string => $self->_default_resources(),
    } ) );
  return  q{bsub -q } . $self->lsf_queue()
    .  q{ } . $self->ref_adapter_pre_exec_string()
    . qq{ $resources $required_job_completion -J $job_name -o $outfile}
    .  q{ 'perl -Mstrict -MJSON -MFile::Slurp -e '"'"'exec from_json(read_file shift@ARGV)->{shift@ARGV} or die q(failed exec)'"'"'}
    .  q{ }.(join q[/],$self->input_path,$self->job_name_root).q{_$}.q{LSB_JOBID}
    .  q{ $}.q{LSB_JOBINDEX'} ;
}

sub _save_arguments {
  my ($self, $job_id) = @_;
  my $file_name = join q[_], $self->job_name_root, $job_id;
  $file_name = join q[/], $self->input_path, $file_name;
  write_file($file_name, to_json $self->_job_args);
  if($self->verbose) {
    $self->log(qq[Arguments written to $file_name]);
  }
  return $file_name;
}

sub _lsf_alignment_command { ## no critic (Subroutines::ProhibitExcessComplexity)
  my ( $self, $l, $is_plex ) = @_;
  my $id_run = $self->id_run;
  my $position = $l->position;
  my $name_root = $id_run . q{_} . $position;
  my $tag_index;
  my $spike_tag;
  my $input_path= $self->input_path;
  my $archive_path= $self->archive_path;
  my $qcpath= $self->qc_path;
  if($is_plex) {
    $tag_index = $l->tag_index;
    $spike_tag = (defined $l->spiked_phix_tag_index and $l->spiked_phix_tag_index == $tag_index);
    $name_root .= q{#} . $tag_index;
    my $lane_dir = qq{lane$position};
    $input_path .= q{/} . $lane_dir;
    $archive_path .= q{/} . $lane_dir;
    $qcpath =~s{([^/]+/?)\z}{$lane_dir/$1}smx; #per plex directory split assumed to be one level up from qc directory
  }
  croak qq{Only one of nonconsented X and autosome human split, separate Y chromosome data, and nonconsented human split may be specified ($name_root)} if (1 < sum $l->contains_nonconsented_xahuman, $l->separate_y_chromosome_data, $l->contains_nonconsented_human);
  croak qq{Nonconsented X and autosome human split, and separate Y chromosome data, must have Homo sapiens reference ($name_root)} if (($l->contains_nonconsented_xahuman or $l->separate_y_chromosome_data) and not $l->reference_genome=~/Homo[ ]sapiens/smx );
  croak qq{Nonconsented human split must not have Homo sapiens reference ($name_root)} if ($l->contains_nonconsented_human and $l->reference_genome=~/Homo[ ]sapiens/smx );
  my $do_rna = $self->_do_rna_analysis($l);
  if( $self->force_p4 or (
      ($do_rna or $self->is_hiseqx_run or $self->_is_v4_run) and
      #allow old school if no reference or if this is the phix spike
      $self->_ref($l,q(fasta)) and
      not $spike_tag
    )){
    #TODO: support these various options in P4 analyses
    croak qq{only paired reads supported ($name_root)} if not $self->is_paired_read;
    croak qq{nonconsented human split not yet supported ($name_root)} if $l->contains_nonconsented_human;
    croak qq{No alignments in bam not yet supported ($name_root)} if not $l->alignments_in_bam;
    my $human_split = $l->contains_nonconsented_xahuman ? q(xahuman) :
                      $l->separate_y_chromosome_data    ? q(yhuman) :
                      q();
    croak qq{Reference required ($name_root)} if not $self->_ref($l,q(fasta));
    return join q( ), q(bash -c '),
                           q(mkdir -p), (join q{/}, $self->archive_path, q{tmp_$}.q{LSB_JOBID}, $name_root) ,q{;},
                           q(cd), (join q{/}, $self->archive_path, q{tmp_$}.q{LSB_JOBID}, $name_root) ,q{&&},
                           q(vtfp.pl -s),
                             q{-keys samtools_executable -vals samtools1_1},
                             q{-keys cfgdatadir -vals $}.q{(dirname $}.q{(readlink -f $}.q{(which vtfp.pl)))/../data/vtlib/},
                             q(-keys aligner_numthreads -vals), q{`echo $}.q{LSB_MCPU_HOSTS | cut -d " " -f2`},
                             q(-keys indatadir -vals), $input_path,
                             q(-keys outdatadir -vals), $archive_path,
                             q(-keys af_metrics -vals), $name_root.q{.bam_alignment_filter_metrics.json},
                             q(-keys rpt -vals), $name_root,
                             q(-keys reference_dict -vals), $self->_ref($l,q(picard)).q(.dict),
                             q(-keys reference_genome_fasta -vals), $self->_ref($l,q(fasta)),
                             q(-keys phix_reference_genome_fasta -vals), $self->phix_reference,
                             q(-keys alignment_filter_jar -vals), $self->_AlignmentFilter_jar,
                             ( $do_rna ? (
                                  q(-keys alignment_reference_genome -vals), $self->_ref($l,q(bowtie2)),
                                  q(-keys library_type -vals), ( $l->library_type =~ /dUTP/smx ? q(fr-firststrand) : q(fr-unstranded) ),
                                  q(-keys transcriptome_val -vals), $self->_transcriptome($l)->transcriptome_index_name(),
                                  q(-keys alignment_method -vals tophat2),
                               ) : (
                                  q(-keys alignment_reference_genome -vals), $self->_ref($l,q(bwa0_6)),
                                  q(-keys bwa_executable -vals bwa0_6),
                                  q(-keys alignment_method -vals bwa_mem),
                             ) ),
                             $human_split ? qq(-keys final_output_prep_target_name -vals split_by_chromosome -keys split_indicator -vals _$human_split) : (),
                             $l->separate_y_chromosome_data ? q(-keys split_bam_by_chromosome_flags -vals S=Y -keys split_bam_by_chromosome_flags -vals V=true) : (),
                             q{$}.q{(dirname $}.q{(dirname $}.q{(readlink -f $}.q{(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_template.json},
                             qq(> run_$name_root.json),
                           q{&&},
                           qq(viv.pl -s -x -v 3 -o viv_$name_root.log run_$name_root.json ),
                           #TODO: shift this horrendous inlining of perl scripts to a qc check the same as alignment_filer_metrics below
                           q{&&},
                           q{perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$}.q{o=npg_qc::autoqc::results::bam_flagstats->new(id_run=>$}.q{ARGV[2], position=>$}.q{ARGV[3]}.($is_plex?q{, tag_index=>$}.q{ARGV[4]}:q()).q{); $}.q{o->parsing_metrics_file($}.q{ARGV[0]); open my$}.q{fh,q(<),$}.q{ARGV[1]; $}.q{o->parsing_flagstats($}.q{fh); close$}.q{fh; $}.q{o->store($}.q{ARGV[-1]) '"'"'},
                             (join q{/}, $archive_path, $name_root.q(.markdups_metrics.txt)),
                             (join q{/}, $archive_path, $name_root.q(.flagstat)),
                             $self->id_run,
                             $position,
                             ($is_plex ? ($tag_index) : ()),
                             $qcpath,
                           q{&&},
                           q{perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$}.q{o=npg_qc::autoqc::results::bam_flagstats->new(human_split=>q(phix), id_run=>$}.q{ARGV[2], position=>$}.q{ARGV[3]}.($is_plex?q{, tag_index=>$}.q{ARGV[4]}:q()).q{); $}.q{o->parsing_metrics_file($}.q{ARGV[0]); open my$}.q{fh,q(<),$}.q{ARGV[1]; $}.q{o->parsing_flagstats($}.q{fh); close$}.q{fh; $}.q{o->store($}.q{ARGV[-1]) '"'"'},
                             (join q{/}, $archive_path, $name_root.q(_phix.markdups_metrics.txt)),
                             (join q{/}, $archive_path, $name_root.q(_phix.flagstat)),
                             $self->id_run,
                             $position,
                             ($is_plex ? ($tag_index) : ()),
                             $qcpath,
                           q{&&},
                           $human_split ? (
                           q{perl -e '"'"'use strict; use autodie; use npg_qc::autoqc::results::bam_flagstats; my$}.q{o=npg_qc::autoqc::results::bam_flagstats->new(human_split=>q(}.$human_split.q{), id_run=>$}.q{ARGV[2], position=>$}.q{ARGV[3]}.($is_plex?q{, tag_index=>$}.q{ARGV[4]}:q()).q{); $}.q{o->parsing_metrics_file($}.q{ARGV[0]); open my$}.q{fh,q(<),$}.q{ARGV[1]; $}.q{o->parsing_flagstats($}.q{fh); close$}.q{fh; $}.q{o->store($}.q{ARGV[-1]) '"'"'},
                             (join q{/}, $archive_path, $name_root.q(_).$human_split.q(.markdups_metrics.txt)),
                             (join q{/}, $archive_path, $name_root.q(_).$human_split.q(.flagstat)),
                             $self->id_run,
                             $position,
                             ($is_plex ? ($tag_index) : ()),
                             $qcpath,
                           q{&&})
                              :(),
                           q{qc --check alignment_filter_metrics --qc_in $}.q{PWD --id_run}, $self->id_run, qq{--position $position --qc_out $qcpath}, ($is_plex ? (qq{--tag_index $tag_index}) : ()),
                         q(');
  }else{
    return join q( ),    $DNA_ALIGNMENT_SCRIPT,
                         q(--id_run),        $self->id_run,
                         q(--position),      $position,
                         ($is_plex ? ( q(--tag_index),     $tag_index ) : ()),
                         q(--input),         ( join q{/}, $input_path, $name_root.q{.bam}),
                         q(--output_prefix), ( join q{/}, $archive_path, $name_root),
                         q(--do_markduplicates),
                         ($self->not_strip_bam_tag ? q(--not_strip_bam_tag) : q() ),
                         ($self->is_paired_read ? q(--is_paired_read) : q(--no-is_paired_read) );
  }

}

sub _generate_command_arguments {
  my ( $self, $positions ) = @_;

  my @positions = @{ $positions };
  if (!@positions) {
    return;
  }
  my $bam_filename = $self->id_run . q{_};
  my $ext = q{.bam};

  my $lane_lims_all = $self->lims->children_ia();
  foreach my $position ( @positions ) {
    my $lane_lims = $lane_lims_all->{$position};
    if (!$lane_lims) {
      if ($self->verbose) {
        $self->log(qq{No lims object for position $position});
      }
      next;
    }
    if ( $self->is_indexed and $lane_lims->is_pool ) { # does the run have an indexing read _and_ does the LIMS have pool information : if so do plex level analyses
      my $plex_lims = $lane_lims->children_ia();
      $plex_lims->{0} ||= st::api::lims->new(driver=>$lane_lims->driver, id_run=>$lane_lims->id_run, position=>$lane_lims->position, tag_index=>0);
      foreach my $tag_index ( @{ $self->get_tag_index_list($position) } ) {
        my $l = $plex_lims->{$tag_index};
        if (!$l) {
          if ($self->verbose) {
            $self->log(qq{No lims object for position $position tag index $tag_index});
          }
          next;
        }
        my $ji = _job_index($position, $tag_index);
        $self->_job_args->{$ji} = $self->_lsf_alignment_command($l,1);
      }
    } else { # do lane level analyses
      my $l = $lane_lims;
      my $ji = _job_index($position);
      $self->_job_args->{$ji} = $self->_lsf_alignment_command($l);
    }
  }
  return;
}

sub _is_v4_run {
  my ($self) = @_;
  return $self->flowcell_id() =~ /A[N-Z]XX\z/smx;
}

sub _do_rna_analysis {
  my ($self, $l) = @_;
  my $lstring = $l->to_string;
  if (!$l->library_type || $l->library_type !~ /(?:cD|R)NA/sxm) {
    if ($self->verbose) {
      $self->log(qq{$lstring - not RNA library type});
    }
    return 0;
  }
  if(not $l->reference_genome =~ /Homo_sapiens|Mus_musculus/smx){
    if ($self->verbose) {
      $self->log(qq{$lstring - Not human or mouse (so skipping RNAseq analysis for now)}); #TODO: RNAseq should work on all eukaryotes?
    }
    return 0;
  }
  if(not $self->_transcriptome($l)->transcriptome_index_name()){
    if ($self->verbose) {
      $self->log(qq{$lstring - no transcriptome set}); #TODO: RNAseq should work without transcriptome?
    }
    return 0;
  }
  if(not $self->is_paired_read){
    if ($self->verbose) {
      $self->log(qq{$lstring - Single end run (so skipping RNAseq analysis for now)}); #TODO: RNAseq should work on single end data
    }
    return 0;
  }
  if ($self->verbose) {
    $self->log(qq{$lstring - Do RNAseq analysis....});
  }
  return 1;
}

sub _transcriptome {
    my ($self, $l) = @_;
    my $t = npg_tracking::data::transcriptome->new (
                {'id_run'     => $l->id_run, #TODO: use lims object?
                 'position'   => $l->position,
                 'tag_index'  => $l->tag_index,
                 ( $self->repository ? ('repository' => $self->repository):())
                });
    return($t);
}

sub _ref {
  my ($self, $l, $aligner) = @_;
  my $lstring = $l->to_string;

  my $href = { 'aligner' => ($aligner||'bowtie2'), 'lims' => $l, };
  if ($self->repository) {
    $href->{'repository'} = $self->repository;
  }
  my $ruser = Moose::Meta::Class->create_anon_class(
       roles => [qw/npg_tracking::data::reference::find/])->new_object($href);
  my @refs;
  try {
    @refs =  @{$ruser->refs};
  } catch {
    if ($self->verbose) {
      $self->log("Error getting reference: $_");
    }
  };

  if (!@refs) {
    if ($self->verbose) {
      $self->log(qq{No reference genome set for $lstring});
    }
    return 0;
  }
  if (scalar @refs > 1) {
    if ($self->verbose) {
      $self->log(qq{Multiple references for $lstring});
    }
    return 0;
  }
  if ($self->verbose) {
    $self->log(qq{Reference set for $lstring: $refs[0]});
  }
  return $refs[0];
}

sub _job_index {
  my ($position, $tag_index) = @_;
  if (!$position) {
    croak 'Position undefined or zero';
  }
  if (defined $tag_index) {
    return sprintf q{%i%03i}, $position, $tag_index;
  }
  return $position;
}

sub _default_resources {
  my ( $self ) = @_;
  my $hosts = 1;
  return (join q[ ], npg_pipeline::lsf_job->new(memory => $MEMORY)->memory_spec(), "-R 'span[hosts=$hosts]'", "-n$NUM_THREADS");
}

no Moose;
__PACKAGE__->meta->make_immutable;
1;
__END__

=head1 NAME

npg_pipeline::archive::file::generation::seq_alignment

=head1 SYNOPSIS

  my $oAfgfq = npg_pipeline::archive::file::generation::seq_alignment->new(
    run_folder => $sRunFolder,
  );

=head1 DESCRIPTION

LSF job creation for seq alignment

=head1 SUBROUTINES/METHODS

=head2 generate - generates and submits lsf job

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Carp

=item English -no_match_vars

=item Readonly

=item Moose

=item Moose::Meta::Class

=item Try::Tiny

=item File::Slurp

=item npg_tracking::data::reference::find

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

David K. Jackson (david.jackson@sanger.ac.uk)

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
