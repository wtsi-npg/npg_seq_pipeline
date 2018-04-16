package npg_pipeline::archive::file::generation::seq_alignment;

use Moose;
use English qw{-no_match_vars};
use Readonly;
use Moose::Meta::Class;
use File::Slurp;
use File::Basename;
use JSON::XS;
use List::Util qw(sum);
use List::MoreUtils qw(any);
use open q(:encoding(UTF8));

use npg_tracking::data::reference::find;
use npg_tracking::data::transcriptome;
use npg_tracking::data::bait;
use npg_tracking::data::gbs_plex;
use npg_pipeline::lsf_job;
use npg_common::roles::software_location;
use st::api::lims;

extends q{npg_pipeline::base};

our $VERSION  = '0';

Readonly::Scalar our $NUM_SLOTS                    => q(12,16);
Readonly::Scalar our $MEMORY                       => q{32000}; # memory in megabytes
Readonly::Scalar our $MORE_MEMORY                  => q{38000}; # idem
Readonly::Scalar our $FORCE_BWAMEM_MIN_READ_CYCLES => q{101};
Readonly::Scalar my  $QC_SCRIPT_NAME               => q{qc};
Readonly::Scalar my  $DEFAULT_SJDB_OVERHANG        => q{74};
Readonly::Scalar my  $REFERENCE_ARRAY_ANALYSIS_IDX => q{3};
Readonly::Scalar my  $REFERENCE_ARRAY_TVERSION_IDX => q{2};
Readonly::Scalar my  $DEFAULT_RNA_ANALYSIS         => q{tophat2};
Readonly::Scalar my  $DEFAULT_JOB_ID_FOR_NO_BSUB   => q{50};

=head2 phix_reference

A path to phix reference for bwa alignment to split phiX spike-in reads

=cut
has 'phix_reference' => (isa        => 'Str',
                         is         => 'rw',
                         required   => 0,
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

  return $ruser->refs->[0];
}

has q{_AlignmentFilter_jar} => (
                           isa        => q{NpgCommonResolvedPathJarFile},
                           is         => q{ro},
                           coerce     => 1,
                           default    => q{AlignmentFilter.jar},
                                );

has q{_SplitBamByChromosomes_jar} => (
                           isa        => q{NpgCommonResolvedPathJarFile},
                           is         => q{ro},
                           coerce     => 1,
                           default    => q{SplitBamByChromosomes.jar},
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

has '_job_mem_reqs' => ( isa     => 'HashRef',
                         is      => 'ro',
                         default => sub { return {};},
                       );

has '_using_alt_reference' => ( isa     => 'Bool',
                                is      => 'rw',
                                default => 0,
                              );

has '_ref_cache' => (isa      => 'HashRef',
                     is       => 'ro',
                     required => 0,
                     default  => sub {return {};},
                    );

has '_do_gbs_plex_analysis' => ( isa     => 'Bool',
                                 is      => 'rw',
                                 default => 0,
                                 documentation => q[Run genotype call analysis],
                                );


sub _create_lane_dirs {
  my ($self, @positions) = @_;

  if(!$self->is_indexed()) {
    $self->info( qq{Run $self->id_run is not multiplex run and no need to split} );
    return;
  }

  my %positions = map { $_=>1 } @positions;
  my @indexed_lanes = grep { $positions{$_} } @{$self->multiplexed_lanes()};
  if(!@indexed_lanes) {
    $self->info( q{None of the lanes specified is multiplexed} );
    return;
  }

  my $output_dir = $self->recalibrated_path() . q{/lane};
  for my $position (@indexed_lanes) {
    my $lane_output_dir = $output_dir . $position;
    if( ! -d $lane_output_dir ) {
      $self->info( qq{creating $lane_output_dir} );
      my $rc = `mkdir -p $lane_output_dir`;
      if ( $CHILD_ERROR ) {
        $self->logcroak(qq{could not create $lane_output_dir\n\t$rc});
      }
    } else {
      $self->info( qq{ already exists: $lane_output_dir} );
    }
  }

  return;
}

sub generate {
  my ( $self, $arg_refs ) = @_;

  my (@lanes) = $self->positions($arg_refs);
  if ( ref $lanes[0] && ref $lanes[0] eq q{ARRAY} ) {   @lanes = @{ $lanes[0] }; }

  $self->_generate_command_arguments(\@lanes);

  my @job_indices = keys %{$self->_job_args};
  if (!@job_indices) {
    $self->debug('Nothing to do');
    return ();
  }

  my $job_id = $self->submit_bsub_command(
    $self->_command2submit($arg_refs->{required_job_completion})
  );

  # bmod jobs that require more memory
  @job_indices = keys %{$self->_job_mem_reqs};
  if (@job_indices) {
    $self->debug('Requesting more memory for alignment jobs');
    my $dummy = $self->submit_bsub_command(
        $self->_bmodcommand2submit($job_id)
    );
  }

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
      resource_string => $self->_default_resources($MEMORY),
    } ) );
  return  q{bsub -q } . $self->lsf_queue()
    .  q{ } . $self->ref_adapter_pre_exec_string()
    . qq{ $resources $required_job_completion -J $job_name -o $outfile}
    .  q{ 'perl -Mstrict -MJSON -MFile::Slurp -Mopen='"'"':encoding(UTF8)'"'"' -e '"'"'exec from_json(read_file shift@ARGV)->{shift@ARGV} or die q(failed exec)'"'"'}
    .  q{ }.(join q[/],$self->input_path,$self->job_name_root).q{_$}.q{LSB_JOBID}
    .  q{ $}.q{LSB_JOBINDEX'} ;
}

sub _save_arguments {
  my ($self, $job_id) = @_;
  my $file_name = join q[_], $self->job_name_root, $job_id;
  $file_name = join q[/], $self->input_path, $file_name;
  write_file($file_name, encode_json $self->_job_args);

  $self->debug(qq[Arguments written to $file_name]);

  return $file_name;
}

sub _bmodcommand2submit {
  my ($self, $job_id) = @_;
  my @job_indices = sort {$a <=> $b} keys %{$self->_job_mem_reqs};
  my $job_name = npg_pipeline::lsf_job->create_array_string(@job_indices);
  # original request must be made again when asking for more memory
  my $resources = ( $self->fs_resource_string( {
      counter_slots_per_job => 4,
      resource_string => $self->_default_resources($MORE_MEMORY),
    } ) );
  return qq{bmod $resources $job_id$job_name};
}

sub _lsf_alignment_command { ## no critic (Subroutines::ProhibitExcessComplexity)
  my ( $self, $l, $is_plex ) = @_;

  $is_plex ||= 0;
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
    $input_path  .= qq{/lane$position};
    $archive_path = $self->lane_archive_path($position);
    $qcpath       = $self->lane_qc_path($position);
  }

  if (1 < sum $l->contains_nonconsented_xahuman, $l->separate_y_chromosome_data, $l->contains_nonconsented_human) {
    $self->logcroak(qq{Only one of nonconsented X and autosome human split, separate Y chromosome data, and nonconsented human split may be specified ($name_root)});
  }
  if (($l->contains_nonconsented_xahuman or $l->separate_y_chromosome_data) and not $l->reference_genome=~/Homo_sapiens/smx ) {
    $self->logcroak(qq{Nonconsented X and autosome human split, and separate Y chromosome data, must have Homo sapiens reference ($name_root)});
  }
  if ($l->contains_nonconsented_human and $l->reference_genome and $l->reference_genome=~/Homo_sapiens/smx ) {
    $self->logcroak(qq{Nonconsented human split must not have Homo sapiens reference ($name_root)});
  }



  ####################################
  # base set of parameters for p4 vtfp
  ####################################
  my $p4_param_vals = {
    samtools_executable => q{samtools1},
    indatadir           => $input_path,
    outdatadir          => $archive_path,
    af_metrics          => $name_root.q{.bam_alignment_filter_metrics.json},
    rpt                 => $name_root,
    phix_reference_genome_fasta => $self->phix_reference,
    alignment_filter_jar => $self->_AlignmentFilter_jar,
  };
  my $p4_ops = {
    prune => [],
    splice => [],
  };

  if(not $spike_tag) {
    push @{$p4_ops->{prune}}, 'fop.*_bmd_multiway:calibration_pu-';
  }

  my $do_rna = $self->_do_rna_analysis($l);


  ## reference for target alignment will be overridden where gbs_plex exists
  ## also any human split will be overriden and alignments will be forced.
  my $do_gbs_plex = $self->_do_gbs_plex_analysis($self->_has_gbs_plex($l));


  my $hs_bwa = ($self->is_paired_read ? 'bwa_aln' : 'bwa_aln_se');
  # continue to use the "aln" algorithm from bwa for these older chemistries (where read length <= 100bp)
  my $bwa = ($self->is_hiseqx_run or $self->_has_newer_flowcell or any {$_ >= $FORCE_BWAMEM_MIN_READ_CYCLES } $self->read_cycle_counts)
            ? 'bwa_mem'
            : $hs_bwa;

  # There will be a new exception to the use of "aln": if you specify a reference
  # with alt alleles e.g. GRCh38_full_analysis_set_plus_decoy_hla, then we will use
  # bwa's "mem"
  $bwa = $self->_is_alt_reference($l) ? 'bwa_mem' : $bwa;

  my $human_split = $do_gbs_plex ? q() :
                    $l->contains_nonconsented_xahuman ? q(xahuman) :
                    $l->separate_y_chromosome_data    ? q(yhuman) :
                    q();

  my $do_target_alignment = ($self->_ref($l,q(fasta))
     and ($l->alignments_in_bam || $do_gbs_plex)
     and not ($l->library_type and $l->library_type =~ /Chromium/smx));

  my $skip_target_markdup_metrics = (not $spike_tag and not $do_target_alignment);

  if($human_split and not $do_target_alignment and not $spike_tag) {
    $do_target_alignment = 1;
    $skip_target_markdup_metrics = 1;

    $p4_param_vals->{final_output_prep_no_y_target} = q[final_output_prep_chrsplit_noaln.json];
  }

  my $nchs = $do_gbs_plex ? q{} : $l->contains_nonconsented_human;
  my $nchs_template_label = $nchs? q{humansplit_}: q{};
  my $nchs_outfile_label = $nchs? q{human}: q{};

  #TODO: allow for an analysis genuinely without phix and where no phiX split work is wanted - especially the phix spike plex....
  #TODO: support these various options below in P4 analyses
  if ((not $self->is_paired_read) and ($do_rna or $nchs)) {
    $self->logcroak(qq{only paired reads supported for RNA or non-consented human ($name_root)});
  }

  ########
  # no target alignment:
  #  splice out unneeded p4 nodes, add -x flag to scramble,
  #   unset the reference for bam_stats and amend the AlignmentFilter command.
  ########
  if(not $do_target_alignment and not $spike_tag) {
      if(not $nchs) {
        push @{$p4_ops->{splice}}, 'src_bam:-alignment_filter:phix_bam_in';
      }
      else {
        push @{$p4_ops->{prune}}, 'aln_tee4_tee4:to_tgtaln-alignment_filter:target_bam_in';
        push @{$p4_ops->{splice}}, 'aln_amp_bamadapterclip_pre_auxmerge:-aln_bam12auxmerge_nchs:no_aln_bam';
      }
      push @{$p4_ops->{splice}}, 'alignment_filter:target_bam_out-foptgt_bmd_multiway:';
      $p4_param_vals->{scramble_reference_flag} = q[-x];
      $p4_param_vals->{stats_reference_flag} = undef;   # both samtools and bam_stats
      $p4_param_vals->{no_target_alignment} = 1;   # adjust AlignmentFilter (bambi select) command
  }

  #################################################################
  # use collected information to update final p4_param_vals entries
  #################################################################
  if($do_target_alignment) {
    $p4_param_vals->{reference_dict} = $self->_ref($l,q(picard)) . q(.dict);
    $p4_param_vals->{reference_genome_fasta} = $self->_ref($l,q(fasta));
  }
  if($nchs) {
    $p4_param_vals->{reference_dict_hs} = $self->_default_human_split_ref(q{picard}, $self->repository);   # always human default
    $p4_param_vals->{hs_reference_genome_fasta} = $self->_default_human_split_ref(q{fasta}, $self->repository);   # always human default
  }

  # handle targeted stats_(bait_stats_analysis) here, handling the interaction with spike tag case
  my $bait_stats_flag = q[];
  my $spike_splicing = q[];
  if(not $spike_tag) {
    if($self->_do_bait_stats_analysis($l)) {
      $p4_param_vals->{bait_regions_file} = $self->_bait($l)->bait_intervals_path();
      push @{$p4_ops->{prune}}, 'fop(phx|hs)_samtools_stats_F0.*00_bait.*-';
    }
    else {
      push @{$p4_ops->{prune}}, 'fop.*samtools_stats_F0.*00_bait.*-';
    }
  }
  else {
    push @{$p4_ops->{prune}}, 'foptgt.*samtools_stats_F0.*00_bait.*-';  # confirm hyphen
    push @{$p4_ops->{splice}}, 'src_bam:-foptgt_bamsort_coord:', 'foptgt_seqchksum_tee:final-scs_cmp_seqchksum:outputchk';
  }


  my $p4_local_assignments = {};
  if($do_gbs_plex){
     $p4_param_vals->{bwa_executable}   = q[bwa0_6];
     $p4_param_vals->{bsc_executable}   = q[bamsort];
     $p4_param_vals->{alignment_method} = $bwa;
     $p4_param_vals->{alignment_reference_genome} = $self->_ref($l,q(bwa0_6));
     $p4_local_assignments->{'final_output_prep_target'}->{'scramble_embed_reference'} = q[1];
     push @{$p4_ops->{splice}}, 'foptgt_bamsort_coord:-foptgt_bmd_multiway:';
     $skip_target_markdup_metrics = 1;
  }
  elsif($do_rna) {
    my $rna_analysis = $self->_analysis($l) // $DEFAULT_RNA_ANALYSIS;
    my $p4_reference_genome_index;
    if($rna_analysis eq q[star]) {
      # most common read length used for RNA-Seq is 75 bp so indices were generated using sjdbOverhang=74
      $p4_param_vals->{sjdb_overhang_val} = $DEFAULT_SJDB_OVERHANG;
      $p4_param_vals->{star_executable} = q[star];
      $p4_reference_genome_index = dirname($self->_ref($l, q(star)));
      # star jobs require more memory
      my $ji;
      if($is_plex){
          $ji = $self->_job_index($position, $tag_index);
      }else{
          $ji = $self->_job_index($position);
      }
      $self->_job_mem_reqs->{$ji} = $MORE_MEMORY;
    }
    else {
      if ($rna_analysis ne $DEFAULT_RNA_ANALYSIS){
        $self->info($l->to_string . qq[- Unsupported RNA analysis: $rna_analysis - running $DEFAULT_RNA_ANALYSIS instead]);
        $rna_analysis = $DEFAULT_RNA_ANALYSIS;
      }
      $p4_param_vals->{library_type} = ( $l->library_type =~ /dUTP/smx ? q(fr-firststrand) : q(fr-unstranded) );
      $p4_param_vals->{transcriptome_val} = $self->_transcriptome($l, q(tophat2))->transcriptome_index_name();
      $p4_reference_genome_index = $self->_ref($l, q(bowtie2));
    }
    if($rna_analysis eq q[tophat2] or $rna_analysis eq q[star]) { # create intermediate file to prevent deadlock
      $p4_param_vals->{align_intfile_opt} = 1;
    }
    $p4_param_vals->{alignment_method} = $rna_analysis;
    $p4_param_vals->{annotation_val} = $self->_transcriptome($l)->gtf_file();
    $p4_param_vals->{quant_method} = q[salmon];
    $p4_param_vals->{salmon_transcriptome_val} = $self->_transcriptome($l, q(salmon))->transcriptome_index_path();
    if($do_target_alignment) { $p4_param_vals->{alignment_reference_genome} = $p4_reference_genome_index; }
    if($nchs) {
      # this human split alignment method is currently the same as the default, but this may change
      $p4_param_vals->{hs_alignment_reference_genome} = $self->_default_human_split_ref(q{bwa0_6}, $self->repository);
      $p4_param_vals->{alignment_hs_method} = $hs_bwa;
    }
  }
  else {
    $p4_param_vals->{bwa_executable} = q[bwa0_6];

    $p4_param_vals->{alignment_method} = $bwa;
    if($do_target_alignment) { $p4_param_vals->{alignment_reference_genome} = $self->_ref($l,q(bwa0_6)); }
    if($nchs) {
      $p4_param_vals->{hs_alignment_reference_genome} = $self->_default_human_split_ref(q{bwa0_6}, $self->repository);
      $p4_param_vals->{alignment_hs_method} = $hs_bwa;
    }
    if(not $self->is_paired_read) {
      $p4_param_vals->{bwa_mem_p_flag} = undef;
    }
  }

  if($human_split) {
    $p4_param_vals->{final_output_prep_target_name} = q[split_by_chromosome];
    $p4_param_vals->{split_indicator} = q{_} . $human_split;

    if($l->separate_y_chromosome_data) {
       $p4_param_vals->{chrsplit_subset_flag} = ['--subset', 'Y,chrY,ChrY,chrY_KI270740v1_random'];
       $p4_param_vals->{chrsplit_invert_flag} = q[--invert];
    }
  }

# write p4 parameters to file
  my $param_vals_fname = join q{/}, $self->_p4_stage2_params_path($position), $name_root.q{_p4s2_pv_in.json};
  write_file($param_vals_fname, encode_json({ assign => [ $p4_param_vals ], assign_local => $p4_local_assignments, ops => $p4_ops }));

  ####################
  # log run parameters
  ####################
  $self->info(q[Using p4]);
  if($nchs) { $self->info(q[  nonconsented_humansplit]) }
  if(not $self->is_paired_read) { $self->info(q[  single-end]) }
  $self->info(q[  do_target_alignment is ] . ($do_target_alignment? q[true]: q[false]));
  $self->info(q[  spike_tag is ] . ($spike_tag? q[true]: q[false]));
  $self->info(q[  human_split is ] . $human_split);
  $self->info(q[  nchs is ] . ($nchs? q[true]: q[false]));
  $self->info(q[  p4 parameters written to ] . $param_vals_fname);
  $self->info(q[  Using p4 template alignment_wtsi_stage2_] . $nchs_template_label . q[template.json]);

  return join q( ), q(bash -c '),
                       q(mkdir -p), (join q{/}, $self->archive_path, q{tmp_$}.q{LSB_JOBID}, $name_root) ,q{;},
                       q(cd), (join q{/}, $self->archive_path, q{tmp_$}.q{LSB_JOBID}, $name_root) ,q{&&},
                       q(vtfp.pl),
                         q{-template_path $}.q{(dirname $}.q{(readlink -f $}.q{(which vtfp.pl)))/../data/vtlib},
                         q(-param_vals), $param_vals_fname,
                         q(-export_param_vals), $name_root.q{_p4s2_pv_out_$}.q/{LSB_JOBID}.json/,
                         q{-keys cfgdatadir -vals $}.q{(dirname $}.q{(readlink -f $}.q{(which vtfp.pl)))/../data/vtlib/},
                         q(-keys aligner_numthreads -vals `npg_pipeline_job_env_to_threads`),
                         q(-keys br_numthreads_val -vals `npg_pipeline_job_env_to_threads --exclude 1 --divide 2`),
                         q(-keys b2c_mt_val -vals `npg_pipeline_job_env_to_threads --exclude 2 --divide 2`),
                         q{$}.q{(dirname $}.q{(dirname $}.q{(readlink -f $}.q{(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_}.$nchs_template_label.q{template.json},
                         qq(> run_$name_root.json),
                       q{&&},
                       qq(viv.pl -s -x -v 3 -o viv_$name_root.log run_$name_root.json ),
                       q{&&},
                       _qc_command('bam_flagstats', $archive_path, $qcpath, $l, $is_plex, undef, $skip_target_markdup_metrics),
                       (grep {$_}
                       ((not $spike_tag)? (join q( ),
                         q{&&},
                         _qc_command('bam_flagstats', $archive_path, $qcpath, $l, $is_plex, 'phix'),
                         q{&&},
                         _qc_command('alignment_filter_metrics', undef, $qcpath, $l, $is_plex),
                       ) : q()),
                       $human_split ? join q( ),
                         q{&&},
                         _qc_command('bam_flagstats', $archive_path, $qcpath, $l, $is_plex, $human_split),
                         : q(),
                       $nchs ? join q( ),
                         q{&&},
                         _qc_command('bam_flagstats', $archive_path, $qcpath, $l, $is_plex, $nchs_outfile_label),
                         : q(),
                       $do_rna ? join q( ),
                         q{&&},
                         _qc_command('rna_seqc', $archive_path, $qcpath, $l, $is_plex),
                         : q(),
                       $do_gbs_plex ? join q( ),
                         q{&&},
                         _qc_command('genotype_call', $archive_path, $qcpath, $l, $is_plex),
                         : q()
                       ),
                     q(');
}

sub _qc_command {##no critic (Subroutines::ProhibitManyArgs)
  my ($check_name, $qc_in, $qc_out, $l, $is_plex, $subset, $skip_markdups_metrics) = @_;

  my $args = {'id_run' => $l->id_run,
              'position'=> $l->position,
              'qc_out' => $qc_out,
              'check' => $check_name,};
  my @flags = ();

  if ($is_plex && defined $l->tag_index) {
    $args->{'tag_index'} = $l->tag_index;
  }

  if ($check_name =~ /^bam_flagstats$/smx and $skip_markdups_metrics) {
      push @flags, q[--skip_markdups_metrics];
  }

  if ($check_name =~ /^bam_flagstats|genotype_call|rna_seqc$/smx) {
    if ($subset) {
      $args->{'subset'} = $subset;
    }
    $args->{'qc_in'}  = $qc_in;
  } else {
    $args->{'qc_in'}  = q[$] . 'PWD';
  }

  my $command = q[];
  foreach my $arg (sort keys %{$args}) {
    $command .= join q[ ], q[ --].$arg, $args->{$arg};
  }

  if(@flags) {
    $command .= q[ ];
    $command .= join q[ ], @flags;
  }

  return $QC_SCRIPT_NAME . $command;
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

    $self->_create_lane_dirs($position);

    my $lane_lims = $lane_lims_all->{$position};
    if (!$lane_lims) {
      $self->debug(qq{No lims object for position $position});
      next;
    }
    if ( $self->is_indexed and $lane_lims->is_pool ) { # does the run have an indexing read _and_ does the LIMS have pool information : if so do plex level analyses
      my $plex_lims = $lane_lims->children_ia();
      $plex_lims->{0} ||= st::api::lims->new(driver=>$lane_lims->driver, id_run=>$lane_lims->id_run, position=>$lane_lims->position, tag_index=>0);
      foreach my $tag_index ( @{ $self->get_tag_index_list($position) } ) {
        my $l = $plex_lims->{$tag_index};
        if (!$l) {
          $self->debug(qq{No lims object for position $position tag index $tag_index});
          next;
        }
        my $ji = $self->_job_index($position, $tag_index);
        $self->_job_args->{$ji} = $self->_lsf_alignment_command($l,1);
        $self->_using_alt_reference($self->_is_alt_reference($l));
      }
    } else { # do lane level analyses
      my $l = $lane_lims;
      my $ji = $self->_job_index($position);
      $self->_job_args->{$ji} = $self->_lsf_alignment_command($l);
    }
  }
  return;
}

sub _has_newer_flowcell { # is HiSeq High Throughput >= V4, Rapid Run >= V2
  my ($self) = @_;
  return $self->flowcell_id() =~ /(?:A[N-Z]|[B-Z][[:upper:]])XX\z/smx;
}

sub _do_rna_analysis {
  my ($self, $l) = @_;
  my $lstring = $l->to_string;
  if (!$l->library_type || $l->library_type !~ /(?:(?:cD|R)NA|DAFT)/sxm) {
    $self->debug(qq{$lstring - not RNA library type: skipping RNAseq analysis});
    return 0;
  }
  if (not $self->is_paired_read) {
    $self->debug(qq{$lstring - Single end run: skipping RNAseq analysis for now}); #TODO: RNAseq should work on single end data
    return 0;
  }
  my $reference_genome = $l->reference_genome();
  my @parsed_ref_genome = $self->_reference($l)->parse_reference_genome($reference_genome);
  my $transcriptome_version = $parsed_ref_genome[$REFERENCE_ARRAY_TVERSION_IDX] // q[];
  if (not $transcriptome_version) {
    $self->debug(qq{$lstring - Reference without transcriptome version: skipping RNAseq analysis});
    return 0;
  }
  $self->debug(qq{$lstring - Do RNAseq analysis....});
  return 1;
}

sub _transcriptome {
  my ($self, $l, $analysis) = @_;
  my $href = {'id_run'     => $l->id_run, #TODO: use lims object?
              'position'   => $l->position,
              'tag_index'  => $l->tag_index,
              ($self->repository ? ('repository' => $self->repository):())
             };
  if (defined $analysis) {
      $href->{'analysis'} = $analysis;
  }
  return npg_tracking::data::transcriptome->new($href);
}

sub _reference {
    my ($self, $l, $aligner) = @_;
    my $href = { 'lims' => $l };
    if (defined $self->repository) {
        $href->{'repository'} = $self->repository;
    }
    if (defined $aligner) {
        $href->{'aligner'} = $aligner;
    }
    return npg_tracking::data::reference->new($href);
}

sub _analysis {
    my ($self, $l) = @_;
    my $lstring = $l->to_string;
    my $reference_genome = $l->reference_genome();
    my @parsed_ref_genome = $self->_reference($l)->parse_reference_genome($reference_genome);
    my $analysis = $parsed_ref_genome[$REFERENCE_ARRAY_ANALYSIS_IDX];
    $self->info(qq[$lstring - Analysis: ] . (defined $analysis ? $analysis : qq[default for $reference_genome]));
    return $analysis;
}

sub _do_bait_stats_analysis {
  my ($self, $l) = @_;
  my $lstring = $l->to_string;
  if(not $self->_ref($l,q(fasta)) or not $l->alignments_in_bam) {
    $self->debug(qq{$lstring - no reference or no alignments set});
    return 0;
  }
  if(not $self->_bait($l)->bait_name){
    $self->debug(qq{$lstring - No bait set});
    return 0;
  }
  if(not $self->_bait($l)->bait_path){
    $self->debug(qq{$lstring - No bait path found});
    return 0;
  }
  $self->debug(qq{$lstring - Doing optional bait stats analysis....});

  return 1;
}

sub _bait{
  my($self,$l) = @_;
  return npg_tracking::data::bait->new (
                {'id_run'     => $l->id_run,
                 'position'   => $l->position,
                 'tag_index'  => $l->tag_index,
                 ( $self->repository ? ('repository' => $self->repository):())
                });
}

sub _has_gbs_plex{
  my ($self, $l) = @_;
  my $lstring = $l->to_string;

  if(not $self->_gbs_plex($l)->gbs_plex_name){
    $self->debug(qq{$lstring - No gbs plex set});
    return 0;
  }
  if(not $self->_gbs_plex($l)->gbs_plex_path){
    $self->logcroak(qq{$lstring - GbS plex set but no gbs plex path found});
  }
  if($l->library_type and $l->library_type !~ /GbS/smx){
    $self->logcroak(qq{$lstring - GbS plex set but library type incompatible});
  }
  $self->debug(qq{$lstring - Doing GbS plex analysis....});

  return 1;
}

sub _gbs_plex{
  my($self,$l) = @_;
  return npg_tracking::data::gbs_plex->new (
                {'id_run'     => $l->id_run,
                 'position'   => $l->position,
                 'tag_index'  => $l->tag_index,
                 ( $self->repository ? ('repository' => $self->repository):())
                });
}


sub _ref {
  my ($self, $l, $aligner) = @_;
  if (!$aligner) {
    $self->logcroak('Aligner missing');
  }
  my $ref_name = $self->_do_gbs_plex_analysis ?
    $l->gbs_plex_name : $l->reference_genome();

  my $ref = $ref_name ? $self->_ref_cache->{$ref_name}->{$aligner} : undef;
  my $lstring = $l->to_string;
  if (!$ref) {
    my $href = { 'aligner' => $aligner, 'lims' => $l, };
    if ($self->repository) {
      $href->{'repository'} = $self->repository;
    }
    my $ruser = $self->_do_gbs_plex_analysis ?
        Moose::Meta::Class->create_anon_class(
            roles => [qw/npg_tracking::data::gbs_plex::find/])->new_object($href) :
        Moose::Meta::Class->create_anon_class(
            roles => [qw/npg_tracking::data::reference::find/])->new_object($href);

    my @refs =  @{$ruser->refs};
    if (!@refs) {
      $self->warn(qq{No reference genome set for $lstring});
    } else {
      if (scalar @refs > 1) {
        if (defined $l->tag_index && $l->tag_index == 0) {
          $self->logwarn(qq{Multiple references for $lstring});
        } else {
          $self->logcroak(qq{Multiple references for $lstring});
        }
      } else {
        $ref = $refs[0];
        if ($ref_name) {
          $self->_ref_cache->{$ref_name}->{$aligner} = $ref;
        }
      }
    }
  }
  if ($ref) {
    $self->info(qq{Reference set for $lstring: $ref});
  }
  return $ref;
}

sub _job_index {
  my ($self, $position, $tag_index) = @_;
  if (!$position) {
    $self->logcroak('Position undefined or zero');
  }
  if (defined $tag_index) {
    return sprintf q{%i%04i}, $position, $tag_index;
  }
  return $position;
}

sub _default_resources {
  my ( $self, $memory ) = @_;
  my $hosts = 1;
  my $num_slots = $self->general_values_conf()->{'seq_alignment_slots'} || $NUM_SLOTS;
  return (join q[ ], npg_pipeline::lsf_job->new(memory => $memory)->memory_spec(), "-R 'span[hosts=$hosts]'", "-n$num_slots");
}

sub _default_human_split_ref {
  my ($self, $aligner, $repos) = @_;

  my $ruser = Moose::Meta::Class->create_anon_class(
          roles => [qw/npg_tracking::data::reference::find/])
          ->new_object({
                         species => q{Homo_sapiens},
                         aligner => $aligner,
                        ($repos ? (q(repository)=>$repos) : ())
                       });

  my $human_ref = $ruser->refs->[0];
  if($aligner eq q{picard}) {
    $human_ref .= q{.dict};
  }

  return $human_ref;
}

sub _is_alt_reference {
    my ($self, $l) = @_;
    my $ref = $self->_ref($l, q{bwa0_6});
    if ($ref) {
      $ref .= q{.alt};
      return -e $ref;
    }
    return;
}

sub _p4_stage2_params_path {
  my ($self, $position) = @_;
  my $path = $self->recalibrated_path;

  if($self->is_multiplexed_lane($position)) {
    $path .= q[/lane] . $position;
  }

  return $path;
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

LSF job creation for alignment

=head1 SUBROUTINES/METHODS

=head2 generate - generates and submits lsf job

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item English

=item Readonly

=item Moose

=item Moose::Meta::Class

=item File::Slurp

=item JSON::XS

=item List::Util

=item List::MoreUtils

=item open

=item st::api::lims

=item npg_tracking::data::reference::find

=item npg_tracking::data::bait

=item npg_tracking::data::gbs_plex

=item npg_tracking::data::transcriptome

=item npg_common::roles::software_location

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

David K. Jackson (david.jackson@sanger.ac.uk)

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2017 Genome Research Ltd

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
