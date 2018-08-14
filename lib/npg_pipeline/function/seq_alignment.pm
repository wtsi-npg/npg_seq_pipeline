package npg_pipeline::function::seq_alignment;

use Moose;
use Moose::Meta::Class;
use namespace::autoclean;
use Readonly;
use File::Slurp;
use File::Basename;
use File::Spec;
use JSON;
use List::Util qw(sum uniq);
use List::MoreUtils qw(all none);
use open q(:encoding(UTF8));

use npg_tracking::data::reference::find;
use npg_tracking::data::transcriptome;
use npg_tracking::data::bait;
use npg_tracking::data::gbs_plex;
use st::api::lims;
use npg_pipeline::function::definition;

use Data::Dumper;

extends q{npg_pipeline::base};
with    q{npg_pipeline::function::util};

our $VERSION  = '0';

Readonly::Scalar my $NUM_SLOTS                    => q(12,16);
Readonly::Scalar my $FS_NUM_SLOTS                 => 4;
Readonly::Scalar my $NUM_HOSTS                    => 1;
Readonly::Scalar my $MEMORY                       => q{32000}; # memory in megabytes
Readonly::Scalar my $MEMORY_FOR_STAR              => q{38000}; # idem
Readonly::Scalar my $FORCE_BWAMEM_MIN_READ_CYCLES => q{101};
Readonly::Scalar my $QC_SCRIPT_NAME               => q{qc};
Readonly::Scalar my $DEFAULT_SJDB_OVERHANG        => q{74};
Readonly::Scalar my $REFERENCE_ARRAY_ANALYSIS_IDX => q{3};
Readonly::Scalar my $REFERENCE_ARRAY_TVERSION_IDX => q{2};
Readonly::Scalar my $DEFAULT_RNA_ANALYSIS         => q{tophat2};
Readonly::Array  my @RNA_ANALYSES                 => qw{tophat2 star salmon};

=head2 phix_reference

A path to Phix reference fasta file to split phiX spike-in reads

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

has 'input_path'      => ( isa        => 'Str',
                           is         => 'ro',
                           lazy_build => 1,
                         );
sub _build_input_path {
  my $self = shift;
  return $self->recalibrated_path();
}

has '_ref_cache' => (isa      => 'HashRef',
                     is       => 'ro',
                     required => 0,
                     default  => sub {return {};},
                    );

has '_job_id' => ( isa        => 'Str',
                   is         => 'ro',
                   lazy_build => 1,
                 );
sub _build__job_id {
  my $self = shift;
  return $self->random_string();
}

has '_num_cpus' => ( isa        => 'ArrayRef',
                     is         => 'ro',
                     lazy_build => 1,
                   );
sub _build__num_cpus {
  my $self = shift;
  return $self->num_cpus2array(
    $self->general_values_conf()->{'seq_alignment_slots'} || $NUM_SLOTS);
}

has '_do_gbs_plex_analysis' => ( isa     => 'Bool',
                                 is      => 'rw',
                                 default => 0,
                               );

sub generate {
  my ( $self ) = @_;

  my @definitions = ();

  ## no critic (ControlStructures::ProhibitUnlessBlocks)
  unless($self->platform_NovaSeq and $self->is_indexed) { # temporary warning
    $self->debug(q{this pipeline is currently intended for NovaSeq pools only});
  }

  my $recal_path = $self->recalibrated_path;
  $self->info(q{  recalibrated_path: } . $recal_path);
  my $archive_path = $self->archive_path;
  $self->info(q{  archive_path: } . $archive_path);
  my $qc_path = $self->qc_path;
  $self->info(q{  qc_path: } . $qc_path);

  for my $lane (@{$self->products->{lanes}}) {
    my $fnr = $lane->file_name_root;
    $self->info(qq{  lanes fnr: $fnr});
  }

  my $ref = {};
  for my $dp (@{$self->products->{data_products}}) {

    my $spiked_phix_tag_index = $dp->lims->spiked_phix_tag_index;
    $self->info(q{  spiked_phix_tag_index: } . (defined $spiked_phix_tag_index? $spiked_phix_tag_index: q[UNDEF]));

    $ref->{'memory'} = $MEMORY; # reset to default
    $ref->{'command'} = $self->_alignment_command($dp, $ref);
    push @definitions, $self->_create_definition($ref, $dp);
  }

  return \@definitions;
}

sub _create_definition {
  my ($self, $ref, $dp) = @_;

  $ref->{'created_by'}      = __PACKAGE__;
  $ref->{'created_on'}      = $self->timestamp();
  $ref->{'identifier'}      = $self->id_run();
  $ref->{'job_name'}        = join q{_}, q{seq_alignment},$self->id_run(),$self->timestamp();
  $ref->{'fs_slots_num'}    = $FS_NUM_SLOTS ;
  $ref->{'num_hosts'}       = $NUM_HOSTS;
  $ref->{'num_cpus'}        = $self->_num_cpus();
  $ref->{'memory'}          = $ref->{'memory'} ? $ref->{'memory'} : $MEMORY;
  $ref->{'command_preexec'} = $self->repos_pre_exec_string();
  $ref->{'composition'}     = $dp->{composition};

  return npg_pipeline::function::definition->new($ref);
}

sub _alignment_command { ## no critic (Subroutines::ProhibitExcessComplexity)
  my ( $self, $dp, $ref) = @_;   # this should be enough?

#######################################################
# fetch base parameters from supplied data_product (dp)
#######################################################
  my $run_vec = [ uniq (map { $_->{id_run} } @{$dp->composition->{components}}) ];
  my $id_run = $run_vec->[0]; # assume unique for the moment
  my $lane_vec = [ uniq (map { $_->{position} } @{$dp->composition->{components}}) ]; # use lane4products?
  my $tags_vec = [ uniq (map { $_->{tag_index} } @{$dp->composition->{components}}) ];
  my $tag_index = $tags_vec->[0]; # assume unique for the moment

  my $is_pool = $dp->{lims}->is_pool;
  my $spike_tag = $dp->{lims}->is_phix_spike;

  my $archive_path= $self->archive_path;
  my $dp_archive_path = $dp->path($self->archive_path);
  my $recal_path= $self->recalibrated_path;
  my $qc_out_path = $dp->qc_out_path($archive_path);
  my $cache10k_path = $dp->short_files_cache_path($self->archive_path);
  my $dp_qc_path = $dp->path($self->qc_path); #??
  my $dp_recal_path = $dp->path($self->recalibrated_path);  #??
  my $reference_genome = $dp->{lims}->reference_genome;
  my $is_tag_zero_product = $dp->is_tag_zero_product;
  $self->info(qq{ is_tag_zero_product: $is_tag_zero_product});

  my @incrams = map { $recal_path . q[/] . $id_run . q[_] . $_ . q[#] . $tag_index . q[.bam] } @{$lane_vec};
  my $incrams_pv = [ map { qq[I=$_] } @incrams ];

  my $s2_filter_files = join q[,], (map { $recal_path . q[/] . $id_run . q[_] . $_ . q[.spatial_filter] } @{$lane_vec} );
  my $spatial_filter_rg_value = join q[,], (map { $id_run . q[_] . $_ . q[#] . $tag_index } @{$lane_vec} );
  my $tag_metrics_files = join q[ ], (map { $archive_path . '/lane' . $_ . q[/qc/] . $id_run . q[_] . $_ . q[.tag_metrics.json] } @{$lane_vec});

  my $name_root = $dp->file_name_root;
  my $working_dir = join q{/}, $archive_path,
                               (join q{_}, q{tmp}, $self->_job_id),
                               $name_root;

  my $rpt_list = $dp->{rpt_list};
  my $bfs_input_file = $dp_archive_path . q[/] . $dp->file_name(ext => 'bam');
  my $af_input_file = $dp->file_name(ext => 'json', suffix => 'bam_alignment_filter_metrics');
  my $fq1_filepath = File::Spec->catdir($cache10k_path, $dp->file_name(ext => 'fastq', suffix => '1'));
  my $fq2_filepath = File::Spec->catdir($cache10k_path, $dp->file_name(ext => 'fastq', suffix => '2'));
  my $fqc1_filepath = File::Spec->catdir($dp_archive_path, $dp->file_name(ext => 'fastqcheck', suffix => '1'));
  my $fqc2_filepath = File::Spec->catdir($dp_archive_path, $dp->file_name(ext => 'fastqcheck', suffix => '2'));

  $self->debug(q{  run_vec: } . join q[,], @{$run_vec});
  $self->debug(q{  lane_vec: } . join q[,], @{$lane_vec});
  $self->debug(q{  tags_vec: } . join q[,], @{$tags_vec});
  $self->debug(qq{  rpt_list: $rpt_list});
  $self->debug(qq{  reference_genome: $reference_genome});
  $self->debug(qq{  is_pool: $is_pool});
  $self->debug(qq{  dp_recal_path: $dp_recal_path});
  $self->debug(qq{  dp_archive_path: $dp_archive_path});
  $self->debug(qq{  dp_qc_path: $dp_qc_path});
  $self->debug(qq{  cache10k_path: $cache10k_path});
  $self->debug(qq{  bfs_input_file: $bfs_input_file});
  $self->debug(qq{  af_input_file: $af_input_file});
#####################

  my $is_plex = defined $tag_index;  #?? TBR
  my $l = $dp->lims;  #??!!  Maybe OK, TBR

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
    samtools_executable => q{samtools},
    outdatadir          => $dp_archive_path,
    subsetsubpath       => q[.npg_cache_10000/], # below outdatadir
    recal_dir           => $recal_path,
    af_metrics          => $af_input_file,
    rpt                 => $name_root,
    phix_reference_genome_fasta => $self->phix_reference,
    s2_id_run => $id_run,
    s2_position => q[POSITION],
    s2_tag_index => $tag_index,
    incrams => $incrams_pv,
    spatial_filter_file => q[DUMMY],
    s2_filter_files => $s2_filter_files,
    spatial_filter_rg_value => $spatial_filter_rg_value,
    tag_metrics_files => $tag_metrics_files,
    s2_se_pe => ($self->is_paired_read)? q{pe} : q{se},
    run_lane_ss_fq1 => $fq1_filepath,
    run_lane_ss_fq2 => $fq2_filepath,
    fqc1 => $fqc1_filepath,
    fqc2 => $fqc2_filepath,
  };
  my $p4_ops = {
    prune => [],
    splice => [],
  };

  if(not $spike_tag) { # remove calibration_pu from all until it copes with multilanes
    push @{$p4_ops->{prune}}, 'fop.*_bmd_multiway:calibration_pu-';
  }
  else {
    push @{$p4_ops->{prune}}, 'fopt.*_bmd_multiway:calibration_pu-';
  }

  if(not $is_plex) {
    push @{$p4_ops->{prune}}, 'ssfqc_tee_ssfqc:subsample-';
    push @{$p4_ops->{prune}}, 'ssfqc_tee_ssfqc:fqc-';
  }

  my $do_rna = $self->_do_rna_analysis($l);

  # Reference for target alignment will be overridden where gbs_plex exists.
  # Also any human split will be overriden and alignments will be forced.
  my $do_gbs_plex = $self->_do_gbs_plex_analysis($self->_has_gbs_plex($rpt_list));

  my $hs_bwa = $self->is_paired_read ? 'bwa_aln' : 'bwa_aln_se';
  # continue to use the "aln" algorithm from bwa for these older chemistries (where read length <= 100bp)
  my $bwa = (($self->platform_MiSeq or $self->is_rapid_run) and (all {$_ < $FORCE_BWAMEM_MIN_READ_CYCLES } $self->read_cycle_counts))
            ? $hs_bwa
            : 'bwa_mem';

  my $human_split = $do_gbs_plex ? q() :
                    $l->contains_nonconsented_xahuman ? q(xahuman) :
                    $l->separate_y_chromosome_data    ? q(yhuman) :
                    q();

  my $is_chromium_lib = $l->library_type && ($l->library_type =~ /Chromium/smx);
  my $do_target_alignment = $is_chromium_lib ? 0
                             : ((not $is_tag_zero_product or $self->align_tag0)
                               && $self->_ref($l,q[fasta])
                               && ($l->alignments_in_bam || $do_gbs_plex));

  $self->info(qq{ do_target_alignment for $name_root is } . ($do_target_alignment?q[TRUE]:q[FALSE]));

  # There will be a new exception to the use of "aln": if you specify a reference
  # with alt alleles e.g. GRCh38_full_analysis_set_plus_decoy_hla, then we will use
  # bwa's "mem"
  $bwa = ($do_target_alignment and $self->_is_alt_reference($l)) ? 'bwa_mem' : $bwa;


  my $skip_target_markdup_metrics = (not $spike_tag and not $do_target_alignment);

  # handle extra stats file for aligned data with reference regions file
  my $do_target_regions_stats = 0;
  if ($do_target_alignment && !$spike_tag && !$human_split && !$do_gbs_plex && !$do_rna) {
    if($self->_do_bait_stats_analysis($rpt_list)){
#      $p4_param_vals->{target_regions_file} = $self->_bait($l)->target_intervals_path();
       $p4_param_vals->{target_regions_file} = $self->_bait($rpt_list)->target_intervals_path();
       $do_target_regions_stats = 1;
    }
    elsif($self->_target_regions_file_path($l, q[target])) {
       $p4_param_vals->{target_regions_file} = $self->_ref($l, q[target]) .q(.interval_list);
       $do_target_regions_stats = 1;
    }
  }
  if($spike_tag) {
    push @{$p4_ops->{prune}}, 'foptgt.*samtools_stats_F0.*_target.*-';
  }
  elsif($do_target_regions_stats) {
    push @{$p4_ops->{prune}}, 'fop(phx|hs)_samtools_stats_F0.*_target.*-';
  }
  elsif( !($human_split and not $do_target_alignment) ){
   push @{$p4_ops->{prune}}, 'fop.*samtools_stats_F0.*_target.*-';
  }

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
  if (not $self->is_paired_read and $nchs) {
    $self->logcroak(qq{only paired reads supported for non-consented human ($name_root)});
  }

  ########
  # no target alignment:
  #  splice out unneeded p4 nodes, add -x flag to scramble,
  #   unset the reference for bam_stats and amend the AlignmentFilter command.
  ########
  if(not $do_target_alignment and not $spike_tag) {
      if(not $nchs) {
        push @{$p4_ops->{splice}}, 'ssfqc_tee_ssfqc:straight_through1:-alignment_filter:phix_bam_in';
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
    if($self->p4s2_aligner_intfile) { $p4_param_vals->{align_intfile_opt} = 1; }
  }
  if($nchs) {
    $p4_param_vals->{reference_dict_hs} = $self->_default_human_split_ref(q{picard}, $self->repository);   # always human default
    $p4_param_vals->{hs_reference_genome_fasta} = $self->_default_human_split_ref(q{fasta}, $self->repository);   # always human default
  }

  # handle targeted stats_(bait_stats_analysis) here, handling the interaction with spike tag case
  my $bait_stats_flag = q[];
  my $spike_splicing = q[];
  if(not $spike_tag) {
    if($self->_do_bait_stats_analysis($rpt_list)) {
#     $p4_param_vals->{bait_regions_file} = $self->_bait($l)->bait_intervals_path();
      $p4_param_vals->{bait_regions_file} = $self->_bait($rpt_list)->bait_intervals_path();
      push @{$p4_ops->{prune}}, 'fop(phx|hs)_samtools_stats_F0.*00_bait.*-';
    }
    else {
      push @{$p4_ops->{prune}}, 'fop.*samtools_stats_F0.*00_bait.*-';
    }
  }
  else {
    push @{$p4_ops->{prune}}, 'foptgt.*samtools_stats_F0.*00_bait.*-';  # confirm hyphen
    push @{$p4_ops->{splice}}, 'ssfqc_tee_ssfqc:straight_through1:-foptgt_bamsort_coord:', 'foptgt_seqchksum_file:-scs_cmp_seqchksum:outputchk';
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
    if (none {$_ eq $rna_analysis} @RNA_ANALYSES){
        $self->info($l->to_string . qq[- Unsupported RNA analysis: $rna_analysis - running $DEFAULT_RNA_ANALYSIS instead]);
        $rna_analysis = $DEFAULT_RNA_ANALYSIS;
    }
    my $p4_reference_genome_index;
    if($rna_analysis eq q[star]) {
      # most common read length used for RNA-Seq is 75 bp so indices were generated using sjdbOverhang=74
      $p4_param_vals->{sjdb_overhang_val} = $DEFAULT_SJDB_OVERHANG;
      $p4_param_vals->{star_executable} = q[star];
      $p4_reference_genome_index = dirname($self->_ref($l, q(star)));
      # star jobs require more memory
      $ref->{'memory'} = $MEMORY_FOR_STAR;
    } elsif ($rna_analysis eq q[tophat2]) {
      $p4_param_vals->{library_type} = ( $l->library_type =~ /dUTP/smx ? q(fr-firststrand) : q(fr-unstranded) );
      $p4_param_vals->{transcriptome_val} = $self->_transcriptome($l, q(tophat2))->transcriptome_index_name();
      $p4_reference_genome_index = $self->_ref($l, q(bowtie2));
    }
    $p4_param_vals->{alignment_method} = $rna_analysis;
    $p4_param_vals->{annotation_val} = $self->_transcriptome($l)->gtf_file();
    $p4_param_vals->{quant_method} = q[salmon];
    $p4_param_vals->{salmon_transcriptome_val} = $self->_transcriptome($l, q(salmon))->transcriptome_index_path();
    $p4_param_vals->{alignment_reference_genome} = $p4_reference_genome_index;
    # create intermediate file to prevent deadlock
    $p4_param_vals->{align_intfile_opt} = 1;
    if($nchs) {
      # this human split alignment method is currently the same as the default, but this may change
      $p4_param_vals->{hs_alignment_reference_genome} = $self->_default_human_split_ref(q{bwa0_6}, $self->repository);
      $p4_param_vals->{alignment_hs_method} = $hs_bwa;
    }
    if(not $self->is_paired_read) {
      $p4_param_vals->{alignment_reads_layout} = 1;
    }
  }
  else {
    my ($organism, $strain, $tversion, $analysis) = $self->_reference($l)->parse_reference_genome($l->reference_genome);

    $p4_param_vals->{bwa_executable} = q[bwa0_6];
    $p4_param_vals->{alignment_method} = ($analysis || $bwa);

    my %methods_to_aligners = (
      bwa_aln => q[bwa0_6],
      bwa_aln_se => q[bwa0_6],
      bwa_mem => q[bwa0_6],
    );
    my %ref_suffix = (
      picard => q{.dict},
      minimap2 => q{.mmi},
    );

    my $aligner = $p4_param_vals->{alignment_method};
    if(exists $methods_to_aligners{$p4_param_vals->{alignment_method}}) {
      $aligner = $methods_to_aligners{$aligner};
    }

    if($do_target_alignment) { $p4_param_vals->{alignment_reference_genome} = $self->_ref($l,$aligner); }
    if(exists $ref_suffix{$aligner}) {
      $p4_param_vals->{alignment_reference_genome} .= $ref_suffix{$aligner};
    }

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
  my $param_vals_fname = join q{/}, $self->_p4_stage2_params_path(q[POSITION]),
                                    $name_root.q{_p4s2_pv_in.json};
  write_file($param_vals_fname, encode_json(
    { assign => [ $p4_param_vals ], assign_local => $p4_local_assignments, ops => $p4_ops }));

  ####################
  # log run parameters
  ####################
  $self->info(q[Using p4]);
  if(not $self->is_paired_read) { $self->info(q[  single-end]) }

  my %info = (
               do_target_alignment         => $do_target_alignment,
               is_chromium_lib             => $is_chromium_lib,
               skip_target_markdup_metrics => $skip_target_markdup_metrics,
               spike_tag                   => $spike_tag,
               nonconsented_humansplit     => $nchs,
               do_gbs_plex                 => $do_gbs_plex,
	       do_rna                      => $do_rna,
             );
  while (my ($text, $value) = each %info) {
    $self->info(qq[  $text is ] . ($value ? q[true] : q[false]));
  }

  $self->info(q[  human_split is ] . ($human_split ? $human_split : q[none]));
  $self->info(q[  p4 parameters written to ] . $param_vals_fname);
  $self->info(q[  Using p4 template alignment_wtsi_stage2_] . $nchs_template_label . q[template.json]);

  my $num_threads_expression = q[npg_pipeline_job_env_to_threads --num_threads ] . $self->_num_cpus->[0];
  my $id = $self->_job_id();
  return join q( ),
    q(bash -c '),
    q(mkdir -p), $working_dir, q{;},
    q(cd), $working_dir,
    q{&&},

    q(vtfp.pl),
    q{-template_path $}.q{(dirname $}.q{(readlink -f $}.q{(which vtfp.pl)))/../data/vtlib},
    q(-param_vals), $param_vals_fname,
    q(-export_param_vals), qq(${name_root}_p4s2_pv_out_${id}.json),
    q{-keys cfgdatadir -vals $}.q{(dirname $}.q{(readlink -f $}.q{(which vtfp.pl)))/../data/vtlib/},
    qq(-keys aligner_numthreads -vals `$num_threads_expression`),
    qq(-keys br_numthreads_val -vals `$num_threads_expression --exclude 1 --divide 2`),
    qq(-keys b2c_mt_val -vals `$num_threads_expression --exclude 2 --divide 2`),
    q{$}.q{(dirname $}.q{(dirname $}.q{(readlink -f $}.q{(which vtfp.pl))))/data/vtlib/alignment_wtsi_stage2_}.$nchs_template_label.q{template.json},
    qq(> run_$name_root.json),
    q{&&},
    qq(viv.pl -s -x -v 3 -o viv_$name_root.log run_$name_root.json ),
    q{&&},
    _qc_command_alt('bam_flagstats', $dp_archive_path, $qc_out_path, $l, $is_plex, undef,
                $skip_target_markdup_metrics, $rpt_list, $name_root, [$bfs_input_file]),
    (grep {$_}
      ($spike_tag ? q() : (join q( ),
        q{&&},
        _qc_command_alt('bam_flagstats', $dp_archive_path, $qc_out_path, $l, $is_plex, 'phix', undef, $rpt_list, $name_root, [$bfs_input_file]),
        q{&&},
        _qc_command_alt('alignment_filter_metrics', undef, $qc_out_path, $l, $is_plex, undef, undef, $rpt_list, $name_root, [$af_input_file]),
      ),

      $human_split ? (join q( ),
        q{&&},
        _qc_command_alt('bam_flagstats', $dp_archive_path, $qc_out_path, $l, $is_plex, $human_split, undef, $rpt_list, $name_root, [$bfs_input_file]),
      ) : q()),

      $nchs ? (join q( ),
        q{&&},
        _qc_command_alt('bam_flagstats', $dp_archive_path, $qc_out_path, $l, $is_plex, $nchs_outfile_label, undef, $rpt_list, $name_root, [$bfs_input_file]),
      ) : q(),

      $do_rna ? (join q( ),
        q{&&},
        _qc_command('rna_seqc', $dp_archive_path, $qc_out_path, $l, $is_plex),
      ) : q(),

      $do_gbs_plex ? (join q( ),
        q{&&},
        _qc_command('genotype_call', $dp_archive_path, $qc_out_path, $l, $is_plex),
      ) : q()
    ),

    q(');
}

sub _qc_command_alt {##no critic (Subroutines::ProhibitManyArgs)
  my ($check_name, $qc_in, $qc_out, $l, $is_plex, $subset, $skip_markdups_metrics, $rpt_list, $filename_root, $input_files) = @_;

  my $args = {
               'rpt_list' => q["] . $rpt_list . q["],
               'filename_root' => $filename_root . ($subset? qq[_$subset]: q[]),
               'qc_out' => $qc_out,
              'check' => $check_name,};
  my @flags = ();

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

  for my $input_file (@{$input_files}) {
    $command .= qq[ --input_files $input_file];
  }

  if(@flags) {
    $command .= q[ ];
    $command .= join q[ ], @flags;
  }

  return $QC_SCRIPT_NAME . $command;
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

sub _do_rna_analysis {
  my ($self, $l) = @_;
  my $lstring = $l->to_string;

  my $analysis    = $self->_analysis($l) // q[];
  my $rna_aligner = $analysis ?  (grep { /^$analysis$/sxm } @RNA_ANALYSES) : q[];

  if (!$l->library_type || $l->library_type !~ /(?:(?:cD|R)NA|DAFT)/sxm) {
    if ($l->library_type && $rna_aligner) {
      $self->debug(qq{$lstring - over-riding library type with rna aligner $analysis});
    }
    else {
      $self->debug(qq{$lstring - not RNA library type: skipping RNAseq analysis});
      return 0;
    }
  }
  my $reference_genome = $l->reference_genome();
  my @parsed_ref_genome = $self->_reference($l)->parse_reference_genome($reference_genome);
  my $transcriptome_version = $parsed_ref_genome[$REFERENCE_ARRAY_TVERSION_IDX] // q[];
  if (not $transcriptome_version) {
    if($rna_aligner) {
       $self->logcroak(qq{$lstring - not possible to run an rna aligner without a transcriptome});
    }
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
    my $reference_genome = $l->reference_genome() // q[];
    my @parsed_ref_genome = $self->_reference($l)->parse_reference_genome($reference_genome);
    my $analysis = $parsed_ref_genome[$REFERENCE_ARRAY_ANALYSIS_IDX];
    $self->info(qq[$lstring - Analysis: ] . (defined $analysis ? $analysis : qq[default for $reference_genome]));
    return $analysis;
}

sub _do_bait_stats_analysis {
# my ($self, $l) = @_;
  my ($self, $rpt_list) = @_;
# my $lstring = $l->to_string;
  my $lstring = $rpt_list;

################################
# disable this check temporarily
################################
##if(not $self->_ref($l,q(fasta)) or not $l->alignments_in_bam or
##   (defined $l->tag_index && $l->tag_index == 0)) {
##  $self->debug(qq{$lstring - no reference or no alignments set});
##  return 0;
##}
################################
################################
# if(not $self->_bait($l)->bait_name){
  if(not $self->_bait($rpt_list)->bait_name){
    $self->debug(qq{$lstring - No bait set});
    return 0;
  }
# if(not $self->_bait($l)->bait_path){
  if(not $self->_bait($rpt_list)->bait_path){
    $self->debug(qq{$lstring - No bait path found});
    return 0;
  }
  $self->debug(qq{$lstring - Doing optional bait stats analysis....});

  return 1;
}

sub _bait{
# my($self,$l) = @_;
  my($self,$rpt_list) = @_;
  return npg_tracking::data::bait->new (
                {
                 'rpt_list' => $rpt_list,
#                'id_run'     => $l->id_run,
#                'position'   => $l->position,
#                'tag_index'  => $l->tag_index,
                 ( $self->repository ? ('repository' => $self->repository):())
                });
}

sub _has_gbs_plex{
# my ($self, $l) = @_;
  my ($self, $rpt_list) = @_;
# my $lstring = $l->to_string;
  my $lstring = $rpt_list;

# if(not $self->_gbs_plex($l)->gbs_plex_name){
  if(not $self->_gbs_plex($rpt_list)->gbs_plex_name){
    $self->debug(qq{$lstring - No gbs plex set});
    return 0;
  }
# if(not $self->_gbs_plex($l)->gbs_plex_path){
  if(not $self->_gbs_plex($rpt_list)->gbs_plex_path){
    $self->logcroak(qq{$lstring - GbS plex set but no gbs plex path found});
  }
########################################
# disable library_type check temporarily
########################################
##if($l->library_type and $l->library_type !~ /^GbS/ismx){
##  $self->logcroak(qq{$lstring - GbS plex set but library type incompatible});
##}
########################################
########################################
  $self->debug(qq{$lstring - Doing GbS plex analysis....});

  return 1;
}

sub _gbs_plex{
# my($self,$l) = @_;
  my($self,$rpt_list) = @_;
  return npg_tracking::data::gbs_plex->new (
                {
                 'rpt_list' => $rpt_list,
#                'id_run'     => $l->id_run,
#                'position'   => $l->position,
#                'tag_index'  => $l->tag_index,
                 ( $self->repository ? ('repository' => $self->repository):())
                });
}

sub _target_regions_file_path {
  my ($self, $l, $aligner) = @_;
  if (!$aligner) {
    $self->logcroak('Aligner missing');
  }
  my $path = 1;
  eval  { $self->_ref($l, $aligner) ; 1; }
  or do { $path = 0; };
  return $path;
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
    my $role  = $self->_do_gbs_plex_analysis ? 'gbs_plex' : 'reference';
    my $ruser = Moose::Meta::Class->create_anon_class(
            roles => ["npg_tracking::data::${role}::find"])->new_object($href);
    my @refs = @{$ruser->refs};
    if (!@refs) {
      $self->warn(qq{No reference genome set for $lstring});
    } else {
      if (scalar @refs > 1) {
        my $m = qq{Multiple references for $lstring};
        (defined $l->tag_index && $l->tag_index == 0)
          ? $self->logwarn($m) : $self->logcroak($m);
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

# temporarily dump all p4s2 params files in no_cal (ignore position)
# if($self->is_multiplexed_lane($position)) {
#   $path .= q[/lane] . $position;
# }

  return $path;
}

__PACKAGE__->meta->make_immutable;

1;

__END__


=head1 NAME

npg_pipeline::function::seq_alignment

=head1 SYNOPSIS

  my $sa = npg_pipeline::function::seq_alignment->new(
    run_folder => $sRunFolder,
  );

=head1 DESCRIPTION

Definition creation for alignment, split by Phix and Human reads
and some QC checks.

=head1 SUBROUTINES/METHODS

=head2 generate

Creates and returns an array of npg_pipeline::function::definition
objects for all entities of the run eligible for alignment and split.

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Readonly

=item Moose

=item Moose::Meta::Class

=item namespace::autoclean

=item File::Slurp

=item JSON

=item List::Util

=item List::MoreUtils

=item open

=item st::api::lims

=item npg_tracking::data::reference::find

=item npg_tracking::data::bait

=item npg_tracking::data::transcriptome

=item npg_tracking::data::gbs_plex

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

David K. Jackson (david.jackson@sanger.ac.uk)

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
