package npg_pipeline::function::seq_alignment;

use Moose;
use Moose::Meta::Class;
use namespace::autoclean;
use Readonly;
use File::Slurp;
use File::Basename;
use File::Spec;
use JSON;
use List::Util qw(sum uniq all none any);
use open q(:encoding(UTF8));
use Try::Tiny;

use npg_tracking::data::reference;
use npg_pipeline::cache::reference::constants qw( $TARGET_REGIONS_DIR $TARGET_AUTOSOME_REGIONS_DIR );
use npg_tracking::data::transcriptome;
use npg_tracking::data::bait;
use npg_tracking::data::gbs_plex;
use npg_pipeline::cache::reference;

extends q{npg_pipeline::base_resource};
with    qw{ npg_pipeline::function::util
            npg_pipeline::product::release };

our $VERSION  = '0';

Readonly::Scalar my $FORCE_BWAMEM_MIN_READ_CYCLES => q{101};
Readonly::Scalar my $QC_SCRIPT_NAME               => q{qc};
Readonly::Scalar my $DEFAULT_SJDB_OVERHANG        => q{74};
Readonly::Scalar my $REFERENCE_ARRAY_ANALYSIS_IDX => q{3};
Readonly::Scalar my $REFERENCE_ARRAY_TVERSION_IDX => q{2};
Readonly::Scalar my $DEFAULT_RNA_ANALYSIS         => q{tophat2};
Readonly::Array  my @RNA_ANALYSES                 => qw{tophat2 star hisat2};
Readonly::Scalar my $PFC_MARKDUP_OPT_DIST         => q{2500};  # distance in pixels for optical duplicate detection on patterned flowcells
Readonly::Scalar my $NON_PFC_MARKDUP_OPT_DIST     => q{100};   # distance in pixels for optical duplicate detection on non-patterned flowcells
Readonly::Scalar my $BWA_MEM_MISMATCH_PENALTY     => q{5};
Readonly::Scalar my $SKIP_MARKDUP_METRICS         => 1;

around 'markdup_method' => sub {
    my $orig = shift;
    my $self = shift;

    my $product = shift;
    $product or $self->logcroak('Product object argument is required');
    my $lims = $product->lims;
    $lims or $self->logcroak('lims object is not defined for a product');
    my $lt = $lims->library_type;
    $lt ||= q[];
    # I've restricted this to library_types which exactly match Duplex-Seq to exclude the old library_type Bidirectional Duplex-seq
    # the Duplex-Seq library prep has been replaced by the NanoSeq library prep, the analysis is the same and the Duplex-Seq library_type is still in use
    # I've added two new library_types Targeted NanoSeq Pulldown Twist and Targeted NanoSeq Pulldown Agilent
    my $mdm =  ($lt eq q[Duplex-Seq] || $lt =~ /^Targeted\sNanoSeq\sPulldown/smx) ? q(duplexseq) : $self->$orig($product);
    $mdm or $self->logcroak('markdup method is not defined for a product');

    return $mdm;
};

has 'phix_reference' => (isa        => 'Str',
                         is         => 'ro',
                         required   => 0,
                         lazy_build => 1,
                        );
sub _build_phix_reference {
  my $self = shift;
  return npg_tracking::data::reference->new({
    species => q{PhiX},
    aligner => q{fasta},
    ($self->repository ? (q(repository)=>$self->repository) : ())
  })->refs->[0];
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

has '_js_scripts_dir' => ( isa        => 'Str',
                           is         => 'ro',
                           required   => 0,
                           lazy_build => 1,
                         );
sub _build__js_scripts_dir {
  my $self = shift;
  return $ENV{'NPG_PIPELINE_JS_SCRIPTS_DIR'}
         || $self->general_values_conf()->{'js_scripts_directory'}
         || $ENV{'NPG_PIPELINE_SCRIPTS_DIR'}
         || $self->general_values_conf()->{'scripts_directory'};
}

has '_do_gbs_plex_analysis' => ( isa     => 'Bool',
                                 is      => 'rw',
                                 default => 0,
                               );

# Used to cache the analysis type determined in _alignment_command for
# later use
has '_rna_analysis' => (
  isa => 'Str',
  is => 'rw'
);


sub generate {
  my ($self, $pipeline_name) = @_;

  my @definitions = ();

  for my $dp (@{$self->products->{data_products}}) {
    my $ref = {};
    my $subsets = [];
    $ref->{'command'} = $self->_alignment_command($dp, $ref, $subsets);
    $self->_save_compositions($dp, $subsets);
    push @definitions, $self->_create_definition($ref, $dp);
  }

  return \@definitions;
}

sub _save_compositions {
  my ($self, $dp, $subsets) = @_;
  my @products = map { $dp->subset_as_product($_) } @{$subsets};
  push @products, $dp;
  foreach my $p (@products) {
    write_file(
      $p->file_path($p->path($self->archive_path), ext => 'composition.json'),
      $p->composition->freeze(with_class_names => 1));
  }
  return;
}

sub _create_definition {
  my ($self, $ref, $dp) = @_;

  $ref->{'job_name'}        = join q{_}, q{seq_alignment},$self->id_run(),$self->timestamp();
  $ref->{'command_preexec'} = $self->repos_pre_exec_string();
  $ref->{'composition'}     = $dp->composition;

  my $special_resource_type;
  if ($self->_rna_analysis && $self->_rna_analysis eq 'star') {
    $special_resource_type = $self->_rna_analysis;
  }

  return $self->create_definition($ref, $special_resource_type);
}

sub _alignment_command { ## no critic (Subroutines::ProhibitExcessComplexity)
  my ( $self, $dp, $ref, $subsets ) = @_;

  ########################################################
  # derive base parameters from supplied data_product (dp)
  ########################################################
  my $rpt_list = $dp->rpt_list;
  my $is_pool = $dp->lims->is_pool;
  my $spike_tag = $dp->lims->is_phix_spike;
  my $reference_genome = $dp->lims->reference_genome || q[UNSPEC];
  my $is_tag_zero_product = $dp->is_tag_zero_product;
  my $run_vec = [ uniq (map { $_->{id_run} } @{$dp->composition->{components}}) ];
  my $id_run = $run_vec->[0]; # assume unique for the moment
  my $tags_vec = [ uniq (map { $_->{tag_index} } @{$dp->composition->{components}}) ];
  my $tag_index = $tags_vec->[0]; # assume unique for the moment
  my $is_plex = defined $tag_index;

  my $archive_path = $self->archive_path;
  my $dp_archive_path = $dp->path($archive_path);
  my $recal_path= $self->recalibrated_path; #?
  my $uses_patterned_flowcell = $self->uses_patterned_flowcell;

  my $qc_out_path = $dp->qc_out_path($archive_path);
  my $cache10k_path = $dp->short_files_cache_path($archive_path);

  if (!$spike_tag) {
    push @{$subsets}, 'phix';
  }

  my (@incrams, @spatial_filter_rg_value);
  for my $elem ($dp->components_as_products) {
    $self->debug(q{  rpt_elem (component): }.$elem->rpt_list);
    push @incrams, File::Spec->catdir($recal_path, $elem->file_name(ext => $self->s1_s2_intfile_format));
    push @spatial_filter_rg_value, $elem->file_name_root;
  }
  my $spatial_filter_rg_value = join q[,], @spatial_filter_rg_value;

  my (@s2_filter_files,@tag_metrics_files);
  for my $ldp ($dp->lanes_as_products) {
    my $ldp_archive_path = $ldp->path($archive_path);
    my $ldp_qc_path = $ldp->qc_out_path($archive_path);
    $self->debug(q{  rpt_elem (lane): }.$ldp->rpt_list);
    push @s2_filter_files, File::Spec->catdir($recal_path, $ldp->file_name(ext => 'spatial_filter'));
    push @tag_metrics_files, File::Spec->catdir($ldp_qc_path, $ldp->file_name(ext => 'tag_metrics.json', ));
  }
  my $s2_filter_files = join q[,], @s2_filter_files;
  my $tag_metrics_files = join q[ ], @tag_metrics_files;

  my $name_root = $dp->file_name_root;
  my $working_dir = join q{/}, $archive_path,
                               (join q{_}, q{tmp}, $self->_job_id),
                               $name_root;

  my $bfs_input_file = $dp_archive_path . q[/] . $dp->file_name(ext => 'bam');
  my $cfs_input_file = $dp_archive_path . q[/] . $dp->file_name(ext => 'cram');
  my $af_input_file = $dp->file_name(ext => 'json', suffix => 'bam_alignment_filter_metrics');
  my $fq1_filepath = File::Spec->catdir($cache10k_path, $dp->file_name(ext => 'fastq', suffix => '1'));
  my $fq2_filepath = File::Spec->catdir($cache10k_path, $dp->file_name(ext => 'fastq', suffix => '2'));
  my $seqchksum_orig_file = File::Spec->catdir($dp_archive_path, $dp->file_name(ext => 'orig.seqchksum'));

  $self->debug(qq{  rpt_list: $rpt_list});
  $self->debug(qq{  reference_genome: $reference_genome});
  $self->debug(qq{  is_tag_zero_product: $is_tag_zero_product});
  $self->debug(qq{  is_pool: $is_pool});
  $self->debug(qq{  dp_archive_path: $dp_archive_path});
  $self->debug(qq{  uses_patterned_flowcell: $uses_patterned_flowcell});
  $self->debug(qq{  cache10k_path: $cache10k_path});
  $self->debug(qq{  bfs_input_file: $bfs_input_file});
  $self->debug(qq{  cfs_input_file: $cfs_input_file});
  $self->debug(qq{  af_input_file: $af_input_file});

  my $l = $dp->lims;

  ################################################
  # check for illegal analysis option combinations
  ################################################
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
    incrams => \@incrams,
    spatial_filter_file => q[DUMMY],
    s2_filter_files => $s2_filter_files,
    spatial_filter_rg_value => $spatial_filter_rg_value,
    tag_metrics_files => $tag_metrics_files,
    s2_se_pe => ($self->is_paired_read)? q{pe} : q{se},
    run_lane_ss_fq1 => $fq1_filepath,
    run_lane_ss_fq2 => $fq2_filepath,
    seqchksum_orig_file => $seqchksum_orig_file,
    s2_input_format => $self->s1_s2_intfile_format,
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
  }

  if($self->platform_NovaSeq) {  # skip spatial filter
    $p4_param_vals->{spatial_filter_switch} = q[off];
  }

  my $do_rna = $self->_do_rna_analysis($dp);

  # Reference for target alignment will be overridden where gbs_plex exists 
  # and the study is not disable the gbs pipeline in the product release config.
  # In the gbs pipeline any human split will be overridden and alignments will be forced.
  my $do_gbs_plex = $self->_do_gbs_plex_analysis(!$is_tag_zero_product && $self->can_run_gbs($dp) && $self->_has_gbs_plex($dp));

  my $hs_bwa = $self->is_paired_read ? 'bwa_aln' : 'bwa_aln_se';
  # continue to use the "aln" algorithm from bwa for these older chemistries (where read length <= 100bp)
  my $bwa = (($self->platform_MiSeq or $self->is_rapid_run) and (all {$_ < $FORCE_BWAMEM_MIN_READ_CYCLES } $self->read_cycle_counts))
            ? $hs_bwa
            : 'bwa_mem';

  my $human_split = $do_gbs_plex ? q() :
                    $l->contains_nonconsented_xahuman ? q(xahuman) :
                    $l->separate_y_chromosome_data    ? q(yhuman) :
                    q();

  my $is_haplotag_lib = $l->library_type && ($l->library_type =~ /Haplotagging/smx);
  if($is_haplotag_lib) {
    $p4_param_vals->{haplotag_processing} = q[on];
    # the samhaplotag tool was developed with data from an i5 "rev comp workflow",
    # its --revcomp flag allows it to work with "standard workflow" i5, as would be
    # obtained from MiSeq or NovaSeq V1.0 reagents, hence the "not"
    if(not $self->is_i5opposite) {
      $p4_param_vals->{ht_revcomp_flag} = q[on];
    }
  }

  my $is_chromium_lib = $l->library_type && ($l->library_type =~ /Chromium/smx);
  my $do_target_alignment = $is_chromium_lib ? 0
                             : ((not $is_tag_zero_product or $self->align_tag0)
                               && $self->_ref($dp, q[fasta])
                               && ($l->alignments_in_bam || $do_gbs_plex));

  $self->info(qq{ do_target_alignment for $name_root is } . ($do_target_alignment?q[TRUE]:q[FALSE]));

  # There will be a new exception to the use of "aln": if you specify a reference
  # with alt alleles e.g. GRCh38_full_analysis_set_plus_decoy_hla and bwakit postalt
  # processing is enabled, then we will use bwa's "mem" with post-processing using
  # the bwa-postalt.js script from bwakit
  if($do_target_alignment and ($self->bwakit or $self->bwakit_enable($dp))) { # two ways to specify bwakit?
    if((my $alt_ref = $self->_alt_reference($dp))) {
      $p4_param_vals->{alignment_method} = $bwa = 'bwa_mem_bwakit';
      $p4_param_vals->{fa_alt_path} = $alt_ref;
      $p4_param_vals->{js_dir} = $self->_js_scripts_dir;
    }
    else {
      $self->info(q[bwakit postalt processing specified, but no alternate haplotypes in reference]);
    }
  }

  my $skip_target_markdup_metrics = ($spike_tag or not $do_target_alignment);

  if($human_split and not $do_target_alignment and not $spike_tag) {
    # human_split needs alignment. The final_output_prep_no_y_target parameter specifies a p4 template
    #  which will undo the alignment from the target product after the split has been done.

    $do_target_alignment = 1;
    $skip_target_markdup_metrics = 1;

    $p4_param_vals->{final_output_prep_no_y_target} = q[final_output_prep_chrsplit_noaln.json];
  }

  # handle extra stats file for aligned data with reference regions file
  my $do_target_regions_stats = 0;
  if ($do_target_alignment && !$spike_tag && !$human_split && !$do_gbs_plex && !$do_rna) {
    if($self->_do_bait_stats_analysis($dp)){
       $p4_param_vals->{target_regions_file} = $self->_bait($rpt_list)->target_intervals_path();
       push @{$p4_ops->{prune}}, 'foptgt.*samtools_stats_F0.*_target_autosome.*-';
       $do_target_regions_stats = 1;
    }
    else {
      my $target_path = $self->_ref($dp, $TARGET_REGIONS_DIR);
      my $target_autosome_path = $self->_ref($dp, $TARGET_AUTOSOME_REGIONS_DIR);
      if ($target_path) {
        $p4_param_vals->{target_regions_file} = $target_path.q(.interval_list);
        $do_target_regions_stats = 1;
        if ($target_autosome_path) {
           $p4_param_vals->{target_autosome_regions_file} = $target_autosome_path.q(.interval_list);
        } else {
           push @{$p4_ops->{prune}}, 'foptgt.*samtools_stats_F0.*_target_autosome.*-';
        }
      }
    }
  }
  if($spike_tag) {
    push @{$p4_ops->{prune}}, 'foptgt.*samtools_stats_F0.*_target.*-';
  }
  elsif($do_target_regions_stats) {
    push @{$p4_ops->{prune}}, 'fop(phx|hs)_samtools_stats_F0.*_target.*-';
  }
  else {
   push @{$p4_ops->{prune}}, 'fop.*samtools_stats_F0.*_target.*-';
  }

  my $nchs = $l->contains_nonconsented_human;
  my $nchs_template_label = $nchs? q{humansplit_}: q{};
  my $nchs_outfile_label = $nchs? q{human}: q{};

  #TODO: allow for an analysis genuinely without phix and where no phiX split work is wanted - especially the phix spike plex....
  #TODO: support these various options below in P4 analyses
  if (not $self->is_paired_read and $nchs) {
    $self->info(qq{single-end and non-consented human ($name_root)});
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
    $p4_param_vals->{reference_dict} = $self->_ref($dp, q(picard)) . q(.dict);
    $p4_param_vals->{reference_genome_fasta} = $self->_ref($dp, q(fasta));
    if($self->p4s2_aligner_intfile) { $p4_param_vals->{align_intfile_opt} = 1; }
    $p4_param_vals->{markdup_method} = $do_gbs_plex ? q[none] : $self->markdup_method($dp);
    $p4_param_vals->{markdup_optical_distance_value} = ($uses_patterned_flowcell? $PFC_MARKDUP_OPT_DIST: $NON_PFC_MARKDUP_OPT_DIST);

    if($p4_param_vals->{markdup_method} eq q[none]) {
      $skip_target_markdup_metrics = 1;

      if(my $pcb=npg_pipeline::cache::reference->instance->get_primer_panel_bed_file($dp, $self->repository)) {
        $p4_param_vals->{primer_clip_bed} = $pcb;
        $self->info(qq[No markdup with primer panel: $pcb]);
      }
      elsif($do_gbs_plex &&
            (my $gbb=npg_pipeline::cache::reference->instance->get_gbs_plex_bed_file($dp, $self->repository))) {
        $p4_param_vals->{primer_clip_bed} = $gbb;
        $self->info(qq[No markdup with gbs primer panel : $gbb]);
      }
      else {
        $p4_param_vals->{primer_clip_method} = q[no_clip];
        $self->info(q[No markdup, no primer panel]);
      }
    }
  }
  elsif(!$do_rna && !$nchs && !$spike_tag && !$human_split && !$is_chromium_lib) {
      push @{$p4_ops->{prune}}, 'fop.*_bmd_multiway:bam-';
  }

  if($nchs) {
    $p4_param_vals->{reference_dict_hs} = $self->_default_human_split_ref(q{picard}, $self->repository);   # always human default
    $p4_param_vals->{hs_reference_genome_fasta} = $self->_default_human_split_ref(q{fasta}, $self->repository);   # always human default
  }

  if(not $self->is_paired_read) {
    # override default markdup method for single read runs as we experience  
    # occasional hangs using default (biobambam)
    $p4_param_vals->{markdup_method} = q[samtools];
    $self->info(q[Overriding markdup method for single-end, always use samtools]);
  }

  # handle targeted stats_(bait_stats_analysis) here, handling the interaction with spike tag case
  if(not $spike_tag) {
    if($self->_do_bait_stats_analysis($dp)) {
      $p4_param_vals->{bait_regions_file} = $self->_bait($rpt_list)->bait_intervals_path();
      push @{$p4_ops->{prune}}, 'fop(phx|hs)_samtools_stats_F0.*00_bait.*-';
    }
    else {
      push @{$p4_ops->{prune}}, 'fop.*samtools_stats_F0.*00_bait.*-';
    }
  }
  else {
    push @{$p4_ops->{prune}}, 'foptgt.*samtools_stats_F0.*00_bait.*-';  # confirm hyphen
    if($p4_param_vals->{markdup_method} and ($p4_param_vals->{markdup_method} eq q[samtools] or $p4_param_vals->{markdup_method} eq q[picard])) {
      push @{$p4_ops->{splice}}, 'ssfqc_tee_ssfqc:straight_through1:-foptgt_000_fixmate:', 'foptgt_000_markdup', 'foptgt_seqchksum_file:-scs_cmp_seqchksum:outputchk'; # the fixmate node only works for mardkup_method samtools (pending p4 node id uniqueness bug fix)
    }
    else {
      push @{$p4_ops->{splice}}, 'ssfqc_tee_ssfqc:straight_through1:-foptgt_000_bamsort_coord:', 'foptgt_000_bammarkduplicates', 'foptgt_seqchksum_file:-scs_cmp_seqchksum:outputchk';
    }
  }

  my $p4_local_assignments = {};
  if($do_gbs_plex){
     $p4_param_vals->{bwa_executable}   = q[bwa0_6];
     $p4_param_vals->{bsc_executable}   = q[bamsort];
     $p4_param_vals->{alignment_method} = $bwa;
     $p4_param_vals->{alignment_reference_genome} = $self->_ref($dp, q(bwa0_6));
     $p4_local_assignments->{'final_output_prep_target'}->{'scramble_embed_reference'} = q[1];
     $skip_target_markdup_metrics = 1;
  }
  elsif($do_rna) {
    my $rna_analysis = $self->_analysis($l->reference_genome, $rpt_list) // $DEFAULT_RNA_ANALYSIS;
    if (none {$_ eq $rna_analysis} @RNA_ANALYSES){
        $self->info($l->to_string . qq[- Unsupported RNA analysis: $rna_analysis - running $DEFAULT_RNA_ANALYSIS instead]);
        $rna_analysis = $DEFAULT_RNA_ANALYSIS;
    }
    $self->_rna_analysis($rna_analysis);
    my $p4_reference_genome_index = $rna_analysis eq q[tophat2] ?
                                    $self->_ref($dp, q(bowtie2)) : $self->_ref($dp, $rna_analysis);
    if($rna_analysis eq q[star]) {
      # most common read length used for RNA-Seq is 75 bp so indices were generated using sjdbOverhang=74
      $p4_param_vals->{sjdb_overhang_val} = $DEFAULT_SJDB_OVERHANG;
      $p4_param_vals->{star_executable} = q[star];
      # STAR uses the name of the directory where the index resides only
      $p4_reference_genome_index = dirname($p4_reference_genome_index);
    } elsif ($rna_analysis eq q[tophat2]) {
      $p4_param_vals->{library_type} = ( $l->library_type =~ /dUTP/smx ? q(fr-firststrand) : q(fr-unstranded) );
      $p4_param_vals->{transcriptome_val} = $self->_transcriptome($rpt_list, q(tophat2))->transcriptome_index_name();
    } elsif ($rna_analysis eq q[hisat2]) {
      $p4_param_vals->{hisat2_executable} = q[hisat2];
      # akin to TopHat2's library_type but HISAT2 also considers
      # if the reads are se or pe to determine value of this parameter
      $p4_param_vals->{rna_strandness} = q[R];
      if($self->is_paired_read) {
        $p4_param_vals->{rna_strandness} .= q[F];
      }
    }
    $p4_param_vals->{alignment_method} = $rna_analysis;
    $p4_param_vals->{annotation_val} = $self->_transcriptome($rpt_list)->gtf_file();
    $p4_param_vals->{quant_method} = q[salmon];
    $p4_param_vals->{salmon_transcriptome_val} = $self->_transcriptome($rpt_list, q(salmon))->transcriptome_index_path();
    $p4_param_vals->{alignment_reference_genome} = $p4_reference_genome_index;
    # create intermediate file to prevent deadlock
    $p4_param_vals->{align_intfile_opt} = 1;

    if(not $self->is_paired_read) {
      $p4_param_vals->{alignment_reads_layout} = 1;
    }
  }
  else {
    # Parse the reference genome for the product
    my ($organism, $strain, $tversion, $analysis) = npg_tracking::data::reference->new(($self->repository ? (q(repository)=>$self->repository) : ()))->parse_reference_genome($l->reference_genome);

    # if a non-standard aligner is specified in ref string select it
    $p4_param_vals->{alignment_method} = ($analysis || $bwa);

    my %methods_to_aligners = (
      bwa_aln => q[bwa0_6],
      bwa_aln_se => q[bwa0_6],
      bwa_mem => q[bwa0_6],
      bwa_mem_bwakit => q[bwa0_6],
      bwa_mem2 => q[bwa0_6],
    );
    my %ref_suffix = (
      picard => q{.dict},
      minimap2 => q{.mmi},
    );

    my $aligner = $p4_param_vals->{alignment_method};
    if(exists $methods_to_aligners{$p4_param_vals->{alignment_method}}) {
      $aligner = $methods_to_aligners{$aligner};
    }

    # BWA MEM2 requires a different executable
    if ($p4_param_vals->{alignment_method} eq q[bwa_mem2]) {
      $p4_param_vals->{bwa_executable} = q[bwa-mem2];
    } else {
      $p4_param_vals->{bwa_executable} = q[bwa0_6];
    }

    my $is_hic_lib = $l->library_type && ($l->library_type =~ /Hi-C/smx);
    if($is_hic_lib) {
      $p4_param_vals->{is_HiC_lib} = 1;
      $p4_param_vals->{bwa_mem_5_flag} = q[on];
      $p4_param_vals->{bwa_mem_S_flag} = q[on];
      $p4_param_vals->{bwa_mem_P_flag} = q[on];
      $p4_param_vals->{bwa_mem_B_value} = $BWA_MEM_MISMATCH_PENALTY;
    }

    if($do_target_alignment) { $p4_param_vals->{alignment_reference_genome} = $self->_ref($dp, $aligner); }
    if(exists $ref_suffix{$aligner}) {
      $p4_param_vals->{alignment_reference_genome} .= $ref_suffix{$aligner};
    }

    if(not $self->is_paired_read) {
      $p4_param_vals->{bwa_mem_p_flag} = undef;
    }
  }

  if($nchs) {
    $p4_param_vals->{hs_alignment_reference_genome} = $self->_default_human_split_ref(q{bwa0_6}, $self->repository);
    $p4_param_vals->{alignment_hs_method} = $hs_bwa;
  }

  if($human_split) {
    $p4_param_vals->{final_output_prep_target_name} = q[split_by_chromosome];
    $p4_param_vals->{split_indicator} = q{_} . $human_split;
    if($l->separate_y_chromosome_data) {
      $p4_param_vals->{chrsplit_subset_flag} = ['--subset', 'Y,chrY,ChrY,chrY_KI270740v1_random'];
      $p4_param_vals->{chrsplit_invert_flag} = q[--invert];
    }
  }

  # update subsets for composition file
  if($human_split) {
    push @{$subsets}, $human_split;
  }
  if($nchs) {
    push @{$subsets}, 'human';
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
  $self->info(q[  markdup_method is ] . ($p4_param_vals->{markdup_method} ? $p4_param_vals->{markdup_method} : q[unspecified]));
  $self->info(q[  markdup_optical_distance is ] . ($p4_param_vals->{markdup_optical_distance} ? $p4_param_vals->{markdup_optical_distance} : q[unspecified]));
  $self->info(q[  p4 parameters written to ] . $param_vals_fname);
  $self->info(q[  Using p4 template alignment_wtsi_stage2_] . $nchs_template_label . q[template.json]);

  my $num_threads_expression = q[npg_pipeline_job_env_to_threads --num_threads ] . $self->get_massaged_resources()->{num_cpus}[0];
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
    _qc_command('bam_flagstats', $dp_archive_path, $qc_out_path, undef,
                $skip_target_markdup_metrics, $rpt_list, $name_root, [$cfs_input_file]),
    (grep {$_}
      ($spike_tag ? q() : (join q( ),
        q{&&},
        _qc_command('bam_flagstats', $dp_archive_path, $qc_out_path, 'phix', $SKIP_MARKDUP_METRICS, $rpt_list, $name_root, [$cfs_input_file]),
        q{&&},
        _qc_command('alignment_filter_metrics', undef, $qc_out_path, undef, undef, $rpt_list, $name_root, [$af_input_file]),
      ),

      $human_split ? (join q( ),
        q{&&},
        _qc_command('bam_flagstats', $dp_archive_path, $qc_out_path, $human_split, $skip_target_markdup_metrics, $rpt_list, $name_root, [$cfs_input_file]),
      ) : q()),

      $nchs ? (join q( ),
        q{&&},
        _qc_command('bam_flagstats', $dp_archive_path, $qc_out_path, $nchs_outfile_label, $SKIP_MARKDUP_METRICS, $rpt_list, $name_root, [$cfs_input_file]),
      ) : q(),

      ($do_rna and not $is_tag_zero_product) ? (join q( ),
        q{&&},
        _qc_command('rna_seqc', $dp_archive_path, $qc_out_path, undef, undef, $rpt_list, $name_root, [$bfs_input_file]),
      ) : q(),

      $do_gbs_plex ? (join q( ),
        q{&&},
        _qc_command('genotype_call', $dp_archive_path, $qc_out_path, undef, undef, $rpt_list, $name_root),
      ) : q(),

      ($do_target_alignment && ! $is_tag_zero_product) ? (join q( ),
        q{&&},
        _qc_command('substitution_metrics', $dp_archive_path, $qc_out_path, undef, undef, $rpt_list, $name_root, [$cfs_input_file]),
      ) : q(),

      ($do_target_alignment && $human_split && ! $is_tag_zero_product)  ? (join q( ),
        q{&&},
        _qc_command('substitution_metrics', $dp_archive_path, $qc_out_path, $human_split, undef, $rpt_list, $name_root, [$cfs_input_file]),
      ) : q(),
    ),
    q(');

}

sub _qc_command {##no critic (Subroutines::ProhibitManyArgs)
  my ($check_name, $qc_in, $qc_out, $subset, $skip_markdups_metrics, $rpt_list, $filename_root, $input_files) = @_;

  my $args = {
               'rpt_list' => q["] . $rpt_list . q["],
               'filename_root' => $filename_root . ($subset? qq[_$subset]: q[]),
               'qc_out' => $qc_out,
              'check' => $check_name,};
  my @flags = ();

  if ($check_name =~ /^bam_flagstats$/smx and $skip_markdups_metrics) {
      push @flags, q[--skip_markdups_metrics];
  }

  if ($check_name =~ /^bam_flagstats|genotype_call|substitution_metrics|rna_seqc$/smx) {
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

sub _do_rna_analysis {
  my ($self, $dp) = @_;

  my $rpt_list = $dp->rpt_list;
  my $reference_genome = $dp->lims->reference_genome;
  my $library_type = $dp->lims->library_type;
  my $is_tag_zero_product = $dp->is_tag_zero_product;

  my $analysis    = $self->_analysis($reference_genome, $rpt_list) // q[];
  my $rna_aligner = $analysis? (grep { /^$analysis$/sxm } @RNA_ANALYSES): q[];

  if (!$library_type || $library_type !~ /(?:(?:cD|R)NA|DAFT)/sxm) {
    if (($library_type || $is_tag_zero_product) && $rna_aligner) { # if tag#0 is being aligned, it should use an explicitly requested RNA aligner
      $self->debug(qq{$rpt_list - over-riding library type with rna aligner $analysis});
    }
    else {
      $self->debug(qq{$rpt_list - not RNA library type: skipping RNAseq analysis});
      return 0;
    }
  }
  my @parsed_ref_genome = npg_tracking::data::reference->new(($self->repository ? (q(repository)=>$self->repository) : ()))->parse_reference_genome($reference_genome);
  my $transcriptome_version = $parsed_ref_genome[$REFERENCE_ARRAY_TVERSION_IDX] // q[];
  if (not $transcriptome_version) {
    if($rna_aligner) {
       $self->logcroak(qq{$rpt_list - not possible to run an rna aligner without a transcriptome});
    }
    $self->debug(qq{$rpt_list - Reference without transcriptome version: skipping RNAseq analysis});
    return 0;
  }

  $self->debug(qq{$rpt_list - Do RNAseq analysis....});
  return 1;
}

sub _transcriptome {
  my ($self, $rpt_list, $analysis) = @_;
  my $href = {
              'rpt_list'   => $rpt_list,
              ($self->repository ? ('repository' => $self->repository):())
             };
  if (defined $analysis) {
      $href->{'analysis'} = $analysis;
  }
  return npg_tracking::data::transcriptome->new($href);
}

sub _analysis {
    my ($self, $reference_genome, $rpt_list) = @_;
    $reference_genome //= q[];
    my @parsed_ref_genome = npg_tracking::data::reference->new(($self->repository ? (q(repository)=>$self->repository) : ()))->parse_reference_genome($reference_genome);
    my $analysis = $parsed_ref_genome[$REFERENCE_ARRAY_ANALYSIS_IDX];
    $self->info(qq[$rpt_list - Analysis: ] . (defined $analysis ? $analysis : qq[default for $reference_genome]));
    return $analysis;
}

sub _do_bait_stats_analysis {
  my ($self, $dp) = @_;

  my $dplims = $dp->lims;
  my $rpt_list = $dp->rpt_list;
  my $is_tag_zero_product = $dp->is_tag_zero_product;

  if(not $self->_ref($dp, q{fasta})
     or not $dplims->alignments_in_bam
     or $is_tag_zero_product) {
    $self->debug(qq{$rpt_list - no reference or no alignments set});
    return 0;
  }
  if(not $self->_bait($rpt_list)->bait_name){
    $self->debug(qq{$rpt_list - No bait set});
    return 0;
  }
  if(not $self->_bait($rpt_list)->bait_path){
    $self->debug(qq{$rpt_list - No bait path found});
    return 0;
  }
  $self->debug(qq{$rpt_list - Doing optional bait stats analysis....});

  return 1;
}

sub _bait{
  my($self,$rpt_list) = @_;
  return npg_tracking::data::bait->new (
                {
                 'rpt_list' => $rpt_list,
                 ( $self->repository ? ('repository' => $self->repository):())
                });
}

sub _has_gbs_plex{
  my ($self, $dp) = @_;

  my $rpt_list = $dp->rpt_list;
  my $library_type = $dp->lims->library_type;

  if(not $self->_gbs_plex($rpt_list)->gbs_plex_name){
    $self->debug(qq{$rpt_list - No gbs plex set});
    return 0;
  }
  if(!$library_type || $library_type !~ /^GbS|GnT\sMDA/ismx){
    $self->debug(qq{$rpt_list - Library type is incompatible with gbs analysis});
    return 0;
  }

  if(not $self->_gbs_plex($rpt_list)->gbs_plex_path){
    $self->logcroak(qq{$rpt_list - GbS plex set but no gbs plex path found});
  }

  $self->debug(qq{$rpt_list - Doing GbS plex analysis....});

  return 1;
}

sub _gbs_plex{
  my($self,$rpt_list) = @_;
  return npg_tracking::data::gbs_plex->new (
                {
                 'rpt_list' => $rpt_list,
                 ( $self->repository ? ('repository' => $self->repository):())
                });
}

sub _ref {
  my ($self, $dp, $aligner) = @_;

  return npg_pipeline::cache::reference->instance->get_path($dp, $aligner, $self->repository, $self->_do_gbs_plex_analysis);
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

sub _alt_reference {
  my ($self, $dp) = @_;
  my $ref = $self->_ref($dp, q{bwa0_6});
  if ($ref) {
    $ref .= q{.alt};
    if(-e $ref) { return $ref; }
    else { return; }
  }
}

sub _p4_stage2_params_path {
  my ($self, $position) = @_;

  my $path = $self->recalibrated_path;

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

=head2 phix_reference

A path to Phix reference fasta file to split phiX spike-in reads

=head2 markdup_method

This method is inherited from npg_pipeline::product role and
changed to return a default value (biobambam) and duplexseq for
the Duplex-Seq library type.

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

=item open

=item Try::Tiny

=item Class::Load

=item npg_tracking::data::reference

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

Copyright (C) 2018,2019,2020 Genome Research Ltd.

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
