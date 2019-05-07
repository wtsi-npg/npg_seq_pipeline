package npg_pipeline::function::p4_stage1_analysis;

use Moose;
use Moose::Meta::Class;
use namespace::autoclean;
use Try::Tiny;
use English qw{-no_match_vars};
use Readonly;
use File::Slurp;
use List::Util qw{sum};
use List::MoreUtils qw{any};
use JSON;
use open q(:encoding(UTF8));

use Data::Dumper;

use npg_pipeline::cache::barcodes;
use npg_pipeline::function::definition;

extends q{npg_pipeline::base};

with 'npg_pipeline::runfolder_scaffold' => {
        -excludes => [qw/create_top_level create_analysis_level/],
     },
     'npg_pipeline::function::util';

our $VERSION  = '0';

Readonly::Scalar my $NUM_SLOTS                    => q(8,16);
Readonly::Scalar my $NUM_HOSTS                    => 1;
Readonly::Scalar my $MEMORY                       => q{12000}; # memory in megabytes
Readonly::Scalar my $FS_RESOURCE                  => 4; # LSF resource counter to control access to staging area file system
Readonly::Scalar my $DEFAULT_I2B_THREAD_COUNT     => 3; # value passed to bambi i2b --threads flag
Readonly::Scalar my $DEFAULT_SPLIT_THREADS_COUNT  => 0; # value passed to samtools split --threads flag

Readonly::Scalar my $TILE_METRICS_INTEROP_CODES => {'cluster density'    => 100,
                                                     'cluster density pf' => 101,
                                                     'cluster count'      => 102,
                                                     'cluster count pf'   => 103,
                                                     'version3_cluster_counts' => ord('t'),
                                                     };

sub generate {
  my $self = shift;

  $self->info(q{Creating definitions to run P4 stage1 analysis for run },
              $self->id_run);

  $self->info(q{Creating P4 stage1 analysis directories for run },
              $self->id_run );
  $self->_create_p4_stage1_dirs();

  my $alims = $self->lims->children_ia;
  my @definitions = ();

  for my $lane_product (@{$self->products->{lanes}}) {

    my $p = $lane_product->composition->{components}->[0]->{position}; # there should be only one element in components

    my $l = $lane_product->lims;
    my $tag_list_file = q{};

    if($l->is_pool) {
      $self->info(qq{Lane $p is indexed, generating tag list});

      $tag_list_file = npg_pipeline::cache::barcodes->new(
        location      => $self->metadata_cache_dir_path,
        lane_lims     => $l,
        index_lengths => $self->_get_index_lengths($l),
        i5opposite    => $self->is_i5opposite ? 1 : 0,
        verbose       => $self->verbose
      )->generate();
    }

    my @generated = $self->_generate_command_params($l, $tag_list_file, $lane_product);
    my ($command, $p4_params, $p4_ops) = @generated;
    push @definitions, $self->_create_definition($l, $command);

    my $pfile_name = join q{/}, $self->p4_stage1_params_paths->{$p},
                                $self->id_run.q{_}.$p.q{_p4s1_pv_in.json};
    write_file($pfile_name, encode_json({'assign' => [ $p4_params, ], 'ops' => $p4_ops, }));
  }

  return \@definitions;
}

has 'p4_stage1_analysis_log_base' => (
                           isa        => 'Str',
                           is         => 'ro',
                           lazy_build => 1,
                         );
sub _build_p4_stage1_analysis_log_base {
  my $self = shift;
  return join q{/}, $self->bam_basecall_path, q{p4_stage1_analysis};
}

has 'p4_stage1_params_paths' => (
                           isa        => 'HashRef',
                           is         => 'ro',
                           lazy_build => 1,
                         );
sub _build_p4_stage1_params_paths {
  my $self = shift;
  my %p4_stage1_params_paths =
    map { $_=> $self->p4_stage1_analysis_log_base . q[/lane] . $_ . q[/param_files]; }
    $self->positions;
  return \%p4_stage1_params_paths;
}

has 'p4_stage1_errlog_paths'  => (
                           isa        => 'HashRef',
                           is         => 'ro',
                           lazy_build => 1,
                         );
sub _build_p4_stage1_errlog_paths {
  my $self = shift;
  my %p4_stage1_errlog_paths =
    map { $_=> $self->p4_stage1_analysis_log_base . q[/lane] . $_ . q[/log]; }
    $self->positions;
  return \%p4_stage1_errlog_paths;
}

has '_num_cpus'               => (
                           isa        => 'ArrayRef',
                           is         => 'ro',
                           lazy_build => 1,
                         );
sub _build__num_cpus {
  my $self = shift;
  return $self->num_cpus2array(
    $self->general_values_conf()->{'p4_stage1_slots'} || $NUM_SLOTS);
}

has '_job_id' => ( isa        => 'Str',
                   is         => 'ro',
                   lazy_build => 1,
                 );
sub _build__job_id {
  my $self = shift;
  return $self->random_string();
}

has 'interop_file_name'  => (
                           isa        => 'Str',
                           is         => 'ro',
                           lazy_build => 1,
                         );
sub _build_interop_file_name {
  my $self = shift;

  return $self->runfolder_path . q{/InterOp/TileMetricsOut.bin};
}

has 'cluster_counts'   => (
                       isa     => 'HashRef',
                       is      => 'ro',
                       lazy_build => 1,
                     );

sub _build_cluster_counts {
  my $self = shift;

  return $self->_parsing_interop($self->interop_file_name);
}

# phix_aligner is used to determine the reference genome. Be aware that
#  setting this will not affect the p4s1_phix_alignment_method.
has 'phix_aligner'  => (
                           isa        => 'Str',
                           is         => 'ro',
                           lazy_build => 1,
                         );
sub _build_phix_aligner {
  my $self = shift;

  my %methods_to_aligners = (
    bwa_aln => q[bwa0_6],
    bwa_aln_se => q[bwa0_6],
    bwa_mem => q[bwa0_6],
  );

  my $aligner = $self->p4s1_phix_alignment_method;

  if(exists $methods_to_aligners{$aligner}) {
    $aligner = $methods_to_aligners{$aligner};
  }

  return $aligner;
}

has 'phix_alignment_reference'  => (
                           isa        => 'Str',
                           is         => 'ro',
                           lazy_build => 1,
                         );
sub _build_phix_alignment_reference {
  my $self = shift;

  return $self->_default_phix_ref($self->phix_aligner, $self->repository);
}

sub _create_definition {
  my ($self, $l, $command) = @_;

  return npg_pipeline::function::definition->new(
    created_by      => __PACKAGE__,
    created_on      => $self->timestamp(),
    identifier      => $self->id_run(),
    job_name        => (join q{_}, q{p4_stage1_analysis},$self->id_run(),$self->timestamp()),
    fs_slots_num    => $self->general_values_conf()->{'p4_stage1_fs_resource'} || $FS_RESOURCE,
    num_hosts       => $NUM_HOSTS,
    num_cpus        => $self->_num_cpus(),
    memory          => $self->general_values_conf()->{'p4_stage1_memory'} || $MEMORY,
    queue           => $npg_pipeline::function::definition::P4_STAGE1_QUEUE,
    command         => $command,
    command_preexec => $self->repos_pre_exec_string(),
    composition     => $self->create_composition($l)
  );
}

sub _create_p4_stage1_dirs {
  my $self = shift;

  my @dirs = (values %{$self->p4_stage1_params_paths},
              values %{$self->p4_stage1_errlog_paths});
  my @errors = $self->make_dir(@dirs);
  if (@errors) {
    $self->logcroak(join qq[\n], @errors);
  } else {
    $self->info(q[Created the following p4 stage1 log directories: ], join q[, ], @dirs);
  }

  return;
}

sub _get_index_lengths {
  my ( $self, $lane_lims ) = @_;

  my @index_length_array;

  if ($lane_lims->inline_index_exists) {
    # Tradis run - treat as a special case
    my $index_start = $lane_lims->inline_index_start;
    my $index_end = $lane_lims->inline_index_end;
    if ($index_start && $index_end) {
      push @index_length_array, $index_end - $index_start + 1;
    }
  } else {
    my $n = 0;
    my @cycle_counts = $self->read_cycle_counts();
    my @reads_indexed = $self->reads_indexed();
    foreach my $n (0..$#cycle_counts) {
      if ($reads_indexed[$n]) { push @index_length_array, $cycle_counts[$n]; }
    }
  }
  return \@index_length_array;
}

#########################################################################################################
# _generate_command_params:
# Determine parameters for the lane from LIMS information and create the hash from which the p4 stage1
#  analysis param_vals file will be generated. Generate the vtfp/viv commands using this param_vals file.
#########################################################################################################
sub _generate_command_params {
  my ($self, $lane_lims, $tag_list_file, $lane_product) = @_;
  my %p4_params = (
                    samtools_executable => q{samtools},
                    bwa_executable => q{bwa0_6}, # be sure that the version of bwa that is picked up is consistent with the phiX reference used for alignment
                    teepot_tempdir => q{.},
                    teepot_wval => q{500},
                    teepot_mval => q{2G},
                    phix_alignment_method => $self->p4s1_phix_alignment_method,
                    reference_phix => $self->phix_alignment_reference,
                    scramble_reference_fasta => $self->_default_phix_ref(q{fasta}, $self->repository),
                    s1_se_pe => ($self->is_paired_read)? q{pe} : q{se},
                    aln_filter_value => q{0x900},
                    s1_output_format => $self->s1_s2_intfile_format,
                  );
  my %p4_ops = ( splice => [], prune => [], );

  my $id_run             = $self->id_run();
  my $position = $lane_lims->position;

  my $runfolder_path     = $self->runfolder_path;
  my $run_folder     = $self->run_folder;
  my $intensity_path     = $self->intensity_path;
  my $archive_path            = $self->archive_path;
  my $qc_path            = $self->qc_path; # NB: the value provided for qc_path is only valid for old-style runfolders
  my $basecall_path = $self->basecall_path;
  my $no_cal_path       = $self->recalibrated_path;
  my $bam_basecall_path  = $self->bam_basecall_path;
  my $lp_archive_path = $lane_product->path($self->archive_path);

  my $full_bam_name  = $bam_basecall_path . q{/}. $id_run . q{_} .$position. q{.bam};

  $p4_params{qc_check_id_run} = $id_run; # used by tag_metrics qc check
  $p4_params{qc_check_qc_in_dir} = $bam_basecall_path; # used by tag_metrics qc check
  $p4_params{qc_check_qc_out_dir} = $lane_product->qc_out_path($self->archive_path); # used by tag_metrics qc check
  $p4_params{tileviz_dir} = $lane_product->tileviz_path($self->archive_path); # used for tileviz
  $p4_params{outdatadir} = $no_cal_path; # base for all (most?) outputs
  $p4_params{lane_archive_path} = $lp_archive_path;
  $p4_params{rpt_list} = $lane_product->rpt_list;
  $p4_params{subsetsubpath} = $lane_product->short_files_cache_path($archive_path);
  $p4_params{seqchksum_file} = $bam_basecall_path . q[/] . $id_run . q[_] . $position . q{.post_i2b.seqchksum}; # full name for the lane-level seqchksum file
  $p4_params{filtered_bam} = $no_cal_path . q[/] . $id_run . q[_] . $position . q{.bam}; # full name for the spatially filtered lane-level file
  $p4_params{unfiltered_cram_file} = $no_cal_path . q[/] . $id_run . q[_] . $position . q{.unfiltered.cram}; # full name for spatially unfiltered lane-level cram file
  $p4_params{md5filename} = $no_cal_path . q[/] . $id_run . q[_] . $position . q{.bam.md5}; # full name for the md5 for the spatially filtered lane-level file
  $p4_params{split_prefix} = $no_cal_path; # location for split bam files

  my $job_name = join q/_/, (q{p4_stage1}, $id_run, $position, $self->timestamp());
  $job_name = q{'} . $job_name . q{'};

  $p4_params{i2b_run_path} = $runfolder_path;
  $p4_params{i2b_thread_count} = $self->general_values_conf()->{'p4_stage1_i2b_thread_count'} || $DEFAULT_I2B_THREAD_COUNT;
  $p4_params{i2b_runfolder} = $run_folder;
  $p4_params{i2b_intensity_dir} = $intensity_path;
  $p4_params{i2b_lane} = $position;
  $p4_params{i2b_basecalls_dir} = $self->basecall_path;
  $p4_params{i2b_rg} = join q[_], $id_run, $position;
  $p4_params{i2b_pu} = join q[_], $self->run_folder, $position;

  my $st_names = $self->_get_library_sample_study_names($lane_lims);

  if($st_names->{library}){
    $p4_params{i2b_library_name} =  $st_names->{library};
  }
  if($st_names->{sample}){
    $p4_params{i2b_sample_aliases} =  $st_names->{sample};
  }
  if($st_names->{study}){
    my $study = $st_names->{study};
    $study =~ s/"/\\"/gmxs;
    $p4_params{i2b_study_name} =  q{"} . $study . q{"};
  }
  if ($self->_extra_tradis_transposon_read) {
    $p4_params{i2b_sec_bc_seq_val} = q[BC];
    $p4_params{i2b_sec_bc_qual_val} = q[QT];
    $p4_params{i2b_bc_seq_val} = q[tr];
    $p4_params{i2b_bc_qual_val} = q[tq];
  }

  if($lane_lims->inline_index_exists) {
    my $index_start = $lane_lims->inline_index_start;
    my $index_end = $lane_lims->inline_index_end;
    my $index_read = $lane_lims->inline_index_read;

    if ($index_start && $index_end && $index_read) {
      $self->info(q{P4 stage1 analysis of a lane with inline indexes});

      my($first, $final) = $self->read1_cycle_range();
      if ($index_read == 1) {
        $p4_params{i2b_bc_read} = 1;
        $index_start += ($first-1);
        $index_end += ($first-1);
        $p4_params{i2b_first_index_0} = $index_start;
        $p4_params{i2b_final_index_0} = $index_end;
        $p4_params{i2b_first_index_1} = $first;
        $p4_params{i2b_final_index_1} = $index_start-1;
        $p4_params{i2b_first_0} = $index_end+1;
        $p4_params{i2b_final_0} = $final;
        if ($self->is_paired_read()) {
          ($first, $final) = $self->read2_cycle_range();
          $p4_params{i2b_first_1} = $first;
          $p4_params{i2b_final_1} = $final;
        }
      } elsif ($index_read == 2) {
        $p4_params{i2b_bc_read} = 2;
        $self->is_paired_read() or $self->logcroak(q{Inline index read (2) does not exist});
        $p4_params{i2b_first_0} = $first;
        $p4_params{i2b_final_0} = $final;
        ($first, $final) = $self->read2_cycle_range();
        $index_start += ($first-1);
        $index_end += ($first-1);
        $p4_params{i2b_first_index_0} = $index_start;
        $p4_params{i2b_final_index_0} = $index_end;
        $p4_params{i2b_first_index_1} = $first;
        $p4_params{i2b_final_index_1} = $index_start-1;
        $p4_params{i2b_first_1} = $index_end+1;
        $p4_params{i2b_final_1} = $final;
      } else {
        $self->logcroak("Invalid inline index read ($index_read)");
      }
      $p4_params{i2b_sec_bc_seq_val} = q{br};
      $p4_params{i2b_sec_bc_qual_val} = q{qr};
    }
  }

  ###  TODO: remove this read length comparison if biobambam will handle this case. Check clip reinsertion.
  if($self->is_paired_read() && !$lane_lims->inline_index_exists) {
    # omit BamAdapterFinder for inline index
    my @range1 = $self->read1_cycle_range();
    my $read1_length = $range1[1] - $range1[0] + 1;
    my @range2 = $self->read2_cycle_range();
    my $read2_length = $range2[1] - $range2[0] + 1;
    if($read1_length != $read2_length) {
      $self->logwarn('P4 stage1 analysis will not yet handle different length forward/reverse reads (no optional adapter detection)');
    }
  }

  if($self->is_multiplexed_lane($position)) {
    if (!$tag_list_file) {
      $self->logcroak('Tag list file path should be defined for multiplexed lane ', $position);
    }

    $p4_params{barcode_file} = $tag_list_file;
    $p4_params{decoder_metrics} = $full_bam_name . q{.tag_decode.metrics};

    my $num_of_plexes_per_lane = $self->_get_number_of_plexes_excluding_control($lane_lims);
    if($num_of_plexes_per_lane == 1) {
      $p4_params{bid_max_no_calls} = $self->general_values_conf()->{single_plex_decode_max_no_calls};
      $p4_params{bid_convert_low_quality_to_no_call_flag} = q[--convert-low-quality];
    }
    push @{$p4_ops{prune}}, q[tee_split:unsplit_bam-]; # no lane-level bam/cram when plexed
  }
  else {
    $self->info(q{P4 stage1 analysis on non-plexed lane});

    push @{$p4_ops{splice}}, q[tee_i2b:baf-bamcollate:]; # skip decode when unplexed
    push @{$p4_ops{prune}}, q[tee_split:split_bam-]; # no split output when unplexed
  }

  if(not $self->adapterfind) {
    push @{$p4_ops{splice}}, q[bamadapterfind];
  }

  # cluster count (used to calculate FRAC for bam subsampling)
  my $cluster_count = $self->cluster_counts->{$position}->{'cluster count pf'};
  $p4_params{cluster_count} = $cluster_count;
  ## no critic (ValuesAndExpressions::ProhibitMagicNumbers)
  if($cluster_count > 0) {
    $p4_params{seed_frac} = sprintf q[%.8f], (10_000.0 / $cluster_count) + $id_run;
  }
  else {
    $self->logwarn("P4 stage1 analysis: cluster count $cluster_count is zero (or less) - setting subsample percentage to zero");
    $p4_params{seed_frac} = sprintf q[%.8f], $id_run + 0.0;
  }

  $p4_params{split_threads_val} = $self->general_values_conf()->{'p4_stage1_split_threads_count'} || $DEFAULT_SPLIT_THREADS_COUNT;

  my $num_threads_expression = q[npg_pipeline_job_env_to_threads --num_threads ] . $self->_num_cpus->[0];
  my $name_root = $id_run . q{_} . $position;
  # allow specification of thread number for some processes in config file. Note: these threads are being drawn from the same pool. Unless
  #  they appear in the config file, their values will be derived from what LSF assigns the job based on the -n value supplied to the bsub
  #  command (see $num_slots in _default_resources()).
  my $aligner_slots = $self->general_values_conf()->{'p4_stage1_aligner_slots'} || qq[`$num_threads_expression --exclude -2 --divide 3`];
  my $samtobam_slots = $self->general_values_conf()->{'p4_stage1_samtobam_slots'} || qq[`$num_threads_expression --exclude -1 --divide 3`];
  my $bamsormadup_slots = $self->general_values_conf()->{'p4_stage1_bamsort_slots'} || qq[`$num_threads_expression --divide 3`];
  my $bamrecompress_slots = $self->general_values_conf()->{'p4_stage1_bamrecompress_slots'} || qq[`$num_threads_expression`];

  my $command = join q( ), q(bash -c '),
                           q(cd), $self->p4_stage1_errlog_paths->{$position}, q{&&},
                           q(vtfp.pl),
                           q{-template_path $}.q{(dirname $}.q{(readlink -f $}.q{(which vtfp.pl)))/../data/vtlib},
                           qq(-o run_$name_root.json),
                           q(-param_vals), (join q{/}, $self->p4_stage1_params_paths->{$position}, $name_root.q{_p4s1_pv_in.json}),
                           q(-export_param_vals), $name_root.q{_p4s1_pv_out_}.$self->_job_id().q/.json/,
                           q{-keys cfgdatadir -vals $}.q{(dirname $}.q{(readlink -f $}.q{(which vtfp.pl)))/../data/vtlib/},
                           qq(-keys aligner_numthreads -vals $aligner_slots),
                           qq(-keys s2b_mt_val -vals $samtobam_slots),
                           qq(-keys bamsormadup_numthreads -vals $bamsormadup_slots),
                           qq(-keys br_numthreads_val -vals $bamrecompress_slots),
                           q{$}.q{(dirname $}.q{(dirname $}.q{(readlink -f $}.q{(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json},
                           q{&&},
                           qq(viv.pl -s -x -v 3 -o viv_$name_root.log run_$name_root.json),
                           q(');

  return ($command, \%p4_params, \%p4_ops);
}

sub _default_resources {
  my ( $self ) = @_;
  my $hosts = 1;
  my $mem = $self->general_values_conf()->{'p4_stage1_memory'} || $MEMORY;
  my $num_slots = $self->general_values_conf()->{'p4_stage1_slots'} || $NUM_SLOTS;
  return (join q[ ], npg_pipeline::lsf_job->new(memory => $mem)->memory_spec(), "-R 'span[hosts=$hosts]'", "-n$num_slots");
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

sub _default_phix_ref {
   my ($self, $aligner, $repos) = @_;

   my $ruser = Moose::Meta::Class->create_anon_class(
          roles => [qw/npg_tracking::data::reference::find/])
          ->new_object({
                         species => q{PhiX},
                         aligner => $aligner,
                        ($repos ? (q(repository)=>$repos) : ())
                       });

  my %ref_suffix = (
    picard => q{.dict},
    minimap2 => q{.mmi},
  );

   my $phix_ref;
   try {
      $phix_ref = $ruser->refs->[0];
     if(exists $ref_suffix{$aligner}) {
       $phix_ref .= $ref_suffix{$aligner}
    }
   } catch {
      $self->warn($_);
   };

   return $phix_ref;
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

  my $num_index_reads = sum (map { $_ ? 1 : 0 } $self->reads_indexed());
  $num_index_reads ||= 0; # sum returns undef for an empty list
  my $is_tradis = any {$_->library_type && $_->library_type =~ /^TraDIS/smx}
                  $self->lims->descendants();
  my $num_main_index_reads = (any {$_->is_pool} $self->lims->children) ? 1 : 0;

  my $num_extra = 0;
  if ($is_tradis) {
    $num_extra = $num_index_reads - $num_main_index_reads;
  }

  return ($num_extra > 0) ? 1 : 0;
}

sub _parsing_interop {
  my ($self, $interop) = @_;

  my $cluster_count_by_lane = {};

  my $version;
  my $length;
  my $data;

  my $template = 'v3f'; # three two-byte integers and one 4-byte float

  open my $fh, q{<}, $interop or
    $self->logcroak(qq{Couldn't open interop file $interop, error $ERRNO});
  binmode $fh, ':raw';

  $fh->read($data, 1) or
    $self->logcroak(qq{Couldn't read file version in interop file $interop, error $ERRNO});
  $version = unpack 'C', $data;

  $fh->read($data, 1) or
    $self->logcroak(qq{Couldn't read record length in interop file $interop, error $ERRNO});
  $length = unpack 'C', $data;

  my $tile_metrics = {};

   ## no critic (ValuesAndExpressions::ProhibitMagicNumbers)
   if( $version == 3) {
     $fh->read($data, 4) or
       $self->logcroak(qq{Couldn't read area in interop file $interop, error $ERRNO});
     my $area = unpack 'f', $data;
     while ($fh->read($data, $length)) {
       $template = 'vVc'; # one 2-byte integer, one 4-byte integer and one 1-byte char
       my ($lane,$tile,$code) = unpack $template, $data;
       if( $code == $TILE_METRICS_INTEROP_CODES->{'version3_cluster_counts'} ){
         $data = substr $data, 7;
         $template = 'f2'; # two 4-byte floats
         my ($cluster_count, $cluster_count_pf) = unpack $template, $data;
         push @{$tile_metrics->{$lane}->{'cluster count'}}, $cluster_count;
         push @{$tile_metrics->{$lane}->{'cluster count pf'}}, $cluster_count_pf;
       }
     }
   } elsif( $version == 2) {
     $template = 'v3f'; # three 2-byte integers and one 4-byte float
     while ($fh->read($data, $length)) {
       my ($lane,$tile,$code,$value) = unpack $template, $data;
       if( $code == $TILE_METRICS_INTEROP_CODES->{'cluster count'} ){
         push @{$tile_metrics->{$lane}->{'cluster count'}}, $value;
       }elsif( $code == $TILE_METRICS_INTEROP_CODES->{'cluster count pf'} ){
         push @{$tile_metrics->{$lane}->{'cluster count pf'}}, $value;
       }
    }

   } else {
     $self->logcroak(qq{Unknown version $version in interop file $interop});
  }

  $fh->close() or
    $self->logcroak(qq{Couldn't close interop file $interop, error $ERRNO});

  my $lanes = scalar keys %{$tile_metrics};
  if( $lanes == 0){
    $self->warn('No cluster count data');
    return $cluster_count_by_lane;
  }

  # calc lane totals
  foreach my $lane (keys %{$tile_metrics}) {
    for my $code (keys %{$tile_metrics->{$lane}}) {
      my $total = 0;
      for ( @{$tile_metrics->{$lane}->{$code}} ){ $total += $_};
      $cluster_count_by_lane->{$lane}->{$code} = $total;
    }
  }

  return $cluster_count_by_lane;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

npg_pipeline::function::p4_stage1_analysis

=head1 SYNOPSIS

  my $p4s1 = npg_pipeline::function::p4_stage1_analysis->new(
    run_folder => $sRunFolder,
  );

=head1 DESCRIPTION

Definition for p4 flow which creates cram files from bcl files, including initial phiX alignment, 
spatial filtering and deplexing of pools where appropriate.

=head1 SUBROUTINES/METHODS

=head2 generate

Creates and returns an array of npg_pipeline::function::definition
objects for all lanes.

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Try::Tiny

=item English -no_match_vars

=item Readonly

=item Moose

=item Moose::Meta::Class

=item namespace::autoclean

=item File::Slurp

=item List::Util

=item List::MoreUtils

=item JSON

=item open

=item npg_pipeline::cache::barcodes

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Kevin Lewis

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2018 Genome Research Limited

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
