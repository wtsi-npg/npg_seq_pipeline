package npg_pipeline::function::p4_stage1_analysis;

use Moose;
use Moose::Meta::Class;
use namespace::autoclean;
use Try::Tiny;
use English qw{-no_match_vars};
use Readonly;
use File::Slurp;
use JSON;
use open q(:encoding(UTF8));

use npg_common::roles::software_location;
use npg_pipeline::cache::barcodes;
use npg_pipeline::function::definition;

extends q{npg_pipeline::base};

our $VERSION  = '0';

Readonly::Scalar my $NUM_SLOTS                    => q(8,16);
Readonly::Scalar my $NUM_HOSTS                    => 1;
Readonly::Scalar my $MEMORY                       => q{12000}; # memory in megabytes
Readonly::Scalar my $FS_RESOURCE                  => 4; # LSF resource counter to control access to staging area file system
Readonly::Scalar my $DEFAULT_I2B_THREAD_COUNT     => 3; # value passed to bambi i2b --threads flag

sub generate {
  my $self = shift;

  $self->info(q{Creating definitions to run P4 stage1 analysis for run },
              $self->id_run);

  $self->info(q{Creating P4 stage1 analysis directories for run },
              $self->id_run );
  $self->_create_p4_stage1_dirs();

  my $alims = $self->lims->children_ia;
  my @definitions = ();

  for my $p ($self->positions()) {

    my $l = $alims->{$p};
    my $tag_list_file = q{};

    if ($self->is_multiplexed_lane($p)) {
      $self->info(qq{Lane $p is indexed, generating tag list});

      $tag_list_file = npg_pipeline::cache::barcodes->new(
        location      => $self->metadata_cache_dir,
        lane_lims     => $l,
        index_lengths => $self->_get_index_lengths($l),
        i5opposite    => $self->is_hiseqx_run,
        verbose       => $self->verbose
      )->generate();
    }

    my @generated = $self->_generate_command_params($l, $tag_list_file);
    my $command = shift @generated;
    push @definitions, $self->_create_definition($l, $command);

    my $pfile_name = join q{/}, $self->p4_stage1_params_paths->{$p},
                                $self->id_run.q{_}.$p.q{_p4s1_pv_in.json};
    my %p4_params = @generated;
    write_file($pfile_name, encode_json {'assign' => [ \%p4_params ]});
  }

  return \@definitions;
}

foreach my $jar_name (qw/Illumina2bam BamAdapterFinder BamIndexDecoder/) {
  has q{_}.$jar_name.q{_jar} => (
                           isa        => q{NpgCommonResolvedPathJarFile},
                           is         => q{ro},
                           coerce     => 1,
                           default    => $jar_name.q{.jar},
                         );
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
    log_file_dir    => $self->make_log_dir( $self->bam_basecall_path ),
    memory          => $self->general_values_conf()->{'p4_stage1_memory'} || $MEMORY,
    command         => $command,
    command_preexec => $self->ref_adapter_pre_exec_string(),
    composition     => $self->create_composition($l)
  );
}

sub _create_p4_stage1_dirs {
  my $self = shift;

  for my $d ( values %{$self->p4_stage1_params_paths},
              values %{$self->p4_stage1_errlog_paths} ) {
     $self->info(qq{creating $d});
     my $rc = `mkdir -p $d`;
     if ( $CHILD_ERROR ) {
       $self->logcroak(qq{could not create $d\n\t$rc});
     }
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
  my ($self, $lane_lims, $tag_list_file) = @_;
  my %p4_params = (
                    samtools_executable => q{samtools1},
                    bwa_executable => q{bwa0_6}, # be sure that the version of bwa that is picked up is consistent with the phiX reference used for alignment
                    teepot_tempdir => q{.},
                    teepot_wval => q{500},
                    teepot_mval => q{2G},
                    reference_phix => $self->_default_phix_ref(q{bwa0_6}, $self->repository),
                    scramble_reference_fasta => $self->_default_phix_ref(q{fasta}, $self->repository),
                  );
  my %i2b_flag_map = (
    I => q[i2b_intensity_dir],
    L => q[i2b_lane],
    B => q[i2b_basecalls_dir],
    RG => q[i2b_rg],
    PU => q[i2b_pu],
    LIBRARY_NAME => q[i2b_library_name],
    SAMPLE_ALIAS => q[i2b_sample_aliases],
    STUDY_NAME => q[i2b_study_name],
    SEC_BC_SEQ => q[i2b_sec_bc_seq_val],
    SEC_BC_QUAL => q[i2b_sec_bc_qual_val],
    BC_SEQ => q[i2b_bc_seq_val],
    BC_QUAL => q[i2b_bc_qual_val],
    BC_READ => q[i2b_bc_read],
    FIRST_INDEX_0 => q[i2b_first_index_0],
    FINAL_INDEX_0 => q[i2b_final_index_0],
    FIRST_INDEX_1 => q[i2b_first_index_1],
    FINAL_INDEX_1 => q[i2b_final_index_1],
    FIRST_0 => q[i2b_first_0],
    FINAL_0 => q[i2b_final_0],
    FIRST_1 => q[i2b_first_1],
    FINAL_1 => q[i2b_final_1],
  );
  my %bid_flag_map = (
    BARCODE_FILE => q[barcode_file],
    METRICS_FILE => q[decoder_metrics],
    MAX_NO_CALLS => q[bid_max_no_calls],
    CONVERT_LOW_QUALITY_TO_NO_CALL => q[bid_convert_low_quality_to_no_call],
  );

  my $id_run             = $self->id_run();
  my $position = $lane_lims->position;

  my $runfolder_path     = $self->runfolder_path;
  my $run_folder     = $self->run_folder;
  my $intensity_path     = $self->intensity_path;
  my $qc_path            = $self->qc_path;
  my $basecall_path = $self->basecall_path;
  my $no_cal_path       = $self->recalibrated_path;
  my $bam_basecall_path  = $self->bam_basecall_path;

  my $full_bam_name  = $bam_basecall_path . q{/}. $id_run . q{_} .$position. q{.bam};

  $p4_params{qc_check_id_run} = $id_run; # used by tag_metrics qc check
  $p4_params{qc_check_qc_in_dir} = $bam_basecall_path; # used by tag_metrics qc check
  $p4_params{qc_check_qc_out_dir} = $qc_path; # used by tag_metrics qc check
  $p4_params{tileviz_dir} = $qc_path . q[/tileviz/] . $id_run . q[_] . $position ; # used by tileviz
  $p4_params{outdatadir} = $no_cal_path; # base for all (most?) outputs
  $p4_params{spatial_filter_file} = $no_cal_path . q[/] . $id_run . q[_] . $position . q{.bam.filter}; # full name for the spatial filter file
  my $spatial_filter_stats_file = $p4_params{spatial_filter_stats} = $no_cal_path . q[/] . $id_run . q[_] . $position . q{.bam.filter.stats}; # full name for the spatial filter stats file (for qc check)
  $p4_params{seqchksum_file} = $bam_basecall_path . q[/] . $id_run . q[_] . $position . q{.post_i2b.seqchksum}; # full name for the lane-level seqchksum file
  $p4_params{filtered_bam} = $no_cal_path . q[/] . $id_run . q[_] . $position . q{.bam}; # full name for the spatially filtered lane-level file
  $p4_params{unfiltered_cram_file} = $no_cal_path . q[/] . $id_run . q[_] . $position . q{.unfiltered.cram}; # full name for spatially unfiltered lane-level cram file
  $p4_params{md5filename} = $no_cal_path . q[/] . $id_run . q[_] . $position . q{.bam.md5}; # full name for the md5 for the spatially filtered lane-level file
  $p4_params{split_prefix} = $no_cal_path . q[/lane] . $position; # location for split bam files

  my $log_folder = $self->make_log_dir($bam_basecall_path);
  my $job_name = join q/_/, (q{p4_stage1}, $id_run, $position, $self->timestamp());
  my $outfile = $log_folder . q{/} . $job_name . q{.%J.out};
  $job_name = q{'} . $job_name . q{'};

  $p4_params{illumina2bam_jar} = $self->_Illumina2bam_jar;
  $p4_params{i2b_run_path} = $runfolder_path;
  $p4_params{i2b_thread_count} = $self->general_values_conf()->{'p4_stage1_i2b_thread_count'} || $DEFAULT_I2B_THREAD_COUNT;
  $p4_params{i2b_runfolder} = $run_folder;
  $p4_params{$i2b_flag_map{q/I/}} = $intensity_path;
  $p4_params{$i2b_flag_map{q/L/}} = $position;
  $p4_params{$i2b_flag_map{q/B/}} = $self->basecall_path;
  $p4_params{$i2b_flag_map{q/RG/}} = join q[_], $id_run, $position;
  $p4_params{$i2b_flag_map{q/PU/}} = join q[_], $self->run_folder, $position;

  my $st_names = $self->_get_library_sample_study_names($lane_lims);

  if($st_names->{library}){
    $p4_params{$i2b_flag_map{q/LIBRARY_NAME/}} =  $st_names->{library};
  }
  if($st_names->{sample}){
    $p4_params{$i2b_flag_map{q/SAMPLE_ALIAS/}} =  $st_names->{sample};
  }
  if($st_names->{study}){
    my $study = $st_names->{study};
    $study =~ s/"/\\"/gmxs;
    $p4_params{$i2b_flag_map{q/STUDY_NAME/}} =  q{"} . $study . q{"};
  }
  if ($self->_extra_tradis_transposon_read) {
    $p4_params{$i2b_flag_map{q/SEC_BC_SEQ/}} = q[BC];
    $p4_params{$i2b_flag_map{q/SEC_BC_QUAL/}} = q[QT];
    $p4_params{$i2b_flag_map{q/BC_SEQ/}} = q[tr];
    $p4_params{$i2b_flag_map{q/BC_QUAL/}} = q[tq];
  }

  if($lane_lims->inline_index_exists) {
    my $index_start = $lane_lims->inline_index_start;
    my $index_end = $lane_lims->inline_index_end;
    my $index_read = $lane_lims->inline_index_read;

    if ($index_start && $index_end && $index_read) {
      $self->info(q{P4 stage1 analysis of a lane with inline indexes});

      my($first, $final) = $self->read1_cycle_range();
      if ($index_read == 1) {
        $p4_params{$i2b_flag_map{q/BC_READ/}} = $p4_params{$i2b_flag_map{q/SEC_BC_READ/}} = 1;
        $index_start += ($first-1);
        $index_end += ($first-1);
        $p4_params{$i2b_flag_map{q/FIRST_INDEX_0/}} = $index_start;
        $p4_params{$i2b_flag_map{q/FINAL_INDEX_0/}} = $index_end;
        $p4_params{$i2b_flag_map{q/FIRST_INDEX_1/}} = $first;
        $p4_params{$i2b_flag_map{q/FINAL_INDEX_1/}} = $index_start-1;
        $p4_params{$i2b_flag_map{q/FIRST_0/}} = $index_end+1;
        $p4_params{$i2b_flag_map{q/FINAL_0/}} = $final;
        if ($self->is_paired_read()) {
          ($first, $final) = $self->read2_cycle_range();
          $p4_params{$i2b_flag_map{q/FIRST_1/}} = $first;
          $p4_params{$i2b_flag_map{q/FINAL_1/}} = $final;
        }
      } elsif ($index_read == 2) {
        $p4_params{$i2b_flag_map{q/BC_READ/}} = $p4_params{$i2b_flag_map{q/SEC_BC_READ/}} = 2;
        $self->is_paired_read() or $self->logcroak(q{Inline index read (2) does not exist});
        $p4_params{$i2b_flag_map{q/FIRST_0/}} = $first;
        $p4_params{$i2b_flag_map{q/FINAL_0/}} = $final;
        ($first, $final) = $self->read2_cycle_range();
        $index_start += ($first-1);
        $index_end += ($first-1);
        $p4_params{$i2b_flag_map{q/FIRST_INDEX_0/}} = $index_start;
        $p4_params{$i2b_flag_map{q/FINAL_INDEX_0/}} = $index_end;
        $p4_params{$i2b_flag_map{q/FIRST_INDEX_1/}} = $first;
        $p4_params{$i2b_flag_map{q/FINAL_INDEX_1/}} = $index_start-1;
        $p4_params{$i2b_flag_map{q/FIRST_1/}} = $index_end+1;
        $p4_params{$i2b_flag_map{q/FINAL_1/}} = $final;
      } else {
        $self->logcroak("Invalid inline index read ($index_read)");
      }
      $p4_params{$i2b_flag_map{q/SEC_BC_SEQ/}} = q{br};
      $p4_params{$i2b_flag_map{q/SEC_BC_QUAL/}} = q{qr};
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


  my $splice_flag = q[];
  my $prune_flag = q[];
  if($self->is_multiplexed_lane($position)) {
    if (!$tag_list_file) {
      $self->logcroak('Tag list file path should be defined for multiplexed lane ', $position);
    }

    $p4_params{bamindexdecoder_jar} = $self->_BamIndexDecoder_jar;
    $p4_params{$bid_flag_map{q/BARCODE_FILE/}} = $tag_list_file;
    $p4_params{$bid_flag_map{q/METRICS_FILE/}} = $full_bam_name . q{.tag_decode.metrics};

    my $num_of_plexes_per_lane = $self->_get_number_of_plexes_excluding_control($lane_lims);
    if($num_of_plexes_per_lane == 1) {
      $p4_params{$bid_flag_map{q/MAX_NO_CALLS/}} = $self->general_values_conf()->{single_plex_decode_max_no_calls};
      $p4_params{bid_convert_low_quality_to_no_call_flag} = q[--convert-low-quality];
    }
  }
  else {
    $self->info(q{P4 stage1 analysis on non-plexed lane});

    # This will avoid using BamIndexDecoder or attempting to split a non-muliplexed lane.
    $splice_flag = q[-splice_nodes '"'"'bamadapterfind:-bamcollate:'"'"'];
    $prune_flag = q[-prune_nodes '"'"'fs1p_tee_split:__SPLIT_BAM_OUT__-'"'"'];
  }

  if(!$self->is_paired_read) {
    $p4_params{phix_alignment_method} = q[bwa_aln_se];
  }

  my $name_root = $id_run . q{_} . $position;
  # allow specification of thread number for some processes in config file. Note: these threads are being drawn from the same pool. Unless
  #  they appear in the config file, their values will be derived from what LSF assigns the job based on the -n value supplied to the bsub
  #  command (see $num_slots in _default_resources()).
  my $aligner_slots = $self->general_values_conf()->{'p4_stage1_aligner_slots'} || q[`npg_pipeline_job_env_to_threads --exclude -2 --divide 3`];
  my $samtobam_slots = $self->general_values_conf()->{'p4_stage1_samtobam_slots'} || q[`npg_pipeline_job_env_to_threads --exclude -1 --divide 3`];
  my $bamsormadup_slots = $self->general_values_conf()->{'p4_stage1_bamsort_slots'} || q[`npg_pipeline_job_env_to_threads --divide 3`];
  my $bamrecompress_slots = $self->general_values_conf()->{'p4_stage1_bamrecompress_slots'} || q[`npg_pipeline_job_env_to_threads`];

  my $i2b_implementation_flag = q[];
  if(my $val = $self->general_values_conf()->{'p4_stage1_i2b_implementation'}) {
    $p4_params{i2b_implementation} = $val;
  }

# the way the CONVERT_LOW_QUALITY_TO_NO_CALL/--convert-low-quality flag is currently handled will only work for bambi decode.
#  So I'll fix bid_implementation to that
  $p4_params{bid_implementation} = q[bambi];

  my $command = join q( ), q(bash -c '),
                           q(cd), $self->p4_stage1_errlog_paths->{$position}, q{&&},
                           q(vtfp.pl),
                           $splice_flag, $prune_flag,
                           qq(-o run_$name_root.json),
                           q(-param_vals), (join q{/}, $self->p4_stage1_params_paths->{$position}, $name_root.q{_p4s1_pv_in.json}),
                           q(-export_param_vals), $name_root.q{_p4s1_pv_out_}.$self->_job_id().q/.json/,
                           q{-keys cfgdatadir -vals $}.q{(dirname $}.q{(readlink -f $}.q{(which vtfp.pl)))/../data/vtlib/},
                           qq(-keys aligner_numthreads -vals $aligner_slots),
                           qq(-keys s2b_mt_val -vals $samtobam_slots),
                           qq(-keys bamsormadup_numthreads -vals $bamsormadup_slots),
                           qq(-keys br_numthreads_val -vals $bamrecompress_slots),
                           $i2b_implementation_flag,
                           q{$}.q{(dirname $}.q{(dirname $}.q{(readlink -f $}.q{(which vtfp.pl))))/data/vtlib/bcl2bam_phix_deplex_wtsi_stage1_template.json},
                           q{&&},
                           qq(viv.pl -s -x -v 3 -o viv_$name_root.log run_$name_root.json),
                           q{&&},
                           qq{qc --check spatial_filter --id_run $id_run --position $position --qc_out $qc_path < $spatial_filter_stats_file},
                           q(');

  return ($command, %p4_params);
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

   my $phix_ref;
   try {
      $phix_ref = $ruser->refs->[0];
      if($aligner eq q{picard}) {
        $phix_ref .= q{.dict};
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

=item JSON

=item open

=item npg_common::roles::software_location

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
