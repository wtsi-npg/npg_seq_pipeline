package npg_pipeline::analysis::harold_calibration_bam;

use Moose;
use Carp;
use English qw{-no_match_vars};
use Cwd;
use Readonly;
use List::MoreUtils qw{any};
use File::Spec;
use File::Basename;

use npg_tracking::util::types;
use npg_pipeline::lsf_job;

our $VERSION = '0';

Readonly::Scalar our $PB_ALIGN_BAM_PREFIX => q{pb_align_};
Readonly::Scalar our $MAKE_STATS_J => 4;
Readonly::Scalar our $MAKE_STATS_MEM => 350;


extends q{npg_pipeline::base};
with qw{
        npg_common::roles::software_location
        npg_pipeline::roles::business::harold_calibration_reqs
};

=head1 NAME

npg_pipeline::analysis::harold_calibration_bam

=head1 SYNOPSIS

  my $oHaroldCalibration = npg_pipeline:analysis::harold_calibration_bam->new();

=head1 DESCRIPTION

Object runner to launch internal calibration instead of CASAVA based calibration

=head1 SUBROUTINES/METHODS

=head2 spatial_filter_path

Absolute path to spatial_filter executable

=cut

has 'spatial_filter_path'  => (
                       is      => 'ro',
                       isa     => 'NpgCommonResolvedPathExecutable',
                       coerce  => 1,
                       default => 'spatial_filter',
                              );

=head2 pb_calibration_bin

Directory where pb bcalibration family executables are 

=cut 

has 'pb_calibration_bin' => (
                       isa     => 'NpgTrackingDirectory',
                       is      => 'ro',
                       lazy    => 1,
                       builder => '_build_pb_calibration_bin',
                            );
sub _build_pb_calibration_bin {
  my $self = shift;
  return dirname($self->spatial_filter_path());
}

sub _generate_illumina_basecall_stats_command {
  my ( $self, $arg_refs ) = @_;

  my $job_dependencies = $arg_refs->{required_job_completion};

  my $basecall_dir = $self->basecall_path();
  my $dir = $self->bam_basecall_path();

  $self->make_log_dir( $dir ); # create a log directory within bam_basecalls

  my $bsub_queue  = $self->lsf_queue;
  my $job_name  =  q{basecall_stats_} . $self->id_run() . q{_} . $self->timestamp();

  my @command;
  push @command, 'bsub';
  push @command, "-q $bsub_queue";
  push @command, qq{-o $dir/log/}. $job_name . q{.%J.out};
  push @command, "-J $job_name";

  my $hosts = 1;
  my $memory_spec = join q[], npg_pipeline::lsf_job->new(memory => $MAKE_STATS_MEM)->memory_spec(), " -R 'span[hosts=$hosts]'";
  push @command, $self->fs_resource_string( {
    resource_string => $memory_spec,
    counter_slots_per_job => $MAKE_STATS_J,
  } );
  push @command,  q{-n } . $MAKE_STATS_J;
  push @command, $job_dependencies || q[];

  push @command, q["]; # " enclose command in quotes

  my $bcl2qseq_path = join q[/], $self->illumina_pipeline_conf()->{olb}, $self->illumina_pipeline_conf()->{bcl_to_qseq};

  my $cmd = join q[ && ],
    qq{cd $dir},
    q{if [[ -f Makefile ]]; then echo Makefile already present 1>&2; else echo creating bcl2qseq Makefile 1>&2; }.
      qq{$bcl2qseq_path -b $basecall_dir -o $dir --overwrite; fi},
    qq[make -j $MAKE_STATS_J Matrix Phasing],
    qq[make -j $MAKE_STATS_J BustardSummary.x{s,m}l];

  push @command,$cmd;

  push @command, q["]; # " closing quote

  return join q[ ], @command;
}

=head2 generate_illumina_basecall_stats

Use Illumina tools to generate the (per run) BustardSummary and IVC reports (from on instrument RTA basecalling).

=cut

sub generate_illumina_basecall_stats{
  my ( $self, $arg_refs ) = @_;
  my @id_runs = $self->submit_bsub_command( $self->_generate_illumina_basecall_stats_command($arg_refs) );
  return @id_runs;
}

=head2 generate_alignment_files

submit the jobs which will generate bam alignment files ready to pass onto calibration table generator

  my $aJobIds = $oHaroldCalibration->generate_alignment_files({
    required_job_completion => $sJobRequirenmentString,
  });

=cut

sub generate_alignment_files {
  my ( $self, $arg_refs ) = @_;

  my $job_ids = [];
  my $job_dependencies = $arg_refs->{'required_job_completion'};

  # create the calibration directory
  my $pb_cal_dir = $self->create_pb_calibration_directory();

  $self->_set_recalibrated_path( $self->pb_cal_path() );

  foreach my $position ( $self->positions ) {
    if ( ! $self->is_spiked_lane( $position ) ){
       $self->warn("Lane $position is not spiked with phiX, no PB_cal alignment job needed");
       next;
    }
    $self->_generate_alignment_file_per_lane({
      position         => $position,
      job_ids          => $job_ids,
      job_dependencies => $job_dependencies
    });
  }

  return @{ $job_ids };
}

=head2 generate_calibration_table

submit the bsub jobs which will create the calibration tables, returning an array of job_ids.

  my $aJobIds = $oHaroldCalibration->generate_calibration_table({
    required_job_completion => $sJobRequirenmentString,
  });

=cut

sub generate_calibration_table {
  my ($self, $arg_refs) = @_;

  if ( !$self->recalibration() ) {
    $self->warn(q{This has been set to run with no recalibration step});
    return ();
  }

  my $job_ids = [];
  my $job_dependencies = $arg_refs->{'required_job_completion'};

  # create the calibration directory
  my $pb_cal_dir = $self->create_pb_calibration_directory();

  $self->_set_recalibrated_path( $self->pb_cal_path() );

  my $snp_file = $self->control_snp_file();

  foreach my $position ( $self->positions ) {
    if ( ! $self->is_spiked_lane( $position ) ){
       $self->warn("Lane $position is not spiked with phiX, no PB_cal calibration table job needed");
       next;
    }
    $self->_generate_calibration_table_per_lane( {
      position         => $position,
      job_ids          => $job_ids,
      job_dependencies => $job_dependencies,
      snp_file         => $snp_file,
    } );
  }

  return @{ $job_ids };
}

=head2 generate_recalibrated_bam

submit the bsub jobs which will recalibrate the lanes, returning an array of job_ids.

  my $aJobIds = $oHaroldCalibration->generate_recalibrated_bam({
    required_job_completion => $sJobRequirenmentString,
  });

=cut

sub generate_recalibrated_bam {
  my ($self, $arg_refs) = @_;

  $self->_bam_merger_cmd();

  my $pb_cal_dir = $self->pb_cal_path();

  if ( ! $self->directory_exists($pb_cal_dir) ) {
    $self->warn(qq{$pb_cal_dir does not exist, not executing jobs});
    return ();
  }

  my $job_ids = [];
  my $job_dependencies = $arg_refs->{'required_job_completion'};

  foreach my $position ( $self->positions ) {
    my $arg_ref_hash = {
      job_ids          => $job_ids,
      position         => $position,
      job_dependencies => $job_dependencies,
    };
    $self->_generate_recalibrated_bam_per_lane( $arg_ref_hash );
  }

  return @{ $job_ids };
}

##########
# private methods

sub _generate_recalibrated_bam_per_lane {
  my ( $self, $arg_refs ) = @_;

  my $lane    = $arg_refs->{'position'};

  my $cal_table_1_to_use = $self->calibration_table_name( {
    id_run   => $self->id_run(),
    position => $lane,
  } );


  my $args_bam= {
    position         => $lane,
    job_dependencies => $arg_refs->{'job_dependencies'},
    ct               => $cal_table_1_to_use,
  };

  my $bsub_command = $self->_recalibration_bsub_command( $args_bam );
  $self->debug($bsub_command);

  push @{ $arg_refs->{'job_ids'} }, $self->submit_bsub_command( $bsub_command );

  return;
}

sub _generate_calibration_table_per_lane {
  my ( $self, $arg_refs ) = @_;

  my $args = {
    position         => $arg_refs->{'position'},
    job_dependencies => $arg_refs->{'job_dependencies'},
    is_spiked_phix   => 1,
    snp_file         => $arg_refs->{'snp_file'},
  };

  my $bsub_command = $self->_calibration_table_bsub_command( $args );

  $self->debug($bsub_command);

  push @{ $arg_refs->{'job_ids'} }, $self->submit_bsub_command($bsub_command);

  return;
}

# generate the alignment file
sub _generate_alignment_file_per_lane {
  my ( $self, $arg_refs ) = @_;

  my $bsub_command = $self->_alignment_file_bsub_command( {
    position         => $arg_refs->{'position'},
    job_dependencies => $arg_refs->{'job_dependencies'},
    ref_seq          => $self->control_ref(),
    is_paired        => $self->is_paired_read(),
    is_spiked_phix   => 1,
  } );

  $self->debug($bsub_command);

  push @{ $arg_refs->{'job_ids'} }, $self->submit_bsub_command($bsub_command);

  return;
}

# generate bsub command for generating the alignment files required
sub _alignment_file_bsub_command {
  my ( $self, $arg_refs ) = @_;

  my $position          = $arg_refs->{'position'};
  my $job_dependencies  = $arg_refs->{'job_dependencies'};
  my $ref_seq           = $arg_refs->{'ref_seq'};
  my $is_paired         = $arg_refs->{'is_paired'};
  my $is_spiked_phix    = $arg_refs->{'is_spiked_phix'};

  my $mem_size    = $self->general_values_conf()->{bam_creation_memory};
  my $timestamp   = $self->timestamp();
  my $bsub_queue  = $self->lsf_queue;
  my $id_run      = $self->id_run();

  my $job_name  = $self->is_paired_read() ? $self->align_job() . q{_} . $id_run . q{_} . $position . q{_paired_} . $timestamp
                :                           $self->align_job() . q{_} . $id_run . q{_} . $position . q{_} . $timestamp
                ;

  my @command;
  push @command, q{cd}, $self->pb_cal_path(), q{&&};
  push @command, $self->pb_calibration_bin() . q{/} . $self->alignment_script();
  push @command,  q{--aln_parms "-t "`npg_pipeline_job_env_to_threads` };
  push @command,  q{--sam_parms "-t "`npg_pipeline_job_env_to_threads --maximum 8` };
  if ($self->spatial_filter) {
    push @command, q{--spatial_filter};
    push @command,  q{--sf_parms "} . q{--region_size } . $self->pb_cal_pipeline_conf()->{region_size} . q{ }
                    . q{--region_mismatch_threshold } . $self->pb_cal_pipeline_conf()->{region_mismatch_threshold} . q{ }
                    . q{--region_insertion_threshold } . $self->pb_cal_pipeline_conf()->{region_insertion_threshold} . q{ }
                    . q{--region_deletion_threshold } . $self->pb_cal_pipeline_conf()->{region_deletion_threshold} . q{ }
                    . q{--tileviz } . $self->qc_path . q{/} . q(tileviz) . q{/} .$id_run. q{_} . $position . q{ }
                    . q{"};
    push @command, q{--bam_join_jar } . $self->_bam_merger_jar;
  };
  push @command,  q{--ref } . $ref_seq;
  if( $is_paired ) {
    push @command, q{--read1 1};
    push @command, q{--read2 2};
  } else {
    push @command, q{--read 0};
  }
  push @command, q{--bam }.$self->bam_basecall_path().q{/}.$id_run.q{_}.$position.q{.bam};
  push @command, q{--prefix } . $PB_ALIGN_BAM_PREFIX . $id_run.q{_}.$position;
  push @command, q{--pf_filter};

  my $job_command = join q[ ], @command;
  $job_command=~s/'/'"'"'/smxg;

  @command = ();
  push @command, 'bsub';
  push @command, "-q $bsub_queue";
  push @command, $self->ref_adapter_pre_exec_string();
  push @command, q{-o }.$self->pb_cal_path().q{/log/}. $job_name . q{.%J.out};
  push @command, "-J $job_name";

  my $hosts = 1;
  my $memory_spec = join q[], npg_pipeline::lsf_job->new(memory => $mem_size)->memory_spec(), " -R 'span[hosts=$hosts]'";
  push @command, $self->fs_resource_string( {
    resource_string => $memory_spec,
    counter_slots_per_job => $self->general_values_conf()->{io_resource_slots},
  } );
  push @command,  q{-n } . $self->general_values_conf()->{bwa_aln_threads};
  push @command, $job_dependencies || q[];
  push @command, "'$job_command'";    # " enclose command in quotes

  return join q[ ], @command;
}



# generate bsub command for recalibrating the lane qseq data
sub _recalibration_bsub_command {
  my ($self, $arg_refs) = @_;
  my $position = $arg_refs->{'position'};
  my $job_dependencies = $arg_refs->{'job_dependencies'};
  my $id_run = $self->id_run();

  my $output_bam = $id_run . q{_} . $position . q{.bam};
  my $output_bam_md5 = $output_bam . q{.md5};
  my $input_bam  = q{../} . $output_bam;
  my $input_bam_md5 = $input_bam . q{.md5};
  my $phix_bam   = $PB_ALIGN_BAM_PREFIX . $id_run . q{_} .$position . q{.bam};

  #pb_calibration_cmd 
  my @command_pb_cal;
  push @command_pb_cal, $self->pb_calibration_bin() . q{/} . $self->recalibration_script();
  push @command_pb_cal, q{--u};
  push @command_pb_cal, q{--bam } . $input_bam;
  if ($self->dif_files_path()) {
    push @command_pb_cal, q{--intensity_dir } . $self->dif_files_path(); # for dif file location, it should be bustard_dir if OLB
  }

  my $cycle_start1 = 1;
  my $alims = $self->lims->associated_child_lims_ia;
  #if read 1 has an inline index reset cycle_start1 to the first cycle after the index
  if ($alims->{$position}->inline_index_exists && $alims->{$position}->inline_index_read == 1) {
     $cycle_start1 += $alims->{$position}->inline_index_end;
  }
  if( !$self->is_paired_read() ){
     push @command_pb_cal, qq{--cstart $cycle_start1};
  }else{
     push @command_pb_cal, qq{--cstart1 $cycle_start1};
     my @r2r = $self->read2_cycle_range();
     my $cycle_start2 = $r2r[0];
     #if read 2 has an inline index reset cycle_start2 to the first cycle after the index
     if ($alims->{$position}->inline_index_exists && $alims->{$position}->inline_index_read == 2) {
        $cycle_start2 += $alims->{$position}->inline_index_end;
     }
     push @command_pb_cal, qq{--cstart2 $cycle_start2};
  }

  my $cl_table1   = $arg_refs->{ct};
  push @command_pb_cal, qq{--ct $cl_table1};

  my $pb_calibration_cmd = join q[ ], @command_pb_cal;
  #finish pb_calibration_cmd;

  #bam merge command
  my $bam_merge_cmd = q{ } . $self->_bam_merger_cmd() . qq{ O=$output_bam ALIGNED=$phix_bam};

  #bjob now
  my $mem_size = $self->mem_score();
  my $timestamp   = $self->timestamp();
  my $bsub_queue  = $self->lsf_queue;
  my $job_name  = $self->score_job() . q{_} . $id_run . q{_} . $position . q{_} . $timestamp;

  my @command;
  push @command, 'bsub';
  push @command, "-q $bsub_queue";
  push @command, q{-o }.$self->pb_cal_path().q{/log/}. $job_name . q{.%J.out};
  push @command, "-J $job_name";

  my $hosts = 1;
  my $memory_spec = join q[], npg_pipeline::lsf_job->new(memory => $mem_size)->memory_spec(), " -R 'span[hosts=$hosts]'";
  push @command, $self->fs_resource_string( {
    resource_string => $memory_spec,
    counter_slots_per_job => 2 * $self->general_values_conf()->{io_resource_slots},
  } );
  push @command, $job_dependencies || q[];

  push @command, q[']; # ' enclose command in quotes
  push @command, q{cd}, $self->pb_cal_path(), q{&&};

  my $check_cl_table = qq{-f $cl_table1};

  my $check_cmd = qq{if [[ -f $phix_bam ]]; then echo phix alignment so merging alignments with 1>&2; set -o pipefail; (if [ $check_cl_table ]; then echo  recalibrated qvals 1>&2; $pb_calibration_cmd ; else echo no recalibration 1>&2; cat $input_bam ; fi;) | };
  if ($self->spatial_filter) {
    $check_cmd .= qq{ ( if [[ -f ${phix_bam}.filter ]]; then echo applying spatial filter 1>&2; } . $self->pb_calibration_bin() . q{/} .
                  qq{spatial_filter -u -a -f -F ${phix_bam}.filter - } .
                  q{2> >( tee /dev/stderr | } . qq{qc --check spatial_filter --id_run $id_run --position $position --qc_out } . $self->qc_path . q{ );} .
                  q{ else echo no spatial filter 1>&2; cat; fi;) | };
  }
  $check_cmd .= qq{$bam_merge_cmd; else echo symlinking as no phix alignment 1>&2; rm -f $output_bam; ln -s $input_bam $output_bam; rm -f $output_bam_md5; ln -s $input_bam_md5 $output_bam_md5; fi};
  $check_cmd =~ s/'/'"'"'/smxg; # cope with any single ' quote in the command when submitting command within single ' quote in bash -c argument - null op here?
  $check_cmd = "bash -c '$check_cmd'"; # >( ...) is a bash'ish

  $check_cmd =~ s/'/'"'"'/smxg; # cope with any single ' quote in the command when submitting command within single ' quote in bsub command line argument
  push @command,$check_cmd;

  push @command, q[']; # ' closing quote

  my $bsub_command = join q[ ], @command;

  return $bsub_command;
}

# generate bsub command for generating the calibration table required
sub _calibration_table_bsub_command {
  my ($self, $arg_refs) = @_;
  my $position = $arg_refs->{'position'};
  my $job_dependencies = $arg_refs->{'job_dependencies'};

  my $mem_size = $self->mem_calibration();
  my $timestamp   = $self->timestamp();
  my $bsub_queue  = $self->lsf_queue;
  my $id_run = $self->id_run();

  my $job_name  = $self->cal_table_job() . q{_} . $id_run . q{_} . $position . q{_} . $timestamp ;

  my @command;
  push @command, 'bsub';
  push @command, "-q $bsub_queue";
  push @command, $self->ref_adapter_pre_exec_string();
  push @command, q{-o }.$self->pb_cal_path().q{/log/}. $job_name . q{.%J.out};
  push @command, "-J $job_name";

  my $hosts = 1;
  my $memory_spec = join q[], npg_pipeline::lsf_job->new(memory => $mem_size)->memory_spec(), " -R 'span[hosts=$hosts]'";
  push @command, $self->fs_resource_string( {
    resource_string => $memory_spec,
    counter_slots_per_job => 2 * $self->general_values_conf()->{io_resource_slots},
  } );
  push @command, $job_dependencies || q[];

  push @command, q["];               # " enclose command in quotes
  push @command, q{cd}, $self->pb_cal_path(), q{&&};
  push @command, $self->pb_calibration_bin() . q{/} . $self->cal_table_script();
  push @command, q{--intensity_dir }. $self->dif_files_path(); # for dif file location, change to bustard if olb
  push @command, q{--t_filter } . $self->t_filter();
  push @command, q{--prefix } . $id_run . q{_} . $position ;

  my $cycle_start1 = 1;
  #if read 1 has an inline index reset cycle_start1 to the first cycle after the index
  my $alims = $self->lims->associated_child_lims_ia;
  if ($alims->{$position}->inline_index_exists && $alims->{$position}->inline_index_read == 1) {
     $cycle_start1 += $alims->{$position}->inline_index_end;
  }
  if( !$self->is_paired_read() ){
     push @command, qq{--cstart $cycle_start1};
  }else{
     push @command, qq{--cstart1 $cycle_start1};
     my @r2r = $self->read2_cycle_range();
     my $cycle_start2 = $r2r[0];
     #if read 2 has an inline index reset cycle_start2 to the first cycle after the index
     if ($alims->{$position}->inline_index_exists && $alims->{$position}->inline_index_read == 2) {
        $cycle_start2 += $alims->{$position}->inline_index_end;
     }
     push @command, qq{--cstart2 $cycle_start2};
  }

  if ( $arg_refs->{is_spiked_phix} ) {
    if (!$arg_refs->{snp_file}) {
      $self->logcroak('SNP file not available');
    }
    push @command, q{--snp } . $arg_refs->{snp_file};
  }

  push @command, qq{--bam ${PB_ALIGN_BAM_PREFIX}${id_run}_${position}.bam};

  push @command, q["];               # " closing quote

  my $bsub_command = join q[ ], @command;
  return $bsub_command;
}

has q{_bam_merger_jar} => (
                           isa        => q{NpgCommonResolvedPathJarFile},
                           is         => q{ro},
                           coerce     => 1,
                           default    => q{BamMerger.jar},
                          );

has q{_bam_merger_cmd} => (isa        => q{Str},
                           is         => q{ro},
                           lazy_build => 1,
                          );

sub _build__bam_merger_cmd{
   my $self = shift;

   return $self->java_cmd . q{ -Xmx1024m}
                    . q{ -jar } . $self->_bam_merger_jar()
                    . q{ CREATE_MD5_FILE=true VALIDATION_STRINGENCY=SILENT KEEP=true I=/dev/stdin REPLACE_QUAL=true};
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

=item Readonly

=item List::MoreUtils

=item File::Basename

=item File::Spec

=item npg_tracking::util::types

=item npg_common::roles::software_location

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Guoying Qi

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2015 Genome Research Ltd

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
