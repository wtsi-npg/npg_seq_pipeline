package npg_pipeline::archive::file::qc;

use Moose;
use Readonly;
use File::Spec;
use Class::Load qw{load_class};

use npg_pipeline::lsf_job;

extends q{npg_pipeline::base};

our $VERSION = '0';

Readonly::Scalar my $QC_SCRIPT_NAME          => q{qc};
Readonly::Scalar my $LSF_MEMORY_REQ          => 6000;
Readonly::Scalar my $LSF_MEMORY_REQ_ADAPTER  => 1500;
Readonly::Scalar my $LSF_INDEX_MULTIPLIER    => 10_000;

has q{qc_to_run}       => (isa      => q{Str},
                           is       => q{ro},
                           required => 1,);

has q{_qc_module_name} => (isa        => q{Str},
                           is         => q{ro},
                           required   => 0,
                           init_arg   => undef,
                           lazy_build => 1,);
sub _build__qc_module_name {
  my $self = shift;
  return q{npg_qc::autoqc::checks::} . $self->qc_to_run;
}

has q{_check_uses_refrepos} => (isa        => q{Bool},
                                is         => q{ro},
                                required   => 0,
                                init_arg   => undef,
                                lazy_build => 1,);
sub _build__check_uses_refrepos {
  my $self = shift;
  return $self->_qc_module_name()->meta()
    ->find_attribute_by_name('repository') || ($self->qc_to_run eq 'adapter') ? 1 : 0;
}

sub BUILD {
  my $self = shift;
  load_class($self->_qc_module_name);
  return;
}

sub run_qc {
  my ($self, $arg_refs) = @_;

  my $qc_to_run = $self->qc_to_run();
  my $id_run = $self->id_run();
  $self->log(qq{Running qc test $qc_to_run on Run $id_run});

  foreach my $position ($self->positions()) {
    if ( $self->is_multiplexed_lane($position) && (-e $self->lane_archive_path( $position ) ) ) {
      my $lane_qc_dir = $self->lane_qc_path( $position );
      if (!-e $lane_qc_dir) {
        mkdir $lane_qc_dir;
      }
    }
  }

  my $required_job_completion = $arg_refs->{'required_job_completion'};
  $required_job_completion ||= q{};

  my @job_ids;
  my $bsub_command = $self->_generate_bsub_command($required_job_completion);
  if ( $bsub_command ) {
    push @job_ids, $self->submit_bsub_command( $bsub_command );
  }

  if ( $self->is_indexed ) {
    $bsub_command = $self->_generate_bsub_command($required_job_completion, 1 );
    if ( $bsub_command ) {
      push @job_ids, $self->submit_bsub_command( $bsub_command );
    }
  }

  return @job_ids;
}

sub _generate_bsub_command {
  my ($self, $required_job_completion, $indexed) = @_;

  my $array_string = $self->_lsf_job_array($indexed);
  if (!$array_string) {
    return;
  }

  my $command = $self->_qc_command($indexed);

  $required_job_completion ||= q{};
  my $timestamp = $self->timestamp();
  my $id_run = $self->id_run();

  my $job_name = join q{_},$QC_SCRIPT_NAME,$self->qc_to_run(),$id_run,$timestamp;

  $self->make_log_dir( $self->qc_path() );
  my $qc_out = $self->qc_path() . q{/log};
  my $out_subscript = q{.%I.%J.out};
  my $outfile = File::Spec->catfile($qc_out, $job_name . $out_subscript);

  $job_name = q{'} . $job_name . $array_string;

  if ($self->qc_to_run eq 'ref_match') {
    # hack to try to alleviate Lustre client multiple simulaneous access bug
    # (ensure elements only run eight at a time)
    $job_name .= q{%8};
  } elsif ( ! $self->no_array_cpu_limit() ) {
    $job_name .= q{%} . $self->array_cpu_limit();
  }
  $job_name .= q{'};

  my $job_sub = q{bsub -q } . ( $self->qc_to_run() eq q[upstream_tags] ? $self->lowload_lsf_queue() : $self->lsf_queue() ) . q{ } .
    #lowload queue for upstream tags as it has qc and tracking db access
    $self->_lsf_options($self->qc_to_run()) . qq{ $required_job_completion -J $job_name -o $outfile};
  if ( $self->_check_uses_refrepos() ) {
    $job_sub .= q{ } . $self->ref_adapter_pre_exec_string();
  }

  $job_sub .= qq{ '$command'};

  if ($self->verbose()) { $self->log($job_sub); }
  return $job_sub;
}

sub _qc_command {
  my ($self, $indexed) = @_;

  my $c = $QC_SCRIPT_NAME;
  $c .= q{ --check=} . $self->qc_to_run();
  $c .= q{ --id_run=} . $self->id_run();

  if ( $self->qc_to_run() eq q[adapter] ) {
    $c .= q{ --file_type=bam};
  }

  my $qc_in;
  my $qc_out_dir;
  my $archive_path      = $self->archive_path;
  my $recalibrated_path = $self->recalibrated_path;

  if ( defined $indexed ) {
    my $tagstr = $self->_tag_index_decode_string();
    my $lanestr = $self->_position_decode_string();
    my $lane_archive_path = File::Spec->catfile($archive_path, q[lane] . $lanestr);
    $qc_in = ( $self->qc_to_run() eq q[adapter]) ?
	  File::Spec->catfile($recalibrated_path, q[lane] . $lanestr) : $lane_archive_path;
    $qc_out_dir =  File::Spec->catfile($lane_archive_path, q[qc]);
    $c .= q{ --position=}  . $lanestr;
    $c .= q{ --tag_index=} . $tagstr;
  } else {
    $c .= q{ --position=}  . $self->lsb_jobindex();
    $qc_in  = $self->qc_to_run() eq q{tag_metrics} ? $self->bam_basecall_path :
        (($self->qc_to_run() eq q[adapter]) ? $recalibrated_path : $archive_path);
    $qc_out_dir = $self->qc_path();
  }

  $c .= qq{ --qc_in=$qc_in --qc_out=$qc_out_dir};

  return $c;
}

sub _should_run {
  my ($self, $position, $tag_index) = @_;

  my $qc = $self->qc_to_run();

  if ($qc =~ /^tag_metrics|upstream_tags|gc_bias|verify_bam_id|genotype|pulldown_metrics$/smx) {
    my $is_multiplexed_lane = $self->is_multiplexed_lane($position);
    if ($qc =~ /^gc_bias|verify_bam_id|genotype|pulldown_metrics$/smx) {
      my $can_run = ((!defined $tag_index) && !$is_multiplexed_lane) ||
	  ((defined $tag_index) && $is_multiplexed_lane);
      if (!$can_run) {
        return;
      }
    } else {
      return (!defined $tag_index) && $is_multiplexed_lane;
    }
  }

  my $init_hash = {
    position  => $position,
    id_run    => $self->id_run(),
  };
  if ( defined $tag_index ) {
    $init_hash->{'tag_index'} = $tag_index;
  }
  if ($self->has_repository && $self->_check_uses_refrepos()) {
    $init_hash->{'repository'} = $self->repository;
  }

  return $self->_qc_module_name()->new($init_hash)->can_run();
}

sub _lsf_job_array {
  my ($self, $indexed) = @_;

  my @lsf_indices = ();
  foreach my $lane ($self->positions()) {
    if ($indexed) {
      foreach my $tag (@{$self->get_tag_index_list($lane)}) {
        if ( $self->_should_run($lane, $tag) ) {
          push @lsf_indices, ( $lane * $LSF_INDEX_MULTIPLIER ) + $tag;
        }
      }
    } else {
      if ( $self->_should_run($lane) ) {
        push @lsf_indices, $lane;
      }
    }
  }
  @lsf_indices = sort { $a <=> $b } @lsf_indices;
  return @lsf_indices ? npg_pipeline::lsf_job->create_array_string(@lsf_indices) : q[];
}

sub _position_decode_string {
  return q{`echo $} . q{LSB_JOBINDEX/} . $LSF_INDEX_MULTIPLIER . q{ | bc`};
}
sub _tag_index_decode_string {
  return q{`echo $} . q{LSB_JOBINDEX%} . $LSF_INDEX_MULTIPLIER . q{ | bc`};
}

sub _lsf_options {
  my ($self, $qc_to_run) = @_;

  my $resources;
  if ($qc_to_run =~ /insert_size|sequence_error|ref_match|pulldown_metrics/smx ) {
    $resources = npg_pipeline::lsf_job->new(memory => $LSF_MEMORY_REQ)->memory_spec();
  } elsif ($qc_to_run eq q[adapter]) {
    $resources = npg_pipeline::lsf_job->new(memory => $LSF_MEMORY_REQ_ADAPTER)->memory_spec() .
      q[ -R 'span[hosts=1]' ] . q{ -n} . $self->general_values_conf()->{qc_adapter_cpu};
  }
  $resources = $self->fs_resource_string( {resource_string => $resources, counter_slots_per_job => 1,} );
  $resources ||= q{};
  return $resources;
}

no Moose;
__PACKAGE__->meta->make_immutable;
1;
__END__

=head1 NAME

npg_pipeline::archive::file::qc

=head1 SYNOPSIS

  my @job_ids;
  $aqc = npg_pipeline::archive::file::qc->new(
    run_folder => $run_folder,
    qc_to_run => q{test},
  );
  my $arg_refs = {
    required_job_completion  => q{-w'done(123) && done(321)'},
    timestamp                => q{20090709-123456},
    id_run                   => 1234,
  }
  @job_ids = $aqc->run_qc($arg_refs);

=head1 DESCRIPTION

Object module responsible for launching LSF autoqc jobs to LSF.

=head1 SUBROUTINES/METHODS

=head2 qc_to_run

Name of the QC check to run.

=head2 BUILD

Constructor helper.

=head2 run_qc

Launches the qc jobs.

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item Readonly

=item File::Spec

=item Class::Load

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2016 Genome Research Limited

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
