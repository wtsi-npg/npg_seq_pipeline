package npg_pipeline::function::autoqc;

use Moose;
use namespace::autoclean;
use Readonly;
use List::MoreUtils qw{any};
use File::Spec;
use Class::Load qw{load_class};

use npg_pipeline::function::definition;

extends q{npg_pipeline::base};
with q{npg_pipeline::function::util};

our $VERSION = '0';

Readonly::Scalar my $QC_SCRIPT_NAME           => q{qc};
Readonly::Scalar my $MEMORY_REQ               => 6000;
Readonly::Scalar my $MEMORY_REQ_BWA           => 8000;
Readonly::Scalar my $MEMORY_REQ_ADAPTER       => 1500;
Readonly::Scalar my $REFMATCH_ARRAY_CPU_LIMIT => 8;

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
  load_class($self->_qc_module_name);
  return $self->_qc_module_name()->meta()
    ->find_attribute_by_name('repository') ? 1 : 0;
}

has q{_is_lane_level_check} => (
                                isa        => q{Bool},
                                is         => q{ro},
                                required   => 0,
                                init_arg   => undef,
                                lazy_build => 1,);
sub _build__is_lane_level_check {
  my $self = shift;
  return $self->qc_to_run() =~ /^ spatial_filter $/smx;
}

has q{_is_lane_level_check4indexed_lane} => (
                                isa        => q{Bool},
                                is         => q{ro},
                                required   => 0,
                                init_arg   => undef,
                                lazy_build => 1,);
sub _build__is_lane_level_check4indexed_lane {
  my $self = shift;
  return $self->qc_to_run() =~ /^ tag_metrics | upstream_tags $/smx;
}

has q{_is_check4target_file} => (
                                isa        => q{Bool},
                                is         => q{ro},
                                required   => 0,
                                init_arg   => undef,
                                lazy_build => 1,);
sub _build__is_check4target_file {
  my $self = shift;
  ##no critic (RegularExpressions::RequireBracesForMultiline)
  return $self->qc_to_run() =~ /^ adapter |
                                  verify_bam_id |
                                  genotype |
                                  pulldown_metrics $/smx;
}

sub create {
  my $self = shift;

  $self->info(sprintf 'Running autoqc check %s for run %i',
                      $self->qc_to_run(), $self->id_run());


  my @definitions = ();
  for my $dp (@{$self->products->{lanes}}) {
    my $h = {'rpt_list' => $dp->{rpt_list}};
    my $l = $dp->lims; #??
    my $is_multiplexed_lane = $l->tags; #??

    push @definitions, $self->_create_definition($dp, $is_multiplexed_lane, 0);
  }

  for my $dp (@{$self->products->{data_products}}) {
    my $h = {'rpt_list' => $dp->{rpt_list}};
    my $l = $dp->lims; #??
    my $is_multiplexed_lane = $l->tags; #??

    push @definitions, $self->_create_definition($dp, $is_multiplexed_lane, 1);
  }

  if (!@definitions) {
    my $ref = $self->_basic_attrs();
    $ref->{'excluded'} = 1;
    push @definitions, npg_pipeline::function::definition->new($ref);
  }

  return \@definitions;
}

sub _create_definition {
  my ($self, $dp, $is_multiplexed_lane, $is_plex) = @_;

  my $h = {'rpt_list' => $dp->{rpt_list}};

  if ($self->_should_run($h, $is_multiplexed_lane, $is_plex)) {
    my $command = $self->_generate_command($dp);
    return $self->_create_definition_object($dp, $command);
  }
  return;
}

sub _basic_attrs {
  my $self = shift;
  return { 'created_by' => __PACKAGE__,
           'created_on' => $self->timestamp(),
           'identifier' => $self->id_run() };
}

sub _create_definition_object {
  my ($self, $dp, $command) = @_;

  my $ref = $self->_basic_attrs();
  my $qc_to_run = $self->qc_to_run;

  $ref->{'job_name'}        = join q{_}, $QC_SCRIPT_NAME, $qc_to_run,
                                         $self->id_run(), $self->timestamp();
  $ref->{'fs_slots_num'}    = 1;
  $ref->{'composition'}     = $dp->{composition};
  $ref->{'command'}         = $command;

  if ($qc_to_run eq q[adapter]) {
    $ref->{'num_cpus'}      = [$self->general_values_conf()->{'qc_adapter_cpu'} || 1];
    if ($ref->{'num_cpus'} > 1) {
      $ref->{'num_hosts'}   = 1;
    }
  }

  $ref->{'apply_array_cpu_limit'} = 1;
  #####
  # Lower value for ref_match to try to alleviate Lustre client multiple
  # simulaneous access bug (ensure elements only run eight at a time).
  #
  if ($qc_to_run eq 'ref_match') {
    $ref->{'array_cpu_limit'} = $REFMATCH_ARRAY_CPU_LIMIT;
  }

  if ($qc_to_run eq q[upstream_tags]) {
    $ref->{'queue'} = $npg_pipeline::function::definition::LOWLOAD_QUEUE;
  }

  if ( ($qc_to_run eq 'adapter') || $self->_check_uses_refrepos() ) {
    $ref->{'command_preexec'} = $self->repos_pre_exec_string();
  }

  if ($qc_to_run =~ /insert_size|sequence_error/smx ) {
    $ref->{'memory'} = $MEMORY_REQ_BWA;
  } elsif ($qc_to_run  =~ /ref_match|pulldown_metrics/smx) {
    $ref->{'memory'} = $MEMORY_REQ;
  } elsif ($qc_to_run eq q[adapter]) {
    $ref->{'memory'} = $MEMORY_REQ_ADAPTER;
  }

  return npg_pipeline::function::definition->new($ref);
}

sub _generate_command {
  my ($self, $dp) = @_;

  my $check     = $self->qc_to_run();
  my $archive_path = $self->archive_path;
  my $recal_path= $self->recalibrated_path;
  my $dp_archive_path = $dp->path($self->archive_path);
  my $cache10k_path = $dp->short_files_cache_path($archive_path);
  my $qc_out_path = $dp->qc_out_path($archive_path);
  my $bamfile_path = File::Spec->catdir($dp_archive_path, $dp->file_name(ext => 'bam'));
  my $tagzerobamfile_path = File::Spec->catdir($recal_path, $dp->file_name(ext => 'bam', suffix => '#0'));
  $tagzerobamfile_path =~ s/_#0/#0/smx;
  my $fq1_filepath = File::Spec->catdir($cache10k_path, $dp->file_name(ext => 'fastq', suffix => '1'));
  my $fq2_filepath = File::Spec->catdir($cache10k_path, $dp->file_name(ext => 'fastq', suffix => '2'));
  my $fqt_filepath = File::Spec->catdir($cache10k_path, $dp->file_name(ext => 'fastq', suffix => 't'));
  my $fqc1_filepath = File::Spec->catdir($dp_archive_path, $dp->file_name(ext => 'fastqcheck', suffix => '1'));
  my $fqc2_filepath = File::Spec->catdir($dp_archive_path, $dp->file_name(ext => 'fastqcheck', suffix => '2'));

  my $c = sprintf '%s --check=%s --rpt_list="%s" --filename_root=%s --qc_out=%s',
                  $QC_SCRIPT_NAME, $check, $dp->{rpt_list}, $dp->file_name_root, $qc_out_path;

  if ($check eq q[insert_size]) {
    $c .= $self->is_paired_read() ? q[ --is_paired_read] : q[ --no-is_paired_read];
  } elsif ($check eq q[qX_yield] && $self->platform_HiSeq) {
    $c .= q[ --platform_is_hiseq];
  }

  #################
  # set input_files
  #################
  ##no critic (RegularExpressions::RequireExtendedFormatting)
  ##no critic (ControlStructures::ProhibitCascadingIfElse)
  if(any { /$check/sm } qw( gc_fraction qX_yield )) {
    $c .= qq[ --input_files=$fqc1_filepath --input_files=$fqc2_filepath];
  }
  elsif(any { /$check/sm } qw( insert_size ref_match sequence_error )) {
    $c .= qq[ --input_files=$fq1_filepath --input_files=$fq2_filepath];
  }
  elsif(any { /$check/sm } qw( adapter genotype verify_bam_id )) {
    $c .= qq{ --input_files=$bamfile_path}; # note: single bam file 
  }
  elsif($check eq q/upstream_tags/) {
    $c .= qq{ --tag0_bam_file=$tagzerobamfile_path}; # note: single bam file 
    $c .= qq{ --cal_path=$recal_path};
  }
  elsif($check eq q/spatial_filter/) {

    my $position = $dp->composition->get_component(0)->position; # lane-level check, so position is unique

    for my $redp (@{$self->products->{data_products}}) {
      # find any merged products with components from this position (lane)
      if(any { $_->{position} == $position } @{$redp->composition->{components}}) {
        my $input_file = File::Spec->catdir($redp->path($self->archive_path), $redp->file_name(ext => 'spatial_filter.stats'));
        $c .= qq{ --input_files=$input_file};
      }
    }
  }
  else {
    ## default input_files [none]?
  }

  return $c;
}

sub _should_run {
  my ($self, $h, $is_multiplexed_lane, $is_plex) = @_;

  my $can_run = 1;

  if ($self->_is_lane_level_check()) {
    return !$is_plex;
  }

  if ($self->_is_lane_level_check4indexed_lane()) {
    return $is_multiplexed_lane && !$is_plex;
  }

  if ($self->_is_check4target_file()) {
    $can_run = ((!$is_plex && !$is_multiplexed_lane) ||
	       ($is_plex && $is_multiplexed_lane));
  }

  if ($can_run) {
    my %init_hash = %{$h};
    if ($self->has_repository && $self->_check_uses_refrepos()) {
      $init_hash{'repository'} = $self->repository;
    }
    if ($self->qc_to_run() eq 'insert_size') {
      $init_hash{'is_paired_read'} = $self->is_paired_read() ? 1 : 0;
    }

    load_class($self->_qc_module_name);
    $can_run = $self->_qc_module_name()->new(\%init_hash)->can_run();
  }

  return $can_run;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

npg_pipeline::function::autoqc

=head1 SYNOPSIS

  my $aqc = npg_pipeline::function::autoqc->new(
    run_folder => $run_folder,
    qc_to_run => q{insert_size},
  );
  my $definitions_array = $aqc->create();

=head1 DESCRIPTION

Autoqc checks jobs definition.

=head1 SUBROUTINES/METHODS

=head2 qc_to_run

Name of the QC check to run, required attribute.

=head2 create

Creates and returns an array of npg_pipeline::function::definition
objects for all entities of the run eligible to run this autoqc check.

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item namespace::autoclean

=item Readonly

=item File::Spec

=item Class::Load

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

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
