package npg_pipeline::function::autoqc;

use Moose;
use namespace::autoclean;
use Readonly;
use File::Spec;
use Class::Load qw{load_class};

use npg_pipeline::function::definition;

extends q{npg_pipeline::base};

our $VERSION = '0';

Readonly::Scalar my $QC_SCRIPT_NAME           => q{qc};
Readonly::Scalar my $MEMORY_REQ               => 6000;
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
  return $self->qc_to_run() =~ /^ verify_bam_id |
                                  genotype |
                                  pulldown_metrics $/smx;
}

sub create {
  my $self = shift;

  $self->info(sprintf 'Running autoqc check %s for run %i',
                      $self->qc_to_run(), $self->id_run());

  my @definitions = ();
  foreach my $p ( $self->positions() ) {
    my $is_multiplexed_lane = $self->is_multiplexed_lane($p);

    my $h = {'id_run' => $self->id_run, 'position' => $p};
    push @definitions, $self->_create_definition($h, $is_multiplexed_lane);

    if ( $self->is_indexed && $is_multiplexed_lane) {
      foreach my $tag_index (@{$self->get_tag_index_list($p)}) {
        my %th = %{$h};
        $th{'tag_index'} = $tag_index;
        push @definitions, $self->_create_definition(\%th, $is_multiplexed_lane);
      }
    }
  }

  if (!@definitions) {
    my $ref = $self->_basic_attrs();
    $ref->{'excluded'} = 1;
    push @definitions, npg_pipeline::function::definition->new($ref);
  }

  return \@definitions;
}

sub _create_definition {
  my ($self, $h, $is_multiplexed_lane) = @_;
  if ($self->_should_run($h, $is_multiplexed_lane)) {
    my $command = $self->_generate_command($h);
    return $self->_create_definition_object($h, $command);
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
  my ($self, $h, $command) = @_;

  my $ref = $self->_basic_attrs();
  my $qc_to_run = $self->qc_to_run;

  $ref->{'job_name'}        = join q{_}, $QC_SCRIPT_NAME, $qc_to_run,
                                         $h->{'id_run'}, $self->timestamp();
  $ref->{'fs_slots_num'}    = 1;
  $ref->{'composition'}     = $self->create_composition($h);
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
    $ref->{'command_preexec'} = $self->ref_adapter_pre_exec_string();
  }

  if ($qc_to_run =~ /insert_size|sequence_error|ref_match|pulldown_metrics/smx ) {
    $ref->{'memory'} = $MEMORY_REQ;
  } elsif ($qc_to_run eq q[adapter]) {
    $ref->{'memory'} = $MEMORY_REQ_ADAPTER;
  }

  return npg_pipeline::function::definition->new($ref);
}

sub _generate_command {
  my ($self, $h) = @_;

  my $check     = $self->qc_to_run();
  my $position  = $h->{'position'};
  my $tag_index = $h->{'tag_index'};
  my $c = sprintf '%s --check=%s --id_run=%i --position=%i',
                  $QC_SCRIPT_NAME, $check, $h->{'id_run'}, $position;

  if (defined $tag_index) {
    $c .= q[ --tag_index=] . $tag_index;
  }

  if ($check eq q[insert_size]) {
    $c .= $self->is_paired_read() ? q[ --is_paired_read] : q[ --no-is_paired_read];
  } elsif ($check eq q[qX_yield] && $self->platform_HiSeq) {
    $c .= q[ --platform_is_hiseq];
  }

  my $qc_out = (defined $tag_index and $check ne q[spatial_filter])? $self->lane_qc_path($position) : $self->qc_path();
  $qc_out or $self->logcroak('Failed to get qc_out directory');
  my $qc_in  = defined $tag_index ? $self->lane_archive_path($position) : $self->archive_path();
  ##no critic (ControlStructures::ProhibitCascadingIfElse)
  if ($check eq q[adapter]) {
    $qc_in  = defined $tag_index
              ? File::Spec->catfile($self->recalibrated_path(), q[lane] . $position)
              : $self->recalibrated_path();
  } elsif ($check eq q[spatial_filter]) {
    $qc_in .= (q[/lane] . $position);
  } elsif ($check eq q[tag_metrics]) {
    $qc_in = $self->bam_basecall_path();
  } elsif ($check eq q[sequence_error] or $check eq q[ref_match] or $check eq q[insert_size]) {
    $qc_in .= q[/.npg_cache_10000]
  }
  $qc_in or $self->logcroak('Failed to get qc_in directory');
  $c .= qq[ --qc_in=$qc_in --qc_out=$qc_out];

  return $c;
}

sub _should_run {
  my ($self, $h, $is_multiplexed_lane) = @_;

  my $tag_index = $h->{'tag_index'};
  my $can_run = 1;

  if ($self->_is_lane_level_check()) {
    return !defined $tag_index;
  }

  if ($self->_is_lane_level_check4indexed_lane()) {
    return $is_multiplexed_lane && !defined $tag_index;
  }

  if ($self->_is_check4target_file()) {
    $can_run = ((!defined $tag_index) && !$is_multiplexed_lane) ||
	       ((defined $tag_index)  && $is_multiplexed_lane);
  }

  if ($self->qc_to_run() eq q[adapter]) {
    $can_run = (defined $tag_index) || !$is_multiplexed_lane;
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
