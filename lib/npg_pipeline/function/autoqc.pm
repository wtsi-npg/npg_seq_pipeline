package npg_pipeline::function::autoqc;

use Moose;
use namespace::autoclean;
use Readonly;
use List::MoreUtils qw{any};
use Class::Load qw{load_class};
use File::Spec::Functions qw{catdir};
use Try::Tiny;

use npg_pipeline::cache::reference;
use npg_qc::autoqc::constants qw/
         $SAMTOOLS_NO_FILTER
         $SAMTOOLS_SEC_QCFAIL_SUPPL_FILTER
                                /;

extends q{npg_pipeline::base_resource};
with q{npg_pipeline::function::util};

our $VERSION = '0';

Readonly::Scalar my $QC_SCRIPT_NAME => q{qc};

Readonly::Array  my @CHECKS_NEED_PAIREDNESS_INFO => qw/
                                                 insert_size
                                                 gc_fraction
                                                 qX_yield
                                                      /;

Readonly::Scalar my $TARGET_FILE_CHECK_RE => qr{ \A
                                              adapter |
                                             bcfstats |
                                        verify_bam_id |
                                             genotype |
                                     pulldown_metrics |
                                     haplotag_metrics
                                                 \Z }xms;

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
    ->find_attribute_by_name('repository') ? 1 : 0;
}

sub BUILD {
  my $self = shift;
  load_class($self->_qc_module_name);
  return;
}

sub create {
  my $self = shift;

  $self->info(sprintf 'Running autoqc check %s for run %i',
                      $self->qc_to_run(), $self->id_run());

  my @definitions = ();
  my %done_as_lane = ();

  if ($self->qc_to_run() eq 'interop') {
    push @definitions, $self->_create_definition4interop();
  } else {

    my $is_lane = 1;
    for my $lp (@{$self->products->{lanes}}) {

      $self->debug(sprintf '  autoqc check %s for lane, rpt_list: %s, is_pool: %s',
                   $self->qc_to_run(), $lp->rpt_list, ($lp->lims->is_pool? q[True]: q[False]));

      $done_as_lane{$lp->rpt_list} = 1;
      if ($self->_should_run($lp, $is_lane)) {
        push @definitions, $self->_create_definition($lp);
      }
    }

    $is_lane = 0;
    for my $dp (@{$self->products->{data_products}}) {
      # skip data_products that have already been processed as lanes
      # (i.e. libraries or single-sample pools)
      $done_as_lane{$dp->{rpt_list}} and next;

      my $tag_index = $dp->composition->get_component(0)->tag_index;
      my $is_plex = (defined $tag_index);

      $self->debug(sprintf
        '  autoqc check %s for data_product, rpt_list: %s, is_plex: %s, is_pool: %s, tag_index: %s',
        $self->qc_to_run(), $dp->{rpt_list}, ($is_plex? q[True]: q[False]),
        ($dp->lims->is_pool? q[True]: q[False]), ($is_plex? $tag_index: q[NONE]));

      if ($self->_should_run($dp, $is_lane, $is_plex)) {
        push @definitions, $self->_create_definition($dp);
      }
    }
  }

  if (!@definitions) {
    push @definitions, $self->create_excluded_definition();
  }

  return \@definitions;
}

sub _create_definition {
  my ($self, $product) = @_;

  my $ref = {
      'job_name'    => $self->_job_name(),
      'composition' => $product->composition,
      'command'     => $self->_generate_command($product)
  };
  if ( ($self->qc_to_run eq 'adapter') || $self->_check_uses_refrepos() ) {
      $ref->{'command_preexec'} = $self->repos_pre_exec_string();
  }
  return $self->create_definition($ref);
}

sub _job_name {
  my $self = shift;
  return join q{_}, $QC_SCRIPT_NAME, $self->qc_to_run,
                    $self->label(), $self->timestamp();
}

sub _create_definition4interop {
  my $self = shift;

  # To level InterOp files directory should be given as qc_in.
  # The check is run once, produces per-lane result object, which are
  # saved in lane-level qc directories.

  my @lane_components = map { $_->composition->get_component(0) }
                        @{$self->products->{lanes}};
  my $run_composition =
    npg_tracking::glossary::composition->new(components => \@lane_components);

  my $ref = {};
  $ref->{'job_name'} = $self->_job_name();
  my $c = sprintf '%s --check=%s --rpt_list="%s" --qc_in=%s',
                  $QC_SCRIPT_NAME,
                  $self->qc_to_run(),
                  $run_composition->freeze2rpt,
                  catdir($self->runfolder_path(), 'InterOp');
  $c .= q[ ] . join q[ ],
    map { "--qc_out=$_" }
    map { $_->qc_out_path($self->archive_path) }
    @{$self->products->{lanes}};

  $ref->{'command'}  = $c;

  return $self->create_definition($ref);
}

sub _generate_command {
  my ($self, $dp) = @_;

  my $check           = $self->qc_to_run();
  my $archive_path    = $self->archive_path;
  my $recal_path      = $self->recalibrated_path;
  my $dp_archive_path = $dp->path($self->archive_path);
  my $cache10k_path   = $dp->short_files_cache_path($archive_path);
  my $qc_out_path     = $dp->qc_out_path($archive_path);

  my $bamfile_path        = $dp->file_path($dp_archive_path, ext => 'bam');
  my $cramfile_path       = $dp->file_path($dp_archive_path, ext => 'cram');

  my $fq1_filepath = $dp->file_path($cache10k_path, ext => 'fastq', suffix => '1');
  my $fq2_filepath = $dp->file_path($cache10k_path, ext => 'fastq', suffix => '2');

  my $c = sprintf '%s --check=%s --rpt_list="%s" --filename_root=%s --qc_out=%s',
                  $QC_SCRIPT_NAME, $check, $dp->{rpt_list}, $dp->file_name_root, $qc_out_path;

  if(any { $check eq $_ } @CHECKS_NEED_PAIREDNESS_INFO) {
    $c .= q[ --] . ($self->is_paired_read() ? q[] : q[no-]) . q[is_paired_read];
  }

  ####################################
  # set input_files or input directory
  ####################################
  ##no critic (RegularExpressions::RequireExtendedFormatting)
  ##no critic (ControlStructures::ProhibitCascadingIfElse)

  if(any { /$check/sm } qw( gc_fraction qX_yield )) {
    my $suffix = ($dp->composition->num_components == 1 &&
                   !defined $dp->composition->get_component(0)->tag_index) # true for a lane
                 ? $SAMTOOLS_NO_FILTER : $SAMTOOLS_SEC_QCFAIL_SUPPL_FILTER;
    $c .= qq[ --qc_in=$dp_archive_path --suffix=$suffix];
    if ($check eq q[qX_yield] && $self->platform_HiSeq) {
      $c .= q[ --platform_is_hiseq];
    }
  }
  elsif(any { /$check/sm } qw( insert_size ref_match sequence_error )) {
    $c .= qq[ --input_files=$fq1_filepath];
    if($self->is_paired_read) {
      $c .= qq[ --input_files=$fq2_filepath];
    }
  }
  elsif(any { /$check/sm } qw( verify_bam_id )) {
    $c .= qq{ --input_files=$bamfile_path}; # note: single bam file
  }
  elsif(any { /$check/sm } qw( adapter bcfstats genotype pulldown_metrics)) {
    $c .= qq{ --input_files=$cramfile_path}; # note: single cram file
  }
  elsif($check eq q/spatial_filter/) {

    my $position = $dp->composition->get_component(0)->position; # lane-level check, so position is unique

    my %qc_in_roots = ();
    for my $redp (@{$self->products->{data_products}}) {
      # find any merged products with components from this position (lane)
      if(any { $_->{position} == $position } @{$redp->composition->{components}}) {
        $qc_in_roots{$self->archive_path} = 1;
      }
    }
    $c .= q[ ] . join q[ ], (map {"--qc_in=$_"} (keys %qc_in_roots));
  }
  elsif($check eq q/review/) {
    $c .= sprintf q{ --qc_in=%s --conf_path=%s --runfolder_path=%s},
      $qc_out_path, $self->conf_path, $self->runfolder_path;
  }
  else {
    ## default input_files [none?]
  }

  return $c;
}

sub _should_run {
  my ($self, $product, $is_lane, $is_plex) = @_;

  my $is_pool = $product->lims->is_pool; # Note - true for merged data.

  if ($self->qc_to_run() eq 'spatial_filter') {
    # Run on individual lanes only.
    return $is_lane && !($self->platform_NovaSeq || $self->platform_NovaSeqX) ? 1 : 0;
  }
  if ($self->qc_to_run() eq 'tag_metrics') {
    # For indexed runs, run on individual lanes if they are pools of samples.
    return $self->is_indexed() && $is_lane && $is_pool ? 1 : 0;
  }
  if ($self->qc_to_run() eq 'review') {
    # This check should never run on tag zero, other entities are fair game.
    if ($is_plex && $product->is_tag_zero_product) {
      return 0;
    }
  }
  if ($self->qc_to_run() =~ $TARGET_FILE_CHECK_RE) {
    # Do not run on tag zero files, unmerged or merged.
    # Do not run on lane-level data if the lane is a pool.
    # Run on lane-level data for a single library.
    # Run on merged plexes or lanes.
    if (($is_plex && $product->is_tag_zero_product) || ($is_lane && $is_pool)) {
      return 0;
    }
  }

  # The rest will abide by the return value of the can_run method of the
  # autoqc check. We have to provide enough argument to the constructor for
  # the object to be created and can_run to execute.
  my %init_hash = ( rpt_list => $product->rpt_list, lims => $product->lims );
  if ($self->has_repository && $self->_check_uses_refrepos()) {
    $init_hash{'repository'} = $self->repository;
  }
  if ($self->qc_to_run() eq 'insert_size') {
    $init_hash{'is_paired_read'} = $self->is_paired_read() ? 1 : 0;
  } elsif ($self->qc_to_run() eq 'review') {
    $init_hash{'product_conf_file_path'} = $self->product_conf_file_path;
    $init_hash{'runfolder_path'} = $self->runfolder_path;
  } elsif ($self->qc_to_run() eq 'genotype') {
    my $ref_fasta = npg_pipeline::cache::reference->instance()
                    ->get_path($product, q(fasta), $self->repository());
    if ($ref_fasta) {
      $init_hash{'reference_fasta'} = $ref_fasta;
    }
  }

  my $class = $self->_qc_module_name();
  my $check_obj;
  try {
    $check_obj = $class->new(\%init_hash);
  } catch {
    delete $init_hash{'lims'};
    $check_obj = $class->new(\%init_hash); # Allow to fail at this stage.
  };

  return $check_obj->can_run();
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

=head2 BUILD

Method called by Moose before returning a new object instance to the
caller. Loads the auto qc check class defined by the qc_to_run attribute
into memory, errors if this fails.

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

=item List::MoreUtils

=item Class::Load

=item File::Spec::Functions

=item Try::Tiny

=item npg_qc::autoqc::constants

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2018,2019,2020,2024 Genome Research Ltd.

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
