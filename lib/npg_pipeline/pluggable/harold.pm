package npg_pipeline::pluggable::harold;

use Moose;
use Carp;
use English qw{-no_match_vars};
use Try::Tiny;
use Readonly;
use File::Spec::Functions qw{catfile};

use npg_pipeline::cache;

extends q{npg_pipeline::pluggable};
with    q{npg_tracking::illumina::run::long_info};

our $VERSION = '0';

=head1 NAME

npg_pipeline::pluggable::harold

=head1 SYNOPSIS

  my $harold = npg_pipeline::pluggable::harold->new(
      id_run => 1234,
      run_folder => q{123456_IL2_1234},
    );

=head1 DESCRIPTION

A collection of common pipeline functions.

=cut

Readonly::Array our @SAVE2FILE_STATUS_FUNCTIONS => qw/
                                    run_analysis_complete
                                    run_qc_complete
                                    run_qc_review_pending
                                    run_run_archived
                                    run_archival_in_progress
                                    run_analysis_in_progress
                                    run_secondary_analysis_in_progress
                                    lane_analysis_in_progress
                                    lane_analysis_complete
                                                     /;

Readonly::Array our @AUTOQC_FUNCTIONS => qw/
                                          qc_genotype 
                                          qc_verify_bam_id 
                                          qc_qX_yield
                                          qc_insert_size
                                          qc_adapter 
                                          qc_ref_match 
                                          qc_sequence_error
                                          qc_gc_fraction 
                                          qc_gc_bias
                                          qc_pulldown_metrics
                                          qc_tag_metrics
                                          qc_upstream_tags
                                          qc_rna_seqc
                                           /;

Readonly::Scalar my $MIN_ARCHIVE_PATH_DEPTH => 3;

=head1 SUBROUTINES/METHODS

=cut

has q{_log_file_name} => (
   isa           => q{Str},
   is            => q{ro},
   lazy_build    => 1,
);
sub _build__log_file_name {
  my $self = shift;
  my $log_name = $self->script_name . q{_} . $self->id_run();
  $log_name .= q{_} . $self->timestamp() . q{.log};
  # If $self->script_name includes a directory path, change / to _
  $log_name =~ s{/}{_}gmxs;
  return $log_name;
}

=head2 BUILD

Called on new construction to ensure that certain parameters are filled/set up

=cut

sub BUILD {
  my $self = shift;
  $self->_inject_save2file_status_functions();
  $self->_inject_autoqc_functions();
  return;
}

=head2 log_file_path

Suggested log file full path.

=cut

sub log_file_path {
  my $self = shift;
  return catfile($self->runfolder_path(), $self->_log_file_name);
}

=head2 prepare

If spider flag is true, runs spidering (creating/reusing LIMs data cache).
Called in the pipeline's main method before executing functions.

=cut

override 'prepare' => sub {
  my $self = shift;
  super();
  if ($self->spider) {
    $self->run_spider();
  }
  return;
};

=head2 run_analysis_complete
=head2 run_qc_complete
=head2 run_qc_review_pending
=head2 run_run_archived
=head2 run_archival_in_progress
=head2 run_analysis_in_progress
=head2 run_secondary_analysis_in_progress
=head2 lane_analysis_in_progress
=head2 lane_analysis_complete

=cut

sub _inject_save2file_status_functions {
  my $self = shift;
  foreach my $function (@SAVE2FILE_STATUS_FUNCTIONS) {
    ##no critic (TestingAndDebugging::ProhibitNoStrict TestingAndDebugging::ProhibitNoWarnings)
    no strict 'refs';
    no warnings 'redefine';
    *{$function}= sub{  my ($self, @args) = @_; return $self->_save_status( $function, @args ); };
  }
  return;
}

sub _save_status {
  my ($self, $function_name, @args) = @_;

  my $status = $function_name;
  my $lane_status = 0;
  $status =~ s/\Arun_//xms;
  if ($status eq $function_name) {
    $status =~ s/\Alane_//xms;
    $lane_status = 1;
  }
  $status =~ s/_/ /xmsg;

  my $required_job_completion = shift @args;

  my $sr = $self->new_with_cloned_attributes(q{npg_pipeline::launcher::status},{
    status           => $status,
    lane_status_flag => $lane_status,
  });

  return $sr->submit({required_job_completion => $required_job_completion});
}

=head2 qc_cache_reads 
=head2 qc_genotype 
=head2 qc_qX_yield
=head2 qc_insert_size
=head2 qc_adapter
=head2 qc_contamination 
=head2 qc_ref_match 
=head2 qc_sequence_error
=head2 qc_gc_fraction 
=head2 qc_gc_bias
=head2 qc_tag_metrics
=head3 qc_rna_seqc

functions to run various autoqc checks.

=cut
sub _inject_autoqc_functions {

  foreach my $function (@AUTOQC_FUNCTIONS) {
    my $qc = $function;
    $qc =~ s/qc_//sm;
    if ($qc eq 'cache_reads') { $qc = 'cache'; }
    ##no critic (TestingAndDebugging::ProhibitNoStrict TestingAndDebugging::ProhibitNoWarnings)
    no strict 'refs';
    no warnings 'redefine';
    *{$function}= sub{  my ($self, @args) = @_;  return $self->_qc_runner($qc, @args); };
  }
  return;
}

################
# single method to actually run qc, which should be given the test from the called method requested
sub _qc_runner {
  my ($self, $qc_to_run, @args) = @_;

  my $required_job_completion = shift @args;
  my @job_ids = $self->new_with_cloned_attributes(q{npg_pipeline::archive::file::qc},
                                                  {qc_to_run  => $qc_to_run,})
     ->run_qc({required_job_completion => $required_job_completion,});

  return @job_ids;
}

=head2 run_spider

Generates cached metadata that are needed by the pipeline
or reuses the existing cache.

Will set the relevant env. variables in the global scope.

The new cache is created in the analysis_path directory.

See npg_pipeline::cache for details.

=cut

sub run_spider {
  my $self = shift;
  try {
    my $cache = npg_pipeline::cache->new(
      'id_run'           => $self->id_run,
      'set_env_vars'     => 1,
      'cache_location'   => $self->analysis_path,
      'lims_driver_type' => $self->lims_driver_type,
      'id_flowcell_lims' => $self->id_flowcell_lims,
      'flowcell_barcode' => $self->flowcell_id
     );
    $cache->setup();
    $self->info(join qq[\n], @{$cache->messages});
  } catch {
    $self->logcroak(qq[Error while spidering: $_]);
  };
  return;
}

=head2 fix_config_files

check config files have correct data, and fix runfolder,instrument,id_run
this will not be a job launched on the farm, and so should be taken into account
i.e. don't expect it to run after something has been submitted, expecting a dependency
=cut

sub fix_config_files {
  my ( $self, @args ) = @_;

  if ( ! $self->no_fix_config_files() ) {
    $self->new_with_cloned_attributes( q{npg_pipeline::analysis::FixConfigFiles} )->run();
  }

  return ();
}

=head2 illumina2bam

=cut

sub illumina2bam {
   my ( $self, @args ) = @_;

   my $illumina2bam = $self->new_with_cloned_attributes(q{npg_pipeline::archive::file::generation::illumina2bam});
   my $required_job_completion = shift @args;
   my @job_ids = $illumina2bam->generate({required_job_completion => $required_job_completion,});
   return @job_ids;
}

=head2 create_summary_link_analysis

function which creates/changes the summary link in the runfolder

=cut

sub create_summary_link_analysis {
  my ($self, @args) = @_;

  # check that this hasn't been expressly turned off
  if ($self->no_summary_link()) {
    $self->info(q{Summary link creation turned off});
    return ();
  }

  my $required_job_completion = shift @args;

  my $rfl = $self->new_with_cloned_attributes(q{npg_pipeline::run::folder::link},{
    folder        => q{analysis},
  });

  my $arg_refs = {
    required_job_completion => $required_job_completion,
  };

  if (!$required_job_completion) {
    $rfl->make_link($arg_refs);
    return ();
  }

  my $job_id = $rfl->submit_create_link($arg_refs);
  return ($job_id);
}

=head2 create_empty_fastq

Creates a full set of empty fastq and fastqcheck files

=cut
sub create_empty_fastq {
  my ( $self ) = @_;
  return $self->new_with_cloned_attributes(
      q{npg_pipeline::archive::file::generation})->create_empty_fastq_files();
}

no Moose;
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

=item Try::Tiny

=item File::Spec::Functions

=item npg_tracking::illumina::run::long_info

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown
Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2016 Genome Research Ltd

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
