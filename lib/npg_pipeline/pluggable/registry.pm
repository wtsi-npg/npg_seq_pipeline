package npg_pipeline::pluggable::registry;

use Moose;
use Carp;
use Readonly;

our $VERSION = '0';

=head1 NAME

npg_pipeline::pluggable::registry

=head1 SYNOPSIS

  my $r = npg_pipeline::pluggable::registry->new();

=head1 DESCRIPTION

A collection of common pipeline functions

=head1 SUBROUTINES/METHODS

=cut

Readonly::Hash my %REGISTRY => (

  'pipeline_start' => {'collection' => 'pipeline_start'},
  'pipeline_end'   => {'collection' => 'pipeline_end'},

  'update_warehouse' => {'collection' => 'update_warehouse'},
  'update_ml_warehouse' => {'collection' => 'update_ml_warehouse'},
  'update_warehouse_post_qc_complete' =>
    {'collection' => 'update_warehouse_post_qc_complete'},
  'update_ml_warehouse_post_qc_complete' =>
    {'collection' => 'update_ml_warehouse_post_qc_complete'},

  'create_archive_directory' => {'runfolder_scaffold' => 'create_dir'},
  'bam2fastqcheck_and_cached_fastq' =>
    {'collection' => 'bam2fastqcheck_and_cached_fastq'},
  'create_summary_link_analysis' =>
    {'current_analysis_link' => 'submit_create_link'},
  'create_empty_fastq' =>
    {'autoqc_input_scaffold' => 'create_empty_fastq_files'},

  'illumina_basecall_stats' => {'illumina_basecall_stats' => 'generate'},
  'p4_stage1_analysis' => {'p4_stage1_analysis' => 'generate'},
  'seq_alignment' => {'seq_alignment' => 'generate'},

  'archive_logs' => {'log_files_archiver' => 'submit_to_lsf'},
  'upload_illumina_analysis_to_qc_database' =>
    {'illumina_qc_archiver' => 'submit_to_lsf'},
  'upload_fastqcheck_to_qc_database' =>
    {'fastqcheck_archiver' => 'submit_to_lsf'},
  'upload_auto_qc_to_qc_database'=>
     {'autoqc_archiver' => 'submit_to_lsf'},

  'bam_cluster_counter_check'=> {'cluster_count' => 'launch'},
  'seqchksum_comparator' => {'seqchksum_comparator' => 'launch'},
                               );

Readonly::Array my @SAVE2FILE_STATUS_FUNCTIONS =>
  qw/
      run_analysis_in_progress
      run_analysis_complete
      run_secondary_analysis_in_progress
      run_qc_review_pending
      run_archival_in_progress
      run_run_archived
      run_qc_complete
      lane_analysis_in_progress
      lane_analysis_complete
    /;

Readonly::Array my @AUTOQC_FUNCTIONS =>
  qw/
      qc_adapter
      qc_gc_bias
      qc_gc_fraction
      qc_genotype
      qc_insert_size
      qc_pulldown_metrics
      qc_qX_yield
      qc_ref_match
      qc_rna_seqc
      qc_sequence_error
      qc_tag_metrics
      qc_upstream_tags
      qc_verify_bam_id 
    /;

has '_registry' => (
  isa           => q{HashRef},
  is            => q{ro},
  lazy_build    => 1,
);
sub _build__registry {
  my $self = shift;

  my $r = {};

  while (my ($function_name, $definition) = each %REGISTRY) {
    my $new_definition = {};
    $new_definition->{'module'} = (keys   %{$definition})[0];
    $new_definition->{'method'} = (values %{$definition})[0];
    $r->{$function_name} = $new_definition;
  }

  foreach my $function_name (@AUTOQC_FUNCTIONS) {
    my $qc = $function_name;
    $qc =~ s/qc_//sm;
    my $definition = {};
    $definition->{'module'} = 'autoqc';
    $definition->{'method'} = 'run_qc';
    $definition->{'params'} = {'qc_to_run'  => $qc};
    $r->{$function_name} = $definition;
  }

  foreach my $function_name (@SAVE2FILE_STATUS_FUNCTIONS) {
    my $status = $function_name;
    my $lane_status = 0;
    $status =~ s/\Arun_//xms;
    if ($status eq $function_name) {
      $status =~ s/\Alane_//xms;
      $lane_status = 1;
    }
    $status =~ s/_/ /xmsg;
    my $definition = {};
    $definition->{'module'} = 'status';
    $definition->{'method'} = 'submit';
    $definition->{'params'} = {'status'           => $status,
                               'lane_status_flag' => $lane_status};
    $r->{$function_name} = $definition;
  }

  foreach my $function_name (qw(archive_to_irods_samplesheet 
                                archive_to_irods_ml_warehouse)) {
    my $definition = {};
    $definition->{'module'} = 'seq_to_irods_archiver';
    $definition->{'method'} = 'submit_to_lsf';
    my $driver_type = $function_name =~ /samplesheet\Z/xms ?
	              'samplesheet' : 'ml_warehouse_fc_cache';
    $definition->{'params'} = {'lims_driver_type'  => $driver_type};
    $r->{$function_name} = $definition;
  }

  return $r;
}

=head2 get_function_implementor

=cut

sub get_function_implementor {
  my ($self, $function_name) = @_;

  if (!$function_name) {
    croak 'Non-empty function name string is required';
  }
  my $implementor = $self->_registry->{$function_name}
    or croak "Handler for '$function_name' is not registered";

  return $implementor;
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

=item Readonly

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

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
