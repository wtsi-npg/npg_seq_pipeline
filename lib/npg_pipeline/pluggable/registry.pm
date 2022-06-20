package npg_pipeline::pluggable::registry;

use Moose;
use namespace::autoclean;
use Carp;
use Readonly;

our $VERSION = '0';

=head1 NAME

npg_pipeline::pluggable::registry

=head1 SYNOPSIS

  my $r = npg_pipeline::pluggable::registry->new();
  $r->get_function_implementor('function_name');

=head1 DESCRIPTION

Mapping from function names to classes implementing them and to
arguments and values which have to be passed to the objects'
constructors at run time.

=head1 SUBROUTINES/METHODS

=cut

Readonly::Hash my %REGISTRY => (

  'pipeline_start' => {'start_stop' => 'pipeline_start'},
  'pipeline_end'   => {'start_stop' => 'pipeline_end'},
  'pipeline_wait4path' => {'start_stop' => 'pipeline_wait4path'},

  'update_ml_warehouse' => {'warehouse_archiver' => 'update_ml_warehouse'},
  'update_ml_warehouse_post_qc_complete' =>
    {'warehouse_archiver' => 'update_ml_warehouse_post_qc_complete'},

  'create_summary_link_analysis' => {'current_analysis_link' => 'create'},

  'p4_stage1_analysis'      => {'p4_stage1_analysis' => 'generate'},
  'stage2pp'                => {'stage2pp' =>
    {method => 'create', pipeline_type => 'stage2pp'}},
  'stage2App'                => {'stage2pp' =>
    {method => 'create', pipeline_type => 'stage2App'}},
  'seq_alignment'           => {'seq_alignment' => 'generate'},
  'bqsr_calc'               => {'bqsr_calc' => 'create'},
  'haplotype_caller'        => {'haplotype_caller' => 'create'},
  'merge_recompress'        => {'merge_recompress' => 'create'},

  'archive_logs'                   => {'log_files_archiver' => 'create'},
  'upload_auto_qc_to_qc_database'  => {'autoqc_archiver' => 'create'},
  'archive_run_data_to_irods'      => {'run_data_to_irods_archiver' => 'create'},
  'remove_intermediate_data'       => {'remove_intermediate_data' => 'create'},
  'pp_archiver'                    => {'pp_archiver' => 'create'},
  'pp_archiver_manifest'           => {'pp_archiver' => 'generate_manifest'},
  'archive_pp_data_to_irods'       => {'pp_data_to_irods_archiver' => 'create'},
  'archive_irods_locations_to_ml_warehouse' => { 'irods_locations_warehouse_archiver' => 'create'},

  'bam_cluster_counter_check'=> {'cluster_count' => 'create'},
  'seqchksum_comparator'     => {'seqchksum_comparator' => 'create'},
  'archive_to_s3'            => {'s3_archiver' => 'create'},
  'cache_merge_component'    => {'cache_merge_component' => 'create'},

  'archive_to_irods_samplesheet' => {'seq_to_irods_archiver' =>
     {method => 'create', lims_driver_type =>'samplesheet'}},
  'archive_to_irods_ml_warehouse' => {'seq_to_irods_archiver' =>
     {method => 'create', lims_driver_type =>'ml_warehouse_fc_cache'}},

  'qc_generic_artic' => {'autoqc::generic' => {method => 'create',
    spec => 'artic', portable_pipeline_name => 'ncov2019-artic-nf'}},
  'qc_generic_ampliconstats' => {'autoqc::generic' => {
    method => 'create', spec => 'ampliconstats',
    portable_pipeline_name => 'ncov2019-artic-nf_ampliconstats'}},
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
      qc_bcfstats
      qc_gc_fraction
      qc_genotype
      qc_insert_size
      qc_interop
      qc_pulldown_metrics
      qc_qX_yield
      qc_ref_match
      qc_rna_seqc
      qc_sequence_error
      qc_tag_metrics
      qc_spatial_filter
      qc_verify_bam_id
      qc_review
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
    my $details = (values %{$definition})[0];
    my $type = ref $details;
    if (not $type) {
      $new_definition->{'method'} = $details;
    } elsif ($type eq 'HASH') {
      my %details_hash = %{$details};
      $new_definition->{'method'} = delete $details_hash{'method'};
      $new_definition->{'params'} = \%details_hash;
    } else {
      croak "Unexpected type $type";
    }
    $r->{$function_name} = $new_definition;
  }

  foreach my $function_name (@AUTOQC_FUNCTIONS) {
    my $qc = $function_name;
    $qc =~ s/qc_//sm;
    my $definition = {};
    $definition->{'module'} = 'autoqc';
    $definition->{'method'} = 'create';
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
    $definition->{'method'} = 'create';
    $definition->{'params'} = {'status'           => $status,
                               'lane_status_flag' => $lane_status};
    $r->{$function_name} = $definition;
  }

  return $r;
}

=head2 get_function_implementor

  my $i = $registry->get_function_implementor('function_name');
  my $module = $i->{'module'};
  my $method = $i->{'method'};
  my $params = $i->{'params'}; # Might be undefined
  my $definitions = $module->new($params)->method();

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

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item namespace::autoclean

=item Carp

=item Readonly

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2018,2019,2020,2021,2022 Genome Research Ltd.

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
