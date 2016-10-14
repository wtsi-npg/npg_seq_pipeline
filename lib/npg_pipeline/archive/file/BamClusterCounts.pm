package npg_pipeline::archive::file::BamClusterCounts;

use Moose;
use Carp;
use English qw{-no_match_vars};
use File::Spec;
use Readonly;
use XML::LibXML;
use List::MoreUtils qw(uniq);

use npg_qc::autoqc::qc_store;
use npg_pipeline::lsf_job;
extends qw{npg_pipeline::base};

our $VERSION = '0';

Readonly::Scalar our $CLUSTER_COUNTS_SCRIPT => q{npg_pipeline_check_bam_file_cluster_count};

#keys used in hash and corresponding codes in tile metrics interop file
Readonly::Scalar our $TILE_METRICS_INTEROP_CODES => {'cluster density'    => 100,
                                                     'cluster density pf' => 101,
                                                     'cluster count'      => 102,
                                                     'cluster count pf'   => 103,
                                                     };
=head1 NAME

npg_pipeline::archive::file::BamClusterCounts

=head1 SYNOPSIS


  my @job_ids;
  eval {
    my $oClusterCounts = npg_pipeline::archive::file::BamClusterCounts->new(
      run_folder          => $run_folder,
      timestamp           => q{20090709-123456},
      id_run              => 1234,
      verbose              => 1, # use if you want logging of the commands sent to LSF
    );

    my $arg_refs = {
      required_job_completion  => q{-w'done(123) && done(321)'},
    }

    @job_ids = $oClusterCounts->launch($arg_refs);
  } or do {
    # your error handling here
  };

  my $oClusterCounts = npg_pipeline::archive::file::ClusterCounts->new(
    run_folder => $run_folder,
    verbose    => 1, # use if you want logging of the commands sent to LSF
  );
  $oClusterCounts->run_cluster_count_check();

=head1 DESCRIPTION

This module is responsible for launching a job, and then processing that job, which will check the cluster
counts are what they are expected to be in bam files comparing with summary xml file

=head1 SUBROUTINES/METHODS

=head2 launch

Method launches lsf job commands and returns an array of job ids retrieved

  my @JobIds = $oClusterCounts->launch({
    required_job_completion  => q{-w'done(123) && done(321)'},
  });

=cut

sub launch {
  my ( $self, $arg_refs ) = @_;

  my @job_ids;

  my @positions = $self->positions();
  if ( ! scalar @positions ) {
    $self->info(q{no positions found, not submitting any jobs});
    return @job_ids;
  }

  $arg_refs->{array_string} = npg_pipeline::lsf_job->create_array_string( @positions );

  push @job_ids, $self->submit_bsub_command( $self->_generate_bsub_command( $arg_refs ) );

  return @job_ids;
}

=head2 run_cluster_count_check

method which goes checking the cluster counts

=cut

sub run_cluster_count_check {
   my $self = shift;

   $self->info('Checking cluster counts are consistent');
   my $max_cluster_count = $self->_bustard_raw_cluster_count();
   $self->info(qq{Raw cluster count: $max_cluster_count});
   my $pass_cluster_count = $self->_bustard_pf_cluster_count();
   $self->info(qq{PF cluster count: $pass_cluster_count});

   my $spatial_filter_processed = $self->_spatial_filter_processed_count();
   my $spatial_filter_failed    = $self->_spatial_filter_failed_count();
   if (defined $spatial_filter_processed) {
     if($self->is_paired_read()){
       $spatial_filter_processed /= 2;
       $spatial_filter_failed /= 2;
     }
     $self->info(qq{Spatial filter applied to $spatial_filter_processed clusters failing $spatial_filter_failed});
     if ($pass_cluster_count != $spatial_filter_processed and
         $max_cluster_count != $spatial_filter_processed) {
       my $msg = qq{Spatial filter processed count ($spatial_filter_processed) matches neither raw ($max_cluster_count) or PF ($pass_cluster_count) clusters};
       $self->logcroak($msg);
     }
     $max_cluster_count = $spatial_filter_processed; # reset to max processed at spatial filter
     $pass_cluster_count -= $spatial_filter_failed;
     if($spatial_filter_failed){
       $self->warn(qq{Passed cluster count drops to $pass_cluster_count});
     }
   }else{
       $self->info(q{Spatial filter not applied (well, not recorded anyway)});
   }

   my $total_bam_cluster_count;
   if ( $self->is_multiplexed_lane($self->position() ) ) {
      $total_bam_cluster_count += $self->_bam_cluster_count_total({plex=>1});
   }else{
      $total_bam_cluster_count += $self->_bam_cluster_count_total({});
   }
   if($self->is_paired_read()){
       $total_bam_cluster_count /= 2;
    }

   $self->info(q{Actual cluster count in bam files: },
               $total_bam_cluster_count);

    if($pass_cluster_count != $total_bam_cluster_count and $max_cluster_count != $total_bam_cluster_count){
        my $msg = qq{Cluster count in bam files not as expected\n\tExpected: $pass_cluster_count or $max_cluster_count\n\tActual:$total_bam_cluster_count };
        $self->logcroak($msg);
    }
    $self->info('Bam files have correct cluster count');

    return 1;
}

has q{position} => (
  isa => q{Int},
  is  => q{ro},
);

#############
# private methods

has q{_bustard_pf_cluster_count} => (
  isa => q{Int},
  is  => q{ro},
  lazy_build => 1,
  writer => q{_set_bustard_pf_cluster_count},
);

sub _build__bustard_pf_cluster_count {
  my ( $self ) = @_;
  return $self->_populate_cluster_counts( q{pf} );
}


has q{_bustard_raw_cluster_count} => (
  isa => q{Int},
  is  => q{ro},
  lazy_build => 1,
  writer => q{_set_bustard_raw_cluster_count},
);

sub _build__bustard_raw_cluster_count {
  my ( $self ) = @_;
  return $self->_populate_cluster_counts( q{raw} );
}

sub _populate_cluster_counts {
  my ( $self, $type ) = @_;

  my $interop = $self->parsing_interop($self->runfolder_path().q{/InterOp/TileMetricsOut.bin});

  my $return;

  foreach my $l (keys %{$interop}) {
    if ( $l != $self->position() ) {
      next;
    }
    $self->_set_bustard_pf_cluster_count( $interop->{$l}->{'cluster count pf'} );
    if ( $type eq q{pf} ) {
      $return = $interop->{$l}->{'cluster count pf'};
    }

    $self->_set_bustard_raw_cluster_count( $interop->{$l}->{'cluster count'} );
    if ( $type eq q{raw} ) {
      $return = $interop->{$l}->{'cluster count'};
    }
  }

  if ( !defined $return ) {
    $self->logcroak(q{Unable to determine a raw and/or pf cluster count});
  }

  return $return;

}

=head2 parsing_interop

given one tile metrics interop file, return a hashref

=cut

sub parsing_interop {
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

  while ($fh->read($data, $length)) {
    my ($lane,$tile,$code,$value) = unpack $template, $data;
    if( $code == $TILE_METRICS_INTEROP_CODES->{'cluster count'} ){
      push @{$tile_metrics->{$lane}->{'cluster count'}}, $value;
    }elsif( $code == $TILE_METRICS_INTEROP_CODES->{'cluster count pf'} ){
      push @{$tile_metrics->{$lane}->{'cluster count pf'}}, $value;
    }
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

has q{_spatial_filter_failed_count} =>(
  isa => q{Maybe[Int]},
  is  => q{ro},
  predicate => q{_has__spatial_filter_failed_count},
  lazy_build => 1,
  writer => q{_set__spatial_filter_failed_count},
);

sub _build__spatial_filter_failed_count {
  my ( $self ) = @_;
  $self->_populate_spatial_filter_counts();
  if(not $self->_has__spatial_filter_failed_count) {
      $self->logcroak('_spatial_filter_failed_count should have been set');
  }
  return $self->_spatial_filter_failed_count();
}

has q{_spatial_filter_processed_count} =>(
  isa => q{Maybe[Int]},
  is  => q{ro},
  predicate => q{_has__spatial_filter_processed_count},
  lazy_build => 1,
  writer => q{_set__spatial_filter_processed_count},
);

sub _build__spatial_filter_processed_count {
  my ( $self ) = @_;
  $self->_populate_spatial_filter_counts();
  if(not $self->_has__spatial_filter_processed_count) {
      $self->logcroak('_spatial_filter_processed_count should have been set');
  }
  return $self->_spatial_filter_processed_count();
}

sub _populate_spatial_filter_counts{
   my ( $self ) = @_;

   my $position = $self->position();
   my $qc_store = npg_qc::autoqc::qc_store->new( use_db => 0 );
   my $collection = $qc_store->load_from_path( $self->qc_path() );
   if( $collection->is_empty() ){
     $self->warn("There are no qc results available for this lane $position in here: ",
                 $self->qc_path);
   }
   my $collection_lane = $collection->slice(q[position], $position);
   my $spatial_filter_collection = $collection_lane->slice('class_name', 'spatial_filter');

   if( $spatial_filter_collection->is_empty() ){
     $self->warn("There is no spatial_filter result available for this lane $position in here: ",
                 $self->qc_path);
   }

   my $results = $spatial_filter_collection->results();
   if(@{$results} > 1){
     $self->logcroak("More than one spatial_filter result available for this lane $position in here: ",
                     $self->qc_path);
   }elsif(@{$results}){
     my $qc_result = $results->[0];
     $self->_set__spatial_filter_processed_count($qc_result->num_total_reads());
     $self->_set__spatial_filter_failed_count($qc_result->num_spatial_filter_fail_reads());
     return $qc_result;
   }
   #set undef for values if no qc results:
   $self->_set__spatial_filter_processed_count();
   $self->_set__spatial_filter_failed_count();
   return;
}


# generates the bsub command
sub _generate_bsub_command {
  my ($self, $arg_refs) = @_;

  my $array_string = $arg_refs->{array_string};
  my $required_job_completion = $arg_refs->{required_job_completion} || q{};
  my $timestamp = $self->timestamp();
  my $id_run = $self->id_run();

  my $job_name = $CLUSTER_COUNTS_SCRIPT . q{_} . $id_run . q{_} . $timestamp;

  my $archive_out = $self->archive_path() . q{/log};
  my $out_subscript = q{.%I.%J.out};
  my $outfile = File::Spec->catfile($archive_out, $job_name . $out_subscript);

  $job_name = q{'} . $job_name . $array_string . q{'};

  my $job_sub = q{bsub -q } . $self->lsf_queue() . qq{ $required_job_completion -J $job_name -o $outfile '};
  $job_sub .= $CLUSTER_COUNTS_SCRIPT;
  $job_sub .= q{ --id_run=} . $id_run;
  $job_sub .= q{ --position=}  . $self->lsb_jobindex();
  $job_sub .= q{ --runfolder_path=} . $self->runfolder_path();
  $job_sub .= q{ --qc_path=} . $self->qc_path();
  $job_sub .= q{ --bam_basecall_path=} . $self->bam_basecall_path();

  if ( $self->verbose() ) {
    $job_sub .= q{ --verbose};
  }

  $job_sub .= q{'};

  $self->debug($job_sub);

  return $job_sub;
}

sub _bam_cluster_count_total {
   my ( $self, $args_ref ) = @_;

   my $plex = $args_ref->{plex};

   my $bam_cluster_count = 0;

   my $qc_store = npg_qc::autoqc::qc_store->new( use_db => 0 );

   my $qc_path = $self->qc_path();
   my $position = $self->position();

   if( $plex ){
      $qc_path =~ s{(?<!lane.)/qc$}{/lane$position/qc}smx;
   }

   my $collection = $qc_store->load_from_path( $qc_path );

   if( !$collection || $collection->is_empty() ){
     $self->info("There is no auto qc results available here: $qc_path");
     return $bam_cluster_count;
   }

   my $collection_lane = $collection->slice(q[position], $position);
   my $bam_flagstats_collection = $collection_lane->slice('class_name', 'bam_flagstats');

   if( !$bam_flagstats_collection || $bam_flagstats_collection->is_empty() ){
     $self->info("There is no bam flagstats available for this lane $position in here: $qc_path");
     return $bam_cluster_count;
   }

   my $bam_flagstats_objs = $bam_flagstats_collection->results();

   foreach my $bam_flagstats (@{$bam_flagstats_objs}){

      if( $bam_flagstats->id_run() != $self->id_run() ){
         next;
      }

      $bam_cluster_count += $bam_flagstats->total_reads();
   }

   return $bam_cluster_count;
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

=item File::Spec

=item List::MoreUtils

=item XML::LibXML

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Guoying Qi

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2014 Genome Research Ltd

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
