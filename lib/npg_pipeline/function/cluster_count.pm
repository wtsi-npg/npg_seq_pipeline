package npg_pipeline::function::cluster_count;

use Moose;
use namespace::autoclean;
use English qw{-no_match_vars};
use File::Spec;
use List::MoreUtils qw{any};
use Readonly;

use npg_qc::autoqc::qc_store;
use npg_pipeline::function::definition;

extends qw{npg_pipeline::base};

our $VERSION = '0';

Readonly::Scalar my $CLUSTER_COUNT_SCRIPT => q{npg_pipeline_check_cluster_count};
Readonly::Scalar my $VERSION_TWO          => 2;
Readonly::Scalar my $VERSION_THREE        => 3;
Readonly::Scalar my $FOUR                 => 4;
Readonly::Scalar my $DATA_OFFSET          => 7;

#keys used in hash and corresponding codes in tile metrics interop file
Readonly::Scalar my $TILE_METRICS_INTEROP_CODES => {'cluster density'    => 100,
                                                    'cluster density pf' => 101,
                                                    'cluster count'      => 102,
                                                    'cluster count pf'   => 103,
                                                    'version3_cluster_counts' => ord('t'),
                                                   };
=head1 NAME

npg_pipeline::function::cluster_count

=head1 SYNOPSIS

  my $oClusterCounts = npg_pipeline::function::cluster_count->new(
    run_folder          => $run_folder,
    timestamp           => q{20090709-123456},
    id_run              => 1234,
  );
  my $definitions= $oClusterCounts->create();

  my $oClusterCounts = npg_pipeline::function::cluster_count->new(
    run_folder => $run_folder,
  );
  $oClusterCounts->run_cluster_count_check();

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 create

Creates and returns function definitions as an array.
Each function definition is created as a npg_pipeline::function::definition
type object. One object is created for a set of lanes (defaults to all lanes).

  my $definitions = $obj->create();

=cut

sub create {
  my $self = shift;

  my $id_run = $self->id_run;

  my $job_name = join q[_], $CLUSTER_COUNT_SCRIPT,
                            $self->id_run(), $self->timestamp;

  my $command = $CLUSTER_COUNT_SCRIPT;
  $command .= q{ --id_run=}            . $id_run;
  $command .= q{ --bam_basecall_path=} . $self->bam_basecall_path();
  $command .= q{ --runfolder_path=}    . $self->runfolder_path();

  for my $dp (@{$self->products->{data_products}}) {
    my $bfs_path = $dp->qc_out_path($self->archive_path);
    $command .= sprintf qq{ --bfs_paths=$bfs_path};
  }

  for my $lane_product (@{$self->products->{lanes}}) {

    my $sf_path = $lane_product->qc_out_path($self->archive_path);

    $command .= sprintf qq{ --sf_paths=$sf_path};
  }

  # TODO: deal with dummy position argument in composition creation
  return [
      npg_pipeline::function::definition->new(
      created_by   => __PACKAGE__,
      created_on   => $self->timestamp(),
      identifier   => $id_run,
      job_name     => $job_name,
      command      => $command,
      composition  => $self->create_composition({id_run => $self->id_run, position => 1,})
    )
  ];
}

=head2 run_cluster_count_check

Checks the cluster count, error if the count is inconsistent.

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

   my $total_bam_cluster_count = $self->_bam_cluster_count_total();

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

has 'bfs_paths' => ( isa        => 'ArrayRef',
                     is         => 'ro',
                     required   => 0,
                     default  => sub {return [];}, # is sub necessary?
                   );

has 'sf_paths' => ( isa        => 'ArrayRef',
                     is         => 'ro',
                     required   => 0,
                     default  => sub {return [];}, # is sub necessary?
                   );

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

  my $bustard_pf_cluster_count;
  my $bustard_raw_cluster_count;
  my @positions = $self->positions();
  foreach my $l (keys %{$interop}) {
    if(not any { $_ == $l } @positions) { next; }

    $bustard_pf_cluster_count += $interop->{$l}->{'cluster count pf'};
    if ( $type eq q{pf} ) {
      $return += $interop->{$l}->{'cluster count pf'};
    }

    $bustard_raw_cluster_count += $interop->{$l}->{'cluster count'};
    if ( $type eq q{raw} ) {
      $return += $interop->{$l}->{'cluster count'};
    }
  }

  if ( !defined $return ) {
    $self->logcroak(q{Unable to determine a raw and/or pf cluster count});
  }

  $self->_set_bustard_pf_cluster_count($bustard_pf_cluster_count);
  $self->_set_bustard_raw_cluster_count($bustard_raw_cluster_count);

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

###  my $template = 'v3f'; # three two-byte integers and one 4-byte float

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

  if( $version == $VERSION_THREE) {
    $fh->read($data, $FOUR) or
      $self->logcroak(qq{Couldn't read area in interop file $interop, error $ERRNO});
    my $area = unpack 'f', $data;
    while ($fh->read($data, $length)) {
      my $template = 'vVc'; # one 2-byte integer, one 4-byte integer and one 1-byte char
      my ($lane,$tile,$code) = unpack $template, $data;
      if( $code == $TILE_METRICS_INTEROP_CODES->{'version3_cluster_counts'} ){
        $data = substr $data, $DATA_OFFSET;
        $template = 'f2'; # two 4-byte floats
        my ($cluster_count, $cluster_count_pf) = unpack $template, $data;
        push @{$tile_metrics->{$lane}->{'cluster count'}}, $cluster_count;
        push @{$tile_metrics->{$lane}->{'cluster count pf'}}, $cluster_count_pf;
      }
    }
  } elsif( $version == $VERSION_TWO) {
     my $template = 'v3f'; # three 2-byte integers and one 4-byte float
     while ($fh->read($data, $length)) {
       my ($lane,$tile,$code,$value) = unpack $template, $data;
       if( $code == $TILE_METRICS_INTEROP_CODES->{'cluster count'} ){
         push @{$tile_metrics->{$lane}->{'cluster count'}}, $value;
       }elsif( $code == $TILE_METRICS_INTEROP_CODES->{'cluster count pf'} ){
         push @{$tile_metrics->{$lane}->{'cluster count pf'}}, $value;
       }
     }
   } else {
     $self->logcroak(qq{Unknown version $version in interop file $interop});
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

sub _populate_spatial_filter_counts {
   my ( $self ) = @_;

  my $spatial_filter_processed_count;
  my $spatial_filter_failed_count;
  for my $sf_path (@{$self->sf_paths}) {

    my $qc_store = npg_qc::autoqc::qc_store->new( use_db => 0 );
    my $collection = $qc_store->load_from_path($sf_path);
    if( $collection->is_empty() ){
      $self->warn(q[There are no spatial_filter qc results available in here: ], $sf_path);
      next;
    }
    my $spatial_filter_collection = $collection->slice('class_name', 'spatial_filter');

    if($spatial_filter_collection->is_empty()) {
      $self->warn(q[There is no spatial_filter result available in here: ], $sf_path);
    }

    my $results = $spatial_filter_collection->results();
    if(@{$results} > 1) {
      $self->logcroak(q[More than one spatial_filter result available in here: ], $sf_path);
    }
    elsif(@{$results}) {
      my $qc_result = $results->[0];

      $spatial_filter_processed_count += $qc_result->num_total_reads();
      $spatial_filter_failed_count += $qc_result->num_spatial_filter_fail_reads();
    }
  }

  $self->_set__spatial_filter_processed_count($spatial_filter_processed_count);
  $self->_set__spatial_filter_failed_count($spatial_filter_failed_count);

  return;
}

sub _bam_cluster_count_total {
  my ($self) = @_;

  my $bam_cluster_count = 0;
  for my $bfs_path (@{$self->bfs_paths}) {

  my $qc_store = npg_qc::autoqc::qc_store->new( use_db => 0 );

    my $collection = $qc_store->load_from_path( $bfs_path );

    if( !$collection || $collection->is_empty() ){
      $self->info("There is no auto qc results available here: $bfs_path");
      next;
    }

    my $bam_flagstats_collection = $collection->slice('class_name', 'bam_flagstats');

    if( !$bam_flagstats_collection || $bam_flagstats_collection->is_empty() ){
      $self->info("There is no bam flagstats available in here: $bfs_path");
      next;
    }

    my $bam_flagstats_objs = $bam_flagstats_collection->results();

    foreach my $bam_flagstats (@{$bam_flagstats_objs}) {
      $bam_cluster_count += $bam_flagstats->total_reads();
    }
  }

  return $bam_cluster_count;
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

=item English

=item Readonly

=item File::Spec

=item npg_qc::autoqc::qc_store

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Guoying Qi
Steven Leonard
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
