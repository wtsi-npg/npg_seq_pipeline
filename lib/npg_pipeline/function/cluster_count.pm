package npg_pipeline::function::cluster_count;

use Moose;
use namespace::autoclean;
use File::Slurp;
use List::Util qw{sum};
use Readonly;

use npg_qc::autoqc::qc_store;
use npg_qc::illumina::interop::parser;
use npg_pipeline::function::definition;

extends qw{npg_pipeline::base};

our $VERSION = '0';

Readonly::Scalar my $CLUSTER_COUNT_SCRIPT => q{npg_pipeline_check_cluster_count};

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
  $command .= q[ ];
  $command .= join q[ ], (map { qq{--lanes=$_} } ($self->positions));
  $command .= q{ --bam_basecall_path=} . $self->bam_basecall_path();
  $command .= q{ --runfolder_path=}    . $self->runfolder_path();

  if($self->bfs_fofp_name) {
    push my @bfs_fps, (map { $_->qc_out_path($self->archive_path) } @{$self->products->{data_products}});
    write_file($self->bfs_fofp_name, (map { "$_\n" } @bfs_fps));
    $command .= q{ --bfs_fofp_name=} . $self->bfs_fofp_name;
  }
  else {
    for my $dp (@{$self->products->{data_products}}) {
      my $bfs_path = $dp->qc_out_path($self->archive_path);
      $command .= sprintf qq{ --bfs_paths=$bfs_path};
    }
  }

  if($self->sf_fofp_name) {
    push my @sf_fps, (map { $_->qc_out_path($self->archive_path) } @{$self->products->{lanes}});
    write_file($self->sf_fofp_name, (map { "$_\n" } @sf_fps));
    $command .= q{ --sf_fofp_name=} . $self->sf_fofp_name;
  }
  else {
    for my $lane_product (@{$self->products->{lanes}}) {
      my $sf_path = $lane_product->qc_out_path($self->archive_path);
      $command .= sprintf qq{ --sf_paths=$sf_path};
    }
  }

  return [
      npg_pipeline::function::definition->new(
      created_by   => __PACKAGE__,
      created_on   => $self->timestamp(),
      identifier   => $id_run,
      job_name     => $job_name,
      command      => $command,
    )
  ];
}

=head2 run_cluster_count_check

Checks the cluster count, error if the count is inconsistent.

=cut

sub run_cluster_count_check {
  my $self = shift;

  my $interop_data = npg_qc::illumina::interop::parser->new(
                       runfolder_path => $self->runfolder_path)->parse();
  my @keys =  @{$self->lanes} ? @{$self->lanes} : keys %{$interop_data->{cluster_count_total}};

  my $max_cluster_count = sum map { $interop_data->{cluster_count_total}->{$_} } @keys;
  $self->info(qq{Raw cluster count: $max_cluster_count});

  my $pass_cluster_count = sum map { $interop_data->{cluster_count_pf_total}->{$_} } @keys;
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
  } else {
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

has 'bfs_fofp_name' => ( isa        => 'Str',
                         is         => 'ro',
                         required   => 0,
                         lazy_build => 1,
                       );
sub _build_bfs_fofp_name {
  my ( $self ) = @_;
  return sprintf q{%s/%s_bfs_fofn.txt}, $self->recalibrated_path, $self->id_run;
}

has 'sf_fofp_name' => ( isa        => 'Str',
                        is         => 'ro',
                        required   => 0,
                        lazy_build => 1,
                      );
sub _build_sf_fofp_name {
  my ( $self ) = @_;
  return sprintf q{%s/%s_sf_fofn.txt}, $self->recalibrated_path, $self->id_run;
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

  my @sf_paths = ();
  if($self->sf_fofp_name) {
    @sf_paths = read_file($self->sf_fofp_name, chomp => 1, ); # more careful existence/contents check?
  }
  else {
    @sf_paths = @{$self->sf_paths};
  }

  my $spatial_filter_processed_count;
  my $spatial_filter_failed_count;
  for my $sf_path (@sf_paths) {

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

  my @bfs_paths = ();
  if($self->bfs_fofp_name) {
    @bfs_paths = read_file($self->bfs_fofp_name, chomp => 1, ); # more careful existence/contents check?
  }
  else {
    @bfs_paths = @{$self->bfs_paths};
  }

  my $bam_cluster_count = 0;
  for my $bfs_path (@bfs_paths) {

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

=item Readonly

=item File::Slurp

=item List::Util

=item npg_qc::autoqc::qc_store

=item npg_qc::illumina::interop::parser

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Guoying Qi
Steven Leonard
Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2019 Genome Research Ltd

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
