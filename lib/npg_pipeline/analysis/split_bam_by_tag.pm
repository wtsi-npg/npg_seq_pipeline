package npg_pipeline::analysis::split_bam_by_tag;

use Moose;
use Carp;
use English qw{-no_match_vars};
use Readonly;

use npg_common::roles::software_location;
use npg_pipeline::lsf_job;

extends q{npg_pipeline::base};

our $VERSION  = '0';

Readonly::Scalar our $DEFAULT_RESOURCES => 4;
Readonly::Scalar our $JAVA_CMD          => q{java};

sub generate {
  my ($self, $arg_refs) = @_;

  my $id_run = $self->id_run();

  $self->log(qq{Creating Jobs to split bam files by tag for $id_run});

  if ( ! $self->is_indexed() ) {
    $self->log( qq{Run $id_run is not multiplex run and no need to split} );
    return;
  }

  my %positions = map{$_=>1} $self->positions();
  my @indexed_lanes = grep {$positions{$_}} @{$self->multiplexed_lanes()};
  if ( ! @indexed_lanes ) {
    $self->log( q{None of the lanes for analysis is multiplexed} );
    return;
  }

  my $output_dir = $self->recalibrated_path() . q{/lane};

  foreach my $position ( @indexed_lanes  ) {
    my $lane_output_dir = $output_dir . $position;
    if ( ! -d $lane_output_dir ) {
       $self->log( qq{creating $lane_output_dir} );
       my $rc = `mkdir -p $lane_output_dir`;
       if ( $CHILD_ERROR ) {
         croak qq{could not create $lane_output_dir\n\t$rc};
       }
    }
  }

  my $position = $self->lsb_jobindex();
  $output_dir .= $position;

  my @job_ids;
  # submits a job (in an array) for each multiplex lane
  if ( scalar @indexed_lanes ) {

    $arg_refs->{'array_string'} = npg_pipeline::lsf_job->create_array_string( @indexed_lanes );
    $arg_refs->{'fs_resource'} = $DEFAULT_RESOURCES;
    $arg_refs->{'bam'} = $self->recalibrated_path().q{/}.$self->id_run().q{_}.$position.q{.bam};
    $arg_refs->{'output_prefix'} = $output_dir.q{/};

    my $job_sub = $self->_generate_bsub_command( $arg_refs );
    push @job_ids, $self->submit_bsub_command( $job_sub );

  }

  return @job_ids;
}

has q{_split_bam_jar} => (
                           isa        => q{NpgCommonResolvedPathJarFile},
                           is         => q{ro},
                           coerce     => 1,
                           default    => q{SplitBamByReadGroup.jar},
                         );
has q{_split_bam_cmd}  => (isa        => q{Str},
                           is         => q{ro},
                           lazy_build => 1,
                          );
sub _build__split_bam_cmd {

   my $self = shift;

   return $JAVA_CMD . q{ -Xmx1024m}
                    . q{ -jar } . $self->_split_bam_jar()
                    . q{ CREATE_MD5_FILE=true VALIDATION_STRINGENCY=SILENT};
}

sub _generate_bsub_command {
  my ($self, $arg_refs) = @_;

  my $required_job_completion = $arg_refs->{'required_job_completion'};
  my $bam                     = $arg_refs->{'bam'};
  my $output_prefix           = $arg_refs->{'output_prefix'};

  my $timestamp = $self->timestamp();

  my $job_name = q{split_bam_by_tag_} . $self->id_run() . q{_} . $timestamp;

  my $outfile = $self->make_log_dir( $self->recalibrated_path() ) . q{/} . $job_name . q{.%I.%J.out};

  my $job_command = $self->_split_bam_cmd() . qq{  I=$bam O=$output_prefix};

  $job_name .= $arg_refs->{'array_string'};

  my $job_sub = q{bsub -q } . $self->lsf_queue() . q{ };
  $job_sub .=  ( $self->fs_resource_string( {
                   counter_slots_per_job => $arg_refs->{'fs_resource'},
               } ) );
  $job_sub .= q{ };
  $job_sub .= qq{$required_job_completion -J $job_name -o $outfile '$job_command'};

  if ($self->verbose()) { $self->log($job_sub); }

  return $job_sub;
}

no Moose;

__PACKAGE__->meta->make_immutable;

1;
__END__

=head1 NAME

npg_pipeline::analysis::split_bam_by_tag

=head1 SYNOPSIS

  my $oAfgfq = npg_pipeline::archive::file::generation::fastq_by_tag->new(run_folder => $sRunFolder);

=head1 DESCRIPTION

Object module which knows how to construct and submits the command line to LSF for splitting bam file by tag.

=head1 SUBROUTINES/METHODS

=head2 generate - generates the bsub jobs and submits them for spliting the bam files by tag, returning an array of job_ids.

  my @job_ids = $oAfgfq->generate({required_job_completion} => q{-w (123 && 321)}});

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Carp

=item English -no_match_vars

=item Readonly

=item Moose

=item npg_common::roles::software_location

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Guoying Qi

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2014 Genome Research Ltd.

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
