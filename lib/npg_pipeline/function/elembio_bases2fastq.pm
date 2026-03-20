package npg_pipeline::function::elembio_bases2fastq;

use Moose;
use namespace::autoclean;
use File::Spec::Functions qw/catdir catfile/;
use File::Slurp qw/write_file/;
use Readonly;

use npg_pipeline::product;

extends q{npg_pipeline::base_resource};

our $VERSION = '0';

Readonly::Scalar my $BASES2FASTQ_SCRIPT => q{bases2fastq};

=head1 NAME

npg_pipeline::function::elembio_bases2fastq

=head1 SYNOPSIS

=head1 DESCRIPTION

Creates lane-level definitions for Elembio C<bases2fastq>. For each selected
lane, a minimal lane-level C<[Samples]> CSV is written beneath the analysis
C<fastq/laneN> directory and used together with a lane selector for a
non-deplexing C<bases2fastq> command. This produces lane-local FASTQ paths
such as C<< <bam_basecalls>/fastq/lane1/Samples/WholeLane_L1_I1.fastq.gz >>
for the first index-read FASTQ for lane 1.

=head1 SUBROUTINES/METHODS

=head2 lane_samples_path

Path to the minimal lane-level C<[Samples]> CSV used for non-deplexing
Elembio C<bases2fastq>.

=cut

has q{fastq_base_dir} => (
  isa        => q{Str},
  is         => q{ro},
  lazy_build => 1,
);
sub _build_fastq_base_dir {
  my $self = shift;
  return catdir($self->bam_basecall_path, q{fastq});
}

=head2 run_parameters_path

Path to the Elembio `RunParameters.json` file.

=cut

has q{run_parameters_path} => (
  isa        => q{Str},
  is         => q{ro},
  lazy_build => 1,
);
sub _build_run_parameters_path {
  my $self = shift;
  my $path = catfile($self->runfolder_path, q{RunParameters.json});
  -f $path or $self->logcroak(qq[Run parameters file $path does not exist]);
  return $path;
}

=head2 generate

Creates one lane-level definition per selected Elembio lane.

=cut

sub generate {
  my $self = shift;

  if ($self->manufacturer ne q{Element Biosciences}) {
    $self->logcroak(
      q[Elembio stage 1 is only supported for Element Biosciences runfolders]
    );
  }

  my @definitions;
  foreach my $lane ($self->_lane_numbers) {
    my @errors = $self->make_dir($self->_lane_dir($lane),
                                 $self->_lane_log_dir($lane));
    if (@errors) {
      $self->logcroak(join qq[\n], @errors);
    }

    my $samples_path = $self->_write_lane_samples_file($lane);
    push @definitions, $self->create_definition({
      job_name    => join(q{_}, q{elembio_bases2fastq}, $self->id_run,
                          $self->timestamp),
      command     => $self->_command($lane, $samples_path),
      composition => npg_pipeline::product->new(
        rpt_list => join q{:}, $self->id_run, $lane
      )->composition,
    });
  }

  return \@definitions;
}

sub _lane_numbers {
  my $self = shift;

  if (@{$self->lanes}) {
    my @lanes = sort {$a <=> $b} @{$self->lanes};
    return @lanes;
  }

  return 1 .. $self->lane_count;
}

sub _lane_dir {
  my ($self, $lane) = @_;
  return catdir($self->fastq_base_dir, q{lane} . $lane);
}

sub _lane_log_dir {
  my ($self, $lane) = @_;
  return catdir($self->_lane_dir($lane), q{log});
}

sub _lane_samples_path {
  my ($self, $lane) = @_;
  return catfile($self->_lane_dir($lane), q{samples.csv});
}

sub _lane_selector {
  my ($self, $lane) = @_;
  return q{L} . $lane . q{R..C..S.};
}

sub _write_lane_samples_file {
  my ($self, $lane) = @_;
  my $path = $self->_lane_samples_path($lane);
  write_file($path, "[Samples]\nSampleName\nWholeLane\n");
  return $path;
}

sub _command {
  my ($self, $lane, $samples_path) = @_;
  my $selector = $self->_lane_selector($lane);

  # bases2fastq selection is subtractive: exclude everything first, then
  # add back the single lane we want to process.
  my @parts = (
    $BASES2FASTQ_SCRIPT,
    q{-e},                   q{'.*'},
    q{-i},                   qq{'$selector'},
  );

  if ($self->is_indexed) {
    push @parts, q{--settings}, q{"I1Fastq,True"};
  }
  if ($self->is_dual_index) {
    push @parts, q{--settings}, q{"I2Fastq,True"};
  }

  push @parts,
    q{--no-projects},
    q{-p `npg_pipeline_job_env_to_threads --num_threads } . $self->get_massaged_resources()->{num_cpus}[0] . q{`},
    $self->runfolder_path,   $self->_lane_dir($lane),
    q{--skip-qc-report},
    q{--force-index-orientation},
    q{--group-fastq},
    q{--split-lanes},
    q{--skip-multi-qc},
    q{--settings},           q{"SpikeInAsUnassigned,False"},
    q{-r},                   $samples_path;

  if ($self->is_paired_read) {
    push @parts, q{--settings}, q{"ReadType,Paired"};
  }

  return join q{ }, @parts;
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

=item File::Spec::Functions

=item File::Slurp

=item JSON

=item Readonly

=item npg_pipeline::product

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Genome Research Ltd.

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2026 Genome Research Ltd.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.
