package npg_pipeline::function::collection;

use Moose;
use namespace::autoclean;

use npg_pipeline::function::definition;

extends q{npg_pipeline::base};

our $VERSION = '0';

=head1 NAME

npg_pipeline::function::collection

=head1 SYNOPSIS

  my $c = npg_pipeline::function::collection->new(
    id_run => 1234,
    run_folder => q{123456_IL2_1234},
  );

=head1 DESCRIPTION

Definition for a step producing fastqcheck files and cached fastq files.

=head1 SUBROUTINES/METHODS

=head2 bam2fastqcheck_and_cached_fastq

Creates and returns command definition for generating and
caching short fastq files that serve as input to autoqc checks.
Th einput to the command is the lane bam file.

=cut

sub bam2fastqcheck_and_cached_fastq {
  my $self = shift;

  my $id_run = $self->id_run();
  my $job_name = join q{_}, q{bam2fastqcheck_and_cached_fastq},
                            $id_run, $self->timestamp();
  my $log_dir = $self->make_log_dir($self->recalibrated_path);

  my $command = sub {
    my ($c, $i, $p) = @_;
    return sprintf '%s/%i_%i.bam', $c, $i, $p;
  };

  my $c = q{generate_cached_fastq}
        . q{ --path } . $self->archive_path()
        . q{ --file } . $self->recalibrated_path();

  my @definitions = ();
  foreach my $p ($self->positions()) {
    push @definitions, npg_pipeline::function::definition->new(
      created_by   => __PACKAGE__,
      created_on   => $self->timestamp(),
      identifier   => $id_run,
      job_name     => $job_name,
      command      => $command->($c, $id_run, $p),
      fs_slots_num => 1,
      log_file_dir => $self->runfolder_path(),
      composition  =>
        $self->create_composition({id_run => $id_run, position => $p})
    );
  }

  return \@definitions;
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
