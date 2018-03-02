package npg_pipeline::function::autoqc_input_scaffold;

use Moose;
use namespace::autoclean;
use File::Spec::Functions qw/catfile/;

use npg_pipeline::function::definition;

extends q{npg_pipeline::base};

our $VERSION = '0';

=head1 NAME

npg_pipeline::function::autoqc_input_scaffold

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 create

Creates empty fastq files while this method is running.
Creates and returns a single function definition as an array.
Function definition is created as a npg_pipeline::function::definition
type object with the immediate_mode attribute value set to true.

=cut

sub create {
  my $self = shift;

  my $forward_end = ($self->is_paired_read() || $self->is_indexed()) ? 1 : undef;
  my @files = ();
  my $apath = $self->archive_path;

  foreach my $position ( $self->positions() ) {
    push @files, catfile($apath, $self->fq_filename($position, undef, $forward_end));
    if ( $self->is_paired_read() ) {
      push @files, catfile($apath, $self->fq_filename($position, undef, 2));
    }
    if ( $self->is_indexed() && $self->is_multiplexed_lane($position) ) {
      push @files, catfile($apath, $self->fq_filename($position, undef, q[t]));
      my $lpath = $self->lane_archive_path($position);
      foreach my $tag_index ( @{ $self->get_tag_index_list( $position ) } ) {
        push @files, catfile($lpath, $self->fq_filename($position, $tag_index, 1));
        if ( $self->is_paired_read() ) {
          push @files, catfile($lpath, $self->fq_filename($position, $tag_index, 2));
        }
      }
    }
  }

  foreach my $file (@files) {
    system "touch $file";
    my $fastqcheck = $file . q[check];
    system "echo '0 sequences, 0 total length' > $fastqcheck";
  }

  my $d = npg_pipeline::function::definition->new(
    created_by     => __PACKAGE__,
    created_on     => $self->timestamp(),
    immediate_mode => 1
  );

  return [$d];
}

=head2 fq_filename

Generates fastq file names.

=cut

sub fq_filename {
  my ($self, $position, $tag_index, $end) = @_;
  return sprintf '%i_%i%s%s.fastq',
    $self->id_run,
    $position,
    $end               ? "_$end"      : q[],
    defined $tag_index ? "#$tag_index" : q[];
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

=item File::Spec

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown
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
