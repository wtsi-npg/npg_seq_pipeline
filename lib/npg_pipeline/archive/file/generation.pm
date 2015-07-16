package npg_pipeline::archive::file::generation;

use Moose;
use File::Spec qw/catfile/;

extends q{npg_pipeline::base};

our $VERSION = '0';

=head1 NAME

npg_pipeline::archive::file::generation

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 create_empty_fastq_files

=cut

sub create_empty_fastq_files {
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

  return ();
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

=item File::Spec

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2015 Genome Research Ltd

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
