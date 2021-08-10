package npg_pipeline::cache::barcodes;

use Moose;
use Carp;
use File::Spec::Functions;
use List::Util qw(max min sum);
use Readonly;
use open q(:encoding(UTF8));

use npg_tracking::util::types;
use npg_pipeline::function::util;
use npg_pipeline::cache::barcodes2;

with 'WTSI::DNAP::Utilities::Loggable';

our $VERSION = '0';

Readonly::Scalar my $TAG_LIST_FILE_HEADER      => qq{barcode_sequence\tbarcode_name\tlibrary_name\tsample_name\tdescription};
# For dual index runs add the expected i5 tag sequences for samples (e.g. phix) which have no i5 tag
# I've extended these to 10 bases. For I5OPPOSITE final `GT` predicted from adapter documentation.
Readonly::Scalar my $SPIKED_PHIX_TAG2        => q{TCTTTCCCTA};
Readonly::Scalar my $SPIKED_PHIX_I5OPPOSITE_TAG2 => q{AGATCTCGGT};

=head1 NAME

npg_pipeline::cache::barcodes

=head1 SYNOPSIS

  my $obj = npg_pipeline::cache::barcodes->new(
    location     => 'some/dir',
    lane_lims    => $run_lane_lims_obj,
    index_lengths => [6,8],
  )->generate;
  my $path = $obj->tag_list_path;

=head1 DESCRIPTION

Creates a tag list file for a lane

=head1 SUBROUTINES/METHODS

=head2 verbose

=cut

has q{verbose}           => (isa        => q{Bool},
                             is         => q{ro},
                             required   => 0,
                            );

=head2 i5opposite

Direction of read for the i5 index read is opposite to that of MiSeq e.g. HiSeqX

=cut

has q{i5opposite}        => (isa        => q{Bool},
                             is         => q{ro},
                             required   => 0,
                             documentation => q{direction of read for the i5 index read is opposite to that of MiSeq e.g. HiSeqX},
                            );

=head2 lane_lims

=cut

has q{lane_lims}         => (isa        => q{st::api::lims},
                             is         => q{ro},
                             required   => 1,
                            );

=head2 location

=cut

has q{location}          => (isa        => q{NpgTrackingDirectory},
                             is         => q{ro},
                             required   => 1,
                            );

=head2 index_lengths

=cut

has q{index_lengths}      => (isa        => q{ArrayRef},
                             is         => q{ro},
                             required   => 1,
                            );

=head2 tag_list_path

=cut

has q{_tag_list_path}     => (isa        => q{Str},
                              is         => q{ro},
                              lazy_build => 1,
                             );
sub _build__tag_list_path {
  my $self = shift;
  return catfile($self->location, sprintf 'lane_%i.taglist', $self->lane_lims->position);
}

=head2 generate

=cut

sub generate {
  my ($self) = @_;

  if (!$self->lane_lims->is_pool) {
    $self->warn('Lane is not a pool');
    return;
  }

  my $position = $self->lane_lims->position;
  if (!$self->lane_lims->tags) {
    $self->logcroak(qq{No tag information available for lane $position});
  }

  my $tags = $self->lane_lims->tags;
  my %lims_data = ();
  my $spiked_phix_tag_index = $self->lane_lims->spiked_phix_tag_index();

  # on a HiSeqX the second index is sequenced in reverse complement order
  foreach my $plex ($self->lane_lims->children) {
    if (my $ti = $plex->tag_index){
      my $tag_sequences = $plex->tag_sequences;
      $lims_data{$ti}->{phix_control} = 0;
      if ( $spiked_phix_tag_index ) {
        $lims_data{$ti}->{phix_control} = ($ti == $spiked_phix_tag_index ? 1 : 0);
      }
      if ( @{$tag_sequences} > 0 ) {
        $lims_data{$ti}->{i7_expected_seq} = $tag_sequences->[0];
        if ( @{$tag_sequences} > 1 ) {
          if ( $self->i5opposite ) {
            $tag_sequences->[1] =~ tr/[ACGT]/[TGCA]/;
            $tag_sequences->[1] = reverse $tag_sequences->[1];
          }
          $lims_data{$ti}->{i5_expected_seq} = $tag_sequences->[1];
        }
      }
      $tags->{$ti} = join q[-], @{$tag_sequences};
    }
  }

  # OLD local code replace by new module
  ####  my ($tag_index_list, $tag_seq_list) = $self->_process_tag_list($tags, $spiked_phix_tag_index);
  my $index_lengths = $self->index_lengths;
  my $barcodes2 = npg_pipeline::cache::barcodes2->new(
     lims_data      => \%lims_data,
     i7_read_length => $index_lengths->[0],
     i5_read_length => (scalar(@{$index_lengths}) == 1 ? 0 : $index_lengths->[1]),
     i5_opposite    => $self->i5opposite ? 1 : 0,
      )->generate();
  my $tag_index_list;
  my $tag_seq_list;
  for my $index (sort keys %{$barcodes2}) {
    push(@{$tag_index_list}, $index);
    if ($barcodes2->{$index}->{i7_read}) {
      if ($barcodes2->{$index}->{i5_read}) {
        push(@{$tag_seq_list}, join q[-], ($barcodes2->{$index}->{i7_read}, $barcodes2->{$index}->{i5_read}));
      } else {
        push(@{$tag_seq_list}, $barcodes2->{$index}->{i7_read});
      }
    }
  }

  if  ($tag_index_list && $tag_seq_list) {
    $self->_check_tag_uniqueness($tag_seq_list);
    if( scalar @{$tag_index_list} != scalar @{$tag_seq_list} ){
      $self->logcroak("The number of tag indexes is not the same as tag list:@{$tag_index_list}\n@{$tag_seq_list}");
    }
    $self->_construct_specific_file_expected_sequence_with_library($self->lane_lims, $tag_index_list, $tag_seq_list);
  } else {
    $self->logcroak(qq{Lane $position: no expected tag sequence or index.});
  }

  return $self->_tag_list_path;
}

sub _construct_specific_file_expected_sequence_with_library {
  my ($self, $lane_lims, $tag_index_list, $tag_seq_list) = @_;

  my $position = $lane_lims->position;
  my $lane_specific_tag_file = $self->_tag_list_path;
  open my $fh, q{>}, $lane_specific_tag_file or
    $self->logcroak(qq{unable to open $lane_specific_tag_file for writing});

  print {$fh} qq{$TAG_LIST_FILE_HEADER} or
    $self->logcroak(q{unable to print});

  my $alims = $lane_lims->children_ia;
  my $num_tags = scalar @{$tag_index_list};
  my $array_index = 0;

  while ($array_index < $num_tags ) {

    my $tag_index = $tag_index_list->[$array_index];
    my $names = npg_pipeline::function::util->get_study_library_sample_names($alims->{$tag_index});
    my ($study_names, $library_names, $sample_names);

    if($names->{study}){
      $study_names = join q{,}, @{$names->{study}};
    }
    if($names->{library}){
      $library_names = join q{,}, @{$names->{library}};
    }
    if($names->{sample}){
      $sample_names = join q{,}, @{$names->{sample}};
    }
    $study_names ||= q{};
    $library_names ||= q{};
    $sample_names ||= q{};

    $study_names =~ s/[\t\n\r]/\ /gmxs;
    $library_names =~ s/[\t\n\r]/\ /gmxs;
    $sample_names =~ s/[\t\n\r]/\ /gmxs;

    print {$fh}  qq{\n}.$tag_seq_list->[$array_index]
                .qq{\t}.$tag_index
                .qq{\t}.$library_names
                .qq{\t}.$sample_names
                .qq{\t}.$study_names
    or $self->logcroak(q{unable to print});

    $array_index++;
  }

  close $fh or $self->logcroak(qq{unable to close $lane_specific_tag_file from writing});
  return 1;
}


sub _check_tag_uniqueness {
  my ($self, $tag_seq_list) = @_;
  my %tag_seq_hash = map { $_ => 1 } @{$tag_seq_list};
  if( scalar keys %tag_seq_hash != scalar @{$tag_seq_list} ){
    $self->logcroak('The given tags after trimming are not unique');
  }
  return 1;
}

no Moose;
__PACKAGE__->meta->make_immutable;

1;

__END__


=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item List::Util

=item Carp

=item Readonly

=item File::Spec::Functions

=item Moose

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2018 Genome Research Limited

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
