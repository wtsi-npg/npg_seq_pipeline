package npg_pipeline::cache::barcodes;

use Moose;
use Carp;
use File::Spec::Functions;
use List::Util qw(max min sum);
use Readonly;
use open q(:encoding(UTF8));

use npg_tracking::util::types;
use npg_pipeline::function::util;

with 'WTSI::DNAP::Utilities::Loggable';

our $VERSION = '0';

Readonly::Scalar my $TAG_LIST_FILE_HEADER => qq{barcode_sequence\tbarcode_name\tlibrary_name\tsample_name\tdescription};

# For dual index runs add the expected i5 tag sequences for samples (e.g. single index phix) which have no i5 tag
# These have been extended to 13 bases to cope with haplotagging runs which have 2x13-bases indexes
# Pad short tags, for i7 we know the 5-base pad, so we can pad a 8-base i7 tag (i.e. dual-index phix) to 13 bases
# for i5 only know the 2-bases pad so currently we can pad a 8-base i5 tag (i.e. dual-index phix) to 10 bases
Readonly::Scalar my $I7_TAG_PAD => q(ATCTC);
Readonly::Scalar my $I5_TAG_PAD => q(AC);
Readonly::Scalar my $I5_TAG_OPP_PAD => q(GTGTA);
Readonly::Scalar my $I5_TAG_MISSING => q(TCTTTCCCTACAC);
Readonly::Scalar my $I5_TAG_OPP_MISSING => q(AGATCTCGGTGGT);

Readonly::Scalar my $I7_PADDED_PHIX => q(ACAACGCAATC);
Readonly::Scalar my $I7_UNPADDED_PHIX_LEN => 8;

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

  my $spiked_phix_tag_index = $self->lane_lims->spiked_phix_tag_index();

  # on a some instruments (HiSeqX, HiSeq4000 and NovaSeq v1.5) the second index is sequenced in reverse complement order
  my %i7_tags = ();
  my %i5_tags = ();
  foreach my $plex ($self->lane_lims->children) {
    if (my $ti = $plex->tag_index){
      my $tag_sequences = $plex->tag_sequences;
      $i7_tags{$ti} = $tag_sequences->[0];
      if ( @{$tag_sequences} == 2 ) {
        if ( $self->i5opposite ) {
          $tag_sequences->[1] =~ tr/[ACGT]/[TGCA]/;
          $tag_sequences->[1] = reverse $tag_sequences->[1];
        }
        $i5_tags{$ti} = $tag_sequences->[1];
      } else {
        $i5_tags{$ti} = q();
      }
    }
  }

  my ($tag_index_list, $tag_seq_list) = $self->_process_tag_list(\%i7_tags, \%i5_tags, $spiked_phix_tag_index);

  if  ($tag_index_list && $tag_seq_list) {
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

sub _process_tag_list {
  my ($self, $i7_tags, $i5_tags, $control_tag_index) = @_;

  my $index_lengths = $self->index_lengths;
  my $i7_read_length = $index_lengths->[0];
  my $i5_read_length = (scalar @{$index_lengths} == 1 ? 0 : $index_lengths->[1]);

  my @tag_index_list = sort keys %{$i7_tags};
  foreach my $index (@tag_index_list) {
    if(!$index){
      $self->warn('The tag index is not available');
      return (undef, undef);
    }
    if (!defined($i7_tags->{$index}) || ($i5_read_length && !defined($i5_tags->{$index}))) {
      $self->warn('The tag sequence are not available');
      return (\@tag_index_list, undef);
    }
  }

  # the old single-index phix tag was already padded from 8-bases to 11-bases, remove the last 3 bases so we can treat it like any other short tag
  foreach my $index (@tag_index_list) {
    if ($control_tag_index && ($index == $control_tag_index) && ($i7_tags->{$index} eq $I7_PADDED_PHIX)) {
      $i7_tags->{$index} = substr $i7_tags->{$index}, 0, $I7_UNPADDED_PHIX_LEN;
    }
  }

  my $trunc_and_pad_i7 = $self->_truncate_and_pad($i7_tags, $control_tag_index, $i7_read_length, $I7_TAG_PAD);
  my $i7_tags_suffix_removed = $self->_remove_common_suffixes($trunc_and_pad_i7, $control_tag_index);

  my $i5_tags_suffix_removed;
  if ($i5_read_length) {
    my $pad_seq = $I5_TAG_PAD;
    my $missing = $I5_TAG_MISSING;
    if ($self->i5opposite){
      #if the i5 tag was sequenced in the opposite direction the pad and missing sequences will be different
      $pad_seq = $I5_TAG_OPP_PAD;
      $missing = $I5_TAG_OPP_MISSING;
    }
    my $trunc_and_pad_i5 = $self->_truncate_and_pad($i5_tags, $control_tag_index, $i5_read_length, $pad_seq, $missing);
    $i5_tags_suffix_removed = $self->_remove_common_suffixes($trunc_and_pad_i5, $control_tag_index);
  }

  my @tag_seq_list = ();
  foreach my $tag_index (@tag_index_list){
    my $tag_seq;
    if ($i5_read_length > 0){
      $tag_seq = join q[-], ($i7_tags_suffix_removed->{$tag_index}, $i5_tags_suffix_removed->{$tag_index});
    } else {
      $tag_seq = $i7_tags_suffix_removed->{$tag_index};
    }
    push @tag_seq_list, $tag_seq;
  }

  $self->_check_tag_uniqueness(\@tag_seq_list);

  return (\@tag_index_list, \@tag_seq_list);
}

sub _truncate_and_pad {##no critic (Subroutines::ProhibitManyArgs
  my ($self, $tags, $control_tag_index, $read_length, $pad_seq, $missing) = @_;
  #getting length to truncate to
  my $max_seq_length = 0;
  foreach my $index (keys %{$tags}){
    next if (!defined $tags->{$index}); #skip if undef
    if ($control_tag_index){
      next if ($index eq $control_tag_index); #just consider real samples 
    }
    my $exp_seq = $tags->{$index};

    if (length($exp_seq) > $max_seq_length){
      $max_seq_length = length $exp_seq;
    }
  }
  my $truncated_length = $read_length;
  if ($max_seq_length < $read_length){
    $truncated_length = $max_seq_length;
  }
  #add missing sequences
  if (defined $missing) {
    foreach my $index (keys %{$tags}){
      if (!$tags->{$index}) {
        $tags->{$index} = $missing;
      }
    }
  }
  #truncate sequences
  foreach my $index (keys %{$tags}){
    $tags->{$index} = substr $tags->{$index},0,$truncated_length;
  }
  #pad sequences
  my $length_of_pad = length $pad_seq;
  foreach my $index (keys %{$tags}){
    my $seq_to_pad = $tags->{$index};
    my $seq_to_pad_length = length $seq_to_pad;
    my $num_bases_to_pad = $truncated_length - $seq_to_pad_length;
    if ($num_bases_to_pad != 0) {
      if ($num_bases_to_pad > $length_of_pad) {
        $self->logcroak('Cannot extend for more bases than in padding sequence');
      }else{
        $tags->{$index} .= substr $pad_seq,0,$num_bases_to_pad;
      }
    }
  }
  return $tags;
}

sub _remove_common_suffixes {
  my ($self, $tags, $control_tag_index) = @_;

  my $num_of_tags = keys %{$tags};
  # check if there is only one non-control sequence
  if ((($control_tag_index)and($num_of_tags <= 2)) or ((!$control_tag_index)and ($num_of_tags < 2))){
    return $tags;
  }

  my %tag_length = map {length $_ => 1 } values %{$tags};
  if (scalar keys %tag_length != 1){
    $self->logcroak('The given tags are different in length: ',values %{$tags});
  }

  # get array of real samples
  my @list_of_tags = ();
  foreach my $index (keys %{$tags}){
    if (defined($tags->{$index})){
      #skip control tag when getting real tags
      if($control_tag_index){
        if ($control_tag_index != $index){
          push @list_of_tags, $tags->{$index};
        }
      }else{
        push @list_of_tags, $tags->{$index};
      }
    }
  }

  #gets the longest common suffix from real samples
  my $current_suffix = $list_of_tags[0];
  foreach my $tag (@list_of_tags){
    $current_suffix = $self->_longest_common_suffix($tag, $current_suffix);
    last if ($current_suffix eq q() );
  }
  #if no common suffix
  if (length $current_suffix == 0){
    return $tags;
  }

  #remove common suffix
  foreach my $index (keys %{$tags}){
    $tags->{$index} = substr $tags->{$index},0,-length($current_suffix);
  }

  return $tags;
}

sub _longest_common_suffix {
  my ($self, $sequence, $current_suffix) = @_;
  for my $position (0..length $current_suffix){
    my $suffix = substr $current_suffix, $position;
    my $part_of_seq = substr $sequence , -(length $suffix);
    if ($part_of_seq eq $suffix){
      return $suffix;
    }
  }
  return q();
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
