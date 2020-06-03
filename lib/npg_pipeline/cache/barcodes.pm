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
  my $spiked_phix_tag_index = $self->lane_lims->spiked_phix_tag_index();

  # on a HiSeqX the second index is sequenced in reverse complement order
  foreach my $plex ($self->lane_lims->children) {
    if (my $ti = $plex->tag_index){
      my $tag_sequences = $plex->tag_sequences;
      if ( @{$tag_sequences} == 2 ) {
        if ( $self->i5opposite ) {
          $tag_sequences->[1] =~ tr/[ACGT]/[TGCA]/;
          $tag_sequences->[1] = reverse $tag_sequences->[1];
        }
      }
      $tags->{$ti} = join q[-], @{$tag_sequences};
    }
  }

  my ($tag_index_list, $tag_seq_list) = $self->_process_tag_list($tags, $spiked_phix_tag_index);

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

# return the absolute length of a "6,8" format length
# (which is the sum of the two lengths)
sub _abs_len {
  my ($self, $tag) = @_;
  my @idx = split /,/smx, $tag;
  my $result = 0;
  foreach my $i (@idx) { $result = $result + $i; }
  return $result;
}

# return either length of the tag
# or len1,len2 in the case of a dual index
sub _get_tag_length {
  my ($self, $tag) = @_;
  my @indexes = split /-/smx, $tag;
  my @lengths = map { length } @indexes;
  my $key = join q{,}, @lengths;
  return $key;
}

# truncate an index of the form "AAAAAA-GGGGGG"
# to a length of the form [4,6]
sub _truncate_index {
  my ($self, $index, $index_lengths, $tag2) = @_;
  my @idx = split /-/smx, $index;
  my @ilengths = @{$index_lengths};
  # if there is only one index length set the second index length to 0
  if (scalar @ilengths == 1) { push @ilengths, 0; }
  # if there is no separator in the index set the second part of the index to $tag2
  if (scalar @idx == 1) { push @idx, defined $tag2 ? $tag2 : q{}; }
  $idx[0] = substr $idx[0], 0, $ilengths[0];
  $idx[1] = substr $idx[1], 0, $ilengths[1];
  my $result = join q{-},@idx;
  # remove the separator if the second part of the truncated index is empty
  if ($result =~ /-$/smx) { chop $result; }
  return $result;
}

sub _check_tag_length {
  my ($self, $tag_seq_list, $tag_index_list, $spiked_phix_tag_index) = @_;

  # ensure no tags are longer than the index length
  # Note: this is not correct for runs where not ALL the index reads appear in the BC(QT) tag i.e. TraDIS it
  # works only because the first index read (the transposon) is longer that the second index read (the tag)
  my @indexed_length_tags = map {$self->_truncate_index($_, $self->index_lengths)} @{$tag_seq_list};

  my %tag_length;
  foreach my $i (0..$#indexed_length_tags) {
    my $tag = $indexed_length_tags[$i];
    push @{$tag_length{$self->_get_tag_length($tag)}}, $tag_index_list->[$i];
  }

  my $number_of_tag_lengths = scalar keys %tag_length;

  if ( $number_of_tag_lengths == 1 ) {
    return \@indexed_length_tags;
  }

  my $tags_ok;
  # we are making the assumption that just 1 tag being too long or too short will be because of a spike
  if ( $number_of_tag_lengths == 2 ) {
    $self->info(q{There are 2 different tag lengths});
    my @temp = keys %tag_length;

    $self->debug(q{Is there only 1 longest or shortest and is it the phix tag?});
    my $phix_entry = 2;
    if ( scalar @{$tag_length{$temp[0]}} == 1 and $spiked_phix_tag_index and $spiked_phix_tag_index == $tag_length{$temp[0]}->[0] ) {
      $phix_entry = 0;
    }
    if ( scalar @{$tag_length{$temp[1]}} == 1 and $spiked_phix_tag_index and $spiked_phix_tag_index == $tag_length{$temp[1]}->[0] ) {
      $phix_entry = 1;
    }
    if ($phix_entry < 2) {
      $self->debug(q{Yes, we have a PhiX tag});
      my $not_phix_entry = ($phix_entry + 1) % 2;
      # Truncate all tags to the non-phix length
      my @temp_lengths = split /,/smx, $temp[$not_phix_entry];
      # if the non-phix tags have two parts add the missing i5 spiked phix tag
      @indexed_length_tags = map { $self->_truncate_index($_, \@temp_lengths, ($self->i5opposite ? $SPIKED_PHIX_I5OPPOSITE_TAG2 : $SPIKED_PHIX_TAG2)) } @indexed_length_tags;
      foreach my $t (@indexed_length_tags) {
        if ($self->_abs_len($self->_get_tag_length($t)) < $self->_abs_len($temp[$not_phix_entry])) {
          $self->logcroak(qq{It looks likes the padded sequence for spiked PhiX $t is too short});
        }
      }
      $tags_ok = 1;
    }

  }

  if ( ! $tags_ok ) {
    $self->error(q{Number of different tag lengths = } . $number_of_tag_lengths);
    $self->error( q{Number at each length:} );
    foreach my $key ( sort { $a <=> $b } keys %tag_length ) {
      my $n = scalar @{$tag_length{$key}};
      $self->error( qq{Length $key: $n.} );
    }
    $self->logcroak(join q{:}, @indexed_length_tags);
  }

  return \@indexed_length_tags;
}

sub _process_tag_list {
  my ($self, $tags, $spiked_phix_tag_index) = @_;

  my @tag_index_list = sort keys %{$tags};

  my @tag_seq_list = ();
  foreach my $tag_index (@tag_index_list){
    if(!$tag_index){
      $self->warn('The tag index is not available');
      return (undef, undef);
    }
    my $tag_seq = $tags->{$tag_index};
    if(!$tag_seq){
      $self->warn('The tag sequence are not available');
      return (\@tag_index_list, undef);
    }
    push @tag_seq_list, $tag_seq;
  }

  my $tag_seq_list_checked = $self->_check_tag_length(\@tag_seq_list, \@tag_index_list, $spiked_phix_tag_index, $tags);

  my $trimmed_tag_seq_list = $self->_trim_tag_common_suffix($tag_seq_list_checked, \@tag_index_list, $spiked_phix_tag_index);

  $self->_check_tag_uniqueness($trimmed_tag_seq_list);

  return (\@tag_index_list, $trimmed_tag_seq_list);
}

#trim common suffix of a list of tag sequences, ignore spiked phix tag
#croak if the tag sequence are different in length or they are all the same
sub _trim_tag_common_suffix {
  my ($self, $tag_seq_list, $tag_index_list, $spiked_phix_tag_index) = @_;

  my $ntags = scalar @{$tag_seq_list};

  if($ntags == 1){
    $self->info('Only one tag found for this lane');
    return $tag_seq_list;
  }

  my %tag_length = map {length $_ => 1 } @{$tag_seq_list};

  if (scalar keys %tag_length != 1){
    $self->logcroak("The given tags are different in length: @{$tag_seq_list}");
  }

  my @common_tag_seq_list;
  foreach my $i (0..($ntags-1)) {
    next if $spiked_phix_tag_index && $tag_index_list->[$i] == $spiked_phix_tag_index;
    push @common_tag_seq_list, $tag_seq_list->[$i];
  }

  $ntags = scalar @common_tag_seq_list;

  if($ntags == 1){
    $self->info('Only one non-phix tag found for this lane');
    return $tag_seq_list;
  }

  my $tag_common_suffix_length = $self->_tag_common_suffix_length(\@common_tag_seq_list);

  if($tag_common_suffix_length == 0){
    $self->info('No need to trim given tags');
    return $tag_seq_list;
  }

  if($tag_length{$tag_common_suffix_length}){
    $self->logcroak("All tags are the same @{$tag_seq_list}");
  }

  my @trimmed_tag_seq_list;

  foreach my $tag (@{$tag_seq_list}){
    push @trimmed_tag_seq_list, substr $tag, 0, - $tag_common_suffix_length;
  }

  return \@trimmed_tag_seq_list;
}

#find the common suffix of length for a list tag sequences
sub _tag_common_suffix_length {
  my ($self, $tag_seq_list) = @_;

  my $common_suffix_length = 0;
  my %tag_length = map {length $_ => 1 } @{$tag_seq_list};
  my $shotest_tag_length = min keys %tag_length;

  while($common_suffix_length < $shotest_tag_length){
    $common_suffix_length++;
    my $previous_suffix = substr $tag_seq_list->[0], - $common_suffix_length;

    foreach my $tag (@{$tag_seq_list}){
      my $current_suffix = substr $tag, - $common_suffix_length;
      if($current_suffix ne $previous_suffix){
	return $common_suffix_length - 1;
      }
    }
  }

  return $common_suffix_length;
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
