package npg_pipeline::cache::barcodes2;

use strict;
use warnings;
use Log::Log4perl qw(:easy);
use Moose;
use namespace::autoclean;
use MooseX::StrictConstructor;
use Carp;
use Readonly;

with qw{MooseX::Getopt};

our $VERSION = '0';

Readonly::Scalar my $I7_TAG_PAD => q(ATCTC);
Readonly::Scalar my $I5_TAG_PAD => q(AC);
Readonly::Scalar my $I5_TAG_OPP_PAD => q(GT);
Readonly::Scalar my $I5_TAG_MISSING => q(TCTTTCCCTACAC);
Readonly::Scalar my $I5_TAG_OPP_MISSING => q(AGATCTCGGTGGT);

Readonly::Scalar my $I7_PADDED_PHIX => q(ACAACGCAATC);
Readonly::Scalar my $I7_UNPADDED_PHIX_LEN => 8;

=head1 NAME

npg_pipeline::cache::barcodes2

=cut

has q{lims_data}         => (isa        => q{HashRef},
                             is         => q{ro},
                             required   => 1,
                            );

has q{i5_read_length}    => (isa        => q{Int},
                             is         => q{ro},
                             required   => 1,
                            );

has q{i7_read_length}    => (isa        => q{Int},
                             is         => q{ro},
                             required   => 1,
                            );

has q{i5_opposite}       => (isa        => q{Bool},
                             is         => q{ro},
                             required   => 1,
                            );


sub BUILD {
  my $self = shift;
  if ($self->i7_read_length == 0){
    croak('Cannot have an i7 read length of 0');
  }
  return;
}


sub generate {
  my ($self) = shift;
  #create data structures
  my $i5_reads;
  my $i7_reads;
  my $padding;
  my $missing;
  my $control_tag_index;
  my $larger_hash;
  my $lims_data = $self->lims_data();
#### TESTING ####
#  use Data::Dumper;
#  print STDERR Dumper($lims_data);
#### TESTING ####
  foreach my $index (keys %{$lims_data}){
    if ($lims_data->{$index}{phix_control}){
      if ($control_tag_index){ # if $control tag has already been assigned a value
        croak 'Two phix controls in lane';
      }else{
        $control_tag_index = $index;
      }
    }
    $i7_reads->{$index} = (defined($lims_data->{$index}->{i7_expected_seq}) ? $lims_data->{$index}->{i7_expected_seq} : '');
    if ($self->i5_read_length) {
      $i5_reads->{$index} = (defined($lims_data->{$index}->{i5_expected_seq}) ? $lims_data->{$index}->{i5_expected_seq} : '');
    }
    #the old single-index phix tag was already padded from 8-bases to 11-bases, remove the last 3 bases so we can treat it like any other short tag
    if ($control_tag_index && ($index == $control_tag_index) && ($i7_reads->{$index} eq $I7_PADDED_PHIX)) {
      $i7_reads->{$index} = substr($i7_reads->{$index}, 0, $I7_UNPADDED_PHIX_LEN);
    }
  }
#### TESTING ####
#  print STDERR "i7_read_length=",$self->i7_read_length," i5_read_length=",$self->i5_read_length,($control_tag_index ? " Control $control_tag_index\n" : " No control\n");
#  for my $index (keys %{$i7_reads}) {print STDERR "i7 $index $i7_reads->{$index}\n" if defined($i7_reads->{$index})};
#  for my $index (keys %{$i5_reads}) {print STDERR "i5 $index $i5_reads->{$index}\n" if defined($i5_reads->{$index})};
#### TESTING ####

  my $trunc_and_pad_i7 = _truncate_and_pad($i7_reads,$control_tag_index,$self->{i7_read_length},$I7_TAG_PAD);
  my $i7_reads_suffix_removed = _remove_common_suffixes($trunc_and_pad_i7,$control_tag_index);
#### TESTING ####
#  print STDERR "trunc_and_pad_i7\n";
#  for my $index (keys %{$trunc_and_pad_i7}) {print STDERR "i7 $index $trunc_and_pad_i7->{$index}\n" if defined($trunc_and_pad_i7->{$index})};
#  print STDERR "i7_reads_suffix_removed\n";
#  for my $index (keys %{$i7_reads_suffix_removed}) {print STDERR "i7 $index $i7_reads_suffix_removed->{$index}\n" if defined($i7_reads_suffix_removed->{$index})};
#### TESTING ####
  my $i5_reads_suffix_removed;
  if ($self->i5_read_length) {
    my $pad = $I5_TAG_PAD;
    my $missing = $I5_TAG_MISSING;
    if ($self->i5_opposite == 1){
      #if the i5 tag was sequenced in the opposite direction the pad and missing sequences will be different
      $pad = $I5_TAG_OPP_PAD;
      $missing = $I5_TAG_OPP_MISSING;
    }
    my $trunc_and_pad_i5 = _truncate_and_pad($i5_reads,$control_tag_index,$self->{i5_read_length},$pad, $missing);
    $i5_reads_suffix_removed = _remove_common_suffixes($trunc_and_pad_i5,$control_tag_index);
#### TESTING ####
#    print STDERR "trunc_and_pad_i5\n";
#    for my $index (keys %{$trunc_and_pad_i5}) {print STDERR "i5 $index $trunc_and_pad_i5->{$index}\n" if defined($trunc_and_pad_i5->{$index})};
#    print STDERR "i5_reads_suffix_removed\n";
#    for my $index (keys %{$i5_reads_suffix_removed}) {print STDERR "i5 $index $i5_reads_suffix_removed->{$index}\n" if defined($i5_reads_suffix_removed->{$index})};
#### TESTING ####
  }

  #add the reads of the i7 and i5 read hashes to new hash(deplexed_reads)
  my $deplexed_reads = {};
  foreach my $index (keys %{$i7_reads_suffix_removed}){
    if ($lims_data->{$index}{phix_control}){
      $deplexed_reads->{$index}{phix_control} = 1;
    }else{
      $deplexed_reads->{$index}{phix_control} = 0;
    }
    if ($i7_reads_suffix_removed->{$index}){
      $deplexed_reads->{$index}{i7_read} = $i7_reads_suffix_removed->{$index};
    }
    if ($self->i5_read_length) {
      if ($i5_reads_suffix_removed->{$index}){
        $deplexed_reads->{$index}{i5_read} = $i5_reads_suffix_removed->{$index};
      }
    }
  }
  return $deplexed_reads;
}


sub _truncate_and_pad {
  my ($reads_href,$control_tag_index,$read_length,$pad_seq,$missing) = @_;
  #getting length to truncate to
  my $max_seq_length = 0;
  foreach my $index (keys %{$reads_href}){
    next if (!defined $reads_href->{$index}); #skip if undef
    if ($control_tag_index){
      next if ($index eq $control_tag_index); #just consider real samples 
    }
    my $exp_seq = $reads_href->{$index};

    if (length($exp_seq) > $max_seq_length){
      $max_seq_length = length $exp_seq;
    }
  }
  my $truncated_length = $read_length;
  if ($max_seq_length < $read_length){
    $truncated_length = $max_seq_length;
  }
  #add missing sequences
  if (defined($missing)) {
    foreach my $index (keys %{$reads_href}){
      if (!$reads_href->{$index}) {
        $reads_href->{$index} = $missing;
      }
    }
  }
  #truncate sequences
  foreach my $index (keys %{$reads_href}){
    $reads_href->{$index} = substr $reads_href->{$index},0,$truncated_length;
  }
  #pad sequences
  my $length_of_pad = length $pad_seq;
  foreach my $index (keys %{$reads_href}){
    next if (!defined $reads_href->{$index}); #skip if undef
    my $seq_to_pad = $reads_href->{$index};
    my $seq_to_pad_length = length $seq_to_pad;
    my $num_bases_to_pad = $truncated_length - $seq_to_pad_length;

    if ($num_bases_to_pad != 0) {
      if ($num_bases_to_pad > $length_of_pad) {
        croak 'Cannot extend for more bases than in padding sequence';
      }else{
        $reads_href->{$index} .= substr $pad_seq,0,$num_bases_to_pad;
      }
    }
  }
  return $reads_href;
}

sub _remove_common_suffixes {
  my ($tags_href,$control_tag_index) = @_;

  my $num_of_tags = keys %{$tags_href};
  # check if there is only one non-control sequence
  if ((($control_tag_index)and($num_of_tags <= 2)) or ((!$control_tag_index)and ($num_of_tags < 2))){
    return $tags_href;
  }

  # get array of real samples
  my @list_of_tags = ();
  foreach my $index (keys %{$tags_href}){
    if (defined($tags_href->{$index})){
      #skip control tag when getting real tags
      if($control_tag_index){
        if ($control_tag_index != $index){
          push @list_of_tags, $tags_href->{$index};
        }
      }else{
        push @list_of_tags, $tags_href->{$index};
      }
    }
  }

  #gets the longest common suffix from real samples
  my $current_suffix = $list_of_tags[0];
  foreach my $tag (@list_of_tags){
    $current_suffix = _longest_common_suffix($tag,$current_suffix);
    last if ($current_suffix eq q() );
  }
  #if no common suffix
  if (length $current_suffix == 0){
    return $tags_href;
  }

  #remove common suffix
  foreach my $index (keys %{$tags_href}){
    $tags_href->{$index} = substr $tags_href->{$index},0,-length($current_suffix);
  }

  return $tags_href;
}

sub _longest_common_suffix {
  my ($sequence,$current_suffix) = @_;
  for my $position (0..length $current_suffix){
    my $suffix = substr $current_suffix, $position;
    my $part_of_seq = substr $sequence , -(length $suffix);
    if ($part_of_seq eq $suffix){
      return $suffix;
    }
  }
  return q();
}

__PACKAGE__->meta->make_immutable;

1;

=head1 NAME

npg_pipeline::cache::barcodes2

=head1 SYNOPSIS

 npg_pipeline::cache::barcodes2->new(
                lims_data       => $lims_data3,

                i5_read_length  => 6,
                i7_read_length  => 8,
                i5_opposite     => 0,
                );

=head1 DESCRIPTION

Updates Majora data and/or id_run cog_sample_meta data.

Takes a hashref of lims data of the form:

$lims_data = {
              1 => {phix_control => 0,
              i5_expected_seq => 'CTCCAGGC',
              i7_expected_seq => 'GCATGATA'
              } 
             };
And the i5 read length, i7 read length and whether it is i5 opposite.
returns truncated , padded and or removed suffixed reads.

=head1 SUBROUTINES/METHODS

=head2 BUILD

Checks attributes are valid.

=head2 generate

Takes the lims data and arguments and returns the truncated, padded and removed suffix reads.

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Log::Log4perl

=item Moose

=item namespace::autoclean

=item MooseX::StrictConstructor

=item Carp

=item MooseX::Getopt

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Fred Dodd

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2021 Genome Reserach Ltd.

This file is part of NPG.

NPG is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

=cut
