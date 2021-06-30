package npg_pipeline::cache::barcodes2;

use strict;
use warnings;
use Log::Log4perl qw(:easy);
use Moose;
use namespace::autoclean;
use MooseX::StrictConstructor;
use Carp;

with qw{MooseX::Getopt};

our $VERSION = '0';

my $I5_TAG_PAD=q(AC);
my $I5_TAG_OPP_PAD=q();
my $I7_TAG_PAD = q(AT);

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
  #TODO add more checks on parameters?

  if ($self->i5_read_length == 0){
    croak('Cannot have an i5 read length of 0');
  }
  if ($self->i7_read_length == 0){
    croak('Cannot have an i7 read length of 0');
  }
  return;
}


sub run {
  my ($self) = shift;
  #create data structures
  my $i5_reads;
  my $i7_reads;
  my $padding;
  my $control_tag_index;
  my $larger_hash;
  my $lims_data = $self->lims_data();
  foreach my $index (keys %{$lims_data}){
    if ($lims_data->{$index}{phix_control}){
      if ($control_tag_index){ # if $control tag has already been assigned a value
        croak 'Two phix controls in lane';
      }else{
        $control_tag_index = $index;
      }
    }
    $i5_reads->{$index} = $lims_data->{$index}->{i5_expected_seq};
    $i7_reads->{$index} = $lims_data->{$index}->{i7_expected_seq};
  }

  # pass in $tag_pad based on i5_opposite
  if ($self->i5_opposite == 0){
    $padding = $I5_TAG_PAD;
  }elsif ($self->i5_opposite == 1){
    $padding = $I5_TAG_OPP_PAD;
  }

  my $trunc_and_pad_i5 = _truncate_and_pad($i5_reads,$control_tag_index,$self->{i5_read_length},$padding);
  my $trunc_and_pad_i7 = _truncate_and_pad($i7_reads,$control_tag_index,$self->{i7_read_length},$I7_TAG_PAD);

  my $i5_reads_suffix_removed = _remove_common_suffixes($trunc_and_pad_i5,$control_tag_index);
  my $i7_reads_suffix_removed = _remove_common_suffixes($trunc_and_pad_i7,$control_tag_index);

  # gets the larger hash to loop through later (the one with control seq)
  if ((keys %{$i5_reads_suffix_removed}) >= (keys %{$i7_reads_suffix_removed})){
    $larger_hash = $i5_reads_suffix_removed;
  }else{
    $larger_hash = $i7_reads_suffix_removed;
  }

  #loops through the keys of the larger hash adding the reads of both i7 and i5 read hashes to new hash(deplexed_reads)
  my $deplexed_reads = {};
  foreach my $index (keys %{$larger_hash}){
    if ($lims_data->{$index}{phix_control}){
      $deplexed_reads->{$index}{phix_control} = 1;
    }else{
      $deplexed_reads->{$index}{phix_control} = 0;
    }
    if ($i5_reads_suffix_removed->{$index}){
      $deplexed_reads->{$index}{i5_read} = $i5_reads_suffix_removed->{$index};
    }
    if ($i7_reads_suffix_removed->{$index}){
      $deplexed_reads->{$index}{i7_read} = $i7_reads_suffix_removed->{$index};
    }
  }
  return $deplexed_reads;
}


sub _truncate_and_pad {
  my ($reads_href,$control_tag_index,$read_length,$pad_seq) = @_;
  #getting length to truncate to
  my $max_seq_length = 0;
  foreach my $index (keys %{$reads_href}){
    next if (!defined $reads_href->{$index}); #skip if undef
    if ($control_tag_index){
      next if ($index eq $control_tag_index); # just consider real samples 
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
  #truncate sequences
  foreach my $index (keys %{$reads_href}){
    next if (!defined $reads_href->{$index}); # skip if undef
    my $exp_seq = $reads_href->{$index};
    $reads_href->{$index} = substr $reads_href->{$index},0,$truncated_length;
  }
  #padding sequences
  foreach my $index (keys %{$reads_href}){
    next if (!defined $reads_href->{$index});# skip if undef
    my $seq_to_pad = $reads_href->{$index};
    my $seq_to_pad_length = length $seq_to_pad;
    my $length_of_pad = length $pad_seq;
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
  my ($reads_href,$control_tag_index) = @_;

  my $num_of_reads = keys %{$reads_href};
  # check if there is only one non-control sequence
  if ((($control_tag_index)and($num_of_reads <= 2)) or ((!$control_tag_index)and ($num_of_reads < 2))){
    croak 'Only one non-control seq';
  }

  my %reads = %{$reads_href};
  # get array of real samples
  my @list_of_reads = ();
  foreach my $index (keys %{$reads_href}){

    #check for common tags
    if (defined($reads_href->{$index})){
      my $count = 0;
      foreach my $index2 (keys %{$reads_href}){
        if ((defined $reads_href->{$index}) and (defined $reads_href->{$index2})){
          if ($reads_href->{$index} eq $reads_href->{$index2}){
            $count++;
          }
        }
      }
      if ($count > 1){
        croak('Common tags - have no way of distinguishing sample');
      }

      #skip control tag when getting real reads
      if($control_tag_index){
        if ($control_tag_index != $index){
          push @list_of_reads, $reads_href->{$index};
        }
      }else{
        push @list_of_reads, $reads_href->{$index};
      }
    }
  }
  #gets the longest common suffix from real samples
  my $current_suffix = $list_of_reads[0];
  foreach my $read (@list_of_reads){
    $current_suffix = _longest_common_suffix($read,$current_suffix);
    last if ($current_suffix eq q() );
  }
  #if no common suffix
  if (length $current_suffix == 0){
    return $reads_href;
  }

  #remove common suffix
  foreach my $index (keys %{$reads_href}){
    $reads_href->{$index} = substr $reads_href->{$index},0,-length($current_suffix);
  }

  if ($control_tag_index){
    foreach my $index (keys %{$reads_href}){
      next if ($index == $control_tag_index);
      if ($reads_href->{$index} eq $reads_href->{$control_tag_index}){
        croak('After removing common suffix, control has a matching real sample');
      }
    }
  }
  return $reads_href;
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

=head2 run

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
