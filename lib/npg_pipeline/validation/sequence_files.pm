package npg_pipeline::validation::sequence_files;

#########
# Copied from 
# svn+ssh://svn.internal.sanger.ac.uk/repos/svn/new-pipeline-dev/data_handling/trunk/lib/npg_validation/runfolder/deletable/sequence_files.pm
# on the 5th of January 2018
#

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;
use Carp;
use File::Basename;
use Perl6::Slurp;
use Try::Tiny;

use st::api::lims;

extends 'npg_tracking::illumina::runfolder';
with    'npg_pipeline::validation::common';

our $VERSION = '0';

=head1 NAME

npg_pipeline::validation::sequence_files

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 archived_for_deletion

Returns true if the sequence files in the staging folder are correctly archived to iRODS.
If any problems are encounted, returns false.

=cut

sub archived_for_deletion {
  my $self = shift;
  return $self->_check_num_files &&
         $self->_check_files_against_lims() &&
         $self->_check_index_files() &&
         $self->_check_md5();
}

=head2 lims_driver_type

Attribute, can have undefuned value. Driver type for
st::api::lims object

=cut

has 'lims_driver_type' => (isa      => 'Maybe[Str]',
                           is       => 'ro',
                           required => 0,
                          );

has '_lane_lims' => (isa        => 'HashRef',
                     is         => 'ro',
                     lazy_build => 1,
                     trigger    => \&_list_not_empty,
                    );
sub _build__lane_lims {
  my $self = shift;

  my $h = {'id_run' => $self->id_run};
  if ($self->lims_driver_type()) {
    $h->{'driver_type'} = $self->lims_driver_type();
  }
  return st::api::lims->new($h)->children_ia;
}

has '_staging_files' => (isa        => 'ArrayRef',
                         is         => 'ro',
                         traits     => ['Array'],
                         lazy_build => 1,
                         trigger    => \&_list_not_empty,
                         handles    => {
                           staging_files     => 'elements',
                           num_staging_files => 'count',
                         },
                        );
sub _build__staging_files {
  my $self = shift;
  my $file_name_glob = $self->id_run . q{_*.} . $self->file_extension;
  ## no critic (CodeLayout::ProhibitParensWithBuiltins)
  my @globs = ( join(q{/}, $self->archive_path(),           $file_name_glob),
                join(q{/}, $self->archive_path(), q{lane*}, $file_name_glob) );
  my @files = glob join(q{ }, @globs);
  ## use critic
  $self->logger->info(join qq{\n}, q{Staging files list}, @files);
  return [sort @files];
}

has '_irods_index_files'  => (isa        => 'ArrayRef',
                              is         => 'ro',
                              traits     => ['Array'],
                              lazy_build => 1,
                              handles    => {
                                irods_index_files     => 'elements',
                                num_irods_index_files => 'count',
                              },
                             );
sub _build__irods_index_files {
  my $self = shift;
  my $i_re = $self->index_file_extension;
  $i_re = qr/[.]$i_re\Z/xms;
  my @i_list   = grep { $_ =~ $i_re }   keys %{$self->collection_files};
  $self->logger->info(join qq{\n}, q{iRODS index files list}, @i_list);
  return [sort @i_list];
}

has '_lims_inferred_files' => (isa        => 'ArrayRef',
                               is         => 'ro',
                               traits     => ['Array'],
                               lazy_build => 1,
                               trigger    => \&_list_not_empty,
                               handles    => {
                                 lims_inferred_files     => 'elements',
                                 num_lims_inferred_files => 'count',
                               },
                              );
sub _build__lims_inferred_files {
  my $self = shift;
  my @list = map { @{$self->_file_list_per_lane($_)} } keys %{$self->_lane_lims()};
  $self->logger->info( join qq{\n}, q{LIMS inferred file list}, @list);
  return [sort @list];
}

sub _list_not_empty {
  my ($self, $array) = @_;
  my @list = ref $array eq 'ARRAY' ? @{$array} : keys %{$array};
  if (scalar @list == 0) {
    croak 'List cannot be empty';
  }
  return;
}

sub _is_multiplexed_lane {
  my ($self, $position) = @_;
  return $self->_lane_lims()->{$position}->is_pool && $self->is_indexed;
}

sub _split_type {
  my ($self, $lims) = @_;
  return $lims->contains_nonconsented_human   ? 'human'   :
       ( $lims->contains_nonconsented_xahuman ? 'xahuman' :
       ( $lims->separate_y_chromosome_data    ? 'yhuman'  : undef ) );
}

sub _check_num_files {
  my $self = shift;
  if( $self->num_irods_files() != $self->num_staging_files() ) {
    $self->logger->logwarn(sprintf
           'Number of sequence files is different:%s%siRODs: %i, staging: %i',
           qq[\n], qq[\t], $self->num_irods_files(), $self->num_staging_files());
    return 0;
  }
  return 1
}

sub _check_files_against_lims {
  my $self = shift;
  my $fully_archived = 1;
  my %seq_list = map {$_ => 1} $self->irods_files();
  foreach my $f ( $self->lims_inferred_files() ) {
    if( !$seq_list{$f} ) {
      $self->logger->logwarn("According to LIMS, file $f is missing in iRODS\n");
      $fully_archived = 0;
    }
  }
  return $fully_archived;
}

sub _check_md5 {
  my $self = shift;

  my $md5_list_irods   = $self->_irods_md5s();
  my $md5_list_staging = $self->_staging_md5s();
  my $md5_correct = 1;

  try {
    foreach my $f ( sort keys %{$md5_list_irods} ) {
      my $md5_irods   = $md5_list_irods->{$f};
      my $md5_staging = $md5_list_staging->{$f};
      if ( !$md5_irods || !$md5_list_irods ) {
        croak "One of md5 values for $f is not defined";
      }
      if( $md5_irods ne $md5_staging ) {
        croak "md5 wrong for ${f}: '$md5_irods' not match '$md5_staging'";
      }
    }
  } catch {
    $self->logger->logwarn($_);
    $md5_correct = 0;
  };
  return $md5_correct;
}

sub _irods_md5s {
  my $self = shift;
  my $md5_list = {};
  foreach my $f ( $self->irods_files() ) {
    $md5_list->{$f} = $self->collection_files()->{$f}->checksum() || q();
  }
  return $md5_list;
}

sub _staging_md5s {
  my $self = shift;
  my $md5_list = {};
  foreach my $f ( $self->staging_files() ) {
    my $md5f = $f . q{.md5};
    $md5_list->{basename($f)} = slurp $md5f, { chomp => 1 } || q();
  }
  return $md5_list;
}

sub _check_index_files {
  my $self = shift;

  my $all_found = 1;
  my %i_list = map {$_ => 1} $self->irods_index_files();
  foreach my $f ( $self->irods_files() ) {
    if ($self->_index_should_exist($f)) {
      my $i = $self->_index_file_name($f);
      if(!$i_list{$i}) {
        $self->logger->logwarn("Index file for $f does not exist");
        $all_found = 0;
      }
    }
  }

  return $all_found;
}

sub _index_should_exist {
  my ($self, $file_name) = @_;
  my $meta = $self->get_metadata(
             $self->collection_files()->{$file_name}, qw/alignment total_reads/);
  #use Test::More; use Data::Dumper; diag $file_name . q[  ] . Dumper( $meta);
  return $meta->{'alignment'} && $meta->{'total_reads'};
}

sub _file_list_per_lane {
  my ($self, $position) = @_;

  my $lane_lims  = $self->_lane_lims()->{$position};
  my $lane_split = $self->_split_type($lane_lims);
  my $phix_split = q[phix];
  my @bam_list_per_lane = ();

  if ( !$self->_is_multiplexed_lane($position) ) {
    my @subsets = (q[], $phix_split);
    if( $lane_split) {
      push @subsets, $lane_split;
    }
    @bam_list_per_lane = map {
      $self->generate_file_name({'id_run'    => $self->id_run,
                                 'position'  => $position,
                                 'split'     => $_})
                                } @subsets;
  } else {
    if (!$lane_lims->is_pool ) {
      croak "Lane $position is not a pool, cannot get information for plexes";
    }
    my @tags = sort keys %{$lane_lims->tags()};
    unshift @tags, 0;
    my $plexes = $lane_lims->children_ia();
    foreach my $tag_index (@tags) {
      my $pl = $plexes->{$tag_index};
      my @subsets = (q[]);
      if (!$pl || !$pl->is_control) {
        push @subsets, $phix_split;
        my $split = ($tag_index == 0) ? $lane_split :$self->_split_type($pl);
        if ($split) {
          push @subsets, $split;
        }
      }
      push @bam_list_per_lane, (map {
        $self->generate_file_name({'id_run'    => $self->id_run,
                                   'position'  => $position,
                                   'tag_index' => $tag_index,
                                   'split'     => $_})
                                    } @subsets );
    }
  }

  return \@bam_list_per_lane;
}

sub _index_file_name {
  my ($self, $f) = @_;
  my $ext = $self->file_extension;
  if ($f !~ /.$ext$/msx) {
    croak "Unexpected extension in $f";
  }
  return join q[.], $f, $self->index_file_extension;
}

__PACKAGE__->meta->make_immutable;

1;
__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item MooseX::StrictConstructor

=item namespace::autoclean

=item Carp

=item File::Basename

=item File::Spec

=item st::api::lims

=item Perl6::Slurp

=item Try::Tiny

=npg_tracking::illumina::runfolder

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Guoying Qi
Steven Leonard
Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2018 GRL

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
