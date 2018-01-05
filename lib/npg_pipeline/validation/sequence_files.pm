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
use File::Spec;
use List::Util qw{first};
use Readonly;

use st::api::lims;
use npg_common::irods::Loader;
use WTSI::NPG::iRODS::DataObject;

with qw{
         npg_tracking::illumina::run::short_info
         npg_tracking::illumina::run::folder
       };
with qw{npg_tracking::illumina::run::long_info};

our $VERSION = '0';

Readonly::Scalar my $FILE_EXTENSION  => q[cram];

=head1 NAME

npg_pipeline::validation::sequence_files

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 id_run

=head2 run_folder

=cut

sub _build_run_folder {
  my ($self) = @_;
  if (! ($self->_given_path or $self->has_id_run or $self->has_name)){
    croak 'need a path to work out a run_folder';
  }
  return first {$_ ne q()} reverse File::Spec->splitdir($self->runfolder_path);
}

=head2 file_extension

File extension for the sequence file format.

=cut

has 'file_extension' => (isa     => 'Str',
                         is      => 'ro',
                         default => $FILE_EXTENSION,
                        );

=head2 index_file_extension

File extension for the sequence file format.

=cut

has 'index_file_extension' => (isa        => 'Str',
                               is         => 'ro',
                               init_arg   => undef,
                               lazy_build => 1,
                              );
sub _build_index_file_extension {
  my $self = shift;
  return $self->file_extension eq $FILE_EXTENSION ? 'crai' : 'bai';
}

=head2 irods

Handle for interaction with iRODS.

=cut

has 'irods' => (isa       => 'WTSI::NPG::iRODS',
                is        => 'ro',
                required  => 1,
               );

=head2 collection

Directory within irods to store results, required.

=cut

has 'collection' => (isa      => 'Str',
                     is       => 'ro',
                     required => 1,
                    );

=head2 verbose

Boolean verbosity flag, false by default

=cut

has 'verbose' => (isa  => 'Bool',
                  is   => 'ro',
                 );

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

has '_lane_lims' => (isa        => 'HashRef',
                     is         => 'ro',
                     lazy_build => 1,
                     trigger    => \&_list_not_empty,
                    );
sub _build__lane_lims {
  my $self = shift;
  return st::api::lims->new(id_run => $self->id_run)->children_ia;
}

has '_staging_files' => (isa        => 'ArrayRef',
                         is         => 'ro',
                         traits     => ['Array'],
                         lazy_build => 1,
                         trigger    => \&_list_not_empty,
                         handles => {
                           staging_files      => 'elements',
                           num_staging_files  => 'count',
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
  if ($self->verbose) {
    _log(qq{\nINFO: staging files list\n} . join qq{\n}, @files);
  }
  return [sort @files];
}

has '_irods_files'  => (isa        => 'ArrayRef',
                        is         => 'ro',
                        traits     => ['Array'],
                        lazy_build => 1,
                        trigger    => \&_list_not_empty,
                        handles => {
                          irods_files      => 'elements',
                          num_irods_files  => 'count',
                        },
                       );
sub _build__irods_files {
  my $self = shift;

  my $seq_re = $self->file_extension;
  $seq_re = qr/\.$seq_re$/xms;
  my $i_re = $self->index_file_extension;
  $i_re = qr/\.$i_re$/xms;

  my @file_list = keys %{npg_common::irods::Loader->new(
    file  => 'none',
    irods => $self->irods)->get_collection_file_list($self->collection())};
  my @seq_list = grep { $_ =~ $seq_re } @file_list;
  my @i_list   = grep { $_ =~ $i_re }   @file_list;

  if ($self->verbose) {
    _log(qq{\nINFO: irods files list\n}       . join qq{\n}, @seq_list);
    _log(qq{\nINFO: irods index files list\n} . join qq{\n}, @i_list);
  }

  $self->_set_irods_index_files([sort @i_list]);

  return [sort @seq_list];
}

has '_irods_index_files'  => (isa        => 'ArrayRef',
                              is         => 'ro',
                              traits     => ['Array'],
                              writer     => '_set_irods_index_files',
                              handles => {
                                irods_index_files      => 'elements',
                                num_irods_index_files  => 'count',
                              },
                             );

has '_lims_inferred_files' => (isa        => 'ArrayRef',
                               is         => 'ro',
                               traits     => ['Array'],
                               lazy_build => 1,
                               trigger    => \&_list_not_empty,
                               handles => {
                                 lims_inferred_files      => 'elements',
                                 num_lims_inferred_files  => 'count',
                               },
                              );
sub _build__lims_inferred_files {
  my $self = shift;
  my @list = map { @{$self->_file_list_per_lane($_)} } keys %{$self->_lane_lims()};
  if ($self->verbose) {
    _log(qq{\nINFO: LIMS inferred file list\n} . join qq{\n}, @list);
  }
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
  return $lims->contains_nonconsented_human   ? 'human' :
       ( $lims->contains_nonconsented_xahuman ? 'xahuman' :
       ( $lims->separate_y_chromosome_data  ? 'yhuman' : undef ) );
}

sub _check_num_files {
  my $self = shift;
  if( $self->num_irods_files() != $self->num_staging_files() ) {
    _log(sprintf 'Number of sequence files is different:%s%siRODs: %i, staging: %i',
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
      _log("According to LIMS, file $f is missing in iRODS");
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
  foreach my $f ( keys %{$md5_list_irods} ) {
    my $md5_irods   = $md5_list_irods->{$f};
    my $md5_staging = $md5_list_staging->{$f};
    if ( !$md5_irods || !$md5_list_irods ) {
      _log("One of md5 values for $f is not defined");
      $md5_correct = 0;
    } else {
      if( $md5_irods ne $md5_staging){
        _log("md5 wrong for ${f}: '$md5_irods' not match '$md5_staging'");
        $md5_correct = 0;
      }
    }
  }
  return $md5_correct;
}

sub _irods_md5s {
  my $self = shift;
  my $md5_list = {};
  foreach my $f ( $self->irods_files() ) {
    my $dobj = WTSI::NPG::iRODS::DataObject->new(
      $self->irods, $self->collection() . q{/} . $f);
    $md5_list->{ basename($f) } = $dobj->is_present ? $dobj->checksum : q();
  }
  return $md5_list;
}

sub _staging_md5s {
  my $self = shift;
  my $md5_list = {};
  foreach my $f ( $self->staging_files() ) {
    $md5_list->{basename($f)} =
      npg_common::irods::Loader->get_file_md5($f) || q();
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
      if(!$i_list{$i}){
        _log("Index file for $f does not exist");
        $all_found = 0;
      }
    }
  }

  return $all_found;
}

sub _index_should_exist {
  my ($self, $bam) = @_;

  $bam = File::Spec->catfile ($self->collection(), $bam);
  my $loader = npg_common::irods::Loader->new(file  => 'none',
                                              irods => $self->irods);
  my $meta_list = $loader->_check_meta_data($bam);
  for my $meta (qw/alignment total_reads/) {
    my $value = $meta_list->{$meta};
    $value                     or croak "'$meta' metadata is missing for $bam";
    (ref $value eq 'HASH')     or croak "Unexpected metadata $meta structure for $bam";
    scalar keys %{$value} == 1 or croak 'One key is expected';
    my ($k, $v) = each %{$value};
    if (!defined $k || $k eq q[]) { # Yes, the value is in the key...
      croak "Value should be defined for $meta";
    }
    if ($k == 0) { return 0; }
  }

  return 1;
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
      $self->_file_name({position  => $position,
                         split     => $_})
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
        $self->_file_name({position  => $position,
                           tag_index => $tag_index,
                           split     => $_})
                                    } @subsets );
    }
  }

  return \@bam_list_per_lane;
}

sub _file_name {
  my ($self, $args_ref) = @_;

  my $split   = $args_ref->{'split'};
  my $tag_index = $args_ref->{'tag_index'};
  my $file_name = join q{_}, $self->id_run(), $args_ref->{'position'};
  if ( defined $tag_index ) {
    $file_name .= q{#} . $tag_index;
  }
  if ( $split ) {
    $file_name .= q{_} . $split;
  }
  return join q[.], $file_name , $self->file_extension;
}

sub _index_file_name {
  my ($self, $f) = @_;
  my $ext = $self->file_extension;
  if ($f !~ /.$ext$/msx) {
    croak "Unexpected extension in $f";
  }
  return join q[.], $f, $self->index_file_extension;
}

sub _log {
  my $m = shift;
  if ($m) {
    warn "$m\n";
  }
  return;
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

=item List::Util

=item st::api::lims

=item npg_common::irods::Loader

=item WTSI::NPG::iRODS::DataObject

=item npg_tracking::illumina::run::short_info

=item npg_tracking::illumina::run::folder

=item npg_tracking::illumina::run::long_info

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Guoying Qi
Marina Gourtovaia E<lt>mg8@sanger.ac.ukE<gt>

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
