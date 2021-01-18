package npg_pipeline::validation::irods;

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;
use File::Basename;
use Perl6::Slurp;
use Try::Tiny;
use English qw/-no_match_vars/;
use Readonly;

use WTSI::NPG::iRODS::Collection;

with 'npg_pipeline::product::release::irods' => {-excludes => 'qc_schema'};
with 'npg_pipeline::validation::common';

our $VERSION = '0';

Readonly::Scalar my $SHIFT_EIGHT => 8;

=head1 NAME

npg_pipeline::validation::irods

=head1 SYNOPSIS

=head1 DESCRIPTION

Validation of files present in iRODS in a given collection
against files present on staging. Only files that belong
to products that should have been arcived to iRODS are
considered. Validaion is successful if all relevant files
from the staging_files attribute are present in IRODS and
have correct checksums.

Presence of unexpected sequence and index files in iRODS
causes validation to fail.

Files that were arcived with the alt_process option are
excluded from validation; their presence does not cause
the validation to fail.

=head1 SUBROUTINES/METHODS

=head2 file_extension

File extension for the sequence file format, required.

=cut

has '+file_extension' => (required => 1,);

=head2 product_entities

=head2 eligible_product_entities

=head2 index_file_extension

=head2 irods_destination_collection

Full iRODS collection path, required.

=cut

has '+irods_destination_collection' => (
  isa        => 'Str',
  is         => 'ro',
  required   => 1,
  lazy_build => 0,
);

=head2 irods

iRODS connection handle, WTSI::NPG::iRODS type object,
required.

=cut

has 'irods' => (
  isa        => 'WTSI::NPG::iRODS',
  is         => 'ro',
  required   => 0,
  lazy_build => 1
);

=head2 staging_files

Per product entity lists of staging files' paths (both bam
or cram files and corresponding index files). Target product's
rpt list is used as a key.

=cut

has 'staging_files' => (
  isa      => 'HashRef',
  is       => 'ro',
  required => 1,
);

=head2 build_eligible_product_entities

Builder method for the eligible_product_entities attribute.

=cut

sub build_eligible_product_entities {
  my $self = shift;
  @{$self->product_entities}
    or $self->logcroak('product_entities array cannot be empty');
  my @p =
    grep { $self->is_for_irods_release($_->target_product) }
    @{$self->product_entities};
  return \@p;
}

=head2 archived_for_deletion

Returns true if the sequence files in the staging folder are
correctly archived to iRODS. If any problems are encounted,
returns false.

=cut

sub archived_for_deletion {
  my $self = shift;

  my $archived = 1;

  if (@{$self->eligible_product_entities}) {
    $archived = $self->_check_files_exist() &&
                $self->_check_checksums();
  } else {
    $self->logwarn(
      'No entity is eligible for archival to iRODS');
    if (scalar keys %{$self->_collection_files} != 0) {
      $self->logwarn(
        'Found product files in iRODS where there should be none');
      $archived = 0;
    }
  }

  return $archived;
}

#####
# Staging files belonging to product entities in
# eligible_product_entities .
#
has '_eligible_staging_files' => (
  isa        => 'HashRef',
  is         => 'ro',
  lazy_build => 1,
);
sub _build__eligible_staging_files {
  my $self = shift;
  (keys %{$self->staging_files}) or
    $self->logcroak('staging_files hash cannot be empty');
  my $h;
  foreach my $e (@{$self->eligible_product_entities}) {
    my $rpt_list = $e->target_product()->composition->freeze2rpt();
    foreach my $path (@{$self->staging_files->{$rpt_list}}) {
      my $name = basename $path;
      if (exists $h->{$name}) {
        $self->logcroak("File name in $path is not unique");
      }
      $h->{$name} = $path;
    }
  }
  return $h;
}

#####
# All sequence and index files in the collection,
# regardless of their location, apart from files
# archived with alt_process metadata attribute set.
#
# Error if the file names in the collection are not
# unique.
#
has '_collection_files'  => (isa        => 'HashRef',
                             is         => 'ro',
                             required   => 0,
                             lazy_build => 1,
                            );
sub _build__collection_files {
  my $self = shift;

  my $coll = WTSI::NPG::iRODS::Collection->new(
               $self->irods,
               $self->irods_destination_collection
             );
  $coll->is_present or $self->logcroak(
    'iRODS collection ' . $self->irods_destination_collection . ' does not exist');

  my $recursive = 1;
  my ($objs, $colls) = $coll->get_contents($recursive);

  my $files = {};
  foreach my $obj (@{$objs}) {

    my $path = $obj->str;
    my ($filename_root, $directories, $suffix) =
      fileparse($path, $self->file_extension, $self->index_file_extension);
    ($suffix && $self->_belongs2main_process($obj)) || next;

    my $filename = $filename_root . $suffix;
    if (exists $files->{$filename}) {
      $self->logcroak("File name in $path is not unique");
    }
    $files->{$filename} = {checksum => $obj->checksum(), path => $path};
  }

  if (! (keys %{$files})) {
    $self->logwarn('Empty list of iRODS files');
  }

  return $files;
}

sub _check_files_exist {
  my $self = shift;

  my $exist = 1;
  foreach my $name (keys %{$self->_eligible_staging_files}) {
    if (!$self->_collection_files->{$name}) {
      my $missing = 1;
      my $iext = $self->index_file_extension;
      if ($name =~ /[.]$iext\Z/xms) {
        $missing = $self->_sequence_file_has_reads($self->_eligible_staging_files->{$name});
      }
      if ($missing) {
        $self->logwarn($self->_eligible_staging_files->{$name} . ' is not in iRODS');
        $exist = 0;
      }
    }
  }

  foreach my $name (keys %{$self->_collection_files}) {
    if (!$self->_eligible_staging_files->{$name}) {
      $self->logwarn($self->_collection_files->{$name}->{'path'} .
                     ' is in iRODS, but not on staging');
      $exist = 0;
    }
  }

  return $exist;
}

sub _check_checksums {
  my $self = shift;

  my $match = 1;
  my $ext = $self->file_extension;

  foreach my $name (keys %{$self->_collection_files}) {
    my $imd5 = $self->_collection_files->{$name}->{'checksum'} || q();
    if (!$imd5) {
      $self->logwarn(
        'Checksum is absent for ' . $self->_collection_files->{$name}->{'path'});
      $match = 0;
    }

    my $file = $self->_eligible_staging_files->{$name} . '.md5';
    if (!-e $file) {
      if ($name =~ /$ext\Z/xms) { # Not all index files have an md5 file
        $self->logwarn($file . ' is absent');
        $match = 0;
      }
    } else {
      my $smd5 = slurp($file, { chomp => 1 });
      if (!$smd5) {
        $self->logwarn('Checksum value is absent in ' . $file);
        $match = 0;
      } else {
        if ($imd5 ne $smd5) {
          $self->logwarn('Checksums do not match for ' . join q[ and ],
                         $self->_eligible_staging_files->{$name},
                         $self->_collection_files->{$name}->{'path'});
          $match = 0;
	}
      }
    }
  }

  return $match;
}

sub _belongs2main_process {
  my ($self, $obj) = @_;
  my @mdata = grep { $_->{'value'} }
              grep { $_->{'attribute'} eq 'alt_process' }
              @{$obj->get_metadata()};
  return !@mdata;
}

sub _sequence_file_has_reads {
  my ($self, $ipath) = @_;

  my $command = 'samtools view ' . $self->index_path2seq_path($ipath);
  my $s;
  my $err;
  my $fh;
  try {
    ##no critic (InputOutput::RequireBriefOpen)
    open $fh, q[-|], $command or $self->logcroak(
      "Failed to open a file handle for reading from command '$command'");
    $s = readline $fh;
    if(defined $s) {
      $s .= readline $fh;
    };
  } catch {
    $err = $_;
  } finally {
    if (defined $fh) {
      close $fh or $self->warn("Fail to close file handle for command '$command'");
      if ($CHILD_ERROR >> $SHIFT_EIGHT) {
        $err = "Error executing command '$command'";
      }
    }
  };

  #####
  # Whatever was the reason we could not run the command, we will return true
  # since we cannot confidently return false.
  #
  if ($err) {
    $self->error($err);
    return 1;
  }

  return (defined $s && length $s);
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

=item File::Basename

=item Perl6::Slurp

=item Try::Tiny

=item English

=item Readonly

=item WTSI::NPG::iRODS::Collection

=item npg_pipeline::product::release::irods

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Steven Leonard
Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2019 GRL

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
