package npg_pipeline::validation::common;

use Moose::Role;
use File::Basename;
use WTSI::NPG::iRODS::Collection;

with 'WTSI::DNAP::Utilities::Loggable';

our $VERSION = '0';

=head1 NAME

npg_pipeline::validation::common

=head2 logger

=head2 collection

Full iRODS collection path, required.

=cut

has 'collection' => (isa       => 'Str',
                     is        => 'ro',
                     required  => 1,
                    );

=head2 irods

Handle for interaction with iRODS, required.

=cut

has 'irods' => (isa       => 'WTSI::NPG::iRODS',
                is        => 'ro',
                required  => 1,
               );

=head2 file_extension

File extension for the sequence file format, required.

=cut

has 'file_extension' => (isa      => 'Str',
                         is       => 'ro',
                         required => 1,
                        );

=head2 index_file_extension

File extension for the sequence file index, cannot be set, inferred.

=cut

has 'index_file_extension' => (isa        => 'Str',
                               is         => 'ro',
                               init_arg   => undef,
                               lazy_build => 1,
                              );
sub _build_index_file_extension {
  my $self = shift;
  my $e = $self->file_extension;
  $e =~ s/m\Z/i/xms;
  return $e;
}

=head2 collection_files

A hash of all files in the iRODS collection, files names are the keys,
values are WTSI::NPG::iRODS::DataObject type object corresponding
to these files.

=cut

has 'collection_files'  => (isa        => 'HashRef',
                            is         => 'ro',
                            required   => 0,
                            lazy_build => 1,
                           );
sub _build_collection_files {
  my $self = shift;

  my $coll = WTSI::NPG::iRODS::Collection->new($self->irods, $self->collection);
  my ($objs, $colls) = $coll->get_contents;
  my %file_list = ();
  foreach my $obj (@{$objs}) {
    my ($filename, $directories, $suffix) = fileparse($obj->str);
    if (exists $file_list{$filename}) {
      $self->logger->logcroak("File $filename is already cached in collection_files builder");
    }
    $file_list{$filename} = $obj;
  }
  if (scalar keys %file_list == 0) {
    $self->logger->logcroak('No files retrieved from ' . $self->collection);
  }

  return \%file_list
}

=head2 irods_files

A hash of all files in the iRODS collection, files names are the keys,
values are WTSI::NPG::iRODS::DataObject type object corresponding
to these files.

=cut

has '_irods_files'  => (isa        => 'ArrayRef',
                        is         => 'ro',
                        traits     => ['Array'],
                        lazy_build => 1,
                        handles    => {
                          irods_files     => 'elements',
                          num_irods_files => 'count',
                        },
                       );
sub _build__irods_files {
  my $self = shift;
  my $seq_re = $self->file_extension;
  $seq_re = qr/[.]$seq_re\Z/xms;
  my @seq_list = grep { $_ =~ $seq_re } keys %{$self->collection_files};
  if (!@seq_list) {
    $self->logger->logcroak('Empty list of iRODS seq files');
  }
  $self->logger->info(join qq{\n}, q{iRODS seq files list}, @seq_list);
  return [sort @seq_list];
}

=head2 get_metadata

Returns a hash of metadata values for an WTSI::NPG::iRODS::DataObject object
for metadata attributes given in a second attribute

 my $meta = $obj->get_metadata(irodsObj, qw/study_id sample_name/);
 print 'Sample name is ' . $meta->{'sample_name'} || q[];
 print 'Study id is ' . $meta->{'study_id'} || q[];

=cut

sub get_metadata {
  my ($self, $obj, @attr_names) = @_;

  my @mdata = @{$obj->get_metadata()};
  my $data = {};
  for my $a (@attr_names) {
    my @m = grep { $_->{'attribute'} eq $a } @mdata;
    if (scalar @m != 1) {
      $self->logger->logcroak(qq[No or too many '$a' meta data for ] . $obj->str());
    }
    my $value = $m[0]->{'value'};
    if (!defined $value || $value eq q[]) {
      $self->logger->logcroak(qq[Undefined or empty '$a' value for ] . $obj->str());
    }
    $data->{$a} = $value;
  }

  return $data;
}

no Moose::Role;

1;
__END__

=head1 NAME

npg_pipeline::validation::sequence_files

=head1 SYNOPSIS

=head1 DESCRIPTION

Moose role. Common functionality for helper modules of
run_is_deletable script.

=head1 SUBROUTINES/METHODS

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose::Role

=item File::Basename

=item WTSI::NPG::iRODS::Collection

=item WTSI::DNAP::Utilities::Loggable

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

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

