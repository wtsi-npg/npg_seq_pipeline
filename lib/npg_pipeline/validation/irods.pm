package npg_pipeline::validation::irods;

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;
use File::Basename;
use Perl6::Slurp;
use Try::Tiny;

use WTSI::NPG::iRODS::Collection;

with qw/ npg_pipeline::validation::common
         npg_pipeline::product::release::irods
         WTSI::DNAP::Utilities::Loggable /;

our $VERSION = '0';

=head1 NAME

npg_pipeline::validation::irods

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 file_extension

File extension for the sequence file format, required.

=cut

has '+file_extension' => (required => 1,);

=head2 index_file_extension

=head2 collection

Full iRODS collection path, required.

=cut

has 'collection' => (
  isa      => 'Str',
  is       => 'ro',
  required => 1,
);

has 'irods' => (
  isa        => 'WTSI::NPG::iRODS',
  is         => 'ro',
  required   => 1,
);

=head2 BUILD

=cut

sub BUILD {
  my $self = shift;
  @{$self->product_entities}
    or $self->logcroak('product_entities array cannot be empty');
  return;
}

=head2 archived_for_deletion

Returns true if the sequence files in the staging folder are correctly archived to iRODS.
If any problems are encounted, returns false.

=cut

sub archived_for_deletion {
  my $self = shift;
  my $all_archived = 1;
  if (@{$self->_eligible_product_entities}) {
    $all_archived = $self->_check_num_files()   &&
                    $self->_check_index_files() &&
                    $self->_check_md5();
  } else {
    $self->logwarn(
      'No entity is eligible for archival to iRODS, not checking');
    # Do we need to check that no files have been archived?
  }
  return $all_archived;
}

has '_eligible_product_entities' => (
  isa        => 'ArrayRef',
  is         => 'ro',
  lazy_build => 1,
);
sub _build__eligible_product_entities {
  my $self = shift;
  my @p =
    grep { $self->is_for_irods_release($_->target_product) }
    @{$self->product_entities};
  return \@p;
}

has '_staging_files' => (
  isa        => 'ArrayRef',
  is         => 'ro',
  lazy_build => 1,
);
sub _build__staging_files {
  my $self = shift;
  my @files = map {$_->staging_files($self->file_extension)}
              @{$self->_eligible_product_entities};
  return \@files;
}

has '_collection_files'  => (isa        => 'HashRef',
                             is         => 'ro',
                             required   => 0,
                             lazy_build => 1,
                            );
sub _build__collection_files {
  my $self = shift;

  my $coll = WTSI::NPG::iRODS::Collection->new($self->irods, $self->collection);
  my ($objs, $colls) = $coll->get_contents;
  my %file_list = ();
  foreach my $obj (@{$objs}) {

    my ($filename_root, $directories, $suffix) =
      fileparse($obj->str, $self->file_extension, $self->index_file_extension);
    $suffix || next;
    my $filename = $filename_root . $suffix;
    if (exists $file_list{$filename}) {
      $self->logcroak("File $filename is already cached in collection_files builder");
    }
    $file_list{$filename} = $obj;
  }
  if (scalar keys %file_list == 0) {
    $self->logcroak('No files retrieved from ' . $self->collection);
  }

  return \%file_list;
}

has '_irods_files'  => (isa        => 'ArrayRef',
                        is         => 'ro',
                        traits     => ['Array'],
                        lazy_build => 1,
                       );
sub _build__irods_files {
  my $self = shift;
  my $seq_re = $self->file_extension;
  $seq_re = qr/[.]$seq_re\Z/xms;
  my @seq_list = grep { $_ =~ $seq_re } keys %{$self->_collection_files};
  if (!@seq_list) {
    $self->logcroak('Empty list of iRODS seq files');
  }
  $self->debug(join qq{\n}, q{iRODS seq files list}, @seq_list);
  return [sort @seq_list];
}

has '_irods_index_files'  => (isa        => 'HashRef',
                              is         => 'ro',
                              lazy_build => 1,
                             );
sub _build__irods_index_files {
  my $self = shift;
  my $i_re = $self->index_file_extension;
  $i_re = qr/[.]$i_re\Z/xms;
  my @i_list   = grep { $_ =~ $i_re } keys %{$self->_collection_files};
  $self->debug(join qq{\n}, q{iRODS index files list}, @i_list);
  return {map {$_ => 1} @i_list};
}

sub _check_num_files {
  my $self = shift;
  my $num_irods_files   = scalar @{$self->_irods_files};
  my $num_staging_files = scalar @{$self->_staging_files};
  if ( $num_irods_files != $num_staging_files ) {
    $self->logwarn("Number of files in iRODS $num_irods_files " .
      "is different from number of staging files $num_staging_files");
    return 0;
  }
  return 1;
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
       $self->logcroak("One of md5 values for $f is not defined");
      }
      if( $md5_irods ne $md5_staging ) {
        $self->logcroak("md5 wrong for ${f}: '$md5_irods' not match '$md5_staging'");
      }
    }
  } catch {
    $self->logwarn($_);
    $md5_correct = 0;
  };
  return $md5_correct;
}

sub _irods_md5s {
  my $self = shift;
  my $md5_list = {};
  foreach my $f ( @{$self->_irods_files()} ) {
    $md5_list->{$f} = $self->_collection_files()->{$f}->checksum() || q();
  }
  return $md5_list;
}

sub _staging_md5s {
  my $self = shift;
  my $md5_list = {};
  foreach my $f ( @{$self->_staging_files} ) {
    my $md5f = $f . q{.md5};
    $md5_list->{basename($f)} = slurp $md5f, { chomp => 1 } || q();
  }
  return $md5_list;
}

sub _check_index_files {
  my $self = shift;

  my $all_found = 1;
  foreach my $f ( @{$self->_irods_files()} ) {
    if ($self->_index_should_exist($f)) {
      my $i = $self->_index_file_name($f);
      if(!exists $self->_irods_index_files->{$i}) {
        $self->logwarn("Index file $i for $f does not exist in iRODS");
        $all_found = 0;
      }
    }
  }

  return $all_found;
}

sub _index_should_exist {
  my ($self, $file_name) = @_;

  my $obj = $self->_collection_files()->{$file_name};
  $obj or $self->logcroak("Object not cached for $file_name");
  my @mdata = @{$obj->get_metadata()};
  my @values = ();
  for my $a (qw/alignment total_reads/) {
    my @m = grep { $_->{'attribute'} eq $a } @mdata;
    if (scalar @m != 1) {
      $self->logcroak(qq[No or too many '$a' meta data for ] . $obj->str());
    }
    my $value = $m[0]->{'value'};
    if (!defined $value || $value eq q[]) {
      $self->logcroak(qq[Undefined or empty '$a' value for ] . $obj->str());
    }
    $value or return 0;
  }

  return 1;
}

sub _index_file_name {
  my ($self, $f) = @_;
  my $ext = $self->file_extension;
  if ($f !~ /.$ext$/msx) {
    $self->logcroak("Unexpected extension in $f");
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

=item File::Basename

=item Perl6::Slurp

=item Try::Tiny

=item WTSI::DNAP::Utilities::Loggable

=item npg_pipeline::product::release::irods

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Guoying Qi
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
