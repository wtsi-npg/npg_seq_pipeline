package npg_pipeline::function::fastqcheck_archiver;

use Moose;
use namespace::autoclean;
use Readonly;

use npg_pipeline::function::definition;

extends qw{npg_pipeline::base};

our $VERSION = '0';

Readonly::Scalar my $SCRIPT_NAME => q{npg_qc_save_files.pl};

sub create {
  my $self = shift;

  my $job_name = join q{_}, q{fastqcheck_loader}, $self->id_run(), $self->timestamp();

  #####
  # The loader disregards directories with no fastqcheck files, so
  # it's safe to give it archival directory where lane-level files
  # are found in case of the old runfolder structure.
  # It skips plex-level files, therefore no need to give plex-level
  # directories to the script.
  #
  my $apath = $self->archive_path();
  my @fqcheck_paths = map { $_->path($apath) } @{$self->products->{'lanes'}};
  push @fqcheck_paths, $apath;

  my $command = join q[ ], $SCRIPT_NAME, map {"--path=$_"} @fqcheck_paths;

  my $d = npg_pipeline::function::definition->new(
    created_by    => __PACKAGE__,
    created_on    => $self->timestamp(),
    identifier    => $self->id_run(),
    job_name      => $job_name,
    command       => $command,
    fs_slots_num  => 1,
    queue         =>
      $npg_pipeline::function::definition::LOWLOAD_QUEUE,
  );

  return [$d];
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

npg_pipeline::function::fastqcheck_archiver

=head1 SYNOPSIS

  my $aaq = npg_pipeline::function::fastqcheck_archiver->new(
    run_folder => 'run_folder'
  );

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 create

Creates and returns a single function definition as an array.
Function definition is created as a npg_pipeline::function::definition
type object.

  my $definitions = $aaq->create();

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item namespace::autoclean

=item Readonly

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown
Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2018 Genome Research Ltd

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
