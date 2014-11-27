package npg_pipeline::run::folder;

use Moose;
our $VERSION = '0';

extends q{npg_pipeline::base};

has q{folder} => (isa => q{Str}, is => q{ro}, writer => q{_set_folder},
                  documentation => q{should be either incoming,analysis or outgoing},);

sub get_instrument_dir {
  my ( $self, $run_folder ) = @_;

  my @path = split m{/}xms, $run_folder;
  pop @path;
  my $inst_dir = join q{/}, @path;
  return $inst_dir;
}

no Moose;
__PACKAGE__->meta->make_immutable;
1;
__END__

=head1 NAME

npg_pipeline::run::folder

=head1 SYNOPSIS

  my $rfderived = npg_pipeline::run::folder::<DerivedClass->new({
    run_folder => $sRunFolder,
  });

=head1 DESCRIPTION

Base class for run_folder based modules

=head1 SUBROUTINES/METHODS

=over

=item get_instrument_dir

 Given a runfolder path, returns the full directory path where the runfolder is

=back

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2014 Genome Research Ltd

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
