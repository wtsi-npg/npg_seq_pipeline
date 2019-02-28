package npg_pipeline::base::config;

use Moose::Role;
use Carp;
use Config::Any;
use FindBin qw($Bin);
use File::Spec::Functions qw(catfile);
use Readonly;
use MooseX::Getopt::Meta::Attribute::Trait::NoGetopt;

use npg_tracking::util::abs_path qw/abs_path/;

our $VERSION = '0';

Readonly::Scalar my $CONF_DIR => q{data/config_files};

=head1 NAME

npg_pipeline::base::config

=head1 SYNOPSIS

=head1 DESCRIPTION

A Moose role providing accessors for pipeline's configuration files.

=head1 SUBROUTINES/METHODS

=head2 local_bin

Absolute path to directory containing the currently running script.

=cut

has q{local_bin} => (
  isa         => q{Str},
  is          => q{ro},
  lazy_build  => 1,
  traits      => ['NoGetopt'],
);
sub _build_local_bin {
  return abs_path($Bin);
}

=head2 conf_path

Absolute path of the directory with the pipeline's configuration files.
Defaults to data/config_files relative to the bin directory.

=cut

has q{conf_path} => (
  isa           => q{Str},
  is            => q{ro},
  lazy_build    => 1,
  documentation => q{A full path to directory containing config files},
);
sub _build_conf_path {
  my $self = shift;
  return abs_path($self->local_bin . "/../$CONF_DIR");
}

=head2 conf_file_path

Given the pipeline configuration file name, returns an absolute path
to this file. Raises an error if the file does not exist.

=cut

sub conf_file_path {
  my ( $self, $conf_name ) = @_;
  my $path = catfile($self->conf_path(), $conf_name);
  $path ||= q{};
  if (!$path || !-f $path) {
    croak "File $path does not exist or is not readable";
  }
  return $path;
}

=head2 read_config

Given a path of the configuration file, reads and parses the file
(Config::Any is used) and returns the content of the file as a hash.

=cut

sub read_config {
  my ( $self, $path ) = @_;
  my $config = Config::Any->load_files({files => [$path], use_ext => 1, });
  if ( scalar @{ $config } ) {
    $config = $config->[0]->{ $path };
  }
  return $config;
}

=head2 general_values_conf

Returns a hashref of configuration details from the relevant configuration file

=cut

has 'general_values_conf' => (
  isa        => q{HashRef},
  is         => q{ro},
  lazy_build => 1,
  traits     => ['NoGetopt'],
  init_arg   => undef,
);
sub _build_general_values_conf {
  my $self = shift;
  return $self->read_config( $self->conf_file_path(q{general_values.ini}) );
}

no Moose::Role;

1;
__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose::Role

=item MooseX::Getopt::Meta::Attribute::Trait::NoGetopt

=item Carp

=item Config::Any

=item File::Spec::Functions 

=item FindBin

=item Readonly

=item npg_tracking::util::abs_path

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

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
