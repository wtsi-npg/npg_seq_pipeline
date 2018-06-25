package npg_pipeline::base;

use Moose;
use namespace::autoclean;
use POSIX qw(strftime);
use Math::Random::Secure qw{irand};

our $VERSION = '0';

extends 'npg_tracking::illumina::runfolder';

with qw{
        MooseX::Getopt
        WTSI::DNAP::Utilities::Loggable
        npg_pipeline::roles::accessor
        npg_pipeline::roles::business::base
        npg_pipeline::roles::business::flag_options
       };

=head1 NAME

npg_pipeline::base

=head1 SYNOPSIS

=head1 DESCRIPTION

A parent class to provide basic functionality to derived objects
within npg_pipeline package

=head1 SUBROUTINES/METHODS

=head2 conf_path

An attribute inherited from npg_pipeline::roles::accesor,
a full path to directory containing config files.

=head2 conf_file_path

Method inherited from npg_pipeline::roles::accessor.

=head2 read_config

Method inherited from npg_pipeline::roles::accessor.

=cut

has [qw/ +npg_tracking_schema
         +slot
         +flowcell_id
         +instrument_string
         +reports_path
         +subpath
         +name
         +tracking_run /] => (metaclass => 'NoGetopt',);

has q{+id_run} => (required => 0,);

=head2 timestamp

A timestring YYYY-MM-DD HH:MM:SS, an attribute with a default
value of current local time.

  my $sTimeStamp = $class->timestamp();

=cut

has q{timestamp} => (
  isa        => q{Str},
  is         => q{ro},
  default    => sub {return strftime '%Y%m%d-%H%M%S', localtime time;},
  metaclass  => 'NoGetopt',
);

=head2 random_string

A method returning a random string, a timestamp attribute concatenated
with a random 32-bit integer between 0 and 2^32.

  my $rs = $class->random_string();

=cut

sub random_string {
  my $self = shift;
  return join q[-], $self->timestamp(), irand();
}

=head2 lanes

Option to push through an arrayref of lanes to work with

=head2 all_lanes

An array of the elements in $class->lanes();

=head2 no_lanes

True if no lanes have been specified

=head2 count_lanes

Returns the number of lanes in $class->lanes()

=cut

has q{lanes} => (
  traits        => ['Array'],
  isa           => q{ArrayRef[Int]},
  is            => q{ro},
  predicate     => q{has_lanes},
  documentation => q{Option to push through selected lanes of a run},
  default       => sub { [] },
  handles       => {
    all_lanes   => q{elements},
    no_lanes    => q{is_empty},
    count_lanes => q{count},
  },
);

=head2 pipeline_name

=cut
sub pipeline_name {
  my $self = shift;
  my $name = ref $self;
  ($name) = $name =~ /(\w+)$/smx;
  $name = lc $name;
  return $name;
}

=head2 general_values_conf

Returns a hashref of configuration details from the relevant configuration file

=cut

has 'general_values_conf' => (
  isa        => q{HashRef},
  is         => q{ro},
  lazy_build => 1,
  metaclass  => 'NoGetopt',
  init_arg   => undef,
);
sub _build_general_values_conf {
  my ( $self ) = @_;
  return $self->read_config( $self->conf_file_path(q{general_values.ini}) );
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item namespace::autoclean

=item MooseX::Getopt

=item POSIX

=item Math::Random::Secure

=item WTSI::DNAP::Utilities::Loggable

=item npg_tracking::illumina::runfolder

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
