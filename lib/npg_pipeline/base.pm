package npg_pipeline::base;

use Moose;
use namespace::autoclean;
use POSIX qw(strftime);
use English qw{-no_match_vars};
use Math::Random::Secure qw{irand};

our $VERSION = '0';

with qw{
        MooseX::Getopt
        WTSI::DNAP::Utilities::Loggable
        npg_pipeline::roles::accessor
        npg_tracking::illumina::run::short_info
        npg_pipeline::roles::business::base
       };

with 'npg_tracking::illumina::run::folder' => {
       -excludes => [qw(pb_cal_path dif_files_path)]
     };

with q{npg_tracking::illumina::run::long_info};
with q{npg_pipeline::roles::business::flag_options};

=head1 NAME

npg_pipeline::base

=head1 SYNOPSIS

=head1 DESCRIPTION

A base class to provide basic functionality to derived objects
within npg_pipeline

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

#####
# This class ties together short_info and path_info,
# so the following _build_run_folder will work
#
sub _build_run_folder {
  my ($self) = @_;
  my @temp = split m{/}xms, $self->runfolder_path();
  my $run_folder = pop @temp;
  return $run_folder;
}

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

=head2 make_log_dir

creates a log_directory in the given directory

  $oMyPackage->make_log_dir( q{/dir/for/base} );

=cut

sub make_log_dir {
  my ( $self, $dir, $owning_group ) = @_;

  my $log_dir = qq{$dir/log};

  $owning_group ||= $ENV{OWNING_GROUP};
  $owning_group ||= $self->general_values_conf()->{group};

  if ( -d $log_dir ) {
    $self->info(qq{$log_dir already exists});
    return $log_dir;
  }

  my $cmd = qq{mkdir -p $log_dir};
  my $output = qx{$cmd};

  $self->debug(qq{Command: $cmd});
  $self->debug(qq{Output:  $output});

  if ( $CHILD_ERROR ) {
    $self->logcroak(qq{unable to create $log_dir:$output});
  }

  if ($owning_group) {
    $self->info(qq{chgrp $owning_group $log_dir});

    my $rc = qx{chgrp $owning_group $log_dir};
    if ( $CHILD_ERROR ) {
      $self->warn("could not chgrp $log_dir\n\t$rc");
    }
  }
  my $rc = qx{chmod u=rwx,g=srxw,o=rx $log_dir};
  if ( $CHILD_ERROR ) {
    $self->warn("could not chmod $log_dir\n\t$rc");
  }

  return $log_dir;
}

=head2 status_files_path

 A directory to save status files to.

=cut
sub status_files_path {
  my $self = shift;
  my $apath = $self->analysis_path;
  if (!$apath) {
    $self->logcroak('Failed to retrieve analysis_path');
  }
  return join q[/], $apath, 'status';
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

=item English

=item Math::Random::Secure

=item WTSI::DNAP::Utilities::Loggable

=item npg_tracking::illumina::run::short_info

=item npg_tracking::illumina::run::long_info

=item npg_tracking::illumina::run::folder

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
