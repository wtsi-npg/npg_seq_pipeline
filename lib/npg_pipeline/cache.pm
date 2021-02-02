package npg_pipeline::cache;

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;
use Readonly;
use Carp;
use POSIX qw(strftime);
use Cwd qw/cwd/;
use File::Copy;
use File::Spec;

use npg_tracking::util::abs_path qw/abs_path/;
use npg_tracking::util::types;
use st::api::lims;
use npg::samplesheet;
use WTSI::DNAP::Warehouse::Schema;

with qw/
         npg_tracking::glossary::run
         npg_tracking::glossary::flowcell
       /;

our $VERSION = '0';

##no citic (RequireLocalizedPunctuationVars)

=head1 NAME

npg_pipeline::cache

=head1 SYNOPSIS

  npg_pipeline::cache->new(id_run         => 78,
                           lims           => [$run_lims->children],
                           set_env_vars   => 1,
                           cache_location => 'my_dir',
                          )->setup;

=head1 SUBROUTINES/METHODS

=head2 id_run
 
Integer run id, required.

=head2 id_flowcell_lims

LIMs specific flowcell id, required.

=head2 mlwh_schema
 
DBIx schema class for ml_warehouse access.

=cut

has 'mlwh_schema' => (
                isa        => 'WTSI::DNAP::Warehouse::Schema',
                is         => 'ro',
                required   => 0,
                lazy_build => 1,);
sub _build_mlwh_schema {
  return WTSI::DNAP::Warehouse::Schema->connect();
}

=head2 lims
 
A reference to an array of child lims objects.

=cut

has 'lims'       => (isa        => 'ArrayRef[st::api::lims]',
                     is         => 'ro',
                     required   => 0,
                     lazy_build => 1,);
sub _build_lims {
  my $self = shift;

  $self->id_flowcell_lims or
    croak 'id_flowcell_lims (batch id) is required';

  my $ref = {
    driver_type      => 'ml_warehouse',
    id_run           => $self->id_run,
    mlwh_schema      => $self->mlwh_schema,
    id_flowcell_lims => $self->id_flowcell_lims
  };

  return [st::api::lims->new($ref)->children];
}

=head2 set_env_vars

A boolean flag indicating whether to set environment variables in global scope.
Defaults to false.

=cut
has 'set_env_vars' => (isa     => 'Bool',
                       is      => 'ro',
                       default => 0,);

=head2 cache_location

An existing directory to create the cache directory in. Defaults to
current directory

=cut
has 'cache_location' => (isa     => 'NpgTrackingDirectory',
                         is      => 'ro',
                         default => sub { cwd; },);

=head2 cache_dir_name

Name of the cache directory, defaults to 'metadata_cache'.

=cut
has 'cache_dir_name' => (isa        => 'Str',
                         is         => 'ro',
                         lazy_build => 1,);
sub _build_cache_dir_name {
  my $self = shift;
  return sprintf 'metadata_cache_%i', $self->id_run;
}

=head2 cache_dir_path

A path to the cache directory.

=cut
has 'cache_dir_path' => (isa        => 'NpgTrackingDirectory',
                         is         => 'ro',
                         lazy_build => 1,);
sub _build_cache_dir_path {
  my $self = shift;
  return File::Spec->catdir($self->cache_location, $self->cache_dir_name);
}

=head2 samplesheet_file_name

Name of the samplesheet file

=cut
has 'samplesheet_file_name' => (isa        => 'Str',
                                is         => 'ro',
                                lazy_build => 1,);
sub _build_samplesheet_file_name {
  my $self = shift;
  return sprintf 'samplesheet_%i.csv', $self->id_run;
}

=head2 samplesheet_file_path

A path of the samplesheet file.

=cut
has 'samplesheet_file_path' => (isa        => 'Str',
                                is         => 'ro',
                                lazy_build => 1,);
sub _build_samplesheet_file_path {
  my $self = shift;
  return File::Spec->catfile($self->cache_dir_path, $self->samplesheet_file_name);
}

=head2 messages

An array of non-error messages, empty by default.

=cut
has 'messages'  => (isa        => 'ArrayRef[Str]',
                    is         => 'ro',
                    default    => sub { [] },);

=head2 setup

Generates cached data. If an existing directory with cached data found,
unless reuse_cache flag is false, will not generate a new cache.
If set_env_vars is true (false by default), will set the relevant env.
variables in the global scope.

=cut
sub setup {
  my $self = shift;

  my $samplesheet_file_var_name = st::api::lims->cached_samplesheet_var_name();
  my $cache_path = abs_path $self->samplesheet_file_path;
  my $given = $ENV{$samplesheet_file_var_name};

  if ($given) {
    $self->_add_message(qq[Samplesheet is given as $given]);
    if (-e $given) {
      $self->_add_message(q[This samplesheet will be used]);
      if (-e $cache_path) {
        my $ts = strftime '%Y%m%d-%H%M%S', localtime time;
        my $moved = join q[_], $cache_path, 'moved', $ts;
        if (rename $cache_path, $moved) {
          $self->_add_message(qq[Renamed existing $cache_path to $moved]);
	} else {
          croak qq[Failed to rename existing $cache_path to $moved];
	}
      }
      if (copy($given, $cache_path)) {
        $self->_add_message(qq[Copied $given to $cache_path]);
      } else {
        croak qq[Failed to copy $given to $cache_path];
      }
    } else {
      croak qq[$samplesheet_file_var_name points to non-existing file $given];
    }
  } else {
    $self->_samplesheet();
  }

  if ($self->set_env_vars) {
    ##no critic (RequireLocalizedPunctuationVars)
    if (-e $self->samplesheet_file_path) {
      $ENV{ $samplesheet_file_var_name } = $cache_path;
      $self->_add_message(qq[$samplesheet_file_var_name is set to $cache_path]);
    } else {
      croak sprintf '%s is not set, samplesheet %s not found',
                     $samplesheet_file_var_name, $self->samplesheet_file_path;
    }
    ##use critic
  }

  return;
}

=head2 env_vars

A list of env. variables names that can be set by this module in global scope.

=cut
sub env_vars {
  return (st::api::lims->cached_samplesheet_var_name());
}

sub _samplesheet {
  my $self = shift;
  if (not -e $self->samplesheet_file_path){

    npg::samplesheet->new(id_run => $self->id_run,
                          lims   => $self->lims,
                          extend => 1,
		          output => $self->samplesheet_file_path,
                         )->process();
    $self->_add_message(q(Samplesheet created at ).$self->samplesheet_file_path);
  }
  return;
}

sub _add_message {
  my ($self, $m) = @_;
  if ($m) {
    push @{$self->messages}, $m;
  }
  return;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 DESCRIPTION

Creates or finds existing cache of lims and other metadata needed to run the pipeline

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Carp

=item Moose

=item MooseX::StrictConstructor

=item namespace::autoclean

=item Readonly

=item Cwd

=item File::Spec

=item File::Copy

=item POSIX

=item npg_tracking::util::abs_path

=item npg_tracking::util::types

=item st::api::lims

=item WTSI::DNAP::Warehouse::Schema

=item npg_tracking::glossary::run

=item npg_tracking::glossary::flowcell

=item npg::samplesheet

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2014,2015,2016,2017,2018,2021 Genome Research Ltd.

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
