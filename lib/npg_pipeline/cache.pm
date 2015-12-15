package npg_pipeline::cache;

use Moose;
use MooseX::StrictConstructor;
use Readonly;
use Carp;
use English qw{-no_match_vars};
use POSIX qw(strftime);
use Cwd qw/cwd/;
use File::Spec;
use File::Copy;
use File::Find;
use File::Path qw/make_path/;

use npg_tracking::util::abs_path qw/abs_path/;
use npg_tracking::util::types;
use npg::api::request;
use npg::api::run;
use npg::api::run_status_dict;
use st::api::lims;
use st::api::lims::warehouse;
use st::api::lims::ml_warehouse;
use npg_warehouse::Schema;
use WTSI::DNAP::Warehouse::Schema;
use npg::samplesheet;

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
                           reuse_cache    => 1,
                           cache_location => 'my_dir',
                          )->setup;

  npg_pipeline::cache->new(id_run           => 78,
                           id_flowcell_lims => '5260271901788',
                           lims_driver_type => 'warehouse',
                           set_env_vars     => 1,
                           reuse_cache      => 1,
                           cache_location   => 'my_dir',
                          )->setup;

  npg_pipeline::cache->new(id_run           => 78,
                           flowcell_barcode => 'HBF2DADXX',
                           set_env_vars     => 1,
                           reuse_cache      => 0,
                           cache_location   => 'my_dir',
                          )->setup;

=head1 SUBROUTINES/METHODS

=head2 id_run
 
Integer run id, required.

=head2 flowcell_barcode

Manufacturer flowcell barcode/id

=head2 id_flowcell_lims

LIMs specific flowcell id.

=head2 lims_driver_type
 
Driver type to be used to build lims accessor,
defaults to xml.

=cut

has 'lims_driver_type'  => (isa        => 'Str',
                            is         => 'ro',
                            required   => 0,
                            default    => sub { mlwarehouse_driver_name() },);

=head2 wh_schema
 
DBIx schema class for old warehouse access.

=cut

has 'wh_schema'  => (isa        => 'npg_warehouse::Schema',
                     is         => 'ro',
                     required   => 0,
                     lazy_build => 1,);
sub _build_wh_schema {
  my $self = shift;
  return npg_warehouse::Schema->connect();
}

=head2 mlwh_schema
 
DBIx schema class for ml_warehouse access.

=cut

has q{mlwh_schema} => (
                isa        => q{WTSI::DNAP::Warehouse::Schema},
                is         => q{ro},
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

  my $clims;
  my $driver_type = $self->lims_driver_type;

  if ($driver_type eq $self->mlwarehouse_driver_name) {

    if (!($self->id_flowcell_lims || $self->flowcell_barcode)) {
      croak 'Neither flowcell barcode nor lims flowcell id is known';
    }

    my $driver = st::api::lims::ml_warehouse->new(
         mlwh_schema      => $self->mlwh_schema,
         id_flowcell_lims => $self->id_flowcell_lims,
         flowcell_barcode => $self->flowcell_barcode );

    my $lims = st::api::lims->new(
        id_flowcell_lims => $self->id_flowcell_lims,
        flowcell_barcode => $self->flowcell_barcode,
        driver           => $driver,
        driver_type      => $driver_type );
    $clims = [$lims->children];

  } elsif ($driver_type eq $self->warehouse_driver_name) {

    if (!$self->id_flowcell_lims) {
      croak "lims_id accessor should be defined for $driver_type driver";
    }

    my $position = 1; # MiSeq runs only
    my $driver =  st::api::lims::warehouse->new(
        npg_warehouse_schema => $self->wh_schema,
        position             => $position,
        tube_ean13_barcode   => $self->id_flowcell_lims );

    my $lims = st::api::lims->new(
        position    => $position,
        driver      => $driver,
        driver_type => $driver_type );
    $clims = [$lims];

  } elsif ($driver_type eq $self->xml_driver_name) {

    my $ref = {
      driver_type => $driver_type,
      id_run      => $self->id_run,
    };
    if ($self->id_flowcell_lims) {
      $ref->{'batch_id'} = $self->id_flowcell_lims;
    }
    $clims = [st::api::lims->new($ref)->children];

  } else {
    croak "Unknown driver type $driver_type";
  }

  return $clims;
}

=head2 reuse_cache

A boolean flag indicating whether to reuse the existing cache if found.
Defaults to true. If set to false, the existing cache directory
will be renamed.

=cut
has 'reuse_cache' => (isa     => 'Bool',
                      is      => 'ro',
                      writer  => '_set_reuse_cache',
                      default => 1,);

=head2 reuse_cache_only

A boolean flag indicating whether to only reuse the existing cache without
ever creating a new one. Defaults to false.

=cut
has 'reuse_cache_only' => (isa     => 'Bool',
                           is      => 'ro',
                           default => 0,);

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
has 'cache_dir_path' => (isa        => 'Str',
                         is         => 'ro',
                         lazy_build => 1,);
sub _build_cache_dir_path {
  my $self = shift;
  return File::Spec->catdir ($self->cache_location, $self->cache_dir_name);
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

=head2 warehouse_driver_name

=head2 mlwarehouse_driver_name

=cut
sub warehouse_driver_name {
  return 'warehouse';
}
sub mlwarehouse_driver_name {
  return 'ml_warehouse';
}

=head2 setup

Generates cached data. If an existing directory with cached data found,
unless reuse_cache flag is false, will not generate a new cache.
If NPG_WEBSERVICE_CACHE_DIR env. variable is set, will not generate
a new cache, no checks of the cache pointed to by this variable
will be made. If set_env_vars is true (false by default), will set
the relevant env. variables in the global scope.

=cut
sub setup {
  my $self = shift;

  my $cache_dir_var_name = npg::api::request->cache_dir_var_name();
  my $samplesheet_file_var_name = st::api::lims->cached_samplesheet_var_name();

  my $copy_cache = 0;
  if ( $ENV{ $cache_dir_var_name } || $ENV{ $samplesheet_file_var_name } ) {
    $copy_cache = 1;
    $self->_set_reuse_cache(0);
  }

  my $cache_exists = (-l $self->cache_dir_path) || (-d $self->cache_dir_path);
  if ($cache_exists) {
    $self->_add_message(q[Found existing cache directory ] . $self->cache_dir_path);
    if ($self->reuse_cache) {
      $self->_add_message(q[Will use existing cache directory]);
    } else {
      my $renamed = $self->_deprecate();
      $self->_add_message(qq[Renamed existing cache directory to $renamed]);
      $cache_exists = 0;
    }
  }

  if (!$cache_exists) {
    if ($self->reuse_cache_only) {
      croak 'Failed to find existing cache directory';
    } else {
      $self->_add_message(q[Will create a new cache directory ] . $self->cache_dir_path);
      $self->create($copy_cache);
    }
  }

  if ($self->set_env_vars) {
    ##no critic (RequireLocalizedPunctuationVars)
    my $cache_path = abs_path $self->cache_dir_path;
    $ENV{ $cache_dir_var_name } = $cache_path;
    $self->_add_message(qq[$cache_dir_var_name is set to $cache_path]);

    if (-e $self->samplesheet_file_path) {
      $cache_path = abs_path $self->samplesheet_file_path;
      $ENV{ $samplesheet_file_var_name } = $cache_path;
      $self->_add_message(qq[$samplesheet_file_var_name is set to $cache_path]);
    } else {
      if ($self->reuse_cache_only) {
        croak 'Failed to find existing samplesheet';
      }
      $self->_add_message(sprintf '%s is not set, samplesheet %s not found]',
                                  $samplesheet_file_var_name, $self->samplesheet_file_path);
    }
    ##use critic
  }

  return;
}

=head2 create

Creates cache directory and generates cached metadata. reuse_cache and set_env_vars
flags are irrelevant for this method.

=cut
sub create {
  my ($self, $copy) = @_;
  if (-e $self->cache_dir_path) {
    croak sprintf '%s already exists, cannot create a new cache directory',
                  $self->cache_dir_path;
  }
  my $dir_created = mkdir $self->cache_dir_path;
  if (!$dir_created) {
    croak sprintf 'Failed to create cache directory %s; error %s',
    $self->cache_dir_path, $ERRNO;
  }

  my $cache_dir_var_name = npg::api::request->cache_dir_var_name();
  if ($copy) {
    if (!$ENV{ $cache_dir_var_name }) {
      local $ENV{ $cache_dir_var_name } = $self->cache_dir_path;
      $self->_xml_feeds();
    }
    $self->_copy_cache();
    local $ENV{ $cache_dir_var_name } = $self->cache_dir_path;
    $self->_samplesheet();
  } else {
    local $ENV{ $cache_dir_var_name } = $self->cache_dir_path;
    $self->_xml_feeds();
    $self->_samplesheet();
  }

  my $st = File::Spec->catdir ($self->cache_dir_path, 'st');
  if (-d $st) {
    my $new_st = File::Spec->catdir ($self->cache_dir_path, 'st_original');
    my $moved = move $st, $new_st;
    if (!$moved) {
      croak sprintf 'Failed to move out of the way st directory (%s to %s), error number %s',
      $st, $new_st, $ERRNO;
    }
  }

  return;
}

=head2 env_vars

A list of env. variables names that can be set by this module in global scope.

=cut
sub env_vars {
  return (npg::api::request->cache_dir_var_name(), st::api::lims->cached_samplesheet_var_name());
}

sub _samplesheet {
  my ($self) = @_;
  if(not -e $self->samplesheet_file_path){

    npg::samplesheet->new(id_run => $self->id_run,
                          lims   => $self->lims,
                          extend => 1,
		          output => $self->samplesheet_file_path,
                         )->process();
    $self->_add_message(q(Samplesheet created at ).$self->samplesheet_file_path);
  }
  return;
}

sub _deprecate {
  my $self = shift;
  if (!-e $self->cache_dir_path) {
    croak sprintf '%s does not exist, nothing to move', $self->cache_dir_path;
  }
  my $ts = strftime '%Y%m%d-%H%M%S', localtime time;
  my $new_dir = join q[_], $self->cache_dir_path, 'moved', $ts;
  my $moved = move $self->cache_dir_path, $new_dir;
  if (!$moved) {
    croak sprintf 'Failed to rename existing cache %s to %s, error number %s',
                   $self->cache_dir_path, $new_dir, $ERRNO;
  }
  return $new_dir;
}

sub _xml_feeds {
  my $self = shift;

  local $ENV{npg::api::request->save2cache_dir_var_name()} = 1;

  my $run = npg::api::run->new({id_run => $self->id_run});
  $run->is_paired_run();
  $run->current_run_status();
  $run->instrument()->model();
  npg::api::run_status_dict->new()->run_status_dicts();

  return;
}

sub _copy_cache {
  my $self = shift;

  my $destination = $self->cache_dir_path;
  my $var_name = st::api::lims->cached_samplesheet_var_name();
  my $sh = $ENV{$var_name};
  if ($sh) {
    if (!-e $sh) {
      croak qq[Samplesheet $sh does not exist];
    }
    copy $sh, File::Spec->catfile($destination, $self->samplesheet_file_name);
    $ENV{$var_name} = q[]; ## no critic (Variables::RequireLocalizedPunctuationVars)
    $self->_add_message("$sh copied to $destination, $var_name unset");
  }

  $var_name = npg::api::request->cache_dir_var_name();
  my $cache_dir = $ENV{$var_name};
  if ($cache_dir) {
    $cache_dir =~ s{/\Z}{}smx;
    if (!-e $cache_dir) {
      croak qq[Cache directory $cache_dir does not exist];
    }
    my $new_cache_dir = $self->cache_dir_path;
    my $copy_file_sub = sub {
      my $file = $File::Find::name;
      if ( -f $file && $file =~ /[.]xml\Z/xms ) {
        my ($volume,$directories,$file_name) = File::Spec->splitpath($file);
        $directories=~s/\Q$cache_dir\E//smx;
        my $subdir = $new_cache_dir . $directories;
        make_path $subdir;
        copy $file, $subdir;
      }
      return;
    };
    find({'wanted'   => $copy_file_sub,
          'follow'   => 0,
          'no_chdir' => 1}, ($cache_dir));
    $self->_add_message("$cache_dir copied to $destination, $var_name unset");
    $ENV{$var_name} = q[]; ## no critic (Variables::RequireLocalizedPunctuationVars)
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

no Moose;

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

=item Readonly

=item Cwd

=item File::Spec

=item File::Copy

=item File::Find

=item File::Path

=item English

=item POSIX

=item npg_tracking::util::types

=item npg::api::request

=item npg::api::run

=item npg::api::run_status_dict

=item st::api::lims

=item st::api::lims::warehouse

=item st::api::lims::ml_warehouse

=item npg_warehouse::Schema

=item WTSI::DNAP::Warehouse::Schema

=item npg_tracking::glossary::run

=item npg::samplesheet

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2015 Genome Research Ltd

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
