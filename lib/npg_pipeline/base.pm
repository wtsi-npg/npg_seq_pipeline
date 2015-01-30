package npg_pipeline::base;

use Moose;
use Moose::Meta::Class;
use Carp;
use Config::Any;
use English qw{-no_match_vars};
use POSIX qw(strftime);
use Sys::Filesystem::MountPoint qw(path_to_mount_point);
use File::Spec::Functions qw(splitdir);
use File::Slurp;
use FindBin qw($Bin);
use Readonly;

our $VERSION = '0';

with qw{MooseX::Getopt
        MooseX::AttributeCloner
        npg_common::roles::log
        npg_tracking::illumina::run::short_info
        npg_tracking::illumina::run::folder
        npg_pipeline::roles::business::base
      };
with qw{npg_tracking::illumina::run::long_info};
with q{npg_pipeline::roles::business::flag_options};

Readonly::Scalar our $DEFAULT_JOB_ID_FOR_NO_BSUB => 50;
Readonly::Scalar our $CONF_DIR                   => q{data/config_files};

$ENV{LSB_DEFAULTPROJECT} ||= q{pipeline};

=head1 NAME

npg_pipeline::base

=head1 SYNOPSIS

In the derived class

  use Moose;
  extends qw{npg_pipeline::base};

Create derived class object

  $oDerived = npg_pipeline::derived->new()

=head1 DESCRIPTION

A base class to provide the following methods to any derived objects within npg_pipeline

=head1 SUBROUTINES/METHODS

=cut

has [qw/ +npg_tracking_schema +slot +flowcell_id +instrument_string +reports_path +subpath +name +tracking_run /] => (metaclass => 'NoGetopt',);

=head2 submit_bsub_command - deals with submitting a command to LSF, retrying upto 5 times if the return code is not 0. It will then croak if it still can't submit

  my $LSF_output = $oDerived->submit_bsub_command($cmd);

Note: If the no_bsub flag is set, then this does not submit a job, but instead logs the command, and returns '50' as a job id

=cut

sub submit_bsub_command {
  my ($self, $cmd) = @_;

  if ( $cmd =~ /bsub/xms) {
    my $common_options = q{};
    if ( $self->has_job_priority() ) {
      $common_options = q{-sp } . $self->job_priority();
    }
    my $select = $self->lsf_resource_select();
    if ($select) {
      $common_options .= qq{ -R 'select[$select]'};
    }
    $cmd =~ s/bsub/bsub $common_options/xms;

    # add job_name_prefix into command
    # we assume that the first -J is to do with the bsub command, any extra will be in the main command, and we don't want to lose this
    my ( $bsub_before_job_name, $jobs_name_plus, @any_extra ) = split /-J/xms, $cmd;
    my $job_name_prefix = $self->job_name_prefix();

    my ( $whitespace ) = $jobs_name_plus =~ /\A(\s+)/xms;
    $jobs_name_plus =~ s/\A\s+//xms;
    $whitespace ||= q{};
    my ( $quote ) = $jobs_name_plus =~ /\A(')/xms;
    $jobs_name_plus =~ s/\A'//xms;
    $quote ||= q{};
    $jobs_name_plus = $whitespace . $quote . $job_name_prefix . $jobs_name_plus;
    $cmd = join '-J', $bsub_before_job_name, $jobs_name_plus, @any_extra;
  }

  # if the no_bsub flag is set
  if ( $self->no_bsub() ) {
    $self->log( qq{***** I would be submitting the following to LSF\n$cmd \n*****} );
    return $DEFAULT_JOB_ID_FOR_NO_BSUB;
  }

  my $count = 1;
  my $job_id;

  my $max_tries_plus_one = $self->general_values_conf()->{max_tries} + 1;
  my $min_sleep = $self->general_values_conf()->{min_sleep};

  while ($count < $max_tries_plus_one) {
    $job_id = qx/$cmd/;
    if ($CHILD_ERROR) {
      $self->log(qq{Error attempting ($count) to submit job $cmd \n\n.\tError code $CHILD_ERROR});
      sleep $min_sleep ** $count;
      $count++;
    } else {
      $count = $max_tries_plus_one;
    }
  }

  if ( $cmd =~ /bsub/xms ) {
    ($job_id) = $job_id =~ /(\d+)/xms;
    if(!$job_id) {
      croak qq{Failed to submit an lsf job for $cmd};
    }
  }
  return $job_id;
}

=head2 script_name

returns the current scripts name (same as $PROGRAM_NAME), unless you have a desire to set on construction

  my $sScriptName = $class->script_name();

=cut

has q{script_name} => (isa => q{Str}, is => q{ro}, metaclass => 'NoGetopt', lazy_build => 1, documentation => q{Advise - Do Not set on construction - unless you need to, it can be built from $PROGRAM_NAME}); ## no critic (ValuesAndExpressions::RequireInterpolationOfMetachars)

sub _build_script_name {
  my ($self) = @_;
  return $PROGRAM_NAME;
}

=head2 time

returns and store localtime so that this can be used for calculations

  my $iTime = $class->time();

=head2 timestamp

returns and stores a timestring YYYY-MM-DD HH:MM:SS of $self->time()

  my $sTimeStamp = $class->timestamp();

=cut

has q{time}               => (isa => q{Int}, is => q{ro},metaclass => 'NoGetopt',lazy_build => 1, documentation => q{Advise - Do Not set on construction, it can be built});
has q{timestamp}          => (isa => q{Str}, is => q{ro},metaclass => 'NoGetopt', lazy_build => 1, documentation => q{Advise - Do Not set on construction, it can be built});

sub _build_time {
  my ($self) = @_;
  return time;
}

sub _build_timestamp {
  my ($self) = @_;
  my $ts = strftime '%Y%m%d-%H%M%S', localtime $self->time();
  return $ts;
}

=head2 lsf_queue

can be provided by the user, or will take default from either LSB_DEFAULTQUEUE environment variable or value for default_lsf_queue in general_values.ini

=cut

has q{lsf_queue}      => (isa => q{Str},  is => q{ro},
                          #metaclass => 'NoGetopt',
                          lazy_build => 1,
                          documentation => q{the lsf_queue you want to submit ordinary jobs to. defaults to LSB_DEFAULTQUEUE or from config file},);

sub _build_lsf_queue {
  my ($self) = @_;
  my $queue = $ENV{LSB_DEFAULTQUEUE} || $self->general_values_conf()->{default_lsf_queue};
  return $queue;
}

=head2 small_lsf_queue

can be provided by the user, or will take default from either LSB_DEFAULTQUEUE environment variable or value for default_lsf_queue in general_values.ini

=cut

has q{small_lsf_queue}  => (isa => q{Str},
                            is => q{ro},
                            lazy_build => 1,
                            documentation => q{the lsf_queue you want to submit small jobs to. defaults to value from config file},
);
sub _build_small_lsf_queue {
  my ($self) = @_;
  return $self->general_values_conf()->{small_lsf_queue};
}

=head2 force_p4

Boolean decision to force on P4 pipeline usage

=cut

has q{force_p4}        => (isa => q{Bool},  is => q{ro}, documentation => q{Boolean decision to force on P4 pipeline usage});

=head2 verbose

boolean to switch on verbose mode

=cut

has q{verbose}        => (isa => q{Bool},  is => q{ro}, documentation => q{Boolean decision to switch on verbose mode});

=head2 use_bases

returns a string suitable for various scripts which shows the number of bases in order of sequencing eg Y54,I6n,y54
an implementation of _build_use_bases needs to be provided for this to work, no provision of this will cause a run_time error,
unless the string is provided on construction

  my $sUseBases = $class->use_bases();

=cut

# a writer has been provided for test purposes
has q{use_bases} => (isa => q{Str}, is => q{ro}, lazy_build => 1,  writer => q{_test_use_bases},
                     documentation => q{string which determines the bases in each reads (i.e. Y54,I6n,y54)},);

=head2 lanes

option to push through an arrayref of lanes to run on

=head2 all_lanes

returns an array of the elements in $class->lanes();

=head2 no_lanes

returns true if no lanes have been specified

=head2 count_lanes

returns the number of lanes in $class->lanes()

=cut

has q{lanes} => (
  traits        => ['Array'],
  isa           => q{ArrayRef[Int]},
  is            => q{ro},
  predicate     => q{has_lanes},
  documentation => q{option to push through selected lanes of a run only},
  default       => sub { [] },
  handles       => {
    all_lanes   => q{elements},
    no_lanes    => q{is_empty},
    count_lanes => q{count},
  },
);

=head2 lsf_resource_select

 This lsf resource will be requested for all lsf jobs by adding, for example, -R 'select[lenny]'

=cut

has q{lsf_resource_select} => (
  isa     => 'Str',
  is      => 'ro',
  lazy    => 1,
  builder => '_build_lsf_resource_select',
  documentation => 'this lsf resource will be requested for all lsf jobs, example: lenny',
);
sub _build_lsf_resource_select {
  my $self = shift;
  return $self->general_values_conf()->{'lsf_resource_select'} || q{};
}

=head2 directory_exists

returns a boolean true or false dependent on the existence of directory

  my $bDirectoryExists = $class->directory_exists($sDirectoryPath);

=cut

sub directory_exists {
  my ($self, $directory_path) = @_;
  if (-d $directory_path) {
    return 1;
  }
  return 0;
}

=head2 lsb_jobindex

returns a useable string which can be dropped into the command which will be launched in the bsub job, where you
need $LSB_JOBINDEX, as this doesn't straight convert if it is required as part of a longer string

=cut

sub lsb_jobindex {
  return q{`echo $}. q{LSB_JOBINDEX`};
}

=head2 fs_resource_string

returns a resource string for the bsub command in format

  -R 'select rusage[nfs_sf=8]'
  -R 'select[nfs_12>=0] rusage[nfs_sf=8]' # we would like to include this, but it doesn't work with lsf6.1

optionally, can take a hashref which contains a resource string to modify and a value to use for the resource counter and
number of slots it will take (for example the number of processors)

  my $sSfResourceString = $oClass->fs_resource_string( {
    total_counter => 56, # defaults to 72 - doesn't work with lsf6.1, so don't bother
    counter_slots_per_job => 4, # defaults to 8
    resource_string => q{-R 'select[mem>8000] rusage[mem=8000] span[hosts=1]'}
  } );

=cut

sub fs_resource_string {
  my ( $self, $arg_refs ) = @_;
  my $resource_string = $arg_refs->{resource_string} || q{-R 'rusage[]'}; # q{-R 'select[] rusage[]'}; for when we can get a differen version of lsf
  my ( $rusage ) = $resource_string =~ /rusage\[(.*?)\]/xms;
  $rusage ||= q{};
  my $new_rusage = $rusage;
  if (!$self->no_sf_resource()) {
    if ( $new_rusage ) {
      $new_rusage .= q{,};
    }
    $new_rusage .= $self->_fs_resource() . q{=} . ( $arg_refs->{counter_slots_per_job} || $self->general_values_conf()->{default_resource_slots} );
    my $seq_irods = $arg_refs->{seq_irods};
    if($seq_irods){
      $new_rusage .= qq{,seq_irods=$seq_irods};
    }
  }
  $resource_string =~ s/rusage\[${rusage}\]/rusage[${new_rusage}]/xms;
  return $resource_string;
}

###############
# config files

=head2 external_script_names_conf
=head2 function_orders_conf
=head2 general_values_conf
=head2 illumina_pipeline_conf
=head2 pb_cal_pipeline_conf
=head2 parallelisation_conf

All of these are accessors which return a hashref of configuration details for the current or declared domain
from the relevant configuration file

=cut

has [ qw{
    external_script_names_conf
    function_order_conf
    general_values_conf
    illumina_pipeline_conf
    pb_cal_pipeline_conf
    parallelisation_conf
  } ] => (
  isa => q{HashRef},
  is  => q{ro},
  lazy_build => 1,
  metaclass => 'NoGetopt',
  init_arg => undef,
);

sub _build_external_script_names_conf {
  my ( $self ) = @_;
  return $self->_get_config_reader( q{external_script_names.ini} );
}
sub _build_function_order_conf {
  my ( $self ) = @_;
  return $self->_get_config_reader( q{function_orders.yml} );
}
sub _build_general_values_conf {
  my ( $self ) = @_;
  return $self->_get_config_reader( q{general_values.ini} );
}
sub _build_illumina_pipeline_conf {
  my ( $self ) = @_;
  return $self->_get_config_reader( q{illumina_pipeline.ini} );
}
sub _build_pb_cal_pipeline_conf {
  my ( $self ) = @_;
  return $self->_get_config_reader( q{pb_cal_pipeline.ini} );
}
sub _build_parallelisation_conf {
  my ( $self ) = @_;
  return $self->_get_config_reader( q{parallelisation.yml} );
}

sub _get_config_reader {
  my ( $self, $conf_name ) = @_;

  my $config = {};

  my $config_file = join q[/], $self->conf_path(), $conf_name;
  if ( -e $config_file ) {
    $config = Config::Any->load_files({files => [$config_file], use_ext => 1, });
    if ( scalar @{ $config } ) {
         $config = $config->[0]->{ $config_file };
    }
    my $domain = $self->domain();
    if (defined $config->{$domain}) {
       $config = $config->{$domain};
    } else {
      croak "cannot find $domain in $config_file";
    }
  } else {
    croak "cannot find $config_file";
  }

  return $config;
}

=head2 domain

accessor to obtain the domain which should be looked for in the config files

=cut

has q{domain} => (
  isa => q{Str},
  is  => q{ro},
  lazy_build => 1,
  documentation => q{domain to utilise for config files. Defaults to $} . q{ENV{TEST} or live},
);

sub _build_domain {
  my ( $self ) = @_;
  return $ENV{TEST} || q{live};
}

has q{conf_path} => (
  isa => q{Str},
  is  => q{ro},
  lazy_build => 1,
  documentation => q{full path to directory of config files},
);
sub _build_conf_path {
  my $self = shift;
  return "$Bin/../$CONF_DIR";
}

=head2 fix_broken_files

Boolean flag to tell the pipeline to fix any missing broken files it can

=cut

has q{fix_broken_files} => (
  isa => q{Bool},
  is  => q{ro},
  documentation => q{boolean flag to fix files that may be missing or broken if it can},
);

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
    if ( $self->verbose && $self->can( q{log} ) ) {
      $self->log( qq{$log_dir already exists} );
    }
    return $log_dir;
  }

  my $cmd = qq{mkdir -p $log_dir};
  my $output = qx{$cmd};

  if ( $self->verbose && $self->can( q{log} ) ) {
    $self->log( qq{Command: $cmd} );
    $self->log( qq{Output:  $output} );
  }

  if ( $CHILD_ERROR ) {
    croak qq{unable to create $log_dir:$output};
  }

  if ( $self->can( q{log} ) ) {
    $self->log( qq{chgrp $owning_group $log_dir} );
  }

  my $rc = qx{chgrp $owning_group $log_dir};
  if ( $CHILD_ERROR ) {
    if ( $self->can( q{log} ) ) {
      $self->log("could not chgrp $log_dir\n\t$rc");                # not fatal
    }
  }
  $rc = qx{chmod u=rwx,g=srxw,o=rx $log_dir};
  if ( $CHILD_ERROR ) {
    $self->log("could not chmod $log_dir\n\t$rc");                # not fatal
  }

  return $log_dir;
}

=head2 job_name_prefix

value to be prepended to job_names to signify something about where they have been launched from (i.e. prod for production). No need to use an underscore in the prefix, this will be added automatically

This can be set in the general_values.ini config file, but will be overridden if given on the command line.

=cut

has q{job_name_prefix_store} => (
  isa => q{Str},
  is  => q{ro},
  init_arg => q{job_name_prefix},
  documentation => q{give all your jobs a prefix to their names},
  predicate => q{has_job_name_prefix_store},
);

sub job_name_prefix {
  my ( $self ) = @_;

  my $job_name_prefix = $self->has_job_name_prefix_store()              ? $self->job_name_prefix_store() . q{_}
                      : $self->general_values_conf()->{job_name_prefix} ? $self->general_values_conf()->{job_name_prefix} . q{_}
                      :                                                   q{}
                      ;

  return $job_name_prefix;
}

=head2 job_priority

enables you to set on construction a priority value to be used for all jobs to LSF. It is up to the user to ensure that it is suitable for the queue the jobs are to be dispatched to. Not setting this will use the queue default

Note, this is an all or nothing approach, it will be used on all jobs, regardless of the queue used (i.e. if you are running some as small)

=cut

has q{job_priority} => (
  isa => q{Int},
  is  => q{ro},
  predicate => q{has_job_priority},
  documentation => q{user defined all or nothing priority for lsf. default is to use the queue value},
);

=head2 _fs_resource - returns the fs_resource for the given runfolder_path

=cut 

has _fs_resource => (
  is => 'ro',
  isa => 'Str',
  lazy_build => 1,
);

sub _build__fs_resource {
  my ($self) = @_;

  if ($ENV{TEST_FS_RESOURCE}) { return $ENV{TEST_FS_RESOURCE}; }

  my $r = join '_', grep {$_} splitdir path_to_mount_point($self->runfolder_path());
  return join q(),$r=~/\w+/xsmg;
}

# this class ties together short_info and path_info, so the following _build_run_folder will work
sub _build_run_folder {
  my ($self) = @_;
  my @temp = split m{/}xms, $self->runfolder_path();
  my $run_folder = pop @temp;
  return $run_folder;
}

=head2 local_path

 a list with a path to bin the code is running from and perl executable the code is running under

=cut
sub local_path {
  my $perl_path = "$EXECUTABLE_NAME";
  $perl_path =~ s/\/perl$//xms;
  return ($Bin, $perl_path);
}

=head2 status_files_path

 a directory to save status files to

=cut
sub status_files_path {
  my $self = shift;
  my $apath = $self->analysis_path;
  if (!$apath) {
    croak 'Failed to retrieve analysis_path';
  }
  return join q[/], $apath, 'status';
}

no Moose;
__PACKAGE__->meta->make_immutable;
1;
__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item Moose::Meta::Class

=item Carp

=item Config::Any

=item English qw{-no_match_vars}

=item File::Slurp

=item File::Spec::Functions 

=item FindBin qw($Bin)

=item Sys::Filesystem::MountPoint 

=item English -no_match_vars

=item POSIX qw(strftime)

=item Readonly

=item MooseX::Getopt

=item MooseX::AttributeCloner

=item npg_common::roles::log

=item npg_tracking::illumina::run::short_info

=item npg_pipeline::roles::business::base

=item npg_tracking::illumina::run::folder

=item npg_pipeline::roles::business::flag_options

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown
Marina Gourtovaia

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
