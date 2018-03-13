package npg_pipeline::executor::lsf::job;

use Moose;
use Readonly;
use English qw{-no_match_vars};
use Carp;
use List::MoreUtils qw/ uniq /;

with 'npg_pipeline::executor::lsf::options';

our $VERSION = '0';

Readonly::Array my @COMPONENTS => qw/
                     queue
                     job_name
                     command
                     log_file
                                    /;

Readonly::Scalar my $DEFAULT_JOB_ID_FOR_NO_BSUB => 50;

=head1 NAME

npg_pipeline::executor::lsf::job

=head1 SYNOPSIS

Work in progress

=head1 SUBROUTINES/METHODS

=cut

has 'definition' => (
  isa      => 'npg_pipeline::function::definition',
  is       => 'ro',
  required => 1,
);

has 'function_name' => (
  isa      => 'Str',
  is       => 'ro',
  required => 1,
);

has 'upstream_job_ids' => (
  isa      => 'ArrayRef',
  is       => 'ro',
  required => 1,
);

=head2 fs_resource

Returns the fs_resource for the given runfolder_path

=cut 

has 'fs_resource' => (
  is         => 'ro',
  isa        => 'Str',
  required    => 0,
);

=head2 fs_resource_string

Returns a resource string for the bsub command in format

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
  my $resource_string = $arg_refs->{'resource_string'} || q{-R 'rusage[]'}; # q{-R 'select[] rusage[]'}; for when we can get a differen version of lsf
  my ( $rusage ) = $resource_string =~ /rusage\[(.*?)\]/xms;
  $rusage ||= q{};
  my $new_rusage = $rusage;
  if (!$self->no_sf_resource()) {
    if ( $new_rusage ) {
      $new_rusage .= q{,};
    }
    $new_rusage .= $self->_fs_resource() . q{=} . ( $arg_refs->{'counter_slots_per_job'} || $self->general_values_conf()->{default_resource_slots} );
    my $seq_irods = $arg_refs->{'seq_irods'};
    if($seq_irods){
      $new_rusage .= qq{,seq_irods=$seq_irods};
    }
  }
  $resource_string =~ s/rusage\[${rusage}\]/rusage[${new_rusage}]/xms;
  return $resource_string;
}

=head2 lsb_jobindex

Returns a useable string which can be dropped into the command which will be launched in the bsub job, where you
need $LSB_JOBINDEX, as this doesn't straight convert if it is required as part of a longer string

=cut

sub lsb_jobindex {
  return q{`echo $}. q{LSB_JOBINDEX`};
}

=head2 generate_command

=cut

sub generate_command {
  my $self = shift;

  my @command = qw/bsub/;
  foreach my $f (@COMPONENTS) {
    my $method = q[_] . $f . '_lsf';
    push @command, $self->$method();
  }

  return q[ ], @command;
}

sub _job_name {
  my $self = shift;
  return $self->definition()->job_name() || $self->function_name();
}

sub _prerequisites_lsf {
  my ($self, @job_ids) = @_;
  if (!@job_ids) {
    $self->logcroak(q{List of job ids is expected});
  }
  @job_ids = map { qq[done($_)] }
             uniq
             sort { $a <=> $b }
             @job_ids;
  return q{-w'}.(join q{ && }, @job_ids).q{'};
}

sub _queue_lsf {
  my $self = shift;
  my $q = $self->definition()->hints()->{'queue'} || 'default_queue';
  $q = $self->_config->{$q} or croak qq[Failed to get LSF queue name for '$q'];
  return $q;
}

sub _log_file_lsf {
  my $self = shift;
  if (!$self->definition()->has_log_file_dir()) {
    croak 'Log file directory is required';
  }
  my $log_file = $self->_job_name();
  $log_file = join q[/], $self->definition()->log_file_dir(), $log_file;
  return join q[ ], q[-o], $log_file;
}

sub _job_name_lsf {
  my $self = shift;
  return q[-J] . $self->_job_name();
}

sub _command_lsf {
  my $self = shift;

  if (!$self->definition()->has_command()) {
    croak 'Command is required';
  }
  return q['] . $self->definition()->command() .q['];
}

1;

__END__

=head1 DESCRIPTION

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item Readonly

=item Carp

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

