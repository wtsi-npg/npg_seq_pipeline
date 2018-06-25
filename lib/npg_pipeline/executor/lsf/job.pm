package npg_pipeline::executor::lsf::job;

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;
use List::MoreUtils qw(uniq);
use Carp;
use Readonly;

use npg_pipeline::function::definition;

with 'npg_pipeline::executor::options';

our $VERSION = '0';

=head1 NAME

npg_pipeline::executor::lsf::job

=head1 SYNOPSIS

=head1 DESCRIPTION

LSF job definition factory.

=head1 SUBROUTINES/METHODS

=cut

Readonly::Scalar my $POSITION_MULTIPLIER => 10_000;

##################################################################
################## Public attributes #############################
##################################################################

=head2 definitions

An array of function definition objects for this LSF job.
An attribute, required, the array cannot be empty.

=cut

has 'definitions' => (
  isa      => 'ArrayRef[npg_pipeline::function::definition]',
  is       => 'ro',
  required => 1,
);

=head2 upstream_job_ids

An array of LSF job ids this job should depend on.
An attribute, defaults to an empty array.

=cut

has 'upstream_job_ids' => (
  isa      => 'ArrayRef',
  is       => 'ro',
  required => 1,
  default  => sub {return [];},
);

=head2 lsf_conf

A hash reference with LSF-relevant configuration.
An attribure, defaults to an empty hash.

=cut

has 'lsf_conf' => (
  isa      => 'HashRef',
  is       => 'ro',
  required => 1,
  default  => sub {return {};},
);

=head2 fs_resource

fs resource for this LSF job.
An string attribute, defaults to an undefined value.

=cut

has 'fs_resource' => (
  is       => 'ro',
  isa      => 'Maybe[Str]',
  required => 1,
  default  => undef,
);

=head2 log_dir

Directory for log files.

=cut

has 'log_dir' => (
  is       => 'ro',
  isa      => 'Str',
  required => 1,
);

=head2 is_array.

A boolean attribute. Is set to true if this LSF job should
be submitted as a job array. Cannot be set via a constructor.

Only run-level jobs should be executed directly. For consistency,
lane-level job for one lane will be treated as arrays.

=cut

has 'is_array' => (
  is         => 'ro',
  isa        => 'Bool',
  lazy_build => 1,
  init_arg   => {},
);
sub _build_is_array {
  my $self = shift;
  return @{$self->definitions()} == 1
         ? $self->definitions->[0]->has_composition() : 1;
}

=head2 commands

A hash reference with all commands for this LSF job.
This attribute cannot be set via a constructor.
If the value of is_array attribute is set to false,
contains one entry. In case of a job array, the
keys are the indexes of the proposed job array.

=cut

has 'commands' => (
  is         => 'ro',
  isa        => 'HashRef',
  lazy_build => 1,
  init_arg   => {},
);
sub _build_commands {
  my $self = shift;
  my $commands = {};
  foreach my $d (@{$self->definitions()}) {
    $commands->{$self->_array_index($d)} = $d->command();
  }
  return $commands;
}

=head2 params

Options of the proposed LSF job as an array reference.
This attribute cannot be set via a constructor.
To be used with the bsub command, the array members
should be concatenated using a single white space.
The parameters do not contain a command that should
be executed by the LSF job.

Each array member represents a full definition, ie
both the bsub command option and its value separated
by a white space. 

As a minimum, the parameters contain the job name,
including array definition if necessary, and log
file path.

=cut

has 'params' => (
  is         => 'ro',
  isa        => 'Str',
  lazy_build => 1,
  init_arg   => {},
);
sub _build_params {
  my $self = shift;
  my @params = grep { defined }   # filter out undefined results
               map  { $self->$_ } # apply method
               map  { q(_) . $_ } # generate private method name
               qw/ 
                   priority
                   dependencies
                   queue
                   job_name
                   memory
                   cpu_host
                   fs_slots
                   irods_slots
                   log_file
                   preexec
                 /;
  return join q[ ], @params;
}

=head2 BUILD

Called by Moose at the end of object instantiation.
Checks object's attributes. Checks definitions array members for
consistency.

=cut

sub BUILD {
  my $self = shift;

  my @definitions =  @{$self->definitions()};
  if (!@definitions) {
    croak 'Array of definitions cannot be empty';
  }

  my $deflate = sub {
    my ($d, $method) = @_;
    my $v = $_->$method;
    $v = defined $v ? $v : 0;
    my $type = ref $v;
    if ($type) {
      if ($type ne 'ARRAY') {
        croak "Unexpected type $type returned by definition method or attribute $method";
      }
      $v = join q[ ], @{$v};
    }
    return $v;
  };

  foreach my $attr (_definition_attr_list()) {

    my $has_method = q[has_] . $attr;
    my @values = uniq
                 map  { $deflate->($_, $has_method) }
                 grep { $_->can($has_method) }
                 @definitions;
    if (@values > 1) {
      croak qq[Inconsistent values for definition predicate method $has_method];
    }

    if ($attr =~ /\A composition | command \Z/smx) {
      next;
    }

    @values = uniq
              map  { $deflate->($_, $attr)  }
              @definitions;
    if (@values > 1) {
      croak qq[Inconsistent values for definition attribute $attr];
    }
  }

  return;
}

##################################################################
############## Public methods ####################################
##################################################################

=head2 jjob_name

The value returned by this method is derived from one of the job
definition objects. The same applies to command_preexec, num_cpus,
num_hosts, memory, queue, fs_slots_num, reserve_irods_slots,
array_cpu_limit and apply_array_cpu_limit methods.

=head2 jcommand_preexec
=head2 jnum_cpus
=head2 jnum_hosts
=head2 jmemory
=head2 jqueue
=head2 jfs_slots_num
=head2 jreserve_irods_slots
=head2 jarray_cpu_limit
=head2 japply_array_cpu_limit
=cut

my $delegation = sub {
  my %alist = map { q[j].$_ => $_ }
              grep { not m{\A (?: composition |
                                  identifier  |
                                  created_by  |
                                  created_on  |
                                  excluded    |
                                  command
                               ) \Z}smx }
              _definition_attr_list();

  return \%alist;
};

has '_lsf_definition' => (
  isa        => 'npg_pipeline::function::definition',
  is         => 'ro',
  lazy_build => 1,
  handles    => $delegation->(),
);
sub _build__lsf_definition {
  my $self = shift;
  return $self->definitions()->[0];
}

##################################################################
############## Private methods ###################################
##################################################################

sub _definition_attr_list {
  return npg_pipeline::function::definition->meta()
         ->get_attribute_list();
}

sub _priority {
  my $self = shift;
  if ($self->has_job_priority()) {
    return q[-sp ] . $self->job_priority();
  }
  return;
}

sub _dependencies {
  my $self = shift;
  if (@{$self->upstream_job_ids()}) {
    my @job_ids = map { qq[done($_)] }
                  uniq
                  sort { $a <=> $b }
                  @{$self->upstream_job_ids()};
    return q{-w'}.(join q{ && }, @job_ids).q{'};
  }
  return;
}

sub _cpu_host {
  my $self = shift;
  my $s;
  if ($self->jnum_cpus()) {
    $s = q[-n ] . join q[,], @{$self->jnum_cpus()};
    if ($self->jnum_hosts()) {
      $s .= sprintf q( -R 'span[hosts=%i]'), $self->jnum_hosts();
    }
  }
  return $s;
}

sub _fs_slots {
  my $self = shift;
  if ($self->fs_resource && $self->jfs_slots_num()) {
    return sprintf q(-R 'rusage[%s=%i]'),
           $self->fs_resource, $self->jfs_slots_num();
  }
  return;
}

sub _preexec {
  my $self = shift;
  if ($self->jcommand_preexec()) {
    return sprintf q[-E '%s'], $self->jcommand_preexec();
  }
  return;
}

sub _irods_slots {
  my $self = shift;
  if ($self->jreserve_irods_slots()) {
    my $num_slots = $self->lsf_conf->{'default_lsf_irods_resource'};
    if (!$num_slots) {
      croak q[default_lsf_irods_resource not set in the LSF conf file];
    }
    return sprintf qq(-R 'rusage[seq_irods=$num_slots]');
  }
  return;
}

sub _memory {
  my $self = shift;
  my $m = $self->jmemory();
  if ($m) {
    return qq(-M ${m} -R 'select[mem>${m}] rusage[mem=${m}]');
  }
  return;
}

sub _queue {
  my $self = shift;

  my $q = $self->jqueue() . '_queue';
  my $queue = $self->lsf_conf->{$q};
  if ($queue) {
    $queue = q[-q ] . $queue;
  } else {
    carp "lsf config file does not have definition for $q";
  }

  return $queue;
}

sub _log_file {
  my $self = shift;

  my $log_name = $self->jjob_name() . q[.%J];
  if ($self->is_array()) {
    $log_name .= q[.%I];
  }
  $log_name   .= q[.out];

  return q[-o ] . join q[/], $self->log_dir(), $log_name;
}

sub _job_name {
  my $self = shift;

  my $job_name = $self->jjob_name();
  my $prefix   = $self->has_job_name_prefix()
                 ? $self->job_name_prefix()
                 : $self->lsf_conf()->{'job_name_prefix'};
  if ($prefix) {
    $job_name  = join q[_], $prefix, $job_name;
  }

  if ($self->is_array()) {
    $job_name .= $self->_array_string();
  }

  if (!$self->no_array_cpu_limit()) {
    my $l = $self->jarray_cpu_limit();
    if (!$l && $self->japply_array_cpu_limit()) {
      $l = $self->lsf_conf->{'array_cpu_limit'};
    }
    if ($l) {
      $job_name .= q[%] . $l;
    }
  }

  return qq[-J '$job_name'];
}

sub _array_index {
  my ($self, $d) = @_;

  my $index = $d->identifier();
  if ($d->has_composition) {
    my $c = $d->composition();
    if ($c->num_components != 1) {
      croak 'Cannot deal with multi-component composition';
    }
    my $component = $c->get_component(0);
    $index = defined $component->tag_index()
      ? $component->position() * $POSITION_MULTIPLIER + $component->tag_index()
      : $component->position();
  }
  return $index;
}

#####
# Converts a list of integers to an LSF job array string
# for appending to the LSF job name.
#
sub _array_string {
  my $self = shift;

  my @lsf_indices = sort { $a <=> $b } keys %{$self->commands()};

  my ($start_run, $end_run);
  my $ret = q{};
  foreach my $entry ( @lsf_indices ) {
    # have we already started looping hrough
    if ( defined $end_run ) {
    # if the number is consecutive, increment end of the run
      if ( $entry == $end_run + 1 ) {
        $end_run = $entry;
        # otherwise, finish up that run, which may just be a single number
      } else {
        if ( $start_run != $end_run ) {
          $ret .= q{-} . $end_run;
        }
        $ret .= q{,} . $entry;
        $start_run = $end_run = $entry;
      }
    # we haven't looped through at least once, so set up
    } else {
      $ret .= $entry;
      $start_run = $end_run = $entry;
    }
  }

  if ( $start_run != $end_run ) {
    $ret .= q{-} . $end_run ;
  }

  return q{[} . $ret . q{]};
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

=item MooseX::StrictConstructor

=item List::MoreUtils

=item Carp

=item Readonly

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

