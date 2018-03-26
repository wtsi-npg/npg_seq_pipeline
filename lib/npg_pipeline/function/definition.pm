package npg_pipeline::function::definition;

use Moose;
use MooseX::StrictConstructor;
use MooseX::Storage;
use MooseX::Aliases;
use namespace::autoclean;
use Readonly;
use Carp;

our $VERSION = '0';

with Storage('format' => 'JSON');

Readonly::Scalar our $SMALL_QUEUE   => q[small];
Readonly::Scalar my  $DEFAULT_QUEUE => q[default];

Readonly::Scalar my $DEFAULT_MIN_CPU_NUM => 1;
Readonly::Array  my @MUST_HAVE_ATTRS => qw/
                                job_name
                                identifier
                                command
                                log_file_dir
                                          /;
=head1 NAME

npg_pipeline::function::definition

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 pack

Inherited from MooseX::Storage

=head2 freeze

Inherited from MooseX::Storage. Serializes an instance of
this class to a JSON string.

=head2 thaw

Inherited from MooseX::Storage. Creates an instant of this
class from a JSON serialization.

=head2 TO_JSON

This method is provided in order to enable JSON serialization
of complex Perl data structures containing instances of
this class. Internally it calls 'pack' to provide a translation
from this class' instance to a serializable Perl data structure.

  use JSON;
  use npg_pipeline::function::definition;

  my $d = npg_pipeline::function::definition->new(
            created_by   => 'module',
            created_on   => 'June 25th',
            excluded     => 1);
  my $json = JSON->new->convert_blessed;
  print $json->pretty->encode({ 'a' => [$d]}) };

=cut

alias TO_JSON => 'pack';

=head2 created_by

Class that created this definition

=cut

has 'created_by' => (
  isa        => 'Str',
  is         => 'ro',
  required   => 1,
);

=head2 created_on

Timestamp

=cut

has 'created_on' => (
  isa        => 'Str',
  is         => 'ro',
  required   => 1,
);

=head2 job_name

Suggested job name

=cut

has 'job_name' => (
  isa        => 'Str',
  is         => 'ro',
  required   => 0,
  predicate  => 'has_job_name',
);

=head2 identifier

Run, library, sample or study identifier - whatever
is common for all steps of a particular pipeline
invocation.

=cut

has 'identifier' => (
  isa        => 'Str',
  is         => 'ro',
  required   => 0,
  predicate  => 'has_identifier',
);

=head2 composition

An npg_tracking::glossary::composition object.
An optional attribute, since composition object cannot
be defined for a run.

=cut

has 'composition' => (
  is         => 'ro',
  isa        => 'npg_tracking::glossary::composition',
  required   => 0,
  predicate  => 'has_composition',
);

=head2 excluded

Boolean flag, false by default. If set to true, the
function should not be executed by the pipeline.

=cut

has 'excluded' => (
  isa        => 'Bool',
  is         => 'ro',
  required   => 0,
);

=head2 immediate_mode

Boolean flag, false by default. If set to true,
the function was executed at the time the implementor
was called.

=cut

has 'immediate_mode' => (
  isa        => 'Bool',
  is         => 'ro',
  required   => 0,
);

=head2 command

Command to execute. Might contain placeholders
in place of some arguments. Cam be undefined if
either excluded or immediate_mode is set to true.

=cut

has 'command' => (
  isa        => 'Str',
  is         => 'ro',
  required   => 0,
  predicate  => 'has_command',
);

=head2 command_preexec

Command to execute prior to executing the main command.
If this command exists with an error, the main command
cannot be executed.

=cut

has 'command_preexec' => (
  isa        => 'Str',
  is         => 'ro',
  required   => 0,
  predicate  => 'has_command_preexec',
);

=head2 log_file_dir

Suggested location for a log file.

=cut

has 'log_file_dir' => (
  isa        => 'Str',
  is         => 'ro',
  required   => 0,
  predicate  => 'has_log_file_dir',
);

=head2 num_cpus

Number of CPUs as an array of integers. If set,
should contain at least one number. Might contain
two numbers defining a suggested range of numbers.

=cut

has 'num_cpus' => (
  isa        => 'ArrayRef[Int]',
  is         => 'ro',
  required   => 0,
  predicate  => 'has_num_cpus',
);

=head2 num_hosts

Number of hosts parallel jobs can be span across.

=cut

has 'num_hosts' => (
  isa        => 'Int',
  is         => 'ro',
  required   => 0,
  predicate  => 'has_num_hosts',
);

=head2 memory

Memory in MB. If not set, less that 2Gb are required. 

=cut

has 'memory' => (
  isa        => 'Int',
  is         => 'ro',
  required   => 0,
  predicate  => 'has_memory',
);

=head2 queue

An LSF queue hint.

=cut

has 'queue' => (
  isa        => 'Str',
  is         => 'ro',
  writer     => '_set_queue',
  predicate  => 'has_queue',
);

=head2 fs_slots_num

=cut

has 'fs_slots_num' => (
  isa        => 'Int',
  is         => 'ro',
  required   => 0,
  predicate  => 'has_fs_slots_num',
);

=head2 reserve_irods_slots

=cut

has 'reserve_irods_slots' => (
  isa        => 'Bool',
  is         => 'ro',
  required   => 0,
  predicate  => 'has_reserve_irods_slots',
);

=head2 array_cpu_limit

The value of the array cpu limit, unset by default. 

=cut

has 'array_cpu_limit' => (
  isa        => 'Int',
  is         => 'ro',
  required   => 0,
  predicate  => 'has_array_cpu_limit',
);

=head2 apply_array_cpu_limit

A boolen flag instructing to apply a default array cpu limit,
false by default.

=cut

has 'apply_array_cpu_limit' => (
  isa        => 'Bool',
  is         => 'ro',
  required   => 0,
  predicate  => 'has_apply_array_cpu_limit',
);

=head2 BUILD

Called by Moose at the end of object instantiation.
Builds 'queue' attribute so that the value serialized.
Throws an error if any of the attributes that are
essential for a definition are not defined.

=cut

sub BUILD {
  my $self = shift;
  if (!$self->excluded && !$self->immediate_mode) {
    for my $a (@MUST_HAVE_ATTRS) {
      my $method = 'has_' . $a;
      if (!$self->$method) {
        croak qq{'$a' should be defined};
      }
    }
    if ($self->has_array_cpu_limit && !$self->apply_array_cpu_limit) {
      croak 'array_cpu_limit is set, apply_array_cpu_limit should be set to true';
    }
    if (!$self->has_queue) {
      $self->_set_queue($DEFAULT_QUEUE);
    }
  }
  return;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item MooseX::StrictConstructor

=item MooseX::Storage

=item MooseX::Aliases

=item namespace::autoclean

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
