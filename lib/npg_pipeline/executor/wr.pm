package npg_pipeline::executor::wr;

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;
use File::Slurp;
use JSON;
use Readonly;
use English qw(-no_match_vars);
use Math::Random::Secure qw(irand);
use Try::Tiny;

use npg_pipeline::runfolder_scaffold;

extends 'npg_pipeline::executor';

with 'npg_pipeline::executor::options' => {
  -excludes => [qw/ no_sf_resource
                    no_bsub
                    no_array_cpu_limit
                    array_cpu_limit/ ]
};
with 'npg_tracking::util::pipeline_config';

our $VERSION = '0';

Readonly::Scalar my $GENERIC_DEP_KEY               => q[wr_group_id];
Readonly::Scalar my $DEFAULT_MEMORY                => 2000;
Readonly::Scalar my $VERTEX_JOB_PRIORITY_ATTR_NAME => q[job_priority];
Readonly::Scalar my $WR_ENV_LIST_DELIM             => q[,];
Readonly::Array  my @ENV_VARS_TO_PROPAGATE => qw/ PATH
                                                  PERL5LIB
                                                  CLASSPATH
                                                  NPG_CACHED_SAMPLESHEET_FILE
                                                  NPG_REPOSITORY_ROOT
                                                  IRODS_ENVIRONMENT_FILE /;

Readonly::Scalar my $WR_LIMIT_GROUPS_OPTION        => q[limit_grps];
Readonly::Hash   my %LIMIT_GROUPS2DEFINITION_ATTRS =>
                    ('irods' => 'reserve_irods_slots',);

=head1 NAME

npg_pipeline::executor::wr

=head1 SYNOPSIS

=head1 DESCRIPTION

Submission of pipeline function definitions for execution by
L<wr workflow runner|https://github.com/VertebrateResequencing/wr>.

=cut

=head1 SUBROUTINES/METHODS

=cut

##################################################################
############## Public methods ####################################
##################################################################

=head2 wr_conf

=cut

has 'wr_conf' => (
  isa        => 'HashRef',
  is         => 'ro',
  lazy_build => 1,
);
sub _build_wr_conf {
  my $self = shift;
  return $self->read_config($self->conf_file_path('wr.json'));
}

=head2 execute

Creates and submits wr jobs for execution.

=cut

override 'execute' => sub {
  my $self = shift;

  my $action = 'defining';
  try {
    foreach my $function ($self->function_graph4jobs()
                               ->topological_sort()) {
      $self->_definitions4function($function);
    }
    $action = 'saving';
    my $json = JSON->new->canonical; # the keys will be sorted
    $self->save_commands4jobs(
         map { $json->encode($_) } # convert every wr definition to JSON
         map { @{$_} }             # expand arrays of wr definitions        
         values %{$self->commands4jobs()}
                               );
    $action = 'submitting';
    $self->_submit();
  } catch {
    $self->logcroak(qq[Error $action wr jobs: $_]);
  };

  return;
};

##################################################################
############## Private attributes and methods ####################
##################################################################

has '_attributes2limit_groups' => (
  isa        => 'HashRef',
  is         => 'ro',
  lazy_build => 1,
);
sub _build__attributes2limit_groups {
  my $self = shift;
  my $h = {};
  for my $group (@{$self->_limit_groups}) {
    $h->{$group} = exists $LIMIT_GROUPS2DEFINITION_ATTRS{$group}
                   ? $LIMIT_GROUPS2DEFINITION_ATTRS{$group}
                   : $group;
  }
  return $h;
}

has '_limit_groups' => (
  isa        => 'ArrayRef',
  is         => 'ro',
  lazy_build => 1,
);
sub _build__limit_groups {
  my $self = shift;

  my $groups = $self->wr_conf->{$WR_LIMIT_GROUPS_OPTION};
  $groups ||= [];
  if (@{$groups}) {
    $groups = [(sort @{$groups})];
    $self->info(q[The following groups might be subject to limits: ] .
                join q[, ], @{$groups});
  } else {
    $self->logwarn(q[No limit groups are configured]);
  }

  return $groups;
}

has '_dependencies' => (
  isa        => 'HashRef[HashRef]',
  is         => 'ro',
  default => sub { return {}; },
);

sub _definitions4function {
  my ($self, $function_name) = @_;

  my $g = $self->function_graph4jobs;
  my $definitions = $self->function_definitions()->{$function_name};
  my $group_id = join q[-], $function_name, $definitions->[0]->identifier(), irand();
  my $outgoing_flag = $self->future_path_is_in_outgoing($function_name);
  my $log_dir = $self->log_dir4function($function_name);
  if ($outgoing_flag) {
    $log_dir = npg_pipeline::runfolder_scaffold->path_in_outgoing($log_dir);
  }
  my $i = 0;

  # Translate each job definition into a WR definition
  # TODO: handle chunk->chunk dependancies better
  my %composition_deps = ();
  foreach my $d (@{$definitions}) {

    my $per_job_group_id = join q[-], $group_id, $i++;
    my $wr_def = $self->_definition4job($function_name, $log_dir, $d);
    $wr_def->{'dep_grps'} = [$group_id, $per_job_group_id];
    # Does this job have a composition?
    my $composition_digest = $d->has_composition ? $d->composition()->digest() : q[];
    if ($composition_digest) {
      # save $per_job_group_id for this function definition here
      push @{$composition_deps{$composition_digest}}, $per_job_group_id;
    }
    $composition_deps{$GENERIC_DEP_KEY} = [$group_id];

    if (!$g->is_source_vertex($function_name)) {
      my @depends_on = ();

      #for each previous node on the graph calc dependencies
      foreach my $prev_func ($g->predecessors($function_name)) {
        $self->_dependencies->{$prev_func} or $self->logcroak(
          "Dependency groups not defined for function $prev_func");
        # take that jobs's individual depgroup if we have a matching entry in the composition hash?
        if ($composition_digest &&
            $self->_dependencies->{$prev_func}->{$composition_digest}) {
          # yes? add the specific dependency
          push @depends_on, @{$self->_dependencies->{$prev_func}->{$composition_digest}};
        } else {
          # no? add the generic dependency, which should be always available
          my @id = @{$self->_dependencies->{$prev_func}->{$GENERIC_DEP_KEY}};
          @id or $self->logcroak("Generic dependency group not defined for function $prev_func");
          push @depends_on, @id;
        }
      }

      if (!@depends_on) {
        $self->logcroak(qq["$function_name" should depend on at least one job]);
      }

      # save completed dependencies for this job definition
      $wr_def->{'deps'} = \@depends_on;
    }

    # create WR report group
    my @report_group = ($d->identifier(), $function_name);
    if ($self->has_job_name_prefix()) {
      unshift @report_group, $self->job_name_prefix();
    }
    $wr_def->{'rep_grp'}  = join q[-], @report_group;

    push @{$self->commands4jobs()->{'function_name'}}, $wr_def;
  }

  # need to save $composition_deps for this function here
  $self->_dependencies->{$function_name} = \%composition_deps;

  return $group_id;
}

# Create WR definition for a single job
sub _definition4job {
  my ($self, $function_name, $log_dir, $d) = @_;

  my $def = {};

  $def->{'memory'} = $d->has_memory() ? $d->memory() : $DEFAULT_MEMORY;
  $def->{'memory'} .= q[M];

  # priority needs to be a number, rather than string, in JSON for wr,
  # hence the addition
  $def->{'priority'} = 0 + $self->function_graph4jobs->get_vertex_attribute(
                           $function_name, $VERTEX_JOB_PRIORITY_ATTR_NAME);

  if ($d->has_num_cpus()) {
    use warnings FATAL => qw(numeric);
    $def->{'cpus'} = int $d->num_cpus()->[0];
  }

  if ($d->queue) {
    my $options = $self->wr_conf()->{$d->queue . '_queue'};
    if ($options) {
      while (my ($key, $value) = each %{$options}) {
        $def->{$key} = $value;
      }
    }
  }

  my @limit_groups = $self->_limit_groups4job($d);
  if (@limit_groups) {
    $def->{$WR_LIMIT_GROUPS_OPTION} = [(sort @limit_groups)];
  }

  my $log_file = sub {
    #####
    # Explicit exclusion for now. In future, if we end up using this function,
    # we might extend function definition to handle jobs without logs.
    # No log is safer for pipeline_wait4path function since the run folder
    # might be moved to outgoing while the function is being executed.
    if ($function_name ne 'pipeline_wait4path') {
      my $log_name = join q[-], $function_name, $d->created_on(),
        $d->has_composition() ? $d->composition()->freeze2rpt () : $d->identifier();
      $log_name   .= $d->chunk_label.q[.out];
      return join q[/], $log_dir, $log_name;
    }
    return;
  };

  my $command = join q[ ], q[(umask 0002 &&], $d->command(), q[)], q[2>&1];
  my $lf = $log_file->();
  if ($lf) {
    #####
    # Ask tee command to append to the log rather than start over.
    # This would replicate behaviour under LSF. If the job is retried,
    # will keep the original log in place.
    $command = join q[ ], $command, q[|], q[tee -a], q["]. $log_file->() . q["];
  }
  $def->{'cmd'} = $command;

  return $def;
}

sub _limit_groups4job {
  my ($self, $d) = @_;

  my @limit_groups = ();
  for my $group ( keys %{$self->_attributes2limit_groups} ) {
    my $method = $self->_attributes2limit_groups->{$group};
    my $set_limit = 0;
    if ($group ne $method) {
      $d->can($method) or $self->logcroak(
        "Limit group '$group' does not map to an existing " .
        'definition object method'
      );
      $set_limit = $d->$method;
    } else {
      if ($d->can($method)) {
        # Have a method that exactly matches the group? - Use it.
        $set_limit = $d->can($method);
      } else {
        # Does the group name match the name of the class that created
        # this definition?
        $set_limit = ($d->created_by =~ /$group/smx);
      }
    }
    $set_limit and push @limit_groups, $group;
  }

  return @limit_groups;
}

sub _wr_add_command {
  my $self = shift;

  # If needed, in future, these options can be passed from the command line
  # or read from a conf. file.

  # Explicitly pass the pipeline's environment to jobs
  my @env_list = ();
  foreach my $var_name (sort @ENV_VARS_TO_PROPAGATE) {
    my $value = $ENV{$var_name};
    if (defined $value && $value ne q[]) {
      push @env_list, "${var_name}=${value}";
    }
  }
  my $stack = join $WR_ENV_LIST_DELIM, @env_list;

  my @common_options = (
          '--cwd'        => '/tmp',
          '--disk'       => 0,
          '--override'   => 2,
          '--retries'    => 1,
          '--env'        => q['] . $stack . q['],
                       );

  return join q[ ], qw/wr add/,
                    @common_options,
                    '-f', $self->commands4jobs_file_path();
}

sub _submit {
  my $self = shift;

  my $cmd = $self->_wr_add_command();
  $self->info(qq[Command to use with wr: $cmd]);
  if ($self->interactive) {
    $self->info(q[Interactive mode, commands not added to wr]);
  } else {
    if (system($cmd) == 0) {
      $self->info(q[Commands successfully added to wr]);
    } else {
      my $e = $CHILD_ERROR || q[];
      $self->logcroak(qq[Error $e running "$cmd"]);
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

=item namespace::autoclean

=item Readonly

=item English

=item JSON

=item Math::Random::Secure

=item Try::Tiny

=item npg_tracking::util::pipeline_config

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

=over

=item Marina Gourtovaia

=back

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2018,2019,2020,2021 Genome Research Ltd.

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
