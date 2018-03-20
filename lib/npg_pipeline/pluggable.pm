package npg_pipeline::pluggable;

use Moose;
use Carp;
use Try::Tiny;
use Graph::Directed;
use File::Spec::Functions;
use List::MoreUtils qw/ any uniq /;
use Class::Load  qw(load_class);
use File::Slurp;
use JSON qw/from_json/;
use Readonly;
use English qw{-no_match_vars};

use npg_tracking::util::abs_path qw(abs_path);
use npg_pipeline::cache;
use npg_pipeline::pluggable::registry;

extends q{npg_pipeline::base};

with qw{ MooseX::AttributeCloner
         npg_pipeline::roles::accessor };

our $VERSION = '0';

Readonly::Scalar my $SUSPENDED_START_FUNCTION => q[pipeline_start];
Readonly::Scalar my $END_FUNCTION             => q[pipeline_end];
Readonly::Scalar my $VERTEX_LSF_JOB_IDS_ATTR_NAME => q[lsf_job_ids];
Readonly::Scalar my $LSF_JOB_IDS_DELIM            => q[-];
Readonly::Array  my @FLAG2FUNCTION_LIST       => qw/ qc_run /;
Readonly::Scalar my $FUNCTION_DAG_FILE_TYPE   => 'json';

our $LSFJOB_DEPENDENCIES = q[];

=head1 NAME

npg_pipeline::pluggable

=head1 SYNOPSIS

=head1 SUBROUTINES/METHODS

=head2 interactive

=cut
has q{interactive}  => (
  isa           => q{Bool},
  is            => q{ro},
  default       => 0,
  documentation =>
  q{If false (default), the pipeline_start job is resumed once all jobs have been successfully submitted},
);

=head2 spider

Toggles spider (creating/reusing cached LIMs data), true by default

=cut

has q{spider} => (
  isa           => q{Bool},
  is            => q{ro},
  default       => 1,
  documentation => q{Toggles spider (creating/reusing cached LIMs data), true by default},
);

=head2 script_name

Current scripts name (from $PROGRAM_NAME)

=cut

has q{script_name} => (
  isa       => q{Str},
  is        => q{ro},
  default   => $PROGRAM_NAME,
  init_arg  => undef,
  metaclass => 'NoGetopt',
);

=head2 function_order

=cut
has q{function_order} => (
  isa           => q{ArrayRef},
  is            => q{ro},
  predicate     => q{has_function_order},
  documentation =>
  q{A reference to an array of function names in the order they should run.},
);

has 'function_list' => (
  isa        => q{Str},
  is         => q{ro},
  lazy_build => 1,
);
sub _build_function_list {
  my $self = shift;
  my $suffix = q();
  foreach my $flag (@FLAG2FUNCTION_LIST) {
    if ($self->can($flag) && $self->$flag) {
      $suffix .= "_$flag";
    }
  }
  return $self->pipeline_name . $suffix;
}
around 'function_list' => sub {
  my $orig = shift;
  my $self = shift;

  my $v = $self->$orig();

  my $file = abs_path($v);
  if (!$file || !-f $file) {
    if ($v !~ /\A\w+\Z/smx) {
      $self->logcroak("Bad function list name: $v");
    }
    try {
      $file = $self->conf_file_path((join q[_],'function_list',$v) . q[.] .$FUNCTION_DAG_FILE_TYPE);
    } catch {
      my $pipeline_name = $self->pipeline_name;
      if ($v !~ /^$pipeline_name/smx) {
        $file = $self->conf_file_path((join q[_],'function_list',$self->pipeline_name,$v) . q[.] .$FUNCTION_DAG_FILE_TYPE);
      } else {
        $self->logcroak($_);
      }
    };
  }

  return $file;
};

=head2 function_list_conf

=cut

has 'function_list_conf' => (
  isa        => q{HashRef},
  is         => q{ro},
  lazy_build => 1,
  metaclass  => 'NoGetopt',
  init_arg   => undef,
);
sub _build_function_list_conf {
  my $self = shift;
  my $input_file = $self->function_list;
  $self->info(qq{Reading function graph $input_file});
  return from_json(read_file($input_file));
}

=head2 function_graph

=cut
has q{function_graph} => (
  isa        => q{Graph::Directed},
  is         => q{ro},
  lazy_build => 1,
  metaclass  => 'NoGetopt',
  init_arg   => undef,
);
sub _build_function_graph {
  my $self = shift;

  my $g = Graph::Directed->new();
  my @nodes;

  if ($self->has_function_order && @{$self->function_order}) {

    my @functions = @{$self->function_order};
    $self->info(q{Function order is set by the user: } .
                join q[, ], @functions);

    unshift @functions, $SUSPENDED_START_FUNCTION;
    push @functions, $END_FUNCTION;
    $self->info(q{Function order to be executed: } .
                join q[, ], @functions);

    my $current = 0;
    my $previous = 0;
    my $total = scalar @functions;

    while ($current < $total) {
      if ($current != $previous) {
        $g->add_edge($functions[$previous], $functions[$current]);
        $previous++;
      }
      $current++;
    }
    @nodes = map { {'id' => $_, 'label' => $_} } @functions;
  } else {
    my $jgraph = $self->function_list_conf();
    foreach my $e (@{$jgraph->{'graph'}->{'edges'}}) {
      ($e->{'source'} and $e->{'target'}) or
	$self->logcroak(q{Both source and target should be defined for an edge});
      $g->add_edge($e->{'source'}, $e->{'target'});
    }
    @nodes = @{$jgraph->{'graph'}->{'nodes'}};
  }

  $g->edges()  or $self->logcroak(q{No edges});
  $g->is_dag() or $self->logcroak(q{Graph is not DAG});

  foreach my $n ( @nodes ) {
    ($n->{'id'} and $n->{'label'}) or
      $self->logcroak(q{Both id and label should be defined for a node});
    my $id = $n->{'id'};
    if ( !$g->has_vertex($id) ) {
      $self->logcroak(qq{Vertex for node $id is missing});
    }
    $g->set_vertex_attribute($id, 'label', $n->{'label'});
  }

  return $g;
}

has q{_log_file_name} => (
   isa           => q{Str},
   is            => q{ro},
   lazy_build    => 1,
);
sub _build__log_file_name {
  my $self = shift;
  my $log_name = $self->script_name . q{_} . $self->id_run();
  $log_name .= q{_} . $self->timestamp() . q{.log};
  # If $self->script_name includes a directory path, change / to _
  $log_name =~ s{/}{_}gmxs;
  return $log_name;
}

=head2 log_file_path

Suggested log file full path.

=cut

sub log_file_path {
  my $self = shift;
  return catfile($self->runfolder_path(), $self->_log_file_name);
}

has q{_cloned_attributes} => (
  isa        => q{HashRef},
  is         => q{ro},
  init_arg   => undef,
  lazy_build => 1,
);
sub _build__cloned_attributes {
  my $self = shift;
  # Using MooseX::AttributeCloner functionality
  return $self->attributes_as_hashref({
    excluded_attributes => [ qw( interactive
                                 script_name
                                 function_list
                                 function_list_conf
                                 function_order
                                 function_graph
                                 spider ) ],
  });
}

has q{_registry} => (
  isa        => q{npg_pipeline::pluggable::registry},
  is         => q{ro},
  init_arg   => undef,
  lazy_build => 1,
);
sub _build__registry {
  return npg_pipeline::pluggable::registry->new()
}

sub _lsf_job_complete_requirements {
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

sub _string_job_ids2list {
  my $string_ids = shift;
  my @ids = split /$LSF_JOB_IDS_DELIM/smx, $string_ids;
  return @ids;
}

sub _list_job_ids2string {
  my @ids = @_;
  return @ids ? join $LSF_JOB_IDS_DELIM, @ids : q[];
}

sub _lsf_predesessors {
  my ($g, $v) = @_;
  #####
  # Recursive function. The recursion ends when we either
  # reach the start point - the vertext that has no predesessors -
  # or a vertex whose all predesessors are LSF jobs.
  #
  my @lsf_job_ids = ();
  foreach my $p (sort $g->predecessors($v)) {
    if ($g->has_vertex_attribute($p, $VERTEX_LSF_JOB_IDS_ATTR_NAME)) {
      push @lsf_job_ids, _string_job_ids2list(
        $g->get_vertex_attribute($p, $VERTEX_LSF_JOB_IDS_ATTR_NAME));
    } else {
      push @lsf_job_ids, _lsf_predesessors($g, $p);
    }
  }
  return @lsf_job_ids;
}

sub _resume {
  my $self = shift;

  my $g = $self->function_graph();
  my $suspended_start_job_id = $g->get_vertex_attribute(
     $SUSPENDED_START_FUNCTION, $VERTEX_LSF_JOB_IDS_ATTR_NAME);
  #####
  # If the pipeline_start job was used to keep all submitted jobs in pending
  # state and if the value of the interactive attribute is false, resume
  # the pipeline_start job thus allowing the pipeline jobs to start running.
  #
  if ($suspended_start_job_id) {
    $self->info(qq{Suspended start job, id $suspended_start_job_id});
    if (!$self->interactive) {
      $self->info(qq{Resuming start job, id $suspended_start_job_id});
      $self->submit_bsub_command(qq{bresume $suspended_start_job_id});
    }
  } else {
    $self->warn(q{No suspended start job.});
  }

  return;
}

sub _kill_jobs {
  my $self = shift;

  if ($self->has_function_graph()) {
    my $g = $self->function_graph();
    my @job_ids =
      reverse
      sort { $a <=> $b }
      map  { _string_job_ids2list($_) }
      grep { $g->get_vertex_attribute($_, $VERTEX_LSF_JOB_IDS_ATTR_NAME) }
      grep { $g->has_vertex_attribute($_, $VERTEX_LSF_JOB_IDS_ATTR_NAME) ? $_ : q[] }
      grep { $_ ne $SUSPENDED_START_FUNCTION }
      $g->vertices();

    if (@job_ids) {
      my $all_jobs = join q{ }, @job_ids;
      $self->info(q{Will try to kill submitted jobs with following ids: },
                  $all_jobs);
      $self->submit_bsub_command(qq{bkill -b $all_jobs});
    } else {
      $self->info(q{Early failure, no jobs to kill});
    }
  } else {
    $self->info(q{Early failure, function graph is not available, no jobs to kill});
  }
  return;
}

sub _clear_env_vars {
  my $self = shift;
  foreach my $var_name (npg_pipeline::cache->env_vars()) {
    if ($ENV{$var_name}) {
      $self->warn(qq[Unsetting $var_name]);
      $ENV{$var_name} = q{}; ## no critic (Variables::RequireLocalizedPunctuationVars)
    }
  }
  return;
}

sub _run_function {
  my ($self, $function_name) = @_;

  my $implementor = $self->_registry()->get_function_implementor($function_name);
  my $module = join q[::], 'npg_pipeline', 'function', $implementor->{'module'};
  load_class $module;
  my $method_name = $implementor->{'method'};
  my $params = $implementor->{'params'} || {};

  #####
  # Pass pipeline's built attributes to the function implementor
  # object. Both classes inherit from npg_pipeline::base, so
  # they have many attributes in common.
  #
  my %attrs = %{$self->_cloned_attributes()};

  #####
  # Use some function-specific attributes that we received
  # from the registry.
  #  
  while (my ($key, $value) = each %{$params}) {
    $attrs{$key} = $value;
  }

  #####
  # Instantiate the function implementor object, call on it the
  # method whose name we received from the registry, return
  # the result.
  #    
  return $module->new(\%attrs)->$method_name();
}

sub _schedule_functions {
  my $self = shift;

  my $g = $self->function_graph();

  #####
  # Topological ordering of a directed graph is a linear ordering of
  # its vertices such that for every directed edge uv from vertex u
  # to vertex v, u comes before v in the ordering, see 
  # https://en.wikipedia.org/wiki/Topological_sorting
  #
  # We need to run some of the functions in the very beginning since
  # they create the analysis directory structure the rest of the job
  # submission code relies on. The graph should be defined in a way
  # that guarantees that topological sort returns functions in 
  # correct order. Also, we need upstream LSF job's ids.
  #
  my @functions = $g->topological_sort();
  $self->info(q{Functions will be called in the following order: } .
                join q[, ], @functions);

  #####
  # Submit the functions for execution.
  #
  # In this implementation LSF jobs are created during function
  # execution. Store returned LSF job ids for further use.
  #
  # Some functions are executed immediately without creating
  # LSF job. Some functions decide not to execute either because
  # the pipeline was called with a flag that prevents them from
  # being executed or because they have to be executed only in
  # a specific context (for example, for human samples only).
  #
  foreach my $function (@functions) {
    my $function_name = $g->get_vertex_attribute($function, 'label');
    if (!$function_name) {
      $self->logcroak(qq{No label for vertex $function});
    }
    $self->info(q{***** Processing }.$function.q{ *****});
    #####
    # Need ids of upstream LSF jobs in order to set correctly dependencies
    # between LSF jobs
    #
    my $dependencies = q[];
    if (!$g->is_source_vertex($function)) {
      my @depends_on = _lsf_predesessors($g, $function);
      if (!@depends_on) {
        $self->logcroak(qq{"$function" should depend on at least one LSF job});
      }
      $dependencies = $self->_lsf_job_complete_requirements(@depends_on);
      $self->info(sprintf q{Setting dependencies for function "%s" to %s},
                              $function, $dependencies);
    }

    #####
    # We've removed a long chain of passing the dependencies to the code
    # that submits the LSF job in a hope that the job can be modified
    # once submitted. This takes hours for large arrays, so, as a temporary
    # measure, we have to restore previously available functionality in a
    # rudimentary way. This will not be necessary once LSF job submission
    # and function definition are properly separated.
    #

    ##no critic (Variables::ProhibitLocalVars)
    local $LSFJOB_DEPENDENCIES = $dependencies;
    my @ids = $self->_run_function($function_name);
    ##use critic
    my $job_ids = _list_job_ids2string(@ids);
    if ($job_ids) {
      $self->info(qq{Saving job ids: ${job_ids}\n});
      $g->set_vertex_attribute($function, $VERTEX_LSF_JOB_IDS_ATTR_NAME, $job_ids);
    } else {
      $self->info(q{Function was either not executed or did not create an LSF job});
    }
  }

  return;
}

=head2 run_spider

Generates cached metadata that are needed by the pipeline
or reuses the existing cache.

Will set the relevant env. variables in the global scope.

The new cache is created in the analysis_path directory.

See npg_pipeline::cache for details.

=cut

sub run_spider {
  my $self = shift;
  try {
    my $cache = npg_pipeline::cache->new(
      'id_run'           => $self->id_run,
      'set_env_vars'     => 1,
      'cache_location'   => $self->analysis_path,
      'lims_driver_type' => $self->lims_driver_type,
      'id_flowcell_lims' => $self->id_flowcell_lims,
      'flowcell_barcode' => $self->flowcell_id
    );
    $cache->setup();
    $self->info(join qq[\n], @{$cache->messages});
  } catch {
    $self->logcroak(qq[Error while spidering: $_]);
  };
  return;
}

=head2 prepare

 Actions that have to be performed by the pipeline before the functions can
 be called, for example, creation of pipeline-specific directories.
 In this module some envronment variables ar eprinted to the log by this method.

=cut
sub prepare {
  my $self = shift;
  foreach my $name (qw/PATH CLASSPATH PERL5LIB/) {
    my $value = $ENV{$name} || q{Not defined};
    $self->info(sprintf '*** %s: %s', $name, $value);
  }
  if ($self->spider) {
    $self->info('Running spider');
    $self->run_spider();
  } else {
    $self->info('Not running spider');
  }
  return;
}

=head2 main

 Runs the pipeline.

=cut
sub main {
  my $self = shift;

  my $error = q{};
  my $when = q{initializing pipeline};
  try {
    $self->prepare();
    $when = q{submitting jobs};
    $self->_schedule_functions();
    $self->_resume();
  } catch {
    $error = qq{Error $when: $_};
    $self->error($error);
    $self->_kill_jobs();
  };
  $self->_clear_env_vars();
  if ($error) {
    # This is the end of the pipeline script.
    # We want to see this error in the pipeline daemon log,
    # so it should be printed to standard error, not to
    # this script's log, which might be a file.
    # We currently tie STDERR so output to standard error
    # goes to this script's log file. Hence the need to
    # untie. Dies not cause an error if STDERR has not been
    # tied. 
    untie *STDERR;
    croak($error);
  }
  return;
}

no Moose;

1;
__END__


=head1 DESCRIPTION

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item MooseX::AttributeCloner

=item Carp

=item Graph::Directed

=item File::Spec::Functions

=item List::MoreUtils

=item Readonly

=item Try:Tiny

=item Class::Load

=item File::Slurp

=item English

=item JSON

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
