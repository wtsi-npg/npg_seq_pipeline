package npg_pipeline::pluggable;

use Moose;
use Carp;
use Try::Tiny;
use Graph::Directed;
use List::MoreUtils qw/ any uniq /;
use Readonly;

use npg_pipeline::cache;

extends q{npg_pipeline::base};

our $VERSION = '0';

Readonly::Scalar my $SUSPENDED_START_FUNCTION => q[pipeline_start];
Readonly::Scalar my $END_FUNCTION             => q[pipeline_end];
Readonly::Scalar my $VERTEX_LSF_JOB_IDS_ATTR_NAME => q[lsf_job_ids];
Readonly::Scalar my $LSF_JOB_IDS_DELIM            => q[-];

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
  q{If false, the pipeline_start job is resumed once all jobs have been successfully submitted},
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

=head2 function_graph

=cut
has 'function_graph' => (
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

  $g->edges()  or croak q{No edges};
  $g->is_dag() or croak q{Graph is not DAG};

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
  if (!$string_ids) {
    croak 'Should have a non-empty string';
  }
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

sub _set_lsf_job_dependencies {
  my $self = shift;

  my $g = $self->function_graph();
  my $suspended_start_job_id;

  #####
  # Examine the graph, set dependencies between LSF jobs,
  # resume all jobs, apart from the one we reserved.
  #
  foreach my $current ($g->topological_sort()) {

    my $this_job_id;
    if ($g->has_vertex_attribute($current, $VERTEX_LSF_JOB_IDS_ATTR_NAME) ) {
      $this_job_id = $g->get_vertex_attribute(
                     $current, $VERTEX_LSF_JOB_IDS_ATTR_NAME);
    } else {
      next;
    }

    if ($g->is_source_vertex($current)) {
      if ($current ne $SUSPENDED_START_FUNCTION) {
        $self->warn(qq{Resuming source vertex "$current"!});
        $self->submit_bsub_command(qq{bresume $this_job_id});
      } else {
        $suspended_start_job_id = $this_job_id;
      }
    } else {
      my @depends_on = _lsf_predesessors($g, $current);
      if (!@depends_on) {
        $self->logcroak(qq{No dependencies for function "$current"});
      }
      my $dependencies = $self->_lsf_job_complete_requirements(@depends_on);
      # This function could have submitted multiple jobs.
      foreach my $j (_string_job_ids2list($this_job_id)) {
        $self->info(sprintf q{Setting dependencies for function "%s" job %s to %s},
                              $current, $j, $dependencies );
        $self->submit_bsub_command(qq{bmod $dependencies $j});
        $self->info(qq{Resuming job $j for function "$current"});
        $self->submit_bsub_command(qq{bresume $j});
      }
    }
  }

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

sub _token_job {
  my ($self, $function_name) = @_;
  my $runfolder_path = $self->runfolder_path();
  my $job_name = join q{_}, $function_name, $self->id_run(), $self->pipeline_name();
  my $out = join q{_}, $function_name, $self->timestamp, q{%J.out};
  $out = join q{/}, $runfolder_path, $out;
  my $cmd = q{bsub -q } . $self->small_lsf_queue() . qq{ -J $job_name -o $out '/bin/true'};
  my $job_id = $self->submit_bsub_command($cmd);
  ($job_id) = $job_id =~ m/(\d+)/ixms;
  return ($job_id);
}

=head2 pipeline_start

First function that might be called by the pipeline.
Submits a suspended token job to LSF. The user-defined functions that are run
as LSF jobs will depend on the successful complition of this job. Therefore,
the pipeline jobs will stay pending till the start job is resumed and gets
successfully completed.

=cut
sub pipeline_start {
  my $self = shift;
  return $self->_token_job($SUSPENDED_START_FUNCTION);
}

=head2 pipeline_end

Last 'catch all' function that might be called by the pipeline.
Submits a token job to LSF. 

=cut
sub pipeline_end {
  my $self = shift;
  return $self->_token_job($END_FUNCTION);
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
  # correct order.
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
    my @ids = $self->$function_name();
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
    $self->_set_lsf_job_dependencies();
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

=item Carp

=item Graph::Directed

=item List::MoreUtils

=item Readonly

=item Try:Tiny

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
