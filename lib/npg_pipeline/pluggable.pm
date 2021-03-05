package npg_pipeline::pluggable;

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;
use Carp;
use Try::Tiny;
use Graph::Directed;
use File::Spec::Functions qw{catfile splitpath};
use File::Basename;
use File::Copy;
use Class::Load qw{load_class};
use File::Slurp;
use JSON qw{from_json};
use Readonly;
use English qw{-no_match_vars};

use npg_tracking::util::abs_path qw{abs_path};
use npg_pipeline::cache;
use npg_pipeline::pluggable::registry;

extends q{npg_pipeline::base};

with qw{ MooseX::AttributeCloner
         npg_pipeline::executor::options
         npg_pipeline::runfolder_scaffold };

our $VERSION = '0';

Readonly::Scalar my $SUSPENDED_START_FUNCTION => q[pipeline_start];
Readonly::Scalar my $END_FUNCTION             => q[pipeline_end];
Readonly::Array  my @FLAG2FUNCTION_LIST       => qw/ qc_run /;
Readonly::Scalar my $FUNCTION_DAG_FILE_TYPE   => q[.json];
Readonly::Scalar my $DEFAULT_EXECUTOR_TYPE    => q[lsf];

=head1 NAME

npg_pipeline::pluggable

=head1 SYNOPSIS

=head1 SUBROUTINES/METHODS

=cut

##################################################################
################## Public attributes, ############################
###### which will be available as script arguments ###############
########## unless their metaclass is NoGetopt, ###################
##################################################################
################## and public methods ############################
##################################################################

############## All about the main pipeline log ###################

=head2 log_file_name

The name for the log file of this pipeline script.

=cut

has q{log_file_name} => (
  isa        => q{Str},
  is         => q{ro},
  lazy_build => 1,
  documentation =>
  q{The name for the log file of this pipeline script.},
);
sub _build_log_file_name {
  my $self = shift;

  my $name;
  if ($self->_has_log_file_path) {
    ($name) = fileparse $self->log_file_path;
  } else {
    $name = $self->_output_file_name_root() . q{.log};
  }

  return $name;
}

=head2 log_file_dir

The directory for the log file of this pipeline script.

=cut

has q{log_file_dir} => (
  isa        => q{Str},
  is         => q{ro},
  lazy_build => 1,
  documentation =>
  q{The directory for the log file of this pipeline script.},
);
sub _build_log_file_dir {
  my $self = shift;

  my $dir;
  if ($self->_has_log_file_path) {
    my $name;
    ($name, $dir) = fileparse $self->log_file_path;
    $dir =~ s{/\Z}{}smx;
  } else {
    $dir = $self->runfolder_path();
  }

  return $dir;
}

=head2 log_file_path

The full path for the log file of this pipeline script.
Computed from log_file_dir and log_file_name.

=cut

has q{log_file_path} => (
  isa           => q{Str},
  is            => q{ro},
  predicate     => '_has_log_file_path',
  lazy_build    => 1,
  documentation =>
  q{The full path for the log file of this pipeline script.},
);
sub _build_log_file_path {
  my $self = shift;
  return catfile($self->log_file_dir(), $self->log_file_name);
}

############## All about functions ###############################

=head2 function_order

A reference to an array of function names run. An optional attribute.
If set, will be used in preference to a graph defined in a file
(see function_list attribute).

=cut

has q{function_order} => (
  isa           => q{ArrayRef},
  is            => q{ro},
  predicate     => q{has_function_order},
  documentation =>
  q{A reference to an array of function names in the order they should run.},
);

=head2 function_list

A lazy-build attribute with a wrapper around it. Is set to an 
absolute path to a JSON file where the function graph is defined.
Can be supplied as a hint for finding the file, will be resolved to
an absolute path.

For example, the following works for archival pipeline

 npg_pipeline::pluggable->new_with_options(
   function_list => 'post_qc_review');

=cut

has q{function_list} => (
  isa           => q{Str},
  is            => q{ro},
  lazy_build    => 1,
  documentation =>
  q{An absolute path to a JSON file where the function graph is defined or a hint},
);
sub _build_function_list {
  my $self = shift;
  my $suffix = q();
  foreach my $flag (@FLAG2FUNCTION_LIST) {
    if ($self->can($flag) && $self->$flag) {
      $suffix .= "_$flag";
    }
  }
  return $self->_pipeline_name . $suffix;
}
around q{function_list} => sub {
  my $orig = shift;
  my $self = shift;

  my $v = $self->$orig();

  my $file = abs_path($v);
  if (!$file || !-f $file) {
    if ($v !~ /\A\w+\Z/smx) {
      $self->logcroak("Bad function list name: $v");
    }
    try {
      $file = $self->conf_file_path((join q[_],'function_list',$v) . $FUNCTION_DAG_FILE_TYPE);
    } catch {
      my $pipeline_name = $self->_pipeline_name;
      if ($v !~ /^$pipeline_name/smx) {
        $file = $self->conf_file_path(
          (join q[_],'function_list',$self->_pipeline_name,$v) . $FUNCTION_DAG_FILE_TYPE);
      } else {
        $self->logcroak($_);
      }
    };
  }

  return $file;
};

=head2 function_graph

A Graph::Directed object representing the functions to be run
as a directed acyclic graph (DAG).

=cut

has q{function_graph} => (
  isa        => q{Graph::Directed},
  is         => q{ro},
  lazy_build => 1,
  init_arg   => undef,
  metaclass  => q{NoGetopt},
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
    my $jgraph = $self->_function_list_conf();
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

=head2 function_definitions

A hash reference of function definitions hashed by function ids.
The values are arrays of npg_pipeline::function::definition objects.

=cut

has q{function_definitions} => (
  isa        => q{HashRef},
  is         => q{ro},
  init_arg   => undef,
  default    => sub {return {};},
  metaclass  => q{NoGetopt},
);

############## All about job execution ###########################

=head2 execute

A boolean flag turning on/off transferring the graph to the executor,
true by default.

=cut

has q{execute} => (
  isa           => q{Bool},
  is            => q{ro},
  default       => 1,
  documentation =>
  q{A flag turning on/off execution, true by default},
);

=head2 executor_type

Executor type. By default commands will be submitted to LSF.
Can be specified in the general configuration file.

=cut

has q{executor_type} => (
  isa           => q{Str},
  is            => q{ro},
  lazy_build    => 1,
  documentation => q{Executor type, defaults to lsf},
);
sub _build_executor_type {
  my $self = shift;
  my $et = $self->general_values_conf()->{'executor_type'};
  return $et ? $et : $DEFAULT_EXECUTOR_TYPE;
}

=head2 executor

Executor object of type specified by the executor_type attribute.

=cut

has q{executor} => (
  isa        => q{Object},
  is         => q{ro},
  init_arg   => undef,
  lazy_build => 1,
  metaclass  => q{NoGetopt},
);
sub _build_executor {
  my $self = shift;

  my $type = $self->executor_type;
  my $module = join q[::], 'npg_pipeline', 'executor', $type;
  load_class $module;
  my $attrs = $self->_common_attributes($module);
  #####
  # The following attributes are not recognised by MooseX::AttributeCloner;
  # NoGetopt metaclass might be the reason. So copying by reference explicitly.
  #
  for my $aname (qw/ function_graph function_definitions /) {
    $attrs->{$aname} = $self->$aname;
  }

  my $ext = $type eq $DEFAULT_EXECUTOR_TYPE ? # LSF
                     $FUNCTION_DAG_FILE_TYPE : q[.txt];
  $attrs->{'commands4jobs_file_path'} =
    $self->_output_file_path(join q[], q[.commands4], uc $type, q[jobs], $ext);

  return $module->new($attrs);
}

############## Everything else ##################################

=head2 BUILD

Called by Moose at the end of object instantiation.
Builds 'local' flag so that it can be passed to functions.

=cut

sub BUILD {
  my $self = shift;
  $self->local();
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
    $when = q{running functions};
    $self->_schedule_functions();
    $when = q{saving definitions};
    $self->_save_function_definitions();
    if ($self->execute()) {
      $self->info(sprintf q{***** Definitions will be submitted for execution to %s *****},
                          uc $self->executor_type());
      $when = q{submitting for execution};
      $self->executor()->execute($self->function_graph, $self->function_definitions);
    } else {
      $self->info(q{***** Submission for execution is switched off *****});
    }
  } catch {
    $error = qq{Error $when: $_};
    $self->error($error);
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
  }

  $self->_create_log_link();

  $error and croak $error;

  return;
}

=head2 spider

Toggles spider (creating/reusing cached LIMs data), true by default.

=cut

has q{spider} => (
  isa           => q{Bool},
  is            => q{ro},
  default       => 1,
  documentation => q{Toggles creating/reusing cached LIMs data, true by default},
);

=head2 prepare

Actions that have to be performed by the pipeline before the functions
can be called, for example, creation of pipeline-specific directories.
In this module some envronment variables are printed to the log by this
method, then, if spider functionality is enabled, LIMs data are cached.

=cut

sub prepare {
  my $self = shift;

  ######
  # Build analysis_path attribute
  #
  $self->analysis_path();

  foreach my $name (qw/PATH CLASSPATH PERL5LIB/) {
    my $value = $ENV{$name} || q{Not defined};
    $self->info(sprintf '*** %s: %s', $name, $value);
  }
  $self->info('Using product configuration file ' . $self->product_conf_file_path);

  if ($self->spider) {
    $self->info('Running spider');
    $self->_run_spider();
  } else {
    $self->info('Not running spider');
  }
  return;
}

##################################################################
############## Private attributes ################################
##################################################################

has q{_function_list_conf} => (
  isa        => q{HashRef},
  is         => q{ro},
  lazy_build => 1,
  init_arg   => undef,
);
sub _build__function_list_conf {
  my $self = shift;
  my $input_file = $self->function_list;
  $self->info(qq{Reading function graph $input_file});
  return from_json(read_file($input_file));
}

has q{_script_name} => (
  isa      => q{Str},
  is       => q{ro},
  default  => $PROGRAM_NAME,
  init_arg => undef,
);

has q{_pipeline_name} => (
  isa        => q{Str},
  is         => q{ro},
  lazy_build => 1,
  init_arg   => undef,
  metaclass  => q{NoGetopt},
);
sub _build__pipeline_name {
  my $self = shift;
  my ($volume, $directories, $name) = splitpath($self->_script_name);
  $name =~ s/\Anpg_pipeline_//xms;
  return $name;
}

has q{_output_file_name_root} => (
  isa        => q{Str},
  is         => q{ro},
  lazy_build => 1,
);
sub _build__output_file_name_root {
  my $self = shift;
  my $name = join q[_], $self->_script_name, $self->label(), $self->random_string();
  # If $self->script_name includes a directory path, change / to _
  $name =~ s{/}{_}gmxs;
  return $name;
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
  return $self->attributes_as_hashref();
}

has q{_registry} => (
  isa      => q{npg_pipeline::pluggable::registry},
  is       => q{ro},
  init_arg => undef,
  default  => sub {return npg_pipeline::pluggable::registry->new();},
);

##################################################################
############## Private methods ###################################
##################################################################

sub _output_file_path {
  my ($self, $suffix) = @_;
  $suffix ||= q[];
  return join q[/], $self->analysis_path(),
                    $self->_output_file_name_root() . $suffix;
}

sub _definitions_file_path {
  my $self = shift;
  return $self->_output_file_path(q[.definitions] . $FUNCTION_DAG_FILE_TYPE);
}

sub _clear_env_vars {
  my $self = shift;
  $self->info();
  foreach my $var_name (npg_pipeline::cache->env_vars()) {
    if ($ENV{$var_name}) {
      $self->info(qq[Unsetting $var_name]);
      $ENV{$var_name} = q{}; ## no critic (Variables::RequireLocalizedPunctuationVars)
    }
  }
  return;
}

sub _common_attributes {
  my ($self, $module) = @_;

  #####
  # Create and return a hash ref that contains a subset of
  # attributes (with values) of this class, which can be used
  # when constructing an instance of a class given by the argument.
  #
  my %attrs = %{$self->_cloned_attributes()};

  my $meta = $module->meta();
  foreach my $attr_name (keys %attrs) {
    if (!$meta->find_attribute_by_name($attr_name)) {
      delete $attrs{$attr_name};
    }
  }
  return \%attrs;
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
  my $attrs = $self->_common_attributes($module);

  #####
  # Use some function-specific attributes that we received
  # from the registry.
  #  
  while (my ($key, $value) = each %{$params}) {
    $attrs->{$key} = $value;
  }

  #####
  # Instantiate the function implementor object, call on it the
  # method whose name we received from the registry, return
  # the result.
  #    
  return $module->new($attrs)->$method_name($self->_pipeline_name);
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

  foreach my $function (@functions) {
    my $function_name = $g->get_vertex_attribute($function, 'label');
    if (!$function_name) {
      $self->logcroak(qq{No label for vertex $function});
    }
    $self->info(qq{***** Processing $function *****});
    my $definitions = $self->_run_function($function_name);
    if (!$definitions || !@{$definitions}) {
      $self->logcroak(q{At least one definition should be returned});
    }
    $self->function_definitions->{$function} = $definitions;
  }

  return;
}

sub _save_function_definitions {
  my $self = shift;

  my $file = $self->_definitions_file_path();
  $self->info( q[]);
  $self->info(qq[***** Writing function definitions to ${file}]);
  $self->info( q[]);
  my $json = JSON->new->convert_blessed->canonical;
  return write_file($file,
                    $json->pretty->encode($self->function_definitions));
}

#####
# Generates cached metadata that are needed by the pipeline
# or reuses the existing cache.
# Will set the relevant env. variables in the global scope.
# The new cache is created in the analysis_path directory.
# See npg_pipeline::cache for details.
#
sub _run_spider {
  my $self = shift;

  #####
  # We assume that the samplesheet is available. Can generate it,
  # but difficult to decide on a standard location.
  #
  return if $self->has_product_rpt_list;

  #####
  # We have a standard samplesheet loation for a run.
  #
  try {
    my $cache = npg_pipeline::cache->new(
      'id_run'           => $self->id_run,
      'set_env_vars'     => 1,
      'cache_dir_path'   => $self->metadata_cache_dir_path(),
      'id_flowcell_lims' => $self->id_flowcell_lims,
    );
    $cache->setup();
    $self->info(join qq[\n], @{$cache->messages});
  } catch {
    $self->logcroak(qq[Error while spidering: $_]);
  };

  return;
}

#####
# Attempts to create in the analysis directory a hard link to
# or, if unsuccessful, a copy of the log file of the pipeline.
# All errors are captured. Best effort.
#
sub _create_log_link {
  my $self = shift;

  (-e $self->analysis_path) or return;

  my $link = catfile($self->analysis_path, $self->log_file_name);
  if (!-e $link) {
    try {
      my $linked = 1;
      if (not link $self->log_file_path, $link) {
        # If cannot hard link, just copy.
        $linked = copy $self->log_file_path, $link;
      }
      $linked and $self->info("Created link or copy $link for the log file");
    } catch {
      $self->warn("Error creating a link or copy: $_");
    };
  } else {
    $self->warn("Will not overwrite existing file $link");
  }

  return;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 DESCRIPTION

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item MooseX::StrictConstructor

=item namespace::autoclean

=item MooseX::AttributeCloner

=item Carp

=item Graph::Directed

=item File::Spec::Functions

=item File::Basename

=item File::Copy

=item Readonly

=item Try:Tiny

=item Class::Load

=item File::Slurp

=item English

=item JSON

=item npg_tracking::util::abs_path

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

=over

=item Andy Brown

=item Marina Gourtovaia

=back

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2014,2015,2016,2017,2018,2019,2020,2021 Genome Research Ltd.

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
