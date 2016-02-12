package npg_pipeline::pluggable;

use Moose;
use Carp;
use Try::Tiny;
use Readonly;

use npg_pipeline::dispatch_tree;
use npg_pipeline::cache;

extends q{npg_pipeline::base};

our $VERSION = '0';

Readonly::Scalar our $SUSPENDED_START_FUNCTION => q[lsf_start];
Readonly::Scalar our $END_FUNCTION             => q[lsf_end];

=head1 NAME

npg_pipeline::pluggable

=head1 SYNOPSIS

=head1 SUBROUTINES/METHODS

=head2 dispatch_tree

The npg_pipeline::dispatch_tree object, which internally stores the tree of dispatched jobs.

=cut
has q{dispatch_tree} => (
  isa        => q{npg_pipeline::dispatch_tree},
  is         => q{ro},
  lazy_build => 1,
  init_arg   => undef,
  metaclass  => q{NoGetopt},
);
sub _build_dispatch_tree {
  return npg_pipeline::dispatch_tree->new();
}

=head2 interactive

=cut
has q{interactive}  => (
  isa           => q{Bool},
  is            => q{ro},
  default       => 0,
  documentation =>
  q{Set to true to resume the start LSF job interactively. By default the start job is resumed once all jobs have been successfully submitted},
);

=head2 function_order

=cut
has q{function_order} => (
  isa           => q{ArrayRef},
  is            => q{ro},
  lazy_build    => 1,
  documentation =>
  q{A reference to an array of function names in the order they should run. Defaults to a list from a configuration file.},
);
sub _build_function_order {
  my $self = shift;

  my $parent = __PACKAGE__;
  my $module = ref $self;

  my $fo = [];
  if ($module ne $parent && $module =~ /^$parent/mxs) {
    $fo =  $self->function_list_conf() || [];
  }
  return $fo;
}
around 'function_order' => sub {
  my $orig = shift;
  my $self = shift;
  my $fo = $self->$orig();
  if ( !@{$fo} || $fo->[0] ne $SUSPENDED_START_FUNCTION ) {
    unshift @{$fo}, $SUSPENDED_START_FUNCTION;
  }
  if ( $fo->[-1] ne $END_FUNCTION ) {
    push @{$fo}, $END_FUNCTION;
  }
  return $fo;
};

=head2 parallelise

Sets of functions that can be run by LSF in parallel.

  my $hParallelise = {
    a => {func_a => 1, func_b => 1,},
    b => {func_g => 1, func_h => 1, func_i => 1,},
  };
  my $class_object = __PACKAGE__->new(parallelise => $hParallelise);

This would mean that func_a and func_b would be submitted to LSF with no dependency on each other,
and, likewise, func_g, func_h and func_i.

=cut
has q{parallelise}  => (
  isa        => q{HashRef},
  is         => q{ro},
  lazy_build => 1,
  metaclass  => q{NoGetopt},
);
sub _build_parallelise {
  my $self = shift;
  return $self->parallelisation_conf();
}

=head2 lsf_job_complete_requirements - takes an array of job ids, and returns a -w'done(X) && done(Y) ....' string for use in lsf job submissions

  my $sLSFJobCompleteRequirements = $class->lsf_job_complete_requirements(@JobIds);

note, this can just take the string returned from a bsub job, as it parses for the number in the string

=cut
sub lsf_job_complete_requirements {
  my ($self, @job_ids) = @_;

  if (scalar@job_ids == 0) {
    return q{}; # jobs won't submit with -w'', so needs to be an empty string if there are no dependencies
  }

  foreach my $job_id (@job_ids) {
    # convert to a done requirement
    $job_id = qq{done($job_id)};
  }

  # return the string which will be inserted as the requirement option
  return q{-w'}.(join q{ && }, @job_ids).q{'};
}

=head2 _finish

Logs information about submitted jobs.

If the lsf_start job was used to keep all submitted jobs in pending state and if the 
value of the interactive attribute is fasle, resumes the lsf_start job thus allowing
the pipeline jobs to start runing.

=cut
sub _finish {
  my $self = shift;

  my @job_ids = $self->dispatch_tree->ordered_job_ids;
  $self->log(q{Total LSF jobs submitted: } . scalar @job_ids);
  $self->log(qq{JSON Dispatch Tree:\n}.$self->dispatch_tree()->tree_as_json().qq{\n});

  if ($self->dispatch_tree->first_function_name eq $SUSPENDED_START_FUNCTION ) {
    my $start_job_id = $job_ids[0];
    $self->log(qq{Suspended start job id $start_job_id});
    if (!$self->interactive) {
      $self->log(q{Resuming start job});
      $self->submit_bsub_command("bresume $start_job_id");
    }
  }

  return;
}

=head2 _kill_jobs

Kills all submitted lsf jobs starting from most recently submitted,
apart from the lsf_start job, which remains suspended.

=cut
sub _kill_jobs {
  my $self = shift;

  my @job_ids = $self->dispatch_tree->ordered_job_ids;
  if (@job_ids) {
    if ($self->dispatch_tree->first_function_name eq $SUSPENDED_START_FUNCTION ) {
      my $start_job_id = shift @job_ids;
    }
    if (@job_ids) {
      @job_ids = reverse @job_ids;
      my $all_jobs = join q{ }, @job_ids;
      $self->log(qq{Will try to kill submitted jobs with following ids: $all_jobs});
      $self->submit_bsub_command("bkill -b $all_jobs");
    }
  }
  return;
}

=head2 _clear_env_vars

Unsets some env variables.

=cut
sub _clear_env_vars {
  my $self = shift;
  foreach my $var_name (npg_pipeline::cache->env_vars()) {
    if ($ENV{$var_name}) {
      $self->log( qq[Unsetting $var_name] );
      $ENV{$var_name} = q{}; ## no critic (Variables::RequireLocalizedPunctuationVars)
    }
  }
  return;
}

=head2 _token_job

Submits a /bin/true job to lsf short queue. Returns the id of the submitted job as a list.

 (my $id) = $obj->_token_job('function_name', 'optional_string_of dependencies');

=cut
sub _token_job {
  my ($self, $function_name, $lsf_dependencies) = @_;

  $lsf_dependencies |= q{};
  my $suspend_flag = $function_name eq $SUSPENDED_START_FUNCTION ? q{-H} : q{};
  my $runfolder_path = $self->runfolder_path();
  my $job_name = join q{_}, $function_name, $self->id_run(), $self->pipeline_name();
  my $out = join q{_}, $function_name, $self->timestamp, q{%J.out};
  $out = join q{/}, $runfolder_path, $out;
  my $cmd = qq{bsub $suspend_flag $lsf_dependencies -q } . $self->small_lsf_queue() . qq{ -J $job_name -o $out '/bin/true'};
  my $job_id = $self->submit_bsub_command($cmd);
  ($job_id) = $job_id =~ m/(\d+)/ixms;
  return ($job_id);
}

=head2 lsf_start

First function that might be called implicitly by the pipeline.
Submits a suspended token job to LSF. The user-defined functions that are run as LSF jobs will
depend on the successful complition of this job. Therefore, the pipeline jobs will stay pending till
the start job is resumed and gets successfully completed.

=cut
sub lsf_start {
  my $self = shift;
  return $self->_token_job($SUSPENDED_START_FUNCTION);
}

=head2 lsf_end

Last function that might be called implicitly by the pipeline.
Submits a token job to LSF. 

=cut
sub lsf_end {
  my ($self, @args) = @_;
  my $required_job_completion = shift @args;
  return $self->_token_job($END_FUNCTION, $required_job_completion);
}

=head2 schedule_functions

Schedules and submits the function for execution.

=cut

sub schedule_functions {
  my $self = shift;

  my @args = ();
  my @functions_to_run_in_order = @{$self->function_order()};
  my %parallelise_functions     = %{ $self->parallelise() };

  my @job_ids;
  my @hold_parallel_ids;

  my $add_to_hold;
  foreach my $function (@functions_to_run_in_order) {

    $self->log(q{***** Processing }.$function.q{ *****});
    my $job_reqs = $self->lsf_job_complete_requirements(@job_ids);
    $self->log(qq{$job_reqs for $function});
    unshift @args, $job_reqs;
    ##############
    # if the previous function was parallelisable
    my $parallel_done;
    if ($add_to_hold) {

      my $parallelise_string = q{};
      foreach my $function (sort keys %{$parallelise_functions{$add_to_hold}}) {
        $parallelise_string .= qq{$function }
      }

      ###############
      # check if this function is also part of the same parallelisable group
      if($parallelise_functions{$add_to_hold}->{$function}) {
        ###############
        # if so, then run, and capture the job ids
        $self->log(qq{Able to parallelise $parallelise_string});
        my $job_deps = $args[0];
        my @jids = $self->$function(@args);
        push @hold_parallel_ids, @jids;
        $self->dispatch_tree->append_to_functions({ function => $function,
            job_ids_launched => \@jids,
            job_dependencies => $job_deps,
          });
        $parallel_done++;
      } else {
        $self->log(qq{$function not parallelisable with $parallelise_string\n\tremoving parallelisable status and setting job id requirements for next level});
        $add_to_hold = q{};
        if (scalar @hold_parallel_ids) {
          @job_ids = @hold_parallel_ids;
        }
        $job_reqs = $self->lsf_job_complete_requirements(@job_ids);
        $self->log(qq{$job_reqs for $function});
        shift @args;
        unshift @args, $job_reqs;
        @hold_parallel_ids = ();
      }
    }
    if (!$parallel_done) {
      my $done;
      foreach my $key (sort keys%parallelise_functions) {
        if($parallelise_functions{$key}->{$function}) {
          $add_to_hold = $key;
          my $parallelise_string = q{};
          foreach my $function (sort keys %{$parallelise_functions{$add_to_hold}}) {
            $parallelise_string .= qq{$function }
          }
          $self->log(qq{Able to parallelise $parallelise_string});
          my $job_deps = $args[0];
          my @jids = $self->$function(@args);        # call the function <--------- !!!
          push @hold_parallel_ids, @jids;
          $self->dispatch_tree->append_to_functions({ function => $function,
            job_ids_launched => \@jids,
            job_dependencies => $job_deps,
          });
          $done++;
          last;
        }
      }
      if (!$done) {
        $self->log(qq{$function is not parallelisable});
        my $job_deps = $args[0];
        my @returned_job_ids = $self->$function(@args);        # call the function <--------- !!!
        if (scalar @returned_job_ids) {
          @job_ids = @returned_job_ids;
        }
        $self->dispatch_tree->append_to_functions({ function => $function,
          job_ids_launched => \@returned_job_ids,
          job_dependencies => $job_deps,
        });
      }
    }
  }

  return 1;
}

=head2 prepare

 Actions that have to be performed by the pipeline before the functions can
 be called, for example, creation of pipeline-specific directories. Does
 nothing in this module.

=cut
sub prepare {
  my $self = shift;
  if ($self->verbose) {
    my $s = '***************************************************';
    $self->log("\n" . $s);
    foreach my $name (qw/PATH CLASSPATH PERL5LIB/) {
      $self->log(sprintf '*** %s: %s', $name, $ENV{$name});
    }
    $self->log($s . "\n");
  }
  return;
}

=head2 main

 Runs the pipeline.

=cut
sub main {
  my $self = shift;

  my $error = q{};
  my $when = q[initializing pipeline];
  try {
    $self->prepare();
    $when = q[submitting jobs];
    $self->schedule_functions();
    $self->_finish();
  } catch {
    $error = qq{Error $when: $_};
    $self->log($error);
    $self->_kill_jobs();
  };
  $self->_clear_env_vars();
  if ($error) {
    ##no critic (InputOutput::RequireCheckedSyscalls)
    print {*STDOUT} $error;
    croak $error;
  }
  return;
}

no Moose;

1;
__END__


=head1 DESCRIPTION

This is a superclass to a pluggable::schema module which should be created to run your schema. The objective
is that you will create a 'flag_waver' pluggable module which will control running functions in a determined
array order, but that those functions can (in theory) be placed in whichever order the user chooses.

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Carp

=item Readonly

=item Moose

=item Try:Tiny

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown
Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2016 Genome Research Ltd

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
