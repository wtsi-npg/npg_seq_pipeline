package npg_pipeline::executor::lsf;

use Moose;
use Sys::Filesystem::MountPoint qw(path_to_mount_point);
use File::Spec;
use Readonly;

use npg_tracking::util::abs_path qw(abs_path network_abs_path);

with qw( npg_pipeline::executor::lsf::options );

our $VERSION = '0';

$ENV{LSB_DEFAULTPROJECT} ||= q{pipeline};

Readonly::Scalar my $DEFAULT_JOB_ID_FOR_NO_BSUB => 50;
Readonly::Scalar my $VERTEX_LSF_JOB_IDS_ATTR_NAME => q[lsf_job_ids];
Readonly::Scalar my $LSF_JOB_IDS_DELIM            => q[-];
Readonly::Scalar my $SUSPENDED_START_FUNCTION => q[pipeline_start];
Readonly::Scalar my $END_FUNCTION             => q[pipeline_end];

=head1 NAME

npg_pipeline::executor::lsf

=head1 SYNOPSIS

Bits and pieces that are not supposed to work. Work in progress.

=head1 SUBROUTINES/METHODS

=cut


has runfolder_path => (
  is         => 'ro',
  isa        => 'Str',
  required   => 0,
);

has function_graph => (
  is         => 'ro',
  isa        => 'Obj',
  required   => 0,
);

=head2 _fs_resource

Returns the fs_resource for the given runfolder_path

=cut 

has _fs_resource => (
  is         => 'ro',
  isa        => 'Str',
  lazy_build => 1,
);
sub _build__fs_resource {
  my ($self) = @_;

  if ($ENV{TEST_FS_RESOURCE}) {
    return $ENV{TEST_FS_RESOURCE};
  }
  my $r = join '_', grep {$_} File::Spec->splitdir(
    network_abs_path path_to_mount_point($self->runfolder_path()));
  return join q(),$r=~/\w+/xsmg;
}

=head2 submit


=cut 

sub submit {
  # Stab
  return;
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

sub _some {
  my $self = shift;
  my $g = $self->function_graph;
  foreach my $function (qw//) {
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

    my @ids = $self->_run_function($function);

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

1;

__END__


=head1 DESCRIPTION

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item File::Spec

=item Readonly

=item npg_tracking::util::abs_path

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
