package npg_pipeline::function::start_stop;

use Moose;
use namespace::autoclean;
use Readonly;
use Math::Random::Secure qw(irand);

use npg_pipeline::function::definition;
use npg_pipeline::runfolder_scaffold;

extends q{npg_pipeline::base};

our $VERSION = '0';

Readonly::Scalar my $NUM_MINS2WAIT => 20;

=head1 NAME

npg_pipeline::function::start_stop

=head1 SYNOPSIS

  my $f = npg_pipeline::function::start_stop->new(
    id_run => 1234,
    run_folder => q{123456_IL2_1234},
  );
  my $definitions;
  $definitions = $f->pipeline_start();
  $definitions = $f->pipeline_stop();
  $definitions = $f->wait4path();

=head1 DESCRIPTION

Definitions for token start and end pipeline steps and also for
other simple steps that do not change the data or the state.

=head1 SUBROUTINES/METHODS

=head2 pipeline_start

First function that might be called by the pipeline.
Creates and returns a token job definition.

=cut

sub pipeline_start {
  my ($self, $pipeline_name) = @_;
  return $self->_token_job($pipeline_name);
}

=head2 pipeline_end

Last 'catch all' function that might be called by the pipeline.
Creates and returns a token job definition. 

=cut

sub pipeline_end {
  my ($self, $pipeline_name) = @_;
  return $self->_token_job($pipeline_name);
}

sub _token_job {
  my ($self, $pipeline_name) = @_;

  my ($package, $filename, $line, $subroutine_name) = caller 1;
  ($subroutine_name) = $subroutine_name =~ /(\w+)\Z/xms;
  $pipeline_name ||= q[];
  my $job_name = join q{_}, $subroutine_name, $self->label(), $pipeline_name;

  my $d = npg_pipeline::function::definition->new(
    created_by    => __PACKAGE__,
    created_on    => $self->timestamp(),
    identifier    => $self->label(),
    job_name      => $job_name,
    command       => '/bin/true',
    num_cpus      => [0],
    queue         =>
      $npg_pipeline::function::definition::SMALL_QUEUE,
  );

  return [$d];
}

=head2 pipeline_wait4path

This function creates a single job which will wait for up to 20 mins for
the run folder to appear in the outgoing directory. If the run folder does
not appear in the outgoing directory within this time, the job will exit with
error code 1. If the original run folder path is not in the analysis directory,
this job should find the expected path in place and finish successfully
immediately.

=cut

sub pipeline_wait4path {
  my $self = shift;

  my $path = npg_pipeline::runfolder_scaffold
             ->path_in_outgoing($self->runfolder_path());
  my $random = irand(); # Will add echoing this random number to the
                        # command so that commands for different invocations
                        # of the pipeline on the same run are not considered
                        # the same by wr.

  my $command = q{bash -c '}
  ##no critic (ValuesAndExpressions::RequireInterpolationOfMetachars)
    . qq{echo $random; COUNTER=0; NUM_ITERATIONS=$NUM_MINS2WAIT; DIR=$path; STIME=60; }
    .  q{while [ $COUNTER -lt $NUM_ITERATIONS ] && ! [ -d $DIR ] ; }
    .  q{do echo $DIR not available; COUNTER=$(($COUNTER+1)); sleep $STIME; done; }
    .  q{EXIT_CODE=0; if [ $COUNTER == $NUM_ITERATIONS ] ; then EXIT_CODE=1; fi; exit $EXIT_CODE;}
  ##use critic
    .  q{'};

  my $job_name = join q{_}, 'wait4path_in_outgoing', $self->label();
  my $d = npg_pipeline::function::definition->new(
    created_by    => __PACKAGE__,
    created_on    => $self->timestamp(),
    identifier    => $self->label(),
    job_name      => $job_name,
    command       => $command,
    num_cpus      => [0],
    command_preexec => "[ -d '$path' ]",
    queue           =>
      $npg_pipeline::function::definition::SMALL_QUEUE,
  );

  return [$d];
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

=item Math::Random::Secure

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2018, 2019 Genome Research Ltd

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
