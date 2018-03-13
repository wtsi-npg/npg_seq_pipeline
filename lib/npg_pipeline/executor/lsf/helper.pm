package npg_pipeline::executor::lsf::helper;

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;
use POSIX;
use Readonly;
use English qw{-no_match_vars};

with qw{ WTSI::DNAP::Utilities::Loggable };

our $VERSION = '0';

Readonly::Scalar my $DEFAULT_MAX_TRIES => 3;
Readonly::Scalar my $DEFAULT_MIN_SLEEP => 1;

Readonly::Scalar my $THOUSANDTH => 0.001;
Readonly::Scalar my $THOUSAND   => 1000;
Readonly::Scalar my $LOW_MEM    => 1;
Readonly::Scalar my $HI_MEM     => 96_000;
Readonly::Scalar my $DEFAULT_MEMORY_UNIT => q{MB};
Readonly::Hash   my %MEMORY_COEFFICIENTS => (
                      $DEFAULT_MEMORY_UNIT => 1,
                                      'KB' => $THOUSANDTH,
                                      'GB' => $THOUSAND,
                                            );

Readonly::Scalar my $DEFAULT_JOB_ID_FOR_NO_BSUB => 50;

=head1 NAME

npg_pipeline::executor::lsf::helper

=head1 SYNOPSIS

=head1 SUBROUTINES/METHODS

=head2 no_bsub

Boolean flag, false by default. If true, the bsub commands
are not executed.

=cut

has q{no_bsub}      => (isa        => q{Bool},
                        is         => q{ro},
                       );

=head2 lsf_conf

LSF configuration hash. Not required, but have to be set for
some of the methods to work.

=cut

has q{lsf_conf}     => (isa        => q{HashRef},
                        required   => 0,
                        is         => q{ro},
                       );

=head2 memory_in_mb

Returns memory in MB.  If memory unit is not given, MB is assumed.

  my $minmb = $obj->memory_in_mb(400);
  my $minmb = $obj->memory_in_mb(400, 'MB');
  my $minmb = $obj->memory_in_mb(4, 'GB');
  my $minmb = $obj->memory_in_mb(4000, 'KB');

=cut

sub memory_in_mb {
  my ($self, $memory, $unit) = @_;

  if (!$memory) {
    $self->logcroak('Memory required');
  }
  my $omemory = $memory;
  {
    use warnings FATAL => qw/numeric/;
    $memory = int $memory;
    if (!$memory || ($omemory ne $memory)) {
      $self->logcroak('Memory should be an integer');
    }
  }

  $unit ||= $DEFAULT_MEMORY_UNIT;
  my $coef = $MEMORY_COEFFICIENTS{$unit};
  if (!$coef) {
    $self->logcroak("Memory unit $unit is not recognised");
  }

  my $memory_requested =$memory * $coef;
  if (($memory_requested > $HI_MEM) || ($memory_requested < $LOW_MEM)) {
    $self->logcroak("Memory $memory $unit out of bounds");
  }

  return POSIX::floor($memory_requested);
}

=head2 memory_spec

Returns an appropriate bsub component. If memory unit is not
given, MB is assumed.

-R 'select[mem>8000] rusage[mem=8000]'  -M8000" on precise

  my $spec = $obj->memory_spec(8000);
  my $spec = $obj->memory_spec(8000, 'MB');
  my $spec = $obj->memory_spec(8, 'GB');
  my $spec = $obj->memory_spec(8000, 'KB');

=cut

sub memory_spec {
  my ($self, $memory, $unit) = @_;
  $memory = $self->memory_in_mb($memory, $unit);
  return "-R 'select[mem>$memory] rusage[mem=$memory]' -M$memory";
}

=head2 create_array_string

 Takes an array of integers, and converts them to an LSF job array string
 for appending to teh LSF job name.

 my $sArrayString = $obj->create_array_string( 1,4,5,6,7,10... );

=cut

sub create_array_string {
  my ($self, @lsf_indices) = @_;

  my ($start_run, $end_run);
  my $ret = q{};
  foreach my $entry ( @lsf_indices ) {
    # have we already started looping through
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

=head2 execute_lsf_command

Executes LSF command, retrying a few times if the return code is not zero.
It will then error if it still cannnot execute the command.

Recognises three LSF commands: bsub, bkill, bmod.

For bsub command returns an id of the new LSF job.

  my $id = $obj->execute_lsf_command($cmd);

If the no_bsub attribute is set to true, the LSF command is not executed,
default test job is is returned.

For non bsub command returns an empty string.

=cut

sub execute_lsf_command {
  my ($self, $cmd) = @_;

  $cmd ||= q[];
  $cmd =~ s/\A\s+//xms;
  $cmd =~ s/\s+\Z//xms;
  if (!$cmd) {
    $self->logcroak('command have to be a non-empty string');
  }

  if ($cmd !~ /^b(?: kill|sub|resume )\s/xms) {
    my $c = (split /\s/xms, $cmd)[0];
    $self->logcroak(qq{'$c' is not one of supported LSF commands});
  }

  my $job_id;
  my $error = 0;

  if ($self->no_bsub()) {
    $job_id =  $DEFAULT_JOB_ID_FOR_NO_BSUB;
  } else {
    local $ENV{'LSB_DEFAULTPROJECT'} ||= q{pipeline};
    my $count = 1;
    my $max_tries_plus_one =
      ($self->lsf_conf()->{'max_tries'} || $DEFAULT_MAX_TRIES) + 1;
    my $min_sleep = $self->lsf_conf()->{'min_sleep'} || $DEFAULT_MIN_SLEEP;

    while ($count < $max_tries_plus_one) {
      $job_id = qx/$cmd/;
      if ($CHILD_ERROR) {
        $error = 1;
        $self->error(qq{Error code $CHILD_ERROR. } .
                     qq{Error attempting ($count) to submit to LSF $cmd.});
        sleep $min_sleep ** $count;
        $count++;
      } else {
        $error = 0;
        $count = $max_tries_plus_one;
      }
    }
  }

  if ($error) {
    $self->logcroak('Failed to submit command to LSF');
  } else {
    if ($cmd =~ /bsub/xms) {
      ($job_id) = $job_id =~ /(\d+)/xms;
    } else {
      $job_id = q[];
    }
  }

  return $job_id;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 DESCRIPTION

A collection of LSF-specific helper methods.

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item List::MoreUtils

=item Moose

=item MooseX::StrictConstructor

=item namespace::autoclean

=item Readonly

=item POSIX

=item English

=item WTSI::DNAP::Utilities::Loggable

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

=over

=item Kate Taylor

=item Andy Brown

=item Marina Gourtovaia

=back

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2018 Genome Research Ltd.

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
