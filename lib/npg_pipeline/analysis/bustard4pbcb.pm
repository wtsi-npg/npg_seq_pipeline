package npg_pipeline::analysis::bustard4pbcb;

use Moose;
use Moose::Util::TypeConstraints;
use Carp;
use Cwd;
use File::Spec::Functions;
use File::Slurp;
use Try::Tiny;
use Readonly;

use npg_pipeline::lsf_job;
extends q{npg_pipeline::base};

our $VERSION = '0';

=head1 NAME

npg_pipeline::analysis::bustard4pbcb

=head1 SYNOPSIS

=head1 DESCRIPTION

OLB bustard preprocessing for the pbcal bam pipeline

=head1 SUBROUTINES/METHODS

=cut

Readonly::Scalar our $MEM_REQ => 13_800; # total MB used by a make
Readonly::Scalar our $CPUS_NUM             => q{8,16};

subtype 'NpgPipelinePluggableObject'
    => as 'Object'
    => where { ref($_) =~ /^npg_pipeline::pluggable/smxi; };

has q{+id_run}        => ( required => 1, );

has q{pipeline}       => ( isa      => q{NpgPipelinePluggableObject},
                           is       => q{ro},
                         );

has q{bustard_home}   => ( isa      => q{Str},
                           is       => q{ro},
                           required => 1,
                         );

has q{script_path}    => ( isa        => q{Str},
                           is         => q{ro},
                           lazy_build => 1,
                         );
sub _build_script_path {
  my $self = shift;
  return catfile($self->illumina_pipeline_conf()->{olb},
                 $self->illumina_pipeline_conf()->{bustard_exe});
}

has q{bustard_dir}   =>  ( isa      => q{Str},
                           is       => q{ro},
                           lazy_build => 1,
                         );
sub _build_bustard_dir {
  my $self = shift;

  $self->log(q[Running Bustard makefile creation]);
  my $bustard_command = $self->_bustard_command();
  $self->log("Bustard command: $bustard_command");
  my $rc = system $bustard_command;
  my @lines = ();
  try {
    @lines = read_file($self->_bustard_output_file());
  };
  if ($rc) {
    my $error= "Bustard command '$bustard_command' failed with code $rc";
    if (@lines) {
      $error .= q[ ] . join q[ ], @lines;
    }
    croak $error;
  }
  if (!@lines) {
    croak q[No bustard output in ] . $self->_bustard_output_file();
  }
  return $self->_get_bustard_dir(@lines);
}

has q{_bustard_output_file} =>  ( isa      => q{Str},
                                  is       => q{ro},
                                  lazy_build => 1,
                                );
sub _build__bustard_output_file {
  my $self = shift;
  return catfile($self->bustard_home, q[bustard_output_] . $self->timestamp() . q[.txt]);
}

sub _get_bustard_dir {
  my ($self, @lines) = @_;
  my $line = q[];
  ##no critic (RegularExpressions::ProhibitEscapedMetacharacters)
  foreach (@lines) {
    if ($_ =~ /^Sequence\ folder/ixms) {
      $line = $_;
      last;
    }
  }
  ## use critic
  if (!$line) {
    croak q[No record about bustard directory (Sequence folder) in ] . $self->_bustard_output_file();
  }
  (my $dir) = $line =~ /:\s+(\S+)$/smx;
  return $dir;
}

sub _bustard_command {
  my ($self) = shift;

  my $timestamp     = $self->timestamp();
  my ($time) = $timestamp =~ /-(\d+)$/smx;
  my $bustard_out   = catfile($self->bustard_home, "bustard_output_$timestamp.txt");
  my @command = ();
  push @command, "LOGNAME=$time";
  push @command, $self->script_path;
  if ( $self->has_override_all_bustard_options() ) {
    push @command, $self->override_all_bustard_options();
  } else {
    push @command, '--make --CIF --keep-dif-files --no-eamss --phasing=lane --matrix=lane';
    my $tile_list = $self->tile_list() || join q[,], map {"s_$_"} $self->positions();
    push @command, "--tiles=$tile_list";
  }
  push @command, $self->bustard_home;
  push @command, '> ' . $self->_bustard_output_file();
  push @command, '2>&1';
  return join q[ ], @command;
}

sub _make_command {
  my ($self, $step_name, $deps) = @_;

  my $position_string = ($step_name =~ /lanes$/smx) ? $self->lsb_jobindex() : q{},
  $deps ||= q{};
  (my $target) = $step_name =~ /(matrix|phasing)/smx;
  if ($target) {
    if ($position_string) {
      $target .= "_$position_string";
    }
    $target .= q{_finished.txt};
  } else {
    $target = $position_string ? "s_$position_string" : 'all';
  }

  my $job_name    = join q[_], 'bustard', $step_name, $self->id_run(), $self->timestamp();
  my $index = $position_string ? q{.%I} : q{};
  my $output_name = $job_name . $index . q{.%J.out};
  $output_name = catfile(q{log} , $output_name);
  if ($position_string) {
    $job_name .= '[' . join(q[,], $self->positions()) . ']';
  }

  my @command = ();
  push @command, 'bsub';
  push @command, "-n $CPUS_NUM";
  push @command, '-q ' . $self->lsf_queue;
  push @command, "-o $output_name";
  push @command, "-J $job_name";
  my $memory_spec = npg_pipeline::lsf_job->new(memory => $MEM_REQ)->memory_spec();
  push @command, $self->pipeline->fs_resource_string( {
    resource_string => qq{$memory_spec -R 'span[hosts=1]'},
    ##no critic (BuiltinFunctions::ProhibitStringySplit)
    counter_slots_per_job => (split q{,}, $CPUS_NUM)[0],
    ##use critic
  });
  if ($deps) { push @command, $deps; }
  push @command, q['make -j `npg_pipeline_job_env_to_threads` ] . qq[$target'];
  return join q[ ], @command;
}

=head2 make

 Submits bustard 'make' jobs for post-run analysis as a single step.

  my @job_ids = $bObj->make($step_name, $required_job_completion);

=cut

sub make {
  my ($self, $step_name, $required_job_completion) = @_;
  if (!$self->pipeline) {
    croak 'To submit a job, pipeline accessor should be set';
  }
  my $working = getcwd();
  chdir $self->bustard_dir;
  my $command = $self->_make_command($step_name, $required_job_completion);
  $self->log("Bustard make command: $command");
  my @ids = $self->pipeline->submit_bsub_command($command);
  chdir $working;
  return @ids;
}

no Moose::Util::TypeConstraints;
no Moose;
__PACKAGE__->meta->make_immutable;

1;

__END__


=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Carp

=item Readonly

=item Moose

=item Moose::Util::TypeConstraints

=item Cwd

=item File::Spec::Functions

=item File::Slurp

=item Try::Tiny

=item npg_pipeline::base

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown
Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2014 Genome Research Ltd

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
