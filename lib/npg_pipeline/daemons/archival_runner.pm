package npg_pipeline::daemons::archival_runner;

use Moose;
use Carp;
use English qw{-no_match_vars};
use Readonly;

extends qw{npg_pipeline::daemons::harold_analysis_runner};

our $VERSION = '0';

Readonly::Scalar our $POST_QC_REVIEW_SCRIPT       => q{npg_pipeline_post_qc_review};
Readonly::Scalar our $ARCHIVAL_PENDING            => q{archival pending};

sub _build_pipeline_script_name {
  return $POST_QC_REVIEW_SCRIPT;
}

sub run {
  my ($self) = @_;
  $self->log(q{Archival daemon running...});
  foreach my $run ($self->runs_with_status($ARCHIVAL_PENDING)) {
    my $id_run = $run->id_run();
    eval {
      $self->log(qq{Considering run $id_run});
      if ($self->seen->{$id_run}) {
        $self->log(qq{Already seen run $id_run, skipping...});
        next;
      }
      if ( $self->staging_host_match($run->folder_path_glob)) {
        $self->run_command($id_run, $self->_generate_command($id_run));
      }
      1;
    } or do {
      $self->log('Problems to process one run ' . $run->id_run() );
      $self->log($EVAL_ERROR);
      next;
    };
  }
  return 1;
}

sub _generate_command {
  my ($self, $id_run) = @_;
  my $cmd = $self->pipeline_script_name() . q{ --verbose --id_run=} . $id_run;
  my $path = join q[:], $self->local_path(), $ENV{PATH};
  $cmd = qq{export PATH=$path;} . $cmd;
  return $cmd;
}

no Moose;
__PACKAGE__->meta->make_immutable;
1;
__END__

=head1 NAME

npg_pipeline::daemons::archival_runner

=head1 SYNOPSIS

  my $runner = npg_pipeline::archival_runner->new();
  $runner->run();

=head1 DESCRIPTION

This module interrogates the npg database for runs with a status of archival pending, and then runs the npg_pipeline_post_qc_review script on each of them

=head1 SUBROUTINES/METHODS

=head2 run - the only method and the only one you need. It does everything.

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item Carp

=item English -no_match_vars

=item Readonly

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown
Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2014 Genome Research Ltd.

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
