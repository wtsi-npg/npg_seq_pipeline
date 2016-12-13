package npg_pipeline::daemon::archival;

use Moose;
use Readonly;
use Try::Tiny;

extends qw{npg_pipeline::daemon};

our $VERSION = '0';

Readonly::Scalar our $POST_QC_REVIEW_SCRIPT => q{npg_pipeline_post_qc_review};
Readonly::Scalar our $ARCHIVAL_PENDING      => q{archival pending};

sub build_pipeline_script_name {
  return $POST_QC_REVIEW_SCRIPT;
}

sub run {
  my $self = shift;

  foreach my $run ($self->runs_with_status($ARCHIVAL_PENDING)) {
    my $id_run = $run->id_run();
    try {
      $self->info(qq{Considering run $id_run});
      if ($self->seen->{$id_run}) {
        $self->info(qq{Already seen run $id_run, skipping...});
      } else {
        if ( $self->staging_host_match($run->folder_path_glob)) {
          my $lims = $self->check_lims_link($run);
          $self->run_command($id_run, $self->_generate_command($id_run, $lims->{'gclp'}));
        }
      }
    } catch {
      $self->error("Error processing run ${id_run}: $_");
    };
  }

  return;
}

sub _generate_command {
  my ($self, $id_run, $gclp) = @_;

  $self->info($gclp ? 'GCLP run' : 'Non-GCLP run');

  my $cmd = $self->pipeline_script_name();
  $cmd = $cmd . ($gclp ? q{ --function_list gclp} : q());
  $cmd = $cmd . q{ --verbose --runfolder_path } . $self->runfolder_path4run($id_run);
  my $path = join q[:], $self->local_path(), $ENV{PATH};
  my $prefix = $self->daemon_conf()->{'command_prefix'};
  if (not defined $prefix) { $prefix=q(); }
  $cmd = qq{export PATH=$path; $prefix$cmd};

  return $cmd;
}

no Moose;
__PACKAGE__->meta->make_immutable;
1;

__END__

=head1 NAME

npg_pipeline::daemon::archival

=head1 SYNOPSIS

  my $runner = npg_pipeline::daemon::archival->new();
  $runner->loop();

=head1 DESCRIPTION

Daemon for invoking the archival pipeline.
Inherits most of functionality, including the loop() method,
from npg_pipeline::base.

=head1 SUBROUTINES/METHODS

=head2 run

Invokes the archival pipeline for runs with a status 'archival pending'.

=head2 build_pipeline_script_name

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item Try::Tiny

=item Readonly

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown
Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2016 Genome Research Ltd.

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
