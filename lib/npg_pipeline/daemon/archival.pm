package npg_pipeline::daemon::archival;

use Moose;
use Readonly;
use Try::Tiny;

extends qw{npg_pipeline::daemon};

our $VERSION = '0';

Readonly::Scalar our $POST_QC_REVIEW_SCRIPT => q{npg_pipeline_post_qc_review};
Readonly::Scalar our $ARCHIVAL_PENDING      => q{archival pending};
Readonly::Scalar  my $ARCHIVAL_IN_PROGRESS  => q{archival in progress};
Readonly::Scalar  my $MAX_NUMBER_NV_RUNS_IN_ARCHIVAL => 4;
Readonly::Scalar  my $OLD_DATED_DIR_NAME    => q[20180717];

sub build_pipeline_script_name {
  return $POST_QC_REVIEW_SCRIPT;
}

sub run {
  my $self = shift;

  my $submitted = 0;
  foreach my $run ($self->runs_with_status($ARCHIVAL_PENDING)) {
    $submitted && last; # Return after submitting one run, give
                        # this run a chance to change status to
                        # 'archival in progress'.
    my $id_run = $run->id_run();
    try {
      $self->info();
      $self->info(qq{Considering run $id_run});
      if ($self->seen->{$id_run}) {
        $self->info(qq{Already seen run $id_run, skipping...});
      } else {
        if ( $self->staging_host_match($run->folder_path_glob) &&
             (!$self->_instrument_model_is_novaseq($run) || $self->_can_start_nv_archival()) ) {
          my ($rf_path, $rf_obj) = $self->runfolder_path4run($id_run);
          if (-e $rf_path) {
            # No qc directory in archive directory in the new run folder structure.
            my $old_style_rf = -e $rf_obj->qc_path;
            $self->check_lims_link($run);
            $self->run_command($id_run, $self->_generate_command($rf_path, $old_style_rf));
            $self->info();
            $self->info(qq{Submitted run $id_run for archival});
            $submitted += 1;
	  } else {
            $self->info(qq{Runfolder $rf_path for run $id_run does not exist on this host, skipping...});
	  }
        }
      }
    } catch {
      $self->error("Error processing run ${id_run}: $_");
    };
  }

  return $submitted;
}

sub _generate_command {
  my ($self, $rf_path, $old_style_rf) = @_;

  my $cmd = $self->pipeline_script_name();
  $cmd = $cmd . qq{ --verbose --runfolder_path $rf_path};
  my $local_path = $self->local_bin(); # This script's bin as
                                       # an absolute path.
  my $saved_local_path = $local_path;
  if ($old_style_rf) {
    $self->info(q{Old style run folder});
    $local_path =~ s{/201[89]\d\d\d\d/bin\Z}{/$OLD_DATED_DIR_NAME/bin}xms;
    if ($local_path eq $saved_local_path) {
       $self->logwarn(q{Failed to change path}); # Cannot exit here since
                                                 # all tests will fail.
       $old_style_rf = 0;
    } else {
      $self->info(qq{Will prepend old dated directory $local_path to PATH});
    }
  }

  my $path = join q[:], $local_path, $ENV{PATH};
  my $prefix = $self->daemon_conf()->{'command_prefix'};
  if (not defined $prefix) { $prefix=q(); }
  $cmd = qq{export PATH=$path; $prefix$cmd};
  if ($old_style_rf) { # The pipeline scripts should be able to locate
                       # lib/perl5 directory parallel to their bin.
    $cmd = qq{unset PERL5LIB; $cmd};
  }

  return $cmd;
}

sub _can_start_nv_archival {
  my $self = shift;

  my @runs = $self->runs_with_status($ARCHIVAL_IN_PROGRESS);
  if (scalar @runs < $MAX_NUMBER_NV_RUNS_IN_ARCHIVAL) {
    return 1;
  }
  my $num_nv = scalar
               grep { $self->_instrument_model_is_novaseq($_) }
               @runs;

  return ($num_nv < $MAX_NUMBER_NV_RUNS_IN_ARCHIVAL);
}

sub _instrument_model_is_novaseq {
  my ($self, $run) = @_;
  return $run->instrument_format->model eq q[NovaSeq];
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

=head2 sleep_time_between_runs

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
