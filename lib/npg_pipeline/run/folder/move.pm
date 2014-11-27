package npg_pipeline::run::folder::move;

use Moose;
use Carp;
use English qw{-no_match_vars};
use Readonly;

extends q{npg_pipeline::run::folder};

our $VERSION = '0';

Readonly::Scalar our $MOVE_FOLDER_SCRIPT  => q{move_run_folder};

sub move_runfolder {
  my $self = shift;

  my $folder = $self->folder();
  my $start_dir = my $end_dir = $self->runfolder_path();
  if ( !-d $start_dir ) {
    croak qq{Failed to find $start_dir to move};
  }

  if ( !$folder || $folder eq q{outgoing} ) {  # moves from analysis to outgoing
    $end_dir  =~ s/incoming/outgoing/xms; # most likely that this is going to say
    $end_dir  =~ s/analysis/outgoing/xms; # analysis, but just to be sure
  } else {                                   # moves from incoming to analysis
    $end_dir  =~ s/incoming/analysis/xms; # a move to analysis would mean that it should be in incoming
  }

  if ( -d $end_dir ) {
    croak qq{$end_dir already exists};
  }

  my @path = split m{/}xms, $end_dir;
  pop@path;
  $end_dir = join q{/}, @path;

  my $cmd  = qq{mv $start_dir $end_dir/};
  $self->log( qq{Move command = $cmd} );
  my $rc = qx/$cmd/;
  if ( $CHILD_ERROR != 0 ) {
    $self->log( qq{runfolder move failed - Error code: $CHILD_ERROR} );
    croak qq{runfolder move failed - Error code: $CHILD_ERROR};
  }

  $self->log( q{runfolder move success} );

  return 1;
}

###############
# responsible for generating the bsub command to be executed
sub _generate_bsub_command {
  my ($self, $arg_refs) = @_;

  my $required_job_completion = $arg_refs->{required_job_completion};
  my $rf_path = $self->runfolder_path();
  my $run_folder = $self->run_folder();
  if ( ! $self->folder() ) {
    $self->_set_folder( q{outgoing} );
  }
  my $folder = $self->folder();
  my $inst_dir = $self->get_instrument_dir($rf_path);

  $inst_dir =~ s/incoming/$folder/xms; ###########
  $inst_dir =~ s/analysis/$folder/xms; # make sure that the folder is what is expected
  $inst_dir =~ s/outgoing/$folder/xms; ###########

  my $job_name = join q{_}, $MOVE_FOLDER_SCRIPT, $self->id_run, $run_folder, q{to}, $self->folder();

  my $bsub_command = qq{bsub $required_job_completion -J $job_name -q } . $self->small_lsf_queue();
  $bsub_command .=  q{ -o } . $self->make_log_dir( $inst_dir ) . q{/} . $job_name . q{_} . $self->timestamp() . q{.out};
  $bsub_command .=  q{ '} . $MOVE_FOLDER_SCRIPT;
  if ($self->folder) {
    $bsub_command .=  q{ --folder } . $self->folder;
  }
  $bsub_command .=  qq{ --run_folder $run_folder --runfolder_path $rf_path};
  $bsub_command .=  q{'};

  return $bsub_command;
}

sub submit_move_run_folder {
  my ($self, $arg_refs) = @_;
  my $cmd = $self->_generate_bsub_command( $arg_refs );
  if ( $self->verbose() ) { $self->log( $cmd ); }
  return $self->submit_bsub_command( $cmd );
}

no Moose;
__PACKAGE__->meta->make_immutable;
1;
__END__

=head1 NAME

npg_pipeline::run::folder::move

=head1 SYNOPSIS

  my $rfm = npg_pipeline::run::folder::move->new({
    run_folder => $sRunFolder,
  });

  eval { $rfm->move_runfolder(); } or do { croak $EVAL_ERROR; };

=head1 DESCRIPTION

Class which can move the run_folder from analysis to outgoing. It can do this directly,
or submit a job to LSF so that it can be done at an appropriate time.

=head1 SUBROUTINES/METHODS

=head2 move_runfolder - handler for moving the runfolder from analysis to outgoing

=head2 submit_move_run_folder - handler for submitting a job to do this to LSF

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item Carp

=item English -no_match_vars

=item Readonly

=item npg_pipeline::run::folder

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown

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
