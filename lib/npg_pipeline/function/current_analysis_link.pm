package npg_pipeline::function::current_analysis_link;

use Moose;
use Carp;
use English qw{-no_match_vars};
use Readonly;
use File::Spec::Functions qw(abs2rel);

extends q{npg_pipeline::base};

our $VERSION = '0';

Readonly::Scalar my $MAKE_LINK_SCRIPT => q{npg_pipeline_create_summary_link};
Readonly::Scalar my $SUMMARY_LINK     => q{Latest_Summary};

sub make_link {
  my $self = shift;

  my $rf_path     = $self->runfolder_path();
  my $recalibrated_path;
  eval {
    $recalibrated_path = $self->recalibrated_path();
    1;
  } or do {
    carp $EVAL_ERROR;
  };

  my $link  = $recalibrated_path;

  my $cur_link;
  my $summary_link = $SUMMARY_LINK;
  if ( -l $summary_link) {
    $cur_link = readlink qq{$rf_path/$summary_link};
  }

  if ($cur_link and $link =~ /\Q$cur_link\E/xms) {
    $self->info(qq{$summary_link link ($cur_link) already points to $link -- not changed.});
  } else {
    if($link =~ m{\A/}smx){
      $link = abs2rel( $link, $rf_path);
    }
    # Because Latest_Summary points to a directory, ln -fs gets
    # confused, so we rm it first.
    my $command = qq{cd $rf_path; rm -f $summary_link; ln -fs $link $summary_link};
    $self->info(qq{Running $command});
    my $rc = `$command`;
    if ($CHILD_ERROR != 0) {
      $self->logcroak(qq{Creating summary link "$command" failed - $EVAL_ERROR - Error code : $CHILD_ERROR});
    }
  }
  return;
}

###############
# responsible for generating the bsub command to be executed
sub _generate_bsub_command {
  my ($self) = @_;

  my $run_folder = $self->run_folder();
  my $job_name = join q{_}, q{create_latest_summary_link}, $self->id_run, $run_folder;
  my $bsub_command = qq{bsub -J $job_name -q } . $self->small_lsf_queue();
  $bsub_command .= q{ -o } . $run_folder . q{/} . $job_name . q{_} . $self->timestamp . q{.out};
  $bsub_command .= q{ '} . $MAKE_LINK_SCRIPT;
  $bsub_command .=  qq{ --run_folder $run_folder --runfolder_path } . $self->runfolder_path;
  $bsub_command .=  q{ --recalibrated_path } . $self->recalibrated_path;
  $bsub_command .=  q{'};

  return $bsub_command;
}

sub submit_create_link {
  my ($self) = @_;
  if ($self->no_summary_link()) {
    $self->info(q{Summary link creation turned off});
    return ();
  }
  my $cmd = $self->_generate_bsub_command();

  return $self->submit_bsub_command($cmd);
}

no Moose;
__PACKAGE__->meta->make_immutable;
1;
__END__

=head1 NAME

npg_pipeline::function::current_analysis_link

=head1 SYNOPSIS

=head1 DESCRIPTION

Class to create a LatestSummary link

=head1 SUBROUTINES/METHODS

=head2 make_link - method to call to make the link to the Latest Summary

    $rfl->make_link();

=head2 submit_create_link - method which generates and submits an LSF job which uses dependencies such that the link will only be generated once the correct folder to point to is there

  my @job_ids = $rfl->submit_create_link();
  
=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item Carp

=item English -no_match_vars

=item Moose

=item Readonly

=item File::Spec::Functions

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown

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
