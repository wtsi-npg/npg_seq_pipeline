package npg_pipeline::function::current_analysis_link;

use Moose;
use namespace::autoclean;
use Try::Tiny;
use English qw(-no_match_vars);
use Readonly;
use File::Spec::Functions qw(abs2rel);

extends q{npg_pipeline::base_resource};

our $VERSION = '0';

Readonly::Scalar my $MAKE_LINK_SCRIPT => q{npg_pipeline_create_summary_link};
Readonly::Scalar my $SUMMARY_LINK     => q{Latest_Summary};

sub make_link {
  my $self = shift;

  my $rf_path = $self->runfolder_path();
  my $recalibrated_path;
  try {
    $recalibrated_path = $self->recalibrated_path();
  } catch {
    $self->logerror($_);
  };

  my $link  = $recalibrated_path;
  my $cur_link;
  my $summary_link = $SUMMARY_LINK;
  if ( -l $summary_link) {
    $cur_link = readlink qq{$rf_path/$summary_link};
  }

  if ($cur_link and $link =~ /\Q$cur_link\E/xms) {
    $self->info(
      qq{$summary_link link ($cur_link) already points to $link , not changed.});
  } else {
    if($link =~ m{\A/}smx){
      $link = abs2rel( $link, $rf_path);
    }
    # Because Latest_Summary points to a directory, ln -fs gets
    # confused, so we rm it first.
    my $command = qq{cd $rf_path; rm -f $summary_link; ln -fs $link $summary_link};
    $self->info(qq{Running $command});
    system($command) == 0 or $self->logcroak(
      qq{Creating summary link "$command" failed, error code: $CHILD_ERROR});
  }
  return;
}

sub create {
  my $self = shift;

  my $ref = { 'identifier' => $self->id_run() };

  if ($self->no_summary_link()) {
    $self->info(q{Summary link creation turned off});
    $ref->{'excluded'} = 1;
  } else {
    my $run_folder = $self->run_folder();
    $ref->{'job_name'} = join q{_}, q{create_latest_summary_link},
                                    $self->id_run(), $run_folder;
    $ref->{'command'} = qq{$MAKE_LINK_SCRIPT --run_folder $run_folder}
                       . q{ --runfolder_path } . $self->runfolder_path()
                       . q{ --recalibrated_path } . $self->recalibrated_path();
  }

  return [$self->create_definition($ref)];
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

npg_pipeline::function::current_analysis_link

=head1 SYNOPSIS

=head1 DESCRIPTION

Each instance of analysis can potentially create a new directory.
Creating a symbolic link in the run folder to a file inside the
analysis directory helps to identify the current version of the
analysis results.

=head1 SUBROUTINES/METHODS

=head2 make_link

Creates a symbolic link to the Latest Summary file

  $obj->make_link();

=head2 create

Creates and returns a single function definition wrapped into an array.
Function definition is created as a npg_pipeline::function::definition
type object.

  my $def_array = $obj->create();

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item namespace::autoclean

=item English -no_match_vars

=item Try::Tiny

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
