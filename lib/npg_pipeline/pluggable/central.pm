package npg_pipeline::pluggable::central;

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;

extends qw{npg_pipeline::pluggable};

our $VERSION = '0';

=head1 NAME

npg_pipeline::pluggable::central

=head1 SYNOPSIS

  npg_pipeline::pluggable::central->new(id_run => 333)->main();

=head1 DESCRIPTION

Pluggable module runner for the main analysis pipeline

=cut

=head1 SUBROUTINES/METHODS

=head2 prepare

 Sets all paths needed during the lifetime of the analysis runfolder.
 Creates any of the paths that do not exist.

=cut

override 'prepare' => sub {
  my $self = shift;
  $self->_set_paths();
  super(); # Correct order!
  return;
};

####
#
# Sets all paths needed during the lifetime of the analysis runfolder.
# Creates any of the paths that do not exist.
#

sub _set_paths {
  my $self = shift;

  my $sep = q[/];

  if ( ! $self->has_intensity_path() ) {
    my $ipath = join $sep, $self->runfolder_path(), q{Data}, q{Intensities};
    if (!-e $ipath) {
      $self->info(qq{Intensities path $ipath not found});
      $ipath = $self->runfolder_path();
    }
    $self->_set_intensity_path( $ipath );
  }
  $self->info('Intensities path: ', $self->intensity_path() );

  if ( ! $self->has_basecall_path() ) {
    my $bpath = join $sep, $self->intensity_path() , q{BaseCalls};
    if (!-e $bpath) {
      $self->warn(qq{BaseCalls path $bpath not found});
      $bpath = $self->runfolder_path();
    }
    $self->_set_basecall_path( $bpath);
  }
  $self->info('BaseCalls path: ' . $self->basecall_path() );

  if( ! $self->has_bam_basecall_path() ) {
    my $bam_basecalls_dir = join $sep, $self->intensity_path(), q{BAM_basecalls_} . $self->timestamp();
    $self->make_log_dir( $bam_basecalls_dir  );
    $self->set_bam_basecall_path( $bam_basecalls_dir );
  }
  $self->info('BAM_basecall path: ' . $self->bam_basecall_path());

  if (! $self->has_recalibrated_path()) {
    $self->_set_recalibrated_path(join $sep, $self->bam_basecall_path(), 'no_cal')
  }
  $self->make_log_dir($self->recalibrated_path());
  $self->info('PB_cal path: ' . $self->recalibrated_path());

  $self->make_log_dir( $self->status_files_path );

  return;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item MooseX::StrictConstructor

=item namespace::autoclean

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Guoying Qi
Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2018 Genome Research Limited

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
