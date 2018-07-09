package npg_pipeline::function::start_stop;

use Moose;
use namespace::autoclean;

use npg_pipeline::function::definition;

extends q{npg_pipeline::base};

our $VERSION = '0';

=head1 NAME

npg_pipeline::function::start_stop

=head1 SYNOPSIS

  my $c = npg_pipeline::function::start_stop->new(
    id_run => 1234,
    run_folder => q{123456_IL2_1234},
  );

=head1 DESCRIPTION

Definitions for token start and end pipeline steps

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
  my $job_name = join q{_}, $subroutine_name, $self->id_run(), $pipeline_name;

  my $d = npg_pipeline::function::definition->new(
    created_by    => __PACKAGE__,
    created_on    => $self->timestamp(),
    identifier    => $self->id_run(),
    job_name      => $job_name,
    command       => '/bin/true',
    queue         =>
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

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

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
