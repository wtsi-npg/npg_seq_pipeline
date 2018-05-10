package npg_pipeline::pluggable::central;

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;

use npg_pipeline::function::runfolder_scaffold;

extends qw{npg_pipeline::pluggable};

our $VERSION = '0';

=head1 NAME

npg_pipeline::pluggable::central

=head1 SYNOPSIS

  npg_pipeline::pluggable::central->new(id_run => 333)->main();

=head1 DESCRIPTION

Pipeline runner for the analysis pipeline.

=cut

=head1 SUBROUTINES/METHODS

=head2 prepare

Inherits from parent's method. Sets all paths needed during the lifetime
of the analysis runfolder. Creates any of the paths that do not exist.
Ater that calls the parent's method.

=cut

override 'prepare' => sub {
  my $self = shift;

  my $output = npg_pipeline::function::runfolder_scaffold->create_top_level($self);
  my @errors = @{$output->{'errors'}};
   if ( @errors ) {
    $self->logcroak(join qq[\n], @errors);
  } else {
    $self->info(join qq[\n], @{$output->{'msgs'}});
  }

  super(); # Corect order

  return;
};

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
