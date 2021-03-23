package npg_pipeline::function::test_function;

use Moose;
use npg_pipeline::function::definition;

extends 'npg_pipeline::base';
with 'npg_pipeline::function::util';

sub test_run {
  my $self = shift;
  my @definitions;
  $self->info('Starting test stage');

  for my $product (@{$self->products->{data_products}}) {
    my $command = sprintf "echo '%s\n%s\n%s\n'",
      $self->id_run,
      join(q(,), $self->positions),
      $self->runfolder_path();

    push @definitions, npg_pipeline::function::definition->new(
        job_name => join('_', $self->id_run, $self->timestamp),
        identifier => $self->label,
        command => $command
    );
  }

  return \@definitions;
}


1;

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
