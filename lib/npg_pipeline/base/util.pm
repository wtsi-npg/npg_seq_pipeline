package npg_pipeline::base::util;

use Moose::Role;
use Carp;
use Math::Random::Secure qw{irand};

our $VERSION = '0';

=head1 NAME

npg_pipeline::base::util

=head1 SYNOPSIS

=head1 DESCRIPTION

Moose role providing utility methods.

=head1 SUBROUTINES/METHODS

=head2 repos_pre_exec_string

Pre-exec string to test the availability of the reference repository.

=cut

sub repos_pre_exec_string {
  my ( $self ) = @_;

  my $string = q{npg_pipeline_preexec_references};
  if ( $self->can('repository') && $self->can('has_repository') && $self->has_repository() ) {
    $string .= q{ --repository } . $self->repository();
  }
  return $string;
}

=head2 num_cpus2array

=cut

sub num_cpus2array {
  my ($self, $num_cpus_as_string) = @_;
  my @numbers = grep  { $_ > 0 }
                map   { int }    # zero if conversion fails
                split /,/xms, $num_cpus_as_string;
  if (!@numbers || @numbers > 2) {
    my $m = 'Non-empty array of up to two numbers is expected';
    $self->can('logcroak') ? $self->logcroak($m) : croak $m;
  }
  return [sort {$a <=> $b} @numbers];
}

=head2 pipeline_name

=cut

sub pipeline_name {
  my $self = shift;
  my $name = ref $self;
  ($name) = $name =~ /(\w+)$/smx;
  $name = lc $name;
  return $name;
}

=head2 random_string

Returns a random string, a random 32-bit integer between 0 and 2^32,
prepended with a value of the timestamp attribute it the latter is available.

  my $rs = $class->random_string();

=cut

sub random_string {
  my $self = shift;
  return ($self->can('timestamp') ? $self->timestamp() . q[-] : q[]) . irand();
}

no Moose::Role;

1;
__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose::Role

=item Carp

=item Math::Random::Secure

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

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
