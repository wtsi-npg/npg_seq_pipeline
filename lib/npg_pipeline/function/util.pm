package npg_pipeline::function::util;

use Moose::Role;

our $VERSION = '0';

=head1 NAME

npg_pipeline::function::util

=head1 SYNOPSIS

=head1 DESCRIPTION

Moose role providing utility methods for function modules.

=head1 SUBROUTINES/METHODS

=head2 repos_pre_exec_string

Pre-exec string to test the availability of the reference repository.

=cut

sub repos_pre_exec_string {
  my $self = shift;
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
    $self->logcroak('Non-empty array of up to two numbers is expected');
  }
  return [sort {$a <=> $b} @numbers];
}

=head2 get_study_library_sample_names

Given a position and a tag_index, return a hash with study, library and sample names. 

=cut

sub get_study_library_sample_names {
  my ($self, $elims) = @_;

  my $sample_names = [];
  my %study_names_hash = ();

  my @alims = $elims->is_pool ? grep {not $_->is_control} $elims->children : ($elims);
  foreach my $al (@alims) {

     my $sample_name = $al->sample_publishable_name();
     if($sample_name){
        push @{$sample_names}, $al->sample_publishable_name;
     }

     my $study_name = $al->study_publishable_name();
     my $study_description = $al->is_control ? 'SPIKED_CONTROL' : $al->study_description;
     if( $study_name ){
        if( $study_description ){
           $study_description =~ s/\r//gmxs;
           $study_description =~ s/\n/\ /gmxs;
           $study_description =~ s/\t/\ /gmxs;
           $study_name .= q{: }.$study_description;
        }
        $study_names_hash{$study_name}++;
     }
  }

  my $library_aref = $elims->library_id ? [$elims->library_id] : [];
  my $href = {
          study    => [keys %study_names_hash],
          library  => $library_aref,
          sample   => $sample_names,
         };
  return $href;
}

no Moose::Role;

1;
__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose::Role

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
