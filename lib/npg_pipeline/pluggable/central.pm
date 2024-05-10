package npg_pipeline::pluggable::central;

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;
use JSON;
use File::Slurp qw(read_file write_file);

extends 'npg_pipeline::pluggable';

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

Saves lane numbers given by the `process_separately_lanes` option to a
JSON file.

=cut

override 'prepare' => sub {
  my $self = shift;

  $self->_scaffold('create_top_level');
  super(); # Correct order, sets up a samplesheet.
  $self->_save_merge_options();
  $self->_scaffold('create_product_level');

  return;
};

sub _scaffold {
  my ($self, $method_name) = @_;

  my $output = $self->$method_name();
  my @errors = @{$output->{'errors'}};
  if ( @errors ) {
    $self->logcroak(join qq[\n], @errors);
  } else {
    $self->info(join qq[\n], @{$output->{'msgs'}});
    $self->info();
  }

  return;
}

sub _save_merge_options {
  my $self = shift;

  my $attr_name = 'process_separately_lanes';
  my @given_lanes = sort {$a <=> $b} @{$self->$attr_name};
  if (@given_lanes) {
    my $cached_options = {};
    my $found = 0;
    my $path = $self->analysis_options_file_path();
    if (-f $path) {
      $cached_options = decode_json(read_file($path));
      if ($cached_options->{$attr_name} && @{$cached_options->{$attr_name}}) {
        my $sep = q[, ];
        my $cached_lanes = join $sep, @{$cached_options->{$attr_name}};
        $self->info("Found cached merge options in $path: " .
                    "lanes $cached_lanes should not be merged.");
        if ($cached_lanes ne join $sep, @given_lanes) {
          $self->logcroak('Lane list from process_separately_lanes attribute ' .
                        'is inconsistent with cached value');
        }
        $found = 1;
      }
    }

    if (!$found) {
      $cached_options->{$attr_name} = \@given_lanes;
      write_file($path, encode_json($cached_options)) or
        $self->logcroak("Failed to write to $path");
    }
  }

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

=item JSON

=item File::Slurp

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Guoying Qi
Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2018,2024 Genome Research Ltd.

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
