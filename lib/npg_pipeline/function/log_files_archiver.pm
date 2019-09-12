package npg_pipeline::function::log_files_archiver;

use Moose;
use namespace::autoclean;
use Readonly;

use npg_pipeline::function::definition;
use npg_pipeline::runfolder_scaffold;

extends qw{npg_pipeline::function::seq_to_irods_archiver};

our $VERSION = '0';

Readonly::Scalar my $SCRIPT_NAME => 'npg_publish_illumina_logs.pl';

override 'create' => sub {
  my $self = shift;

  my $ref = $self->basic_definition_init_hash();

  if (!$ref->{'excluded'}) {

    $self->assign_common_definition_attrs(
      $ref, (join q{_}, q{publish_logs}, $self->label()));

    $ref->{'command'} = join q[ ],
      $SCRIPT_NAME,
      q{--collection},     $self->irods_destination_collection(),
      q{--runfolder_path}, npg_pipeline::runfolder_scaffold
                             ->path_in_outgoing($self->runfolder_path()),
      q{--id_run},         $self->id_run();
  }

  return [npg_pipeline::function::definition->new($ref)];
};

override 'irods_destination_collection' => sub {
  return join q[/], super(), q[log];
};

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

npg_pipeline::function::log_files_archiver

=head1 SYNOPSIS

  my $fsa = npg_pipeline::function::log_files_archiver->new(
    run_folder => 'run_folder'
  );

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 create

Creates and returns a single function definition as an array.
Function definition is created as a npg_pipeline::function::definition
type object.

  my @job_ids = $fsa->create();

=head2 irods_destination_collection

Returns iRODS destination collection for log files belonging
to this run.

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item namespace::autoclean

=item Readonly

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Jennifer Liddle
Marinan Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2019 Genome Research Ltd.

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
