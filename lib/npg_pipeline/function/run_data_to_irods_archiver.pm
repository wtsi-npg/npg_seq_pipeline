package npg_pipeline::function::run_data_to_irods_archiver;

use Moose;
use namespace::autoclean;
use Readonly;

use npg_pipeline::function::definition;

extends qw{npg_pipeline::function::seq_to_irods_archiver};

our $VERSION = '0';

Readonly::Scalar my $PUBLISH_SCRIPT_NAME => q{npg_publish_illumina_run.pl};

override 'create' => sub {
  my $self = shift;

  my $ref = $self->basic_definition_init_hash();

  if (!$ref->{'excluded'}) {

    my $job_name_prefix = join q{_}, q{publish_run_data2irods}, $self->id_run();
    $self->assign_common_definition_attrs($ref, $job_name_prefix);

    my $command = join q[ ],
      $PUBLISH_SCRIPT_NAME,
      q{--restart_file},     $self->restart_file_path($job_name_prefix),
      q{--max_errors},       $self->num_max_errors(),
      q{--collection},       $self->irods_destination_collection(),
      q{--source_directory}, $self->runfolder_path(),
      q{--include},          q['RunInfo.xml'],
      q{--include},          q['[Rr]unParameters.xml'],
      q{--include},          q[InterOp],
      q{--id_run},           $self->id_run;

    $self->info(qq[iRODS loader command "$command"]);
    $ref->{'command'} = $command;
  }

  return [npg_pipeline::function::definition->new($ref)];
};

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

npg_pipeline::function::run_data_to_irods_archiver

=head1 SYNOPSIS

  my $archiver = npg_pipeline::function::run_data_to_irods_archiver
                 ->new(runfolder_path => '/some/path'
                       id_run         => 22);
  my $definitions = $archiver->create();

=head1 DESCRIPTION

Defines a job for publishing Illumina run data to iRODS.

=head1 SUBROUTINES/METHODS

=head2 create

Creates and returns a single function definition as an array.
Function definition is created as a npg_pipeline::function::definition
type object.

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item Readonly

=item namespace::autoclean

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2018 Genome Research Ltd.

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
