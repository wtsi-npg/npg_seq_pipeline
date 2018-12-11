package npg_pipeline::function::seq_to_irods_archiver;

use Moose;
use namespace::autoclean;
use Readonly;

use npg_pipeline::function::definition;

extends qw{npg_pipeline::base};
with    qw{npg_pipeline::function::util};

our $VERSION = '0';

Readonly::Scalar my $PUBLISH_SCRIPT_NAME => q{npg_publish_illumina_run.pl};
Readonly::Scalar my $NUM_MAX_ERRORS      => 20;
Readonly::Scalar my $IRODS_ROOT_NON_NOVASEQ_RUNS => q[/seq];
Readonly::Scalar my $IRODS_ROOT_NOVASEQ_RUNS     => q[/seq/illumina/runs];

sub create {
  my $self = shift;

  my $ref = {
    'created_by' => __PACKAGE__,
    'created_on' => $self->timestamp(),
    'identifier' => $self->id_run(),
  };

  if ($self->no_irods_archival) {
    $self->info(q{Archival to iRODS is switched off.});
    $ref->{'excluded'} = 1;
  } else {
    my $job_name_prefix = join q{_}, q{publish_illumina_run}, $self->id_run();
    $ref->{'job_name'}  = join q{_}, $job_name_prefix, $self->timestamp();
    $ref->{'fs_slots_num'} = 1;
    $ref->{'reserve_irods_slots'} = 1;
    $ref->{'queue'} = $npg_pipeline::function::definition::LOWLOAD_QUEUE;
    $ref->{'command_preexec'} =
      qq{npg_pipeline_script_must_be_unique_runner -job_name="$job_name_prefix"};

    my $publish_log_name = join q[_], $job_name_prefix, $self->random_string();
    $publish_log_name .= q{.restart_file.json};

    my $max_errors = $self->general_values_conf()->{'publish2irods_max_errors'} || $NUM_MAX_ERRORS;
    my $command = join q[ ],
      $PUBLISH_SCRIPT_NAME,
      q{--collection},     $self->irods_destination_collection(),
      q{--archive_path},   $self->archive_path(),
      q{--runfolder_path}, $self->runfolder_path(),
      q{--restart_file},   (join q[/], $self->archive_path(), $publish_log_name),
      q{--max_errors},     $max_errors;

    if ($self->qc_run) {
      $command .= q{ --alt_process qc_run};
    }

    my @positions = $self->positions();
    my $position_list = q{};
    if (scalar @positions < scalar $self->lims->children) {
      foreach my $p  (@positions){
        $position_list .= qq{ --positions $p};
      }
      $command .=  $position_list;
    }

    if($self->has_lims_driver_type) {
      $command .= q{ --driver-type } . $self->lims_driver_type;
    }

    $ref->{'command'} = $command;
  }

  return [npg_pipeline::function::definition->new($ref)];
}

sub irods_destination_collection {
  my $self = shift;
  return join q[/],
    $self->platform_NovaSeq() ? $IRODS_ROOT_NOVASEQ_RUNS : $IRODS_ROOT_NON_NOVASEQ_RUNS,
    $self->id_run;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

npg_pipeline::function::seq_to_irods_archiver

=head1 SYNOPSIS

  my $fsa = npg_pipeline::function::seq_to_irods_archiver->new(
    run_folder => 'run_folder'
  );

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 create

Creates and returns a single function definition as an array.
Function definition is created as a npg_pipeline::function::definition
type object.

  my @job_ids = $fsa->submit_to_lsf();

=head2 irods_destination_collection

Returns iRODS destination collection for this run.

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item Readonly

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Guoying Qi
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
