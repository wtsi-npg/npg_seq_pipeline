package npg_pipeline::function::warehouse_archiver;

use Moose;
use namespace::autoclean;

use npg_pipeline::function::definition;

extends q{npg_pipeline::base};

our $VERSION = '0';

=head1 NAME

npg_pipeline::function::warehouse_archiver

=head1 SYNOPSIS

  my $c = npg_pipeline::function::warehouse_archiver->new(
    id_run => 1234,
    run_folder => q{123456_IL2_1234},
  );

=head1 DESCRIPTION

A collection of definitions for updating warehouses

=head1 SUBROUTINES/METHODS

=head2 update_warehouse

Creates command definition to update run data in the npg tables
of the warehouse.

=cut

sub update_warehouse {
  my ($self, $flag) = @_;
  return $self->_update_warehouse_command('warehouse_loader', $flag);
}

=head2 update_warehouse_post_qc_complete

Creates command definition to update run data in the npg tables
of the warehouse at a stage when the runfolder is moved to the
outgoing directory.

=cut

sub update_warehouse_post_qc_complete {
  my $self = shift;
  return $self->update_warehouse('post_qc_complete');
}

=head2 update_ml_warehouse

Creates command definition to update run data in the npg tables
of the ml warehouse.

=cut

sub update_ml_warehouse {
  my ($self, $flag) = @_;
  return $self->_update_warehouse_command('npg_runs2mlwarehouse', $flag);
}

=head2 update_ml_warehouse_post_qc_complete

Creates command definition to update run data in the npg tables
of the ml warehouse at a stage when the runfolder is moved to the
outgoing directory.

=cut

sub update_ml_warehouse_post_qc_complete {
  my $self = shift;
  return $self->update_ml_warehouse('post_qc_complete');
}

sub _update_warehouse_command {
  my ($self, $loader_name, $post_qc_complete) = @_;

  my $d;
  my $id_run = $self->id_run;

  if ($self->no_warehouse_update) {
    $self->warn(q{Update to warehouse is switched off.});
    $d = npg_pipeline::function::definition->new(
      created_by   => __PACKAGE__,
      created_on   => $self->timestamp(),
      identifier   => $id_run,
      excluded     => 1
    );
  } else {

    my $command = qq{$loader_name --verbose --id_run $id_run};
    if ($loader_name eq 'warehouse_loader') {
      $command .= q{ --lims_driver_type };
      $command .= $post_qc_complete ? 'ml_warehouse_fc_cache' : 'samplesheet';
    }
    my $job_name = join q{_}, $loader_name, $id_run, $self->pipeline_name;
    my $path = $self->make_log_dir($self->recalibrated_path());

    my $prereq = q[];
    if ($post_qc_complete) {
      $path = $self->path_in_outgoing($path);
      $prereq = "[ -d '$path' ]";
    }

    my $ref = {
      created_by   => __PACKAGE__,
      created_on   => $self->timestamp(),
      identifier   => $id_run,
      command      => $command,
      queue        =>
        $npg_pipeline::function::definition::LOWLOAD_QUEUE
    };

    if ($post_qc_complete) {
      $path = $self->path_in_outgoing($path);
      $job_name .= '_postqccomplete';
      $ref->{'command_preexec'} = "[ -d '$path' ]";
    }
    $ref->{'log_file_dir'} = $path;
    $ref->{'job_name'}     = $job_name;

    $d = npg_pipeline::function::definition->new($ref);
  }

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
