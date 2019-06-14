package npg_pipeline::function::warehouse_archiver;

use Moose;
use namespace::autoclean;
use Readonly;

use npg_pipeline::function::definition;
use npg_pipeline::runfolder_scaffold;

extends q{npg_pipeline::base};

our $VERSION = '0';

Readonly::Scalar my $OLD_WH_LOADER_NAME => q{warehouse_loader};
Readonly::Scalar my $MLWH_LOADER_NAME   => q{npg_runs2mlwarehouse};

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
  my ($self, $pipeline_name, $flag) = @_;
  return $self->_update_warehouse_command($OLD_WH_LOADER_NAME, $pipeline_name, $flag);
}

=head2 update_warehouse_post_qc_complete

Creates command definition to update run data in the npg tables
of the warehouse at a stage when the runfolder is moved to the
outgoing directory.

=cut

sub update_warehouse_post_qc_complete {
  my ($self, $pipeline_name) = @_;
  return $self->update_warehouse($pipeline_name, 'post_qc_complete');
}

=head2 update_ml_warehouse

Creates command definition to update run data in the npg tables
of the ml warehouse.

=cut

sub update_ml_warehouse {
  my ($self, $pipeline_name, $flag) = @_;
  return $self->_update_warehouse_command($MLWH_LOADER_NAME, $pipeline_name, $flag);
}

=head2 update_ml_warehouse_post_qc_complete

Creates command definition to update run data in the npg tables
of the ml warehouse at a stage when the runfolder is moved to the
outgoing directory.

=cut

sub update_ml_warehouse_post_qc_complete {
  my ($self, $pipeline_name) = @_;
  return $self->update_ml_warehouse($pipeline_name, 'post_qc_complete');
}

sub _update_warehouse_command {
  my ($self, $loader_name, $pipeline_name, $post_qc_complete) = @_;

  my $m = q{};
  if ($self->no_warehouse_update) {
    $m = q{Update to warehouse is switched off.};
  } elsif ($self->has_product_rpt_list && $loader_name eq $OLD_WH_LOADER_NAME) {
    $m = q{Update to the old warehouse for individual products is switched off.};
  }

  my $d;
  if ($m) {
    $self->warn($m);
    $d = npg_pipeline::function::definition->new(
      created_by   => __PACKAGE__,
      created_on   => $self->timestamp(),
      identifier   => $self->label,
      excluded     => 1
    );
  } else {
    $pipeline_name ||= q[];
    my $job_name = join q{_}, $loader_name, $self->label, $pipeline_name;
    my $command = qq{$loader_name --verbose };

    if ($self->has_product_rpt_list) {
      $self->logcroak(q{Not implemented for individual products});
    } else {
      $command .= q{--id_run } . $self->id_run;
      if ($loader_name eq $OLD_WH_LOADER_NAME) {
        $command .= q{ --lims_driver_type };
        $command .= $post_qc_complete ? 'ml_warehouse_fc_cache' : 'samplesheet';
      }
      if ($post_qc_complete) {
        $job_name .= '_postqccomplete';
      }
    }

    $d = npg_pipeline::function::definition->new(
      created_by => __PACKAGE__,
      created_on => $self->timestamp(),
      identifier => $self->label,
      command    => $command,
      num_cpus   => [0],
      job_name   => $job_name,
      queue      =>
        $npg_pipeline::function::definition::LOWLOAD_QUEUE
    );
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

=item Readonly

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2019 Genome Research Ltd

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
