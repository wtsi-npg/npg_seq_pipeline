package npg_pipeline::function::seq_to_irods_archiver;

use Moose;
use namespace::autoclean;
use Readonly;

use npg_pipeline::function::definition;

extends 'npg_pipeline::base';
with    qw{npg_pipeline::function::util
           npg_pipeline::runfolder_scaffold
           npg_pipeline::product::release
           npg_pipeline::product::release::irods};

our $VERSION = '0';

Readonly::Scalar my $PUBLISH_SCRIPT_NAME => q{npg_publish_illumina_run.pl};
Readonly::Scalar my $NUM_MAX_ERRORS      => 20;

has 'lims_driver_type' => (
  required => 0,
  isa      => 'Str',
  is       => 'ro',
);

sub create {
  my $self = shift;

  my $ref = $self->basic_definition_init_hash();
  my @definitions = ();

  if (!$ref->{'excluded'}) {

    $self->ensure_restart_dir_exists();
    my $job_name_prefix = join q{_}, q{publish_seq_data2irods}, $self->label();

    my $command = join q[ ],
      $PUBLISH_SCRIPT_NAME,
      q{--max_errors},     $self->num_max_errors();

    if($self->lims_driver_type) {
      $command .= q{ --driver-type } . $self->lims_driver_type;
    }

    my $run_collection = $self->irods_destination_collection();
    foreach my $product (@{$self->products->{'data_products'}}) {
      if ($self->is_for_irods_release($product)) {
        my %dref = %{$ref};
        $dref{'composition'}           = $product->composition;
        $dref{'command'} = sprintf '%s --restart_file %s --collection %s --source_directory %s',
          $command,
          $self->restart_file_path($job_name_prefix, $product),
          $self->irods_product_destination_collection($run_collection, $product),
	  $product->path($self->archive_path());
        $self->assign_common_definition_attrs(\%dref, $job_name_prefix);
        push @definitions, npg_pipeline::function::definition->new(\%dref);
      }
    }

    if (!@definitions) {
      $self->info(q{No products to archive to iRODS});
      $ref->{'excluded'} = 1;
    }
  }

  return @definitions ? \@definitions : [npg_pipeline::function::definition->new($ref)];
}

sub basic_definition_init_hash {
  my $self = shift;

  if ($self->has_product_rpt_list) {
    $self->logcroak(q{Not implemented for individual products});
  }

  my $ref = {
    'created_by' => ref $self,
    'created_on' => $self->timestamp(),
    'identifier' => $self->label(),
  };

  if ($self->no_irods_archival) {
    $self->info(q{Archival to iRODS is switched off.});
    $ref->{'excluded'} = 1;
  }

  return $ref;
}

sub assign_common_definition_attrs {
  my ($self, $ref, $job_name_prefix) = @_;

  $ref->{'job_name'}  = join q{_}, $job_name_prefix, $self->timestamp();
  $ref->{'fs_slots_num'} = 1;
  $ref->{'reserve_irods_slots'} = 1;
  $ref->{'queue'} = $npg_pipeline::function::definition::LOWLOAD_QUEUE;
  $ref->{'command_preexec'} =
    qq{npg_pipeline_script_must_be_unique_runner -job_name="$job_name_prefix"};

  return;
}

sub num_max_errors {
  my $self = shift;
  return $self->general_values_conf()->{'publish2irods_max_errors'} || $NUM_MAX_ERRORS;
}

sub restart_file_path {
  my ($self, $job_name_prefix, $product_obj) = @_;
  my $file_name = join q[_], $job_name_prefix, $self->random_string();
  if ($product_obj) {
    $file_name .= q[_] . $product_obj->composition->digest();
  }
  $file_name .= q{.restart_file.json};
  return join q[/], $self->irods_publisher_rstart_dir_path(), $file_name;
}

sub ensure_restart_dir_exists {
  my $self = shift;
  ####
  # Create a directory for publisher's restart files.
  # The directory is normally created by the analysis runfolder
  # scaffold, but might be absent for run folders with older
  # analysis results.
  $self->make_dir($self->irods_publisher_rstart_dir_path());
  return;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

npg_pipeline::function::seq_to_irods_archiver

=head1 SYNOPSIS

  my $archiver = npg_pipeline::function::seq_to_irods_archiver
                 ->new(runfolder_path => '/some/path'
                       id_run         => 22);
  my $definitions = $archiver->create();
  my $idest = $archiver->irods_destination_collection();

=head1 DESCRIPTION

Defines a job for publishing sequencing data to iRODS. Only product
data and their accompanying files are published by this job. Some
studies might be configured not to publish their products ti iRODS.

=head1 SUBROUTINES/METHODS

=head2 lims_driver_type

An optional attribute, the name of the lims driver to pass to the
iRODS loader script.

=head2 create

Creates and returns a single function definition as an array.
Function definition is created as a npg_pipeline::function::definition
type object.

=head2 basic_definition_init_hash

Creates and returns a hash reference suitable for initialising a basic
npg_pipeline::function::definition object. created_by, created_on and
identifier attributes are assigned. If no_irods_archival flag is set,
thw excluded attribute is set to true.

=head2 assign_common_definition_attrs

Given a hash reference as an argument, adds job_name, fs_slots_num,
reserve_irods_slots, queue and command_preexec key-value pairs
to the hash.

=head2 num_max_errors

Returns the maximum number of errors aftre wich the iRODs publisher
should exit.

=head2 restart_file_path

Given a job name prefix, returns a full path of the iRODS publisher
restart file.

=head2 ensure_restart_dir_exists

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

=over

=item Guoying Qi

=item Marina Gourtovaia

=back

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2018,2019,2020,2021 Genome Research Ltd.

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
