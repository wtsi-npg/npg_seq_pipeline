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
Readonly::Scalar my $OLD_DATED_DIR_NAME  => q[20180717];

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

    if ($self->qc_run) {
      $command .= q{ --alt_process qc_run};
    }

    if($self->has_lims_driver_type) {
      $command .= q{ --driver-type } . $self->lims_driver_type;
    }

    my $old_dated_dir = $self->_find_old_dated_dir();
    if ($old_dated_dir) {
      $command .= q{ --restart_file } . $self->restart_file_path($job_name_prefix);
      my @positions = $self->positions();
      my $position_list = q{};
      if (scalar @positions < scalar $self->lims->children) {
        foreach my $p  (@positions){
          $position_list .= qq{ --positions $p};
        }
        $command .= $position_list;
      }
      $command .= join q[ ], q[],
        q{--archive_path},   $self->archive_path(),
        q{--runfolder_path}, $self->runfolder_path();
      # Relying on PATH being the same on the host where we run the
      # pipeline script and on the host where the job is going to be
      # executed.
      $command = join q[;], "export PATH=$old_dated_dir/bin:".$ENV{PATH},
                            "export PERL5LIB=$old_dated_dir/lib/perl5",
                            $command;
      $self->info(qq[iRODS loader command "$command"]);
      $ref->{'command'} = $command;
      $self->assign_common_definition_attrs($ref, $job_name_prefix);
      push @definitions, npg_pipeline::function::definition->new($ref);
    } else {
      my $run_collection = $self->irods_destination_collection();
      foreach my $product (@{$self->products->{'data_products'}}) {
        if ($self->is_for_irods_release($product)) {
          my %dref = %{$ref};
          $dref{'array_cpu_limit'}       = 1; # One job at a time
          $dref{'apply_array_cpu_limit'} = 1;
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

sub _find_old_dated_dir {
  my $self = shift;

  my $old_dated_dir;
  if (-e $self->qc_path()) {
    $self->info('Old style runfolder - have to use old iRODS loader');

    my $local_bin = $self->local_bin(); # This pipeline script's bin as
                                        # an absolute path.
    my ($dated_directory_root) = $local_bin =~ /(.+)201[89]\d\d\d\d\/bin\Z/xms;
    if ($dated_directory_root) {
      $old_dated_dir = $dated_directory_root . $OLD_DATED_DIR_NAME;
      if (-e $old_dated_dir) {
        $self->info(qq{Found old dated directory $old_dated_dir});
      } else {
        undef $old_dated_dir;
        $self->logwarn(qq{Old dated directory $old_dated_dir does not exist});
      }
    } else {
      $self->logwarn(q{Failed to find old dated directory});
    }
  }

  return $old_dated_dir;
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

Defines a job for publishing sequencing data to iRODS. For new style
run folders only product data and their accompanyig files will be
published by this job.

=head1 SUBROUTINES/METHODS

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

Guoying Qi
Marina Gourtovaia

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
