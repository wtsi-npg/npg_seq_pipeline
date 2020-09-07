package npg_pipeline::function::showback;

use Moose;
use namespace::autoclean;
use File::Find;
use Cwd;
use Readonly;
use DateTime::Format::Strptime;
use JSON;

use npg_pipeline::function::definition;
use npg_pipeline::executor::showback_wr;
use npg_pipeline::executor::showback_lsf;
extends qw{npg_pipeline::base};

our $VERSION = '0';

Readonly::Scalar my $SEQCHKSUM_SCRIPT => q{npg_pipeline_showback};

=head1 NAME

npg_pipeline::function::showback
  
=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=cut

=head2 create

Creates and returns a per-run function definition in an array.
The function definition is created as a npg_pipeline::function::definition
type object.

=cut

has 'executor_type' => ( isa    => 'Str',
                         is     => 'ro',
                         required => 0,
                       );

sub create {
  my $self = shift;

  my $job_name = join q{_}, 'showback', $self->id_run(), $self->timestamp();
  my $command = $SEQCHKSUM_SCRIPT;
  $command .= q{ --id_run=} . $self->id_run();
  $command .= q[ ];
  $command .= join q[ ], (map { qq{--lanes=$_} } ($self->positions));
  $command .= q{ --archive_path=} . $self->archive_path();
  $command .= q{ --analysis_path=} . $self->analysis_path();
  $command .= q{ --executor_type=} . $self->executor_type();
  if ($self->verbose() ) {
    $command .= q{ --verbose};
  }

  my @definitions = ();

  push @definitions, npg_pipeline::function::definition->new(
    created_by   => __PACKAGE__,
    created_on   => $self->timestamp(),
    identifier   => $self->id_run(),
    job_name     => $job_name,
    command      => $command,
  );

  return \@definitions;
}

=head2 process_run

Read and store runtime data for one run

=cut

sub process_run {
  my ($self) = @_;

  my $wd = getcwd();
  $self->info('cwd: ', $wd);
  $self->info('id_run: ', $self->id_run);
  $self->info('archive directory: ', $self->archive_path());
  $self->info('analysis directory: ', $self->analysis_path());
  $self->info('qc directory: ', $self->qc_path());
  $self->info('executor_type: ', $self->executor_type());

  if ($self->executor_type() eq 'lsf') {
    my $sb = npg_pipeline::executor::showback_lsf->new(
        id_run => $self->id_run
    );
    $sb->processfiles();
  } elsif ($self->executor_type() eq 'wr') {
    my $sb = npg_pipeline::executor::showback_wr->new(
        id_run => $self->id_run
    );
    $sb->processfiles();
  } else {
    $self->logcroak('Unknown executor type: ' . $self->executor_type());
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

=item namespace::autoclean

=item Readonly

=item File::Find;

=item Cwd;

=item Readonly;

=item DateTime::Format::Strptime; 

=item JSON;

=item npg_pipeline::function::definition;

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Jennifer Liddle

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2020 Genome Research Ltd

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

