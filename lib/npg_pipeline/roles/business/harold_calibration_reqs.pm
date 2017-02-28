package npg_pipeline::roles::business::harold_calibration_reqs;

use Moose::Role;
use English qw{-no_match_vars};
use Carp;
use Readonly;

requires qw{directory_exists};

our $VERSION = '0';

# hard-coded default parameters for running harold_calibration steps, can be overriden on the command line
# whilst these can be overridden, they are requested to be how the pipeline operates, so we don't
# want them in a config file
Readonly::Scalar our $CAL_TABLE_JOB             => q{PB_cal_table};
Readonly::Scalar our $SCORE_JOB                 => q{PB_cal_score};
Readonly::Scalar our $ALIGN_JOB                 => q{PB_cal_align};
Readonly::Scalar our $PB_DIRECTORY              => q{PB_cal};

=head1 NAME

npg_pipeline::roles::business::harold_calibration_reqs

=head1 SYNOPSIS

  package MyPackage;
  use Moose;
  ...
  with qw{npg_pipeline::roles::business::harold_calibration_reqs};

=head1 DESCRIPTION

This role is designed to be able to apply all the harold calibration variables, including lsf requirements,
for internal running of the harold calibration steps.

Note, your class must provide the following methods

 'directory_exists'

=head1 SUBROUTINES/METHODS
=cut

has q{random} => (isa => q{Int}, is => q{ro}, lazy_build => 1,
  documentation => q{Default from pb_cal_pipeline.ini},);

sub _build_random {
  my ( $self ) = @_;
  return $self->pb_cal_pipeline_conf()->{random};
}

has q{t_filter} => (isa => q{Int}, is => q{ro}, lazy_build => 1,
  documentation => q{t_filter value},);

sub _build_t_filter {
  my ( $self ) = @_;
  return $self->pb_cal_pipeline_conf()->{t_filter};
}

has q{mem_calibration} => (isa => q{Int}, is => q{ro}, lazy_build => 1,
  documentation => q{memory to be used for calibration table creation jobs},);

sub _build_mem_calibration {
  my ( $self ) = @_;
  return $self->pb_cal_pipeline_conf()->{mem_calibration};
}

has q{mem_score} => (isa => q{Int}, is => q{ro}, lazy_build => 1,
  documentation => q{memory to be used for scoring jobs},);

sub _build_mem_score {
  my ( $self ) = @_;
  return $self->pb_cal_pipeline_conf()->{mem_score};
}

has q{cal_table_job} => (isa => q{Str}, is => q{ro}, lazy_build => 1,
  documentation => qq{Default : $CAL_TABLE_JOB},);

sub _build_cal_table_job { return $CAL_TABLE_JOB; }

has q{cal_table_script} => (isa => q{Str}, is => q{ro}, lazy_build => 1,
  documentation => q{Default from pb_cal_pipeline.ini},);

sub _build_cal_table_script {
  my ( $self ) = @_;
  return $self->pb_cal_pipeline_conf()->{cal_table_script};
}

has q{align_job} => (isa => q{Str}, is => q{ro}, lazy_build => 1,
  documentation => qq{Default : $ALIGN_JOB},);

sub _build_align_job { return $ALIGN_JOB; }

has q{alignment_script} => (isa => q{Str}, is => q{ro}, lazy_build => 1,
  documentation => q{Default from pb_cal_pipeline.ini},);

sub _build_alignment_script {
  my ( $self ) = @_;
  return $self->pb_cal_pipeline_conf()->{alignment_script};
}

has q{recalibration_script} => (isa => q{Str}, is => q{ro}, lazy_build => 1,
  documentation => q{Default from pb_cal_pipeline.ini},);

sub _build_recalibration_script {
  my ( $self ) = @_;
  return $self->pb_cal_pipeline_conf()->{recalibration_script};
}

has q{pb_directory} => (isa => q{Str}, is => q{ro}, lazy_build => 1, init_arg => undef);

sub _build_pb_directory { return $PB_DIRECTORY; }

has q{score_job} => (isa => q{Str}, is => q{ro}, lazy_build => 1,
  documentation => qq{Default : $SCORE_JOB},);

sub _build_score_job { return $SCORE_JOB; }

has q{region_size} => (isa => q{Int}, is => q{ro}, lazy_build => 1,
  documentation => q{Default in pb_cal_pipeline.ini},);

sub _build_region_size {
  my ( $self ) = @_;
  return $self->pb_cal_pipeline_conf()->{region_size};
}

=head2 calibration_table_name

generates the calibration table name expected, requiring the id_run and read to be passes in
if no control lane can be worked out, will return an empty string

  my $sCalibrationTableName = $class->calibration_table_name( $iIdRun, $iRead );

=cut

sub calibration_table_name {
  my ($self, $arg_refs ) = @_;
  my $id_run = $arg_refs->{id_run};
  if( $arg_refs->{read} ) {
    $self->logcroak(q{read is a deprecated argument});
  }
  my $position = $arg_refs->{position};
  # set the mode
  if( $arg_refs->{mode} ) {
    $self->logcroak(q{mode is a deprecated argument});
  }

  if ( ! $position ) {
    $self->warn(q{no position obtained});
    return q{};
  }

  return $id_run . q{_} . $position . $self->pb_cal_pipeline_conf()->{cal_table_suffix};
}


=head2 create_pb_calibration_directory

checks for the existence of a pb_calibration directory and if it doesn't exist, will create it

returns the path of the pb_calibration directory

=cut

sub create_pb_calibration_directory {
  my ( $self ) = @_;

  my $pb_cal_dir = $self->pb_cal_path();

  if ( ! $self->directory_exists( $pb_cal_dir ) ) {
    $self->info(qq{Creating $pb_cal_dir});

    my $output = qx[mkdir $pb_cal_dir];
    if ($CHILD_ERROR) {
      $self->logcroak(qq{Unable to create $pb_cal_dir});
    }

    $self->info(qq{Created : $output});
  }

  $self->make_log_dir( $pb_cal_dir );

  return $pb_cal_dir;
}

1;
__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose::Role

=item Carp

=item English -no_match_vars

=item Readonly

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2014 Genome Research Ltd

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
