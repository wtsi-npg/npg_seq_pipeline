package npg_pipeline::executor::lsf::helper;

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;
use List::MoreUtils qw/any/;
use Sys::Hostname;
use Carp;
use POSIX;
use Readonly;

use npg_tracking::util::types;
with qw{npg_common::roles::software_location};

our $VERSION = '0';

Readonly::Scalar my $THOUSANDTH => 0.001;
Readonly::Scalar my $THOUSAND   => 1000;
Readonly::Scalar my $LOW_MEM    => 1;
Readonly::Scalar my $HI_MEM     => 96_000;

=head1 NAME

npg_pipeline::executor::lsf::helper

=head1 SYNOPSIS

=head1 SUBROUTINES/METHODS

=head2 memory - for inputting required memory 

=cut

has q{memory}       => (isa        => q{NpgTrackingPositiveInt},
                        is         => q{ro},
                       );

=head2 memory_units - one of KB, MB, GB; defaults to MB. 

=cut

has q{memory_units} => (isa => q{Str},
                        is => q{ro},
                        default => 'MB',
                       );

=head2 memory_units - one of KB, MB, GB; defaults to MB. 

=cut

has q{memory_in_mb} => (isa => q{NpgTrackingPositiveInt},
                        is => q{ro},
                        lazy_build => 1,
                       );

sub _build_memory_in_mb {
  my $self = shift;

  my $memory = $self->memory();
  my $memory_units = $self->memory_units();

  if (!($self->_is_valid_memory())) {
    croak "lsf_job cannot handle request for memory $memory $memory_units";
  }
  if (!($self->_is_valid_memory_unit())) {
    croak "lsf_job does not recognise requested memory unit $memory_units";
  }

  return POSIX::floor($self->memory * $self->_find_memory_factor($self->memory_units));
}

=head2 lsadmin_cmd lsadmin command to build the correct memory_limit
 
=cut

has 'lsadmin_cmd'   => ( is      => 'ro',
                         isa     => 'NpgCommonResolvedPathExecutable',
                         coerce  => 1,
                         default => 'lsadmin',
                      );


=head2 memory_spec - returns an appropriate bsub component 

-R 'select[mem>8000] rusage[mem=8000]'  -M8000000" on lenny
-R 'select[mem>8000] rusage[mem=8000]'  -M8000" on precise

=cut

sub memory_spec {
  my ($self) = @_;

  my $memory_limit = $self->_scale_mem_limit();
  my $resource_memory = $self->memory_in_mb();
  my $memory_spec = "-R 'select[mem>$resource_memory] rusage[mem=$resource_memory]' -M$memory_limit";
  return $memory_spec;
}

=head2 _is_valid_memory

Checks that memory requested is more than $LOW_MEM and less than $HI_MEM

=cut

sub _is_valid_memory {
  my ($self) = @_;

  my $memory_requested =$self->memory() * $self->_find_memory_factor($self->memory_units());

  my $match = (($memory_requested < $HI_MEM) && ($memory_requested > $LOW_MEM));
  my $ret = ($match eq q{}) ? 0 : 1;
  return $ret;
}

=head2 _is_valid_memory_unit

=cut

sub _is_valid_memory_unit {
  my ($self) = @_;
  my @valid_memory_units = qw(KB MB GB);

  my $ret = any { ($_ ) && ($_ eq $self->memory_units) } @valid_memory_units;
  return $ret;
}

=head2  _is_valid_lsf_memory_unit

=cut

sub _is_valid_lsf_memory_unit {
  my ($self, $lsf_memory_units) = @_;
  my @valid_lsf_memory_units = qw(KB MB);

  my $ret = any { $_ && $_ eq $lsf_memory_units } @valid_lsf_memory_units;
  return $ret;
}

=head2 _find_memory_units

=cut

sub _find_memory_units {
  my ($self) = @_;

  my $hostname = hostname;
  $hostname =~ s/\n//smx;

  my $cmd = $self->lsadmin_cmd.q{ showconf lim }.$hostname.
            q{ | grep LSF_UNIT_FOR_LIMITS} .
            q{ || echo "LSF_UNIT_FOR_LIMITS = KB"};

  my $version = `$cmd`;
  my ($text, $equals, $unit) = split / /sm, $version;
  $unit =~ s/\n//smx;
  if ((defined $unit) && $self->_is_valid_lsf_memory_unit($unit)) {
    return $unit;
  } else {
    croak qq{Cannot get LSF_UNIT_FOR_LIMITS via lsadmin ($cmd)};
  }

}

=head2 _scale_mem_limit

=cut

sub _scale_mem_limit {
  my ($self) = @_;

  my $memory_in_mb = $self->memory_in_mb();

  my $ret = ($self->_find_memory_units() eq 'KB') ? ($memory_in_mb * $THOUSAND) : $memory_in_mb;

  return $ret;
}

=head2 _find_memory_factor

=cut

sub _find_memory_factor {
  my ($self, $unit) = @_;
  my $ret = (($unit eq 'KB') ? $THOUSANDTH : (($unit eq 'MB') ? 1 : (($unit eq 'GB') ? $THOUSAND : 1)));
  return $ret;
}

=head2 create_array_string

 Takes an array of integers, and then converts them to an LSF job array string
 for appending to a job_name

 my $sArrayString = $oClass->create_array_string( 1,4,5,6,7,10... );

=cut

sub create_array_string {
  my ( $self, @lsf_indices ) = @_;

  my ( $start_run, $end_run, $ret );
  $ret = q{};
  foreach my $entry ( @lsf_indices ) {
    # have we already started looping through
    if ( defined $end_run ) {
    # if the number is consecutive, increment end of the run
      if ( $entry == $end_run + 1 ) {
        $end_run = $entry;
        # otherwise, finish up that run, which may just be a single number
      } else {
        if ( $start_run != $end_run ) {
          $ret .= q{-} . $end_run;
        }
        $ret .= q{,} . $entry;
        $start_run = $end_run = $entry;
      }
    # we haven't looped through at least once, so set up
    } else {
      $ret .= $entry;
      $start_run = $end_run = $entry;
    }
  }

  if ( $start_run != $end_run ) {
    $ret .= q{-} . $end_run ;
  }
  $ret = q{[} . $ret . q{]};

  return $ret;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 DESCRIPTION

A collection of LSF-specific helper methods.

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item List::MoreUtils

=item Sys::Hostname

=item Moose

=item MooseX::StrictConstructor

=item namespace::autoclean

=item Carp

=item Readonly

=item POSIX

=item npg_tracking::util::types

=item npg_common::roles::software_location

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

=over

=item Kate Taylor

=item Andy Brown

=item Marina Gourtovaia

=back

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
