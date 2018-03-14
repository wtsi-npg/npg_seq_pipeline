package npg_pipeline::executor::lsf::options;

use Moose::Role;

our $VERSION = '0';

=head1 NAME

npg_pipeline::executor::lsf::options

=head1 SYNOPSIS

=head1 DESCRIPTION

Pipeline options to be used by the LSF scheduler.

=head1 SUBROUTINES/METHODS

=head2 interactive

=cut

has q{interactive}  => (
  isa           => q{Bool},
  is            => q{ro},
  default       => 0,
  documentation =>
  q{If false (default), the pipeline_start job is resumed } .
  q{once all jobs have been successfully submitted},
);

=head2 job_name_prefix

Value to be prepended to LSF job names.
Underscore is added automatically after the prefix

This can be set in the lsf.ini config file,
but will be overridden if given on the command line.

=cut

has q{job_name_prefix} => (
  isa           => q{Str},
  is            => q{ro},
  predicate     => q{has_job_name_prefix},
  documentation => q{LSF jobs name prefix},
);

=head2 job_priority

A priority value to be used for all jobs to LSF.
Not setting this will use the LSF queue default.
Will be used on all LSF jobs, regardless of the queue used.

=cut

has q{job_priority} => (
  isa           => q{Int},
  is            => q{ro},
  predicate     => q{has_job_priority},
  documentation => q{User defined LSF jobs priority},
);

=head2 no_sf_resource

Do not use sf resource tokens; set if working outside the npg sequencing farm

=cut

has q{no_sf_resource} => (
  isa           => q{Bool},
  is            => q{ro},
  documentation =>
  q{Do not use sf resource tokens},
);

=head2 no_bsub

Disable submitting any jobs to LSF, so the full pipeline can be run,
and all commands logged.

=cut

has q{no_bsub} => (
  isa           => q{Bool},
  is            => q{ro},
  documentation =>
  q{LSF executor will not submit jobs, it will log them instead},
);

=head2 no_array_cpu_limit

Flag option to allow job arrays to flood, if able, the farm

=cut

has q{no_array_cpu_limit} => (
  isa           => q{Bool},
  is            => q{ro},
  documentation =>
  q{Allow job arrays to keep launching if cpus available},
);

=head2 array_cpu_limit

The largest number of cpus which each job array can use at a time
shoudl be applied only if no_array_cpu_limit not set

=cut

has q{array_cpu_limit} => (
  isa           => q{Int},
  is            => q{ro},
  predicate     => q{has_array_cpu_limit},
  documentation =>
  q{The largest number of CPUs that an LSF job array can use at a time},
);

1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose::Role

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
