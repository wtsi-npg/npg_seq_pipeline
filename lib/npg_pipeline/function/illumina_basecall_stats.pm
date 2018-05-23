package npg_pipeline::function::illumina_basecall_stats;

use Moose;
use namespace::autoclean;
use Readonly;

use npg_pipeline::function::definition;

extends 'npg_pipeline::base';
with    'npg_common::roles::software_location';

our $VERSION = '0';

Readonly::Scalar my $MAKE_STATS_J   => 4;
Readonly::Scalar my $MAKE_STATS_MEM => 350;

=head1 NAME

npg_pipeline::function::illumina_basecall_stats

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 bcl2qseq

Absolute path to executable that generates Illumina basecall stats 

=cut 

has 'bcl2qseq' => ( isa        => 'NpgCommonResolvedPathExecutable',
                    is         => 'ro',
                    coerce     => 1,
                    lazy_build => 1,);
sub _build_bcl2qseq {
  return 'setupBclToQseq.py'
}

=head2 create

Creates a definition for a job which uses Illumina tools to generate
the (per run) BustardSummary and IVC reports (from on instrument RTA basecalling).
Returns an array with a single npg_pipeline::function::definition object.

The excluded attribute of the object will be set to true for a HiSeq
instrument run.

=cut

sub create {
  my $self = shift;

  my $ref = {
    'created_by' => __PACKAGE__,
    'created_on' => $self->timestamp(),
    'identifier' => $self->id_run()
  };

  if ( $self->is_hiseqx_run ) {
    $self->info(q{HiSeqX sequencing instrument, illumina_basecall_stats will not be run});
    $ref->{'excluded'} = 1;
  } else {
    my $basecall_dir = $self->basecall_path();
    my $dir = $self->bam_basecall_path();

    $ref->{'job_name'} = join q{_}, q{basecall_stats}, $self->id_run(), $self->timestamp();
    $ref->{'memory'}       = $MAKE_STATS_MEM;
    $ref->{'num_cpus'}     = [$MAKE_STATS_J];
    $ref->{'fs_slots_num'} = $MAKE_STATS_J;
    $ref->{'num_hosts'}    = 1;

    my $bcl2qseq_path = $self->bcl2qseq;
    my $cmd = join q[ && ],
      qq{cd $dir},
      q{if [[ -f Makefile ]]; then echo Makefile already present 1>&2; else echo creating bcl2qseq Makefile 1>&2; } .
        qq{$bcl2qseq_path -b $basecall_dir -o $dir --overwrite; fi},
      qq[make -j $MAKE_STATS_J Matrix Phasing],
      qq[make -j $MAKE_STATS_J BustardSummary.x{s,m}l];
    $ref->{'command'} = $cmd;
  }

  return [npg_pipeline::function::definition->new($ref)];
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

=item npg_common::roles::software_location

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Steven Leonard

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
