package npg_pipeline::analysis::illumina_basecall_stats;

use Moose;
use Readonly;
use npg_pipeline::lsf_job;

extends 'npg_pipeline::base';
with    'npg_common::roles::software_location';

our $VERSION = '0';

Readonly::Scalar our $MAKE_STATS_J => 4;
Readonly::Scalar our $MAKE_STATS_MEM => 350;

=head1 NAME

  npg_pipeline::analysis::illumina_basecall_stats

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

sub _generate_command {
  my ( $self, $arg_refs ) = @_;

  my $job_dependencies = $arg_refs->{'required_job_completion'};

  my $basecall_dir = $self->basecall_path();
  my $dir = $self->bam_basecall_path();

  $self->make_log_dir( $dir ); # create a log directory within bam_basecalls

  my $bsub_queue  = $self->lsf_queue;
  my $job_name  =  q{basecall_stats_} . $self->id_run() . q{_} . $self->timestamp();

  my @command;
  push @command, 'bsub';
  push @command, "-q $bsub_queue";
  push @command, qq{-o $dir/log/}. $job_name . q{.%J.out};
  push @command, "-J $job_name";

  my $hosts = 1;
  my $memory_spec = join q[], npg_pipeline::lsf_job->new(memory => $MAKE_STATS_MEM)->memory_spec(), " -R 'span[hosts=$hosts]'";
  push @command, $self->fs_resource_string( {
    resource_string       => $memory_spec,
    counter_slots_per_job => $MAKE_STATS_J,
  } );
  push @command,  q{-n } . $MAKE_STATS_J;
  push @command, $job_dependencies || q[];

  push @command, q["]; # " enclose command in quotes

  my $bcl2qseq_path = $self->bcl2qseq;
  my $cmd = join q[ && ],
    qq{cd $dir},
    q{if [[ -f Makefile ]]; then echo Makefile already present 1>&2; else echo creating bcl2qseq Makefile 1>&2; }.
      qq{$bcl2qseq_path -b $basecall_dir -o $dir --overwrite; fi},
    qq[make -j $MAKE_STATS_J Matrix Phasing],
    qq[make -j $MAKE_STATS_J BustardSummary.x{s,m}l];

  push @command,$cmd;

  push @command, q["]; # " closing quote

  return join q[ ], @command;
}

=head2 generate

Use Illumina tools to generate the (per run) BustardSummary
and IVC reports (from on instrument RTA basecalling).

=cut

sub generate {
  my ( $self, $arg_refs ) = @_;
  return $self->submit_bsub_command($self->_generate_command($arg_refs));
}

no Moose;

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item Readonly

=item npg_common::roles::software_location

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Steven Leonard

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2017 Genome Research Ltd

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
