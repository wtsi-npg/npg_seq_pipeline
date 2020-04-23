package npg_pipeline::function::showback;

use Moose;
use namespace::autoclean;
use File::Find;
use Cwd;
use Readonly;
use DateTime::Format::Strptime;
use JSON;

use npg_pipeline::function::definition;

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
  $command .= q{ --bam_basecall_path=} . $self->bam_basecall_path();
  $command .= q{ --executor_type==} . $self->executor_type();
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

=head2 processfiles_wr

Run 'wr status' and parse the output into a JSON file

=cut

sub processfiles_wr
{
    my $self = shift;

    my %jobs;

    open my $wrstatus, q(-|), q(wr status -i - -z -o j --limit 0| jq -c .[]) or $self->logcroak(q(Can't open wr command));

    while (<$wrstatus>) {
	my $id = q();
        my $rec = decode_json($_);
        $_   = $rec->{'Cmd'};
	if ($rec->{State} ne 'complete') { next; };
        my $repgroup = $rec->{'RepGroup'};
        if ( $repgroup =~ m/^\S*(\d{5})\-/smx ) { next if ($self->id_run ne $1); }
	if ( $repgroup =~ m/^\S*-(\w+)$/smx ) { $id = $1; }
	if (!$id) { next; }
        my $cores = $rec->{'Cores'};
        my $ws    = $rec->{'Walltime'};
        my $cs    = $rec->{'CPUtime'};

	$jobs{$id}->{num} += 1;
	$jobs{$id}->{tcpu} += $cs;
	$jobs{$id}->{tslot} += ($ws * $cores);

    }

    close $wrstatus or $self->logcroak(q(Can't close wrstatus));

    my %results;
    $results{'jobs'} = \%jobs;
    mkdir $self->qc_path();
    open my $fh, '>', $self->qc_path . '/showback.json' or $self->logcroak('Can\'t create file ' . $self->qc_path . '/showback.json');
    print {$fh} to_json(\%results) or $self->logcroak(q(print failed));
    close $fh or $self->logcroak(q(Can't close showback.json));
    return;
}

=head2 processfiles_lsf

Read the lsf output files and parse the output into a JSON file

=cut

sub processfiles_lsf
{
    my $self = shift;

    my @m=qw(num tslot tcpu);
    my ($strp,%h,%hmt);
    $strp=DateTime::Format::Strptime->new(pattern=>q(%a %b %d %T %Y));

    my @files;
    find(sub { if (/.out$/smx) { push @files, $File::Find::name; } } , $self->bam_basecall_path() . '/log');

    foreach my $f (@files) {
        my $file_content = read_file($f);
	## no critic (RegularExpressions)
	my($nslot,$h,$ds,$de,$tcpu) = $file_content =~ m{^Job was executed on host\(s\) \<(\d*)\*?([^>]+).*^Started at ([^\n]*).*^Results reported on ([^\n]*).*^\s+CPU time :\s+(\d+\.\d+) sec\.}sm;
        $nslot||=1;
        ($ds,$de)=map{$strp->parse_datetime($_)}($ds,$de);
        if (!$ds) { next; }
        my $twall=($de->subtract_datetime_absolute($ds))->seconds;
        my $tslot=$twall*$nslot;
        my $bn=basename($f);
        my ($t)=$bn=~/^(\S+?)[._]\d{2,}[._]/smx;
        my $num=1;
        for my $m(@m) {
            my $tmp=eval{\${$m}};
            $h{$t}->{$m}+=$tmp;
            $hmt{substr $h,0,2+2 }->{$m}+=$tmp;
        }
    }

    my %results;
    $results{'jobs'} = \%h;
    $results{'hosts'} = \%hmt;
    mkdir $self->qc_path();
    open my $fh, '>', $self->qc_path . '/showback.json' or $self->logcroak('Can\'t create file ' . $self->qc_path . '/showback.json');
    print {$fh} to_json(\%results) or $self->logcroak(q(print failed));
    close $fh or $self->logcroak(q(Can't close showback.json));
    return;
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
  $self->info('basecall directory: ', $self->bam_basecall_path());
  $self->info('qc directory: ', $self->qc_path());
  $self->info('executor_type: ', $self->executor_type());

  if ($self->executor_type() eq 'lsf') {
    $self->processfiles_lsf();
  } elsif ($self->executor_type() eq 'wr') {
    $self->processfiles_wr();
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

Copyright (C) 2018 Genome Research Ltd

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

