package npg_pipeline::executor::showback_wr;

use Moose;
use namespace::autoclean;
use File::Find;
use Cwd;
use Readonly;
use DateTime::Format::Strptime;
use JSON;

#use npg_pipeline::function::definition;
#use npg_pipeline::executor::
extends qw{npg_pipeline::base};

our $VERSION = '0';

=head1 NAME

npg_pipeline::executor::showback_wr
  
=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=cut

=head2 processfiles

Run 'wr status' and parse the output into a JSON file

=cut

sub processfiles
{
    my $self = shift;

    my %jobs;

    $self->info('running wr status looking for run ', $self->id_run);

    open my $wrstatus, q(-|), q(wr status -i - -z -o j --limit 0| jq -c .[]) or $self->logcroak(q(Can't open wr command));

    while (<$wrstatus>) {
        my $id = q();
        my $rec = decode_json($_);
        $_   = $rec->{'Cmd'};
        if ($rec->{State} ne 'complete') { next; };
        my $repgroup = $rec->{'RepGroup'};
        my $idrun = q();
        my $prefix = q();
        if ( $repgroup =~ m/^(\S*)\-(\d{5})\-/smx ) { $prefix = $1; $idrun = $2; }
        elsif ($repgroup =~ m/^\S*(\d{5})\-/smx ) { $idrun = $1; }
        next if ($self->id_run ne $idrun);
        if ( $repgroup =~ m/^\S*-(\w+)$/smx ) { $id = $1; }
        if (!$id) { next; }
        my $cores = $rec->{'Cores'};
        my $ws    = $rec->{'Walltime'};
        my $cs    = $rec->{'CPUtime'};

        if ($prefix) { $id = $prefix . q(-) . $id; }
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

