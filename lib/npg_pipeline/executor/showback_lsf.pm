package npg_pipeline::executor::showback_lsf;

use Moose;
use namespace::autoclean;
use File::Find;
use Cwd;
use Readonly;
use DateTime::Format::Strptime;
use JSON;
use File::Slurp;

extends qw{npg_pipeline::base};

our $VERSION = '0';

=head1 NAME

npg_pipeline::executor::showback_lsf
  
=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=cut

=head2 processfiles

Read the lsf output files and parse the output into a JSON file

=cut

sub processfiles
{
    my $self = shift;

    my @m=qw(num tslot tcpu);
    my ($strp,%h,%hmt);
    $strp=DateTime::Format::Strptime->new(pattern=>q(%a %b %d %T %Y));

    my @files;
    find(sub { if (/.out$/smx) { push @files, $File::Find::name; } } , $self->analysis_path() . '/log');

    foreach my $f (@files) {
        my $file_content = read_file($f);
        ## no critic (RegularExpressions)
        my($nslot,$h,$ds,$de,$tcpu) = $file_content =~ m{^Job was executed on host\(s\) \<(\d*)\*?([^>]+).*^Started at ([^\n]*).*^Results reported on ([^\n]*).*^\s+CPU time :\s+(\d+\.\d+) sec\.}sm;

        if (!$ds) { next; }
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

