#########
# Author:        David K. Jackson
# Created:       2011-11-29
#

package npg::samplesheet::auto;

use Moose;
use Try::Tiny;
use File::Basename;
use Readonly;
use File::Copy;
use File::Spec::Functions;

use npg::samplesheet;
use npg_tracking::Schema;
use st::api::lims::samplesheet;

our $VERSION = '0';

Readonly::Scalar our $DEFAULT_SLEEP => 90;

with q(MooseX::Log::Log4perl);

=head1 NAME

npg::samplesheet::auto

=head1 VERSION

=head1 SYNOPSIS

  use npg::samplesheet::auto;
  use Log::Log4perl qw(:easy);
  BEGIN{ Log::Log4perl->easy_init({level=>$INFO,}); }
  npg::samplesheet::auto->new()->loop(); # in a daemon

=head1 DESCRIPTION

Class for creating  MiSeq samplesheets automatically for runs which are pending.

=head1 SUBROUTINES/METHODS

=cut

has 'npg_tracking_schema' => (
  'isa' => 'npg_tracking::Schema',
  'is' => 'ro',
  'lazy_build' => 1,
  'metaclass' => 'NoGetopt',
);
sub _build_npg_tracking_schema { my$s=npg_tracking::Schema->connect(); return $s}

has _miseq => ( q(is) => q(ro), lazy_build => 1 );
sub _build__miseq {
  my $self=shift;
  return $self->npg_tracking_schema->resultset(q(InstrumentFormat))->find({q(model)=>q(MiSeq)});
}
has _pending => ( q(is) => q(ro), lazy_build => 1 );
sub _build__pending {
  my $self=shift;
  return $self->npg_tracking_schema->resultset(q(RunStatusDict))->find({q(description)=>q(run pending)});
}

has sleep_interval => ( q(is) => q(ro), q(isa) => q(Int), default => $DEFAULT_SLEEP );

=head2 loop

Repeat the process step with the intervening sleep interval.

=cut
sub loop {my $self = shift; while(1){ $self->process(); sleep $self->sleep_interval;} return;};

=head2 process

Find all pending MiSeq runs and create a samplesheet for them if one does not already exist.

=cut
sub process {
  my $self = shift;
  my $rt = $self->_pending->run_statuses->search({iscurrent=>1})->related_resultset(q(run));
  my $rs = $rt->search({q(run.id_instrument_format)=>$self->_miseq->id_instrument_format});
  $self->log->debug( $rs->count. q[ ] .($self->_miseq->model). q[ runs marked as ] .($self->_pending->description));
  while(my$r=$rs->next){
    my $id_run = $r->id_run;
    $self->log->debug( join q[,],$id_run,$r->instrument->name);
    my$ss=npg::samplesheet->new(run=>$r);
    my$o=$ss->output;
    my $generate_new = 1;

    if(-e $o) {
      my $other_id_run = _id_run_from_samplesheet($o);
      if ($other_id_run && $other_id_run == $id_run) {
        $self->log->info(qq($o already exists for $id_run));
        $generate_new = 0;
      } else {
        $self->log->info(qq(Will move existing $o));
        _move_samplesheet($o);
      }
    }

    if ($generate_new) {
      try {
        $ss->process;
        $self->log->info(qq($o created for run ).($r->id_run));
      } catch {
        $self->log->error(qq(Trying to create $o for run ).($r->id_run).qq( experienced error: $_));
      }
    }
  }
  return;
}

sub _id_run_from_samplesheet {
  my $file_path = shift;
  my $id_run;
  try {
    my $sh = st::api::lims::samplesheet->new(path => $file_path);
    $sh->data; # force to parse the file
    if ($sh->id_run) {
      $id_run = int $sh->id_run;
    }
  };
  return $id_run;
}

sub _move_samplesheet {
  my $file_path = shift;

  my($filename, $dirname) = fileparse($file_path);
  $dirname =~ s/\/$//smx; #drop last forward slash if any
  my $dirname_dest = $dirname . '_old';
  my $filename_dest = $filename . '_invalid';
  my $moved;
  if (-d $dirname_dest) {
    $moved = move($file_path, catdir($dirname_dest, $filename_dest));
  }
  if (!$moved) {
    move($file_path, catdir($dirname, $filename_dest));
  }
  return;
}

no Moose;
1;
__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item File::Basename

=item File::Copy

=item Moose

=item MooseX::Log::Log4perl

=item Readonly

=item File::Spec::Functions

=item Try::Tiny

=item npg_tracking::Schema

=item npg::samplesheet

=item st::api::lims::samplesheet

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

David K. Jackson E<lt>david.jackson@sanger.ac.ukE<gt>

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2011 GRL, by David K. Jackson 

This file is part of NPG.

NPG is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

=cut

