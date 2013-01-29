#########
# Author:        David K. Jackson
# Created:       2011-11-29
#

#package npg::samplesheet::auto; use Moose; use Try::Tiny; with q(MooseX::Log::Log4perl); use npg::samplesheet; use npg_tracking::Schema; has npg_tracking_schema => (isa => q(npg_tracking::Schema), is => q(ro), lazy_build => 1); has sleep_interval => ( q(is) => q(ro), q(isa) => q(Int), default => 90); sub _build_npg_tracking_schema {return npg_tracking::Schema->connect();}  has _miseq => ( q(is) => q(ro), lazy_build => 1 );  sub _build__miseq { my $self=shift; $self->npg_tracking_schema->resultset(q(InstrumentFormat))->find({model=>q(MiSeq)});}  has _pending => ( q(is) => q(ro), lazy_build => 1 );  sub _build__pending { my $self=shift; $self->npg_tracking_schema->resultset(q(RunStatusDict))->find({description=>q(run pending)});}  sub loop {my $self = shift; while(1){ $self->main; sleep $self->sleep_interval} }; sub main { my $self = shift; my $rs = $self->_pending->run_statuses->search({iscurrent=>1})->related_resultset(q(run))->search({q(run.id_instrument_format)=>$self->_miseq->id_instrument_format}); $self->log->debug( $rs->count." ".($self->_miseq->model)." runs marked as ".($self->_pending->description)); while(my$r=$rs->next){$self->log->debug( join",",$r->id_run,$r->instrument->name); my$ss=npg::samplesheet->new(npg_tracking_schema=>$self->npg_tracking_schema, run=>$r, id_run=>$r->id_run); my$o=$ss->output; if(-e $o){ $self->log->debug(qq($o already exists))}else{ try { $ss->process; $self->log->info(qq($o created for run ).($r->id_run)); } catch {  $self->log->error(qq(Trying to create $o for run ).($r->id_run).qq( experienced error: $_)); } } }  } no Moose; 1; use Log::Log4perl qw(:easy); BEGIN{ Log::Log4perl->easy_init({level=>$INFO,}); }

package npg::samplesheet::auto;
use Moose;
use Try::Tiny;
use npg::samplesheet;
use npg_tracking::Schema;
use Readonly;

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
    $self->log->debug( join q[,],$r->id_run,$r->instrument->name);
    my$ss=npg::samplesheet->new(run=>$r);
    my$o=$ss->output;
    if(-e $o){ $self->log->debug(qq($o already exists))}else{
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


no Moose;
1;
__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item strict

=item warnings

=item Moose

=item MooseX::Log::Log4perl

=item Readonly

=item Carp

=item Try::Tiny

=item npg_tracking::Schema

=item npg::samplesheet

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

