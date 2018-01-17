package npg_pipeline::validation::autoqc_files;

#########
# Copied from 
# svn+ssh://svn.internal.sanger.ac.uk/repos/svn/new-pipeline-dev/data_handling/trunk/lib/npg_validation/runfolder/deletable/autoqc.pm
# on the 5th of January 2018
#

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;
use Readonly;
use Try::Tiny;
use List::MoreUtils qw/none/;
use File::Basename;

use npg_qc::Schema;
use npg_qc::autoqc::role::result;

with qw / npg_tracking::glossary::run
          npg_pipeline::validation::common /;

our $VERSION = '0';

Readonly::Scalar my $NO_TAG         => -1;
Readonly::Scalar my $DEFAULT_VALUE  => 'default_value';

Readonly::Array  my @COMMON_CHECKS         => qw/ qX_yield
                                                  adapter
                                                  gc_fraction
                                                  insert_size
                                                  ref_match
                                                  sequence_error
                                                  fastqcheck
                                                /;

Readonly::Array  my @LANE_LEVELCHECKS4POOL => qw/ tag_metrics
                                                  upstream_tags
                                                /;

Readonly::Array  my @WITH_SUBSET_CHECKS    => qw/ bam_flagstats
                                                  samtools_stats
                                                  sequence_summary
                                                /;

has 'skip_checks'    => ( isa           => 'ArrayRef',
                          is            => 'ro',
                          required      => 0,
                          default       => sub { [] },
                        );

has 'is_paired_read' => ( isa           => 'Bool',
                          is            => 'ro',
                          required      => 0,
                          lazy_build    => 1,
                        );
sub _build_is_paired_read {
  my $self = shift;
  my $attr_name = 'is_paired_read';
  my $meta = $self->get_metadata(
             $self->collection_files->{($self->irods_files())[0]}, ($attr_name));
  return $meta->{$attr_name};
}

sub fully_archived {
  my $self = shift;

  try {
    $self->_qc_schema;
  } catch {
    $self->logger->warn(qq[Cannot connect to qc database: $_]);
    return 0;
  };

  my $count = scalar @{$self->_queries};
  if ($count == 0) {
    $self->logger->warn('No queries to run for autoqc');
    return 0;
  }

  my $skip_checks = $self->_parse_excluded_checks();

  foreach my $query (@{$self->_queries}) {
    my $skip = $self->_query_to_be_skipped($query, $skip_checks);
    $self->logger->info(sprintf '%s "%s"',
                        $skip ? 'Skipping' : 'Executing query for ',
                        $self->_query2string($query));
    $count = $count - ( $skip || $self->_result_exists($query) );
  }
  return !$count; #if all results exist, $count should be zero at the end
}

has '_qc_schema' => ( isa        => 'npg_qc::Schema',
                      is         => 'ro',
                      required   => 0,
                      lazy_build => 1,
                    );
sub _build__qc_schema {
  return npg_qc::Schema->connect();
}

has '_catalogue' => ( isa        => 'HashRef',
                      is         => 'ro',
                      required   => 0,
                      lazy_build => 1,
                    );
sub _build__catalogue {
  my $self = shift;

  my $c = {};
  foreach my $file ( $self->irods_files ) {
    my $ids = $self->parse_file_name($file);
    my $lane = $ids->{'position'};
    my $tag_index = defined $ids->{'tag_index'} ? $ids->{'tag_index'} : $DEFAULT_VALUE;
    my $split = $ids->{'split'} || $DEFAULT_VALUE;
    $c->{$lane}->{$tag_index}->{$split} = 1;
  }
  return $c;
}

has '_queries'  => ( isa        => 'ArrayRef',
                     is         => 'ro',
                     required   => 0,
                     lazy_build => 1,
                   );
sub _build__queries {
  my $self = shift;

  my @queries = ();

  foreach my $position ( keys %{$self->_catalogue} ) {

    my $lane_is_plexed = !exists $self->_catalogue->{$position}->{$DEFAULT_VALUE};
    my $query = {'position' => $position};
    $query->{'tag_index'} =  _value4query($DEFAULT_VALUE);

    if ( $lane_is_plexed ) {
      ## no critic (BuiltinFunctions::ProhibitComplexMappings)
      ## no critic (ValuesAndExpressions::ProhibitCommaSeparatedStatements)
      push @queries,
        map { my %q; %q = %{$query}, $q{'check'} = $_; \%q; }
        (@LANE_LEVELCHECKS4POOL, @COMMON_CHECKS);
    }

    $query->{'subset'}    =  _value4query($DEFAULT_VALUE);
    foreach my $tag_index (keys %{$self->_catalogue->{$position}}) {
      foreach my $split ( keys %{$self->_catalogue->{$position}->{$tag_index}} ) {
        my @checks = @WITH_SUBSET_CHECKS;
        if ($split eq $DEFAULT_VALUE) {
          push @checks, @COMMON_CHECKS;
	} elsif ($split eq 'phix' ||
            ($split =~ /human/smx && !$self->_catalogue->{$position}->{$tag_index}->{'phix'})) {
          push @checks, 'alignment_filter_metrics';
        }
        ## no critic (BuiltinFunctions::ProhibitComplexMappings)
        ## no critic (ValuesAndExpressions::ProhibitCommaSeparatedStatements)
        push @queries,
          map { my %q; %q = %{$query},
                $q{'check'}     = $_;
                _values2query(\%q, $tag_index, $split);
                \%q;
              }
          @checks;
      }
    }
  }
  return \@queries;
}

sub _parse_excluded_checks {
  my $self = shift;

  my $skip_checks = {};

  foreach my $check ( @{$self->skip_checks} ) {
    my @parsed = split /[+]/smx, $check;
    my $name = shift @parsed;
    $skip_checks->{$name} = \@parsed;
  }

  return $skip_checks;
}

sub _query_to_be_skipped {
  my ($self, $query, $skip_checks) = @_;

  my $check_name = $query->{'check'};
  my $skip = exists $skip_checks->{$check_name} ? 1 : 0;
  my $skip_subset = $skip_checks->{$check_name};
  if ( $skip_subset && @{$skip_subset} &&
    ( !$query->{'subset'} || none { $query->{'subset'} eq $_ } @{$skip_subset}) ) {
    $skip = 0;
  }

  return $skip;
}

sub _value4query {
  my $value = shift;
  return $value eq $DEFAULT_VALUE ? undef : $value;
}

sub _values2query {
  my ($q, $tag_index, $subset) = @_;

  my $check_name = $q->{'check'};
  $q->{'tag_index'} = _value4query($tag_index);

  if ( none { $_ eq $check_name } @WITH_SUBSET_CHECKS ) {
    delete $q->{'subset'};
  } else {
    $q->{'subset'} = _value4query($subset);
  }

  return;
}

sub _result_exists {
  my ($self, $query) = @_;

  my $desc = $self->_query2string($query);
  my $check_name = delete $query->{'check'};
  my ($name, $class_name) = npg_qc::autoqc::role::result->class_names($check_name);

  $query->{'id_run'} = $self->id_run;

  if ($check_name eq 'fastqcheck') {
    $query->{'tag_index'} = $query->{'tag_index'} // $NO_TAG;
    my $count = $self->_qc_schema->resultset($class_name)->search($query)->count;
    my $pool = !exists $self->_catalogue->{$query->{'position'}}->{$DEFAULT_VALUE};
    my $expected = 1;
    ## no critic (ControlStructures::ProhibitPostfixControls)
    $expected++ if ($pool && ($query->{'tag_index'} == $NO_TAG));
    $expected++ if $self->is_paired_read;
    ## use critic
    if ($count != $expected) {
      $self->logger->warn(qq[Expected $expected results got $count for "$desc"]);
      return 0;
    }
    return 1;
  }
  my $count = $self->_qc_schema->resultset($class_name)->search_autoqc($query, 1)->count;
  if ($check_name eq 'insert_size') {
    my $expected = $self->is_paired_read ? 1 : 0;
    if ($count != $expected) {
      $self->logger->warn(qq[Expected $expected results got $count for "$desc"]);
      return 0;
    }
    return 1;
  }

  if ($count == 0) {
    $self->logger->warn(qq[Result not found for "$desc"\n]);
    return 0;
  }

  return 1;
}

sub _query2string {
  my ($self, $query) = @_;
  return sprintf 'check %s, id_run=%i and position=%i and tag index=%s, split %s',
    $query->{'check'},
    $self->id_run,
    $query->{'position'},
    $query->{'tag_index'} // 'undef',
    $query->{'subset'} || $query->{'human_split'} || q[none];
}

__PACKAGE__->meta->make_immutable;
no Moose;

1;
__END__

=head1 NAME

npg_pipeline::validation::autoqc_files

=head1 SYNOPSIS

  my $rf = npg_pipeline::validation::autoqc_files
           ->new(irods          => $irods,
                 logger         => $logger,
                 id_run         => 1234,
                 collection     => '/irods/1234',
                 file_extension => 'cram');
  my $is_archived = $rf->fully_archived;

=head1 DESCRIPTION

  Compares a set of archived bam files againsts a set of autoqc results for
  a run and decides whether all relevant autoqc results have been archived.
  Autoqc results that can easily be produced again from bam files are omitted.
  Presence of fastqcheck files in the archive is checked.
  
  A full comparison is performed. If at least one autoqc result is missing,
  the outcome is false, otherwise true is returned. If the verbose attribute
  is set, a path to each considered bam file is printed to STDERR and a
  representation of each query to find the autoqc result is printed to STDERR.
  In non-verbose mode (default) only the queries for missing results are printed.

=head1 SUBROUTINES/METHODS

=head2 is_paired_read

  A flag defining whether there are reverse reads.

=head2 skip_checks

  An optional array of autoqc check names to disregard. If a subset is concatenated
  (use -) with the check name, only this subset will be disregarded for this check.

  Setting this array to [qw/adaptor samtools_stats-phix/] ensure that absence of
  all adaptor results and absence of samtools_stats results for phix subsets will be
  disregarded.

=head2 fully_archived

  Returns true if all expected autoqc data are found, otherwise returns false.

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item MooseX::StrictConstructor

=item namespace::autoclean

=item Readonly

=item Try::Tiny

=item List::MoreUtils

=item File::Basename

=item npg_qc::Schema

=item npg_qc::autoqc::role::result

=item npg_tracking::glossary::run

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia E<lt>mg8@sanger.ac.ukE<gt>

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2018 GRL

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
