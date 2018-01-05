package npg_pipeline::validation::autoqc_files;

#########
# Copied from 
# svn+ssh://svn.internal.sanger.ac.uk/repos/svn/new-pipeline-dev/data_handling/trunk/lib/npg_validation/runfolder/deletable/autoqc.pm
# on the 5th of January 2018
#

use Moose;
use Readonly;
use Carp;
use Try::Tiny;
use List::MoreUtils qw/none/;

use npg_qc::Schema;
use npg_qc::autoqc::role::result;
use npg_common::irods::Loader;

with qw{npg_common::irods::iRODSCapable
        npg_common::irods::Repository
        npg_tracking::glossary::run};

our $VERSION = '0';

Readonly::Scalar my $NO_TAG         => -1;
Readonly::Scalar my $DEFAULT_VALUE  => 'default_value';

Readonly::Array  my @COMMON_CHECKS        => qw/ qX_yield
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

Readonly::Array  my @WITH_SUBSET_CHECKS =>    qw/ bam_flagstats
                                                  samtools_stats
                                                  sequence_summary
                                                /;

has 'exclude_bam'   => (isa           => q{Bool},
                        is            => q{rw},
                        documentation => q{flag to exclude bam file loading},
                       );

has 'skip_checks'   => ( isa           => 'ArrayRef',
                         is            => 'ro',
                         required      => 0,
                         default       => sub { [] },
                       );

has 'irods_files'   => ( isa           => 'ArrayRef',
                         is            => 'ro',
                         required      => 0,
                         lazy_build    => 1,
                       );
sub _build_irods_files {
  my $self = shift;
  my $bam_re;
  if ($self->exclude_bam) {
    $bam_re = q(cram$);
  } else {
    $bam_re = q(bam$);
  }
  my @files = ();
  try {
    @files = grep { /$bam_re/msx }
      keys %{npg_common::irods::Loader->new
        (irods => $self->irods)->get_collection_file_list
          ($self->run_collection)};
  } catch {
    carp qq[Error getting a list of bam files form IRODs: $_];
  };
  return \@files;
}

has 'is_paired_read' => ( isa           => 'Bool',
                          is            => 'ro',
                          required      => 0,
                          lazy_build    => 1,
                        );

sub _build_is_paired_read {
  my $self = shift;
  if( !@{$self->irods_files} ){
    croak qq[No irods file list for run $self->id_run];
  }
  my $paired = 0;
  foreach my $file (@{$self->irods_files}) {
    my $irods_file = File::Spec->catfile($self->run_collection, $file);
    my $remote_meta_data = npg_common::irods::Loader->new
      (irods => $self->irods)->_check_meta_data($irods_file);
    if (!exists($remote_meta_data->{is_paired_read})) {
      croak qq[No is_paired_read meta data for irods file $irods_file];
    }
    my $values_in_irods = $remote_meta_data->{is_paired_read};
    my @current_meta_values = keys %{$values_in_irods};
    if (!scalar @current_meta_values) {
      croak qq[No is_paired_read values for irods file $irods_file];
    } elsif (scalar @current_meta_values > 1) {
      croak qq[Multiple is_paired_read values for irods file $irods_file];
    }
    $paired = $current_meta_values[0];
    last;
  }
  return $paired;
}

sub fully_archived {
  my $self = shift;

  try {
    $self->_qc_schema;
  } catch {
    carp qq[Cannot connect to qc database: $_];
    return 0;
  };

  my $count = scalar @{$self->_queries};
  if ($count == 0) {
    carp 'No queries to run for autoqc';
    return 0;
  }

  my $skip_checks = $self->_parse_excluded_checks();

  foreach my $query (@{$self->_queries}) {
    my $skip = $self->_query_to_be_skipped($query, $skip_checks);
    if ($self->verbose) {
      warn sprintf '%s "%s"%s',
        $skip ? 'Skipping' : 'Executing query for ',
        $self->_query2string($query),
        qq[\n];
    }
    $count = $count - ( $skip || $self->_result_exists($query) );
  }
  return !$count; #if all results exist, $count should be zero at the end
}

sub _parse_excluded_checks {
  my $self = shift;

  my $skip_checks = {};

  foreach my $check ( @{$self->skip_checks} ) {
    my @parsed = split /\+/smx, $check;
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

has '_qc_schema'   =>  ( isa           => 'npg_qc::Schema',
                         is            => 'ro',
                         required      => 0,
                         lazy_build    => 1,
                       );
sub _build__qc_schema {
  my $self = shift;
  return npg_qc::Schema->connect();
}

has '_catalogue'          => ( isa           => 'HashRef',
                               is            => 'ro',
                               required      => 0,
                               lazy_build     => 1,
                             );
sub _build__catalogue {
  my $self = shift;
  return $self->file_catalogue($self->irods_files, $DEFAULT_VALUE, $DEFAULT_VALUE);
}

has '_queries'            => ( isa           => 'ArrayRef',
                               is            => 'ro',
                               required      => 0,
                               lazy_build     => 1,
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
      carp qq[Expected $expected results got $count for "$desc"];
      return 0;
    }
    return 1;
  }
  my $count = $self->_qc_schema->resultset($class_name)->search_autoqc($query, 1)->count;
  if ($check_name eq 'insert_size') {
    my $expected = $self->is_paired_read ? 1 : 0;
    if ($count != $expected) {
      carp qq[Expected $expected results got $count for "$desc"];
      return 0;
    }
    return 1;
  }

  if ($count == 0) {
    carp qq[Result not found for "$desc"\n];
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

  my $rf = npg_pipeline::validation::autoqc_files->new(id_run => 1234, verbose => 1);
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

=head2 irods_files

  A reference to an array of *.bam/*.cram files for a run found in IRODs repository

=head2 is_paired_read

  A flag defining whether there are reverse reads. Currently defaults to true and is not used.

=head2 verbose
 
  A boolean flag switching on and off verbosity.

=head2 exclude_bam

  A boolean option excluding iRODS bam files from consideration.

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

=item Readonly

=item Try::Tiny

=item List::MoreUtils

=item npg_qc::Schema

=item npg_qc::autoqc::role::result

=item npg_common::irods::Loader

=item npg_common::irods::iRODSCapable

=item npg_common::irods::Repository

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
