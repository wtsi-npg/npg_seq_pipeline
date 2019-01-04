package npg_pipeline::validation;

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;
use Carp;
use Try::Tiny;
use Readonly;
use File::Find;
use Perl6::Slurp;

use npg_tracking::glossary::composition;
use npg_pipeline::cache;

extends q{npg_pipeline::base};
with    q{npg_pipeline::validation::common};

our $VERSION = '0';

Readonly::Array  my @NPG_DELETABLE_UNCOND => ('run cancelled', 'data discarded');
Readonly::Array  my @NPG_DELETABLE_STATES => (@NPG_DELETABLE_UNCOND,'qc complete');
Readonly::Scalar my $MIN_KEEP_DAYS        => 14;
Readonly::Scalar my $CRAM_FILE_EXTENSION  => q[cram];
Readonly::Scalar my $BAM_FILE_EXTENSION   => q[bam];
Readonly::Scalar my $DEFAULT_IRODS_ROOT   => q[/seq];
Readonly::Scalar my $STAGING_TAG          => q[staging];

=head1 NAME

npg_pipeline::validation

=head1 SYNOPSIS

=head1 SUBROUTINES/METHODS

=cut

##################################################################
################## Public attributes #############################
###### which will be available as script arguments ###############
########## unless their metaclass is NoGetopt ####################
##################################################################

############## Boolean flags #####################################

=head2 ignore_lims

=cut

has q{ignore_lims} => (
  isa           => q{Bool},
  is            => q{ro},
  documentation =>
  q{Toggles ignoring products list derived from LIMs data, false by default},
);

=head2 ignore_npg_status

=cut

has q{ignore_npg_status} => (
  isa           => q{Bool},
  is            => q{ro},
  documentation =>
  q{Toggles ignoring npg run status, false by default},
);

=head2 ignore_time_limit

=cut

has q{ignore_time_limit} => (
  isa           => q{Bool},
  is            => q{ro},
  documentation =>
  q{Toggles ignoring time limit, false by default},
);

=head2 ignore_autoqc

=cut

has q{ignore_autoqc} => (
  isa           => q{Bool},
  is            => q{ro},
  documentation =>
  q{Toggles ignoring mismatch in number/attribution of autoqc results, false by default},
);

=head2 ignore_irods

=cut

has q{ignore_irods} => (
  isa           => q{Bool},
  is            => q{ro},
  documentation =>
  q{Toggles skipping a check of files in iRODS, false by default},
);

=head2 use_cram

=cut

has q{use_cram} => (
  isa           => q{Bool},
  is            => q{ro},
  default       => 1,
  documentation =>
  q{Toggles between using cram and bam files, true by default},
);

=head2 remove_staging_tag

=cut

has q{remove_staging_tag} => (
  isa           => q{Bool},
  is            => q{ro},
  documentation =>
  q{toggles an option to remive run's staging tag, false by default},
);

############## Other public attributes #####################################

=head2 file_extension

=cut

has q{+file_extension} => ( lazy_build => 1, );
sub _build_file_extension {
  my $self = shift;
  return $self->use_cram ? $CRAM_FILE_EXTENSION : $BAM_FILE_EXTENSION;
}

=head2 min_keep_days

=cut

has q{min_keep_days} => (
  isa           => q{Int},
  is            => q{ro},
  default       => $MIN_KEEP_DAYS,
  documentation => q{Minimum number of days not to keep the run},
);

=head2 skip_autoqc_check

=cut

has q{skip_autoqc_check} => (
  isa           => q{ArrayRef},
  is            => q{ro},
  required      => 0,
  default       => sub {[]},
  documentation =>
  q{A list of autoqc check names to exclude from checking },
);

=head2 lims_driver_type

=cut

has q{lims_driver_type} => (
  isa           => q{Str},
  is            => q{ro},
  default       => 'samplesheet',
  documentation => q{st::api::lims driver type, defaults to samplesheet},
);

=head2 staging_files

=cut

has q{+staging_files}  => (
  required   => 0,
  lazy_build => 1,
  metaclass  => 'NoGetopt',
);
sub _build_staging_files {
  my $self = shift;

  my $files = {'sequence_files' => [], 'composition_files' => []};
  my $ext   = $self->file_extension;
  my $c_ext = $self->composition_file_extension;
  my $wanted = sub {
    if ($File::Find::name =~ /[.]$c_ext\Z/xms) {
      push @{$files->{'composition_files'}}, $File::Find::name;
    } elsif ($File::Find::name =~ /[.]$ext\Z/xms) {
      push @{$files->{'sequence_files'}}, $File::Find::name;
    }
  };
  find($wanted, no_chdir => 1, follow => 1, $self->archive_path);
  if (!@{$files->{'composition_files'}}) {
    $self->logcroak('No composition files found in ' . $self->archive_path);
  }
  if (!@{$files->{'sequence_files'}}) {
    $self->logcroak("No .$ext files found in " . $self->archive_path);
  }

  return $files;
}

############## Public methods ###################################################

=head2 run

=cut

sub run {
  my $self = shift;

  my $deletable = $self->_npg_tracking_deletable('unconditional');
  my $vars_set  = 0;

  if (!$deletable) {
    $vars_set = $self->_vars_from_samplesheet();
  }

  $deletable = $deletable || (
              $self->_npg_tracking_deletable() &&
              $self->_time_limit_deletable()   &&
              $self->_lims_deletable()         &&
              $self->_staging_seq_deletable()  &&
              $self->_irods_seq_deletable()    &&
              $self->_autoqc_deletable()
                             );

  #########
  # unset env variables
  #
  if ($vars_set) {
    for my $var ( npg_pipeline::cache->env_vars() ) {
      ##no critic (RequireLocalizedPunctuationVars)    
      $ENV{$var} = q[];
    }
  }

  if ($deletable && $self->remove_staging_tag) {
    $self->tracking_run->unset_tag($STAGING_TAG);
    $self->warn('Staging tag is removed for run ' . $self->id_run);
  }

  return $deletable;
}

############## Private attributes and methods #########################################

sub _set_vars_from_samplesheet {
  my $self = shift;

  my $vars_set = 0;

  if ($self->lims_driver_type eq 'samplesheet') {
    #########
    # Find the samplesheet and set env vars
    #
    my $cache = npg_pipeline::cache->new(
      set_env_vars       => 1,
      id_run             => $self->id_run,
      cache_location     => $self->analysis_path()
    );
    if ( none { $ENV{$_} } $cache->env_vars() ) {
      $cache->setup();
      for (@{$cache->messages}) { $self->info($_) };
      $vars_set = 1;
    } else {
      $self->info('One of ' . join(q[,], $cache->env_vars()) .
             ' is set, not looking for existing samplesheet');
    }
  }

  return $vars_set;
}

has q{_run_status_obj} => (
  isa           => q{npg_tracking::Schema::Result::RunStatus},
  is            => q{ro},
  lazy_build    => 1,
);
sub _build__run_status_obj {
  my $self = shift;
  return $self->tracking_run->current_run_status;
}

sub _time_limit_deletable {
  my $self = shift;

  if ($self->ignore_time_limit) {
    return 1;
  }

  my $id_run = $self->id_run;
  my $delta_days = DateTime->now()->delta_days($self->_run_status_obj->date())->in_units('days');
  my $deletable = $delta_days >= $self->min_keep_days;
  my $m = qq[time_limit: $id_run last status change was $delta_days days ago. ] .
          $deletable ? q[Deletable] : q[NOT deletable];
  $self->warn($m);

  return $deletable;
}

sub _npg_tracking_deletable {
  my ($self, $unconditional) = @_;

  if ($self->ignore_npg_status) {
    return 1;
  }

  my $crsd = $self->_run_status_obj->run_status_dict->description();
  my $message = sprintf q[npg_tracking: status of run %i is '%s',],
                $self->id_run, $crsd;
  my $deletable;

  if ( $unconditional ) {
    $deletable = ( any { $_ eq $crsd } @NPG_DELETABLE_UNCOND ) &&
                 ( $self->ignore_time_limit || $self->time_limit_deletable() );
    if ($deletable) {
      $self->warn(qq[$message unconditionally deletable]);
    }
    return $deletable;
  }

  $deletable = any { $_ eq $crsd } @NPG_DELETABLE_STATES;
  $message .= ($deletable ? q[] : q[NOT ]) . q[deletable];
  $self->warn($message);

  return $deletable;
}

sub _irods_seq_deletable {
  my $self = shift;

  if ($self->ignore_irods) {
    return 1;
  }

  my $deletable = npg_pipeline::validation::sequence_files
      ->new( logger              => $self->logger,
             collection          => $self->irods_destination_collection,
             file_extension      => $self->file_extension,
             staging_files       => $self->staging_files
           )->archived_for_deletion();

  my $m = q[iRODS: run ] . $self->id_run . $deletable ?
          q[ sequence files archived. Deletable] : q[ NOT deletable];
  $self->warn($m);

  return $deletable;
}

sub _autoqc_deletable {
  my $self = shift;

  if ($self->ignore_autoqc) {
    return 1;
  }

  my $deletable = npg_pipeline::validation::autoqc_files
      ->new( logger         => $self->logger,
             skip_checks    => $self->skip_autoqc_check,
             is_paired_read => $self->is_paired_read ? 1 : 0,
             staging_files  => $self->staging_files )->fully_archived();

  my $m = q[Autoqc: run] . $self->id_run . $deletable ?
          q[ autoqc results fully archived. Deletable] : q[ is NOT deletable];
  $self->warn($m);

  return $deletable;
}

sub _staging_seq_deletable {
  my $self = shift;

  my $checked_ok = 1;
  my $ext   = $self->file_extension;
  my $c_ext = $self->composition_file_extension;

  ##no critic (RegularExpressions::ProhibitUnusedCapture)
  my $re = /(.+[.])$c_ext\Z/smx;
  ##use critic
  foreach my $f (@{$self->staging_files->{'composition_files'}}) {
    my ($root) = $f =~ $re;
    $root or $self->logcroak("Failed to get file name root from $f");
    my $seq_file = $root . $ext;
    if (!exists $self->staging_files->{'sequence_files'}->{$seq_file}) {
      $self->logwarn("File $seq_file does not exists for composition file $f");
      $checked_ok = 0;
    }
  }

  ##no critic (RegularExpressions::ProhibitUnusedCapture)
  $re = /(.+[.])$ext\Z/smx;
  ##use critic
  foreach my $f (@{$self->staging_files->{'sequence_files'}}) {
    my ($root) = $f =~ $re;
    $root or $self->logcroak("Failed to get file name root from $f");
    my $comp_file = $root . $c_ext;
    if (!exists $self->staging_files->{'composition_files'}->{$comp_file}) {
      $self->logwarn("File $comp_file does not exists for seq data file $f");
      $checked_ok = 0;
    }
  }

  return $checked_ok;
}

sub _lims_deletable {
  my $self = shift;

  if ($self->ignore_lims) {
    return 1;
  }

  my @flags = ();
  my $count = 0;

  # For each product, compute composition JSON file and check that it exists
  # in the run folder. Then consider possible subsets.
  foreach my $product (@{$self->products->{'data_products'}}) {

    push @flags, $self->_composition_file_exists($product);
    $count++;

    # File for a phix split should always exist.
    push @flags, $self->_composition_file_exists($product->subset_as_product('phix'));
    $count++;

    my $hsplit = q[];
    if (!$product->lims->gbs_plex_name) {
      if ($product->lims->contains_nonconsented_human) {
        $hsplit = 'human';
      } elsif ($product->lims->contains_nonconsented_xahuman) {
        $hsplit = 'xahuman';
      } elsif ($product->lims->separate_y_chromosome_data) {
        $hsplit = 'yhuman';
      }
    }

    if ($hsplit) {
      push @flags, $self->_composition_file_exists($product->subset_as_product($hsplit));
      $count++;
    }
  }

  my $deletable = none { $_ == 0 } @flags;
  if ($deletable) {
    my $found_count = scalar @{$self->staging_files->{'composition_files'}};
    if ($found_count != $count) {
      $deletable = 0;
      $self->logwarn("Expected $count composition files, found $found_count");
    }
  }

  return $deletable;
}

sub _composition_file_exists {
  my ($self, $product) = @_;

  my $exists = 1;
  my $file_path = $product->file_path(
    $product->path($self->archive_dir), ext => $self->composition_file_extension);
  if (any { $_ eq $file_path } @{$self->staging_files->{'composition_files'}}) {
    my $composition = npg_tracking::glossary::composition->thaw(slurp($file_path));
    if ($composition->digest ne $product->composition->digest) {
      $exists = 0;
      $self->logwarn("Wrong composition in $file_path");
    }
  } else {
    $exists = 0;
    $self->logwarn("File $file_path is missing for entity " . $product->composition->freeze());
  }

  return $exists;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 DESCRIPTION

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item MooseX::StrictConstructor

=item namespace::autoclean

=item Readonly

=item File::Find

=item Perl6::Slurp

=item npg_tracking::glossary::composition

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Steven Leonard
Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2019 Genome Research Ltd

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
