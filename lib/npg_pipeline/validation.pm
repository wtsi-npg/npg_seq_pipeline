package npg_pipeline::validation;

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;
use Try::Tiny;
use Readonly;
use File::Find;
use List::MoreUtils qw/any none/;

use npg_tracking::glossary::composition;
use npg_pipeline::cache;
use npg_pipeline::validation::entity;
use npg_pipeline::validation::irods;
use npg_pipeline::validation::autoqc;
use WTSI::NPG::iRODS;
use npg_qc::Schema;

extends  q{npg_pipeline::base};
with    qw{npg_pipeline::validation::common
           npg_pipeline::product::release::irods};

our $VERSION = '0';

Readonly::Array  my @NPG_DELETABLE_UNCOND => ('run cancelled', 'data discarded');
Readonly::Array  my @NPG_DELETABLE_STATES => (@NPG_DELETABLE_UNCOND,'qc complete');
Readonly::Scalar my $MIN_KEEP_DAYS        => 14;
Readonly::Scalar my $CRAM_FILE_EXTENSION  => q[cram];
Readonly::Scalar my $BAM_FILE_EXTENSION   => q[bam];
Readonly::Scalar my $DEFAULT_IRODS_ROOT   => q[/seq];
Readonly::Scalar my $STAGING_TAG          => q[staging];

Readonly::Array  my @NO_SCRIPT_ARG_ATTRS  => qw/
                                                p4s1_phix_alignment_method
                                                p4s2_aligner_intfile 
                                                adapterfind
                                                s1_s2_intfile_format
                                                intensity_path
                                                no_summary_link
                                                no_warehouse_update
                                                no_irods_archival 
                                                recalibrated_path
                                                basecall_path
                                                align_tag0
                                                local
                                                qc_run
                                                repository
                                                index_length
                                                index_file_extension
                                                lanes
                                                id_flowcell_lims
                                                conf_path
                                               /;

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

Boolean attribute, toggles ignoring products list derived from
LIMs data, false by default.

=cut

has q{ignore_lims} => (
  isa           => q{Bool},
  is            => q{ro},
  documentation =>
  q{Toggles ignoring products list derived from LIMs data, false by default},
);

=head2 ignore_npg_status

Boolean attribute, toggles ignoring npg run status, false by default.

=cut

has q{ignore_npg_status} => (
  isa           => q{Bool},
  is            => q{ro},
  documentation =>
  q{Toggles ignoring npg run status, false by default},
);

=head2 ignore_time_limit

Boolean attribute, toggles ignoring time limit, false by default,

=cut

has q{ignore_time_limit} => (
  isa           => q{Bool},
  is            => q{ro},
  documentation =>
  q{Toggles ignoring time limit, false by default},
);

=head2 ignore_autoqc

Boolean attribute, toggles ignoring mismatch in number/attribution
of autoqc results, false by default.

=cut

has q{ignore_autoqc} => (
  isa           => q{Bool},
  is            => q{ro},
  documentation =>
  q{Toggles ignoring mismatch in number/attribution of autoqc results, false by default},
);

=head2 ignore_irods

Boolean attribute, toggles skipping a check of files in iRODS,
false by default.

=cut

has q{ignore_irods} => (
  isa           => q{Bool},
  is            => q{ro},
  documentation =>
  q{Toggles skipping a check of files in iRODS, false by default},
);

=head2 use_cram

Boolean attribute, toggles between using cram and bam files,
true by default,

=cut

has q{use_cram} => (
  isa           => q{Bool},
  is            => q{ro},
  default       => 1,
  documentation =>
  q{Toggles between using cram and bam files, true by default},
);

=head2 per_product_archive

A boolean attribute indicating whether a per-product staging archive
is being used. True if a qc directory is not present in the archive
directory for the analysis.

=cut

has 'per_product_archive' => (
  isa        => 'Bool',
  is         => 'ro',
  required   => 0,
  lazy_build => 1,
  documentation =>
  q{Toggles between per-product and flat staging archive},
);
sub _build_per_product_archive {
  my $self = shift;
  return not -e $self->qc_path;
}

=head2 remove_staging_tag

Boolean attribute, ttoggles an option to remove run's staging tag,
false by default.

=cut

has q{remove_staging_tag} => (
  isa           => q{Bool},
  is            => q{ro},
  documentation =>
  q{Toggles an option to remove run's staging tag, false by default},
);

############## Other public attributes #####################################


#####
# Amend inherited attributes which we do not want to show up as scripts' arguments.
# This is in addition to what is done in the parent class.
#
has [map {q[+] . $_ }  @NO_SCRIPT_ARG_ATTRS] => (metaclass => 'NoGetopt',);


=head2 file_extension

String attribute.
Value set to 'cram if use_cram flag is true.
Example: 'cram'. 

=cut

has q{+file_extension} => ( lazy_build => 1, );
sub _build_file_extension {
  my $self = shift;
  return $self->use_cram ? $CRAM_FILE_EXTENSION : $BAM_FILE_EXTENSION;
}

=head2 min_keep_days

Integer attribute, minimum number of days not to keep the run.

=cut

has q{min_keep_days} => (
  isa           => q{Int},
  is            => q{ro},
  default       => $MIN_KEEP_DAYS,
  documentation => q{Minimum number of days not to keep the run},
);

=head2 skip_autoqc_check

A list of autoqc check names to exclude from checking.

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

st::api::lims driver type, defaults to samplesheet.

=cut

has q{lims_driver_type} => (
  isa           => q{Str},
  is            => q{ro},
  default       => 'samplesheet',
  documentation => q{st::api::lims driver type, defaults to samplesheet},
);

=head2 product_entities

An array of npg_pipeline::validation::entity objects.

=cut

has q{+product_entities}  => (
  required   => 0,
  lazy_build => 1,
  metaclass  => 'NoGetopt',
);
sub _build_product_entities {
  my $self = shift;

  my @e = ();
  my $per_product_archive = $self->per_product_archive ? 1 : 0;

  foreach my $product (@{$self->products->{'data_products'}}) {

    # File for a phix split should always exist, unless
    # the tag is for spiked PhiX.
    my @subsets = ();
    if (!$product->lims->is_control) {
      push @subsets, 'phix';
    }
    if (!$product->lims->gbs_plex_name) {
      if ($product->lims->contains_nonconsented_human) {
        push @subsets, 'human';
      } elsif ($product->lims->contains_nonconsented_xahuman) {
        push @subsets, 'xahuman';
      } elsif ($product->lims->separate_y_chromosome_data) {
        push @subsets, 'yhuman';
      }
    }

    push @e, npg_pipeline::validation::entity->new(
                   target_product       => $product,
                   subsets              => \@subsets,
                   per_product_archive  => $per_product_archive,
                   staging_archive_root => $self->archive_path);
  }

  @e or $self->logcroak('No data products found');

  return \@e;
}

=head2 irods

Instance of WTSI::NPG::iRODS class.

=cut

has 'irods' => (
  isa        => 'WTSI::NPG::iRODS',
  is         => 'ro',
  required   => 0,
  lazy_build => 1,
  metaclass  => 'NoGetopt',
);
sub _build_irods {
  return WTSI::NPG::iRODS->new();
}

=head2 qc_schema

npg_qc::Schema database connection.

=cut

has 'qc_schema' => (
  isa        => 'npg_qc::Schema',
  is         => 'ro',
  required   => 0,
  lazy_build => 1,
  metaclass  => 'NoGetopt',
);
sub _build_qc_schema {
  return npg_qc::Schema->connect();
}

############## Public methods ###################################################

=head2 run

=cut

sub run {
  my $self = shift;

  my $deletable = $self->_npg_tracking_deletable('unconditional');
  my $vars_set  = 0;

  if (!$deletable) {
    $vars_set = $self->_set_vars_from_samplesheet();
  }

  try {
    $deletable = $deletable || (
              $self->_npg_tracking_deletable() &&
              $self->_time_limit_deletable()   &&
              $self->_lims_deletable()         &&
              $self->_staging_deletable()      &&
              $self->_irods_seq_deletable()    &&
              $self->_autoqc_deletable()
                               );
  } catch {
    my $e = $_;
    $self->error(sprintf 'Error assessing run %i: %s', $self->id_run, $e);
  };

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
    $self->info('Staging tag is removed for run ' . $self->id_run);
  }

  return $deletable;
}

############## Private attributes and methods #########################################

has q{_run_status_obj} => (
  isa           => q{npg_tracking::Schema::Result::RunStatus},
  is            => q{ro},
  lazy_build    => 1,
);
sub _build__run_status_obj {
  my $self = shift;
  return $self->tracking_run->current_run_status;
}

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

sub _time_limit_deletable {
  my $self = shift;
  $self->debug('Assessing time limit...');

  if ($self->ignore_time_limit) {
    $self->info('Time limit ignored.');
    return 1;
  }

  my $delta_days = DateTime->now()->delta_days(
                   $self->_run_status_obj->date())->in_units('days');
  my $deletable = $delta_days >= $self->min_keep_days;
  my $m = sprintf 'Time limit: %i last status change was %i days ago, %sdeletable.',
          $self->id_run, $delta_days, $deletable ? q[] : q[NOT ];
  $self->info($m);

  return $deletable;
}

sub _npg_tracking_deletable {
  my ($self, $unconditional) = @_;
  $self->debug('Assessing run status...');

  if (!$unconditional && $self->ignore_npg_status) {
    $self->info('NPG tracking run status ignored.');
    return 1;
  }

  my $crsd = $self->_run_status_obj->run_status_dict->description();
  my $message = sprintf q[NPG tracking: status of run %i is '%s', ],
                $self->id_run, $crsd;
  my $deletable;

  if ( $unconditional ) {
    $deletable = ( any { $_ eq $crsd } @NPG_DELETABLE_UNCOND ) &&
                 ( $self->ignore_time_limit || $self->time_limit_deletable() );
    if ($deletable) {
      $self->info(qq[$message unconditionally deletable.]);
    }
    return $deletable;
  }

  $deletable = any { $_ eq $crsd } @NPG_DELETABLE_STATES;
  $message .= ($deletable ? q[] : q[NOT ]) . q[deletable.];
  $self->info($message);

  return $deletable;
}

sub _irods_seq_deletable {
  my $self = shift;
  $self->debug('Assessing sequencing data files in iRODS...');

  if ($self->ignore_irods) {
    $self->info('iRODS check ignored');
    return 1;
  }

  my $deletable = npg_pipeline::validation::irods
      ->new( collection       => $self->irods_destination_collection,
             file_extension   => $self->file_extension,
             product_entities => $self->product_entities,
             irods            => $self->irods,
           )->archived_for_deletion();

  my $m = sprintf 'Presence of seq. files in iRODS: run %i %sdeletable',
          $self->id_run , $deletable ? q[] : q[NOT ];
  $self->info($m);

  return $deletable;
}

sub _autoqc_deletable {
  my $self = shift;
  $self->debug('Assessing autoqc results in the database...');

  if ($self->ignore_autoqc) {
    $self->info('Autoqc results check ignored');
    return 1;
  }

  my $deletable = npg_pipeline::validation::autoqc
      ->new( qc_schema        => $self->qc_schema,
             skip_checks      => $self->skip_autoqc_check,
             is_paired_read   => $self->is_paired_read ? 1 : 0,
             product_entities => $self->product_entities )->fully_archived();

  my $m = sprintf 'Presence of autoqc results in the database: run %i %sdeletable',
          $self->id_run , $deletable ? q[] : q[NOT ];
  $self->info($m);

  return $deletable;
}

sub _lims_deletable {
  my $self = shift;
  $self->debug('Assessing LIMs data and products...');

  if ($self->ignore_lims) {
    $self->info('LIMs data ignored');
    return 1;
  }

  my $deletable = 1;
  foreach my $entity (@{$self->product_entities}) {
    foreach my $file ($entity->staging_files($self->file_extension)) {
      if (!-e $file) {
        $self->logwarn("File $file is missing for entity " . $entity->description());
        $deletable = 0;
      }
    }
  }

   my $m = sprintf 'Consistency of staging sequence files listing with LIMs: run %i %sdeletable',
          $self->id_run , $deletable ? q[] : q[NOT ];
  $self->info($m);

  return $deletable;
}

sub _staging_deletable {
  my $self = shift;

  my @files_found = ();
  my $ext   = $self->file_extension;
  my $wanted = sub {
    if ($File::Find::name =~ /[.]$ext\Z/xms) {
      push @files_found, $File::Find::name;
    }
  };
  find($wanted, $self->archive_path);

  my %files_expected =
    map { $_ => 1 }
    map { $_->staging_files($self->file_extension) }
    @{$self->product_entities};

  $self->debug(join qq{\n}, q{Expected staging files list}, (sort keys %files_expected));

  my $deletable = 1;
  foreach my $file (@files_found) {
    if (!$files_expected{$file}) {
      $self->logwarn("Staging file $file is not expected");
      $deletable = 0;
    }
  }

  my $m = sprintf 'Check for unexpected files on staging: run %i %sdeletable',
          $self->id_run , $deletable ? q[] : q[NOT ];
  $self->info($m);

  return $deletable;
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

=item List::MoreUtils

=item Try::Tiny

=item WTSI::NPG::iRODS

=item npg_qc::Schema

=item npg_tracking::glossary::composition

=item npg_pipeline::base

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
