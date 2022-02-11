package npg_pipeline::validation;

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;
use Try::Tiny;
use Readonly;
use File::Find;
use List::MoreUtils qw/any none/;
use List::Util qw/max/;
use File::Basename;
use Carp;

use npg_tracking::util::abs_path qw/abs_path/;
use npg_tracking::util::types;
use npg_tracking::glossary::composition;
use npg_pipeline::cache;
use npg_pipeline::product::release;
use npg_pipeline::validation::entity;
use npg_pipeline::validation::irods;
use npg_pipeline::validation::s3;
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
Readonly::Scalar my $DEFAULT_IRODS_ROOT   => q[/seq];
Readonly::Scalar my $STAGING_TAG          => q[staging];
Readonly::Scalar my $DO_NOT_DELETE_NAME   => q[npg_do_not_delete];

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
                                                qc_path
                                                align_tag0
                                                local
                                                repository
                                                index_length
                                                index_file_extension
                                                file_extension
                                                lanes
                                                id_flowcell_lims
                                                conf_path
                                                logger
                                                workflow_type
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
  q{Toggles ignoring mismatch in number/attribution of autoqc results, } .
  q{false by default},
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
is being used. Defaults to true.

=cut

has 'per_product_archive' => (
  isa        => 'Bool',
  is         => 'ro',
  required   => 0,
  default    => 1,
  documentation =>
  q{Toggles between per-product and flat staging archive},
);

=head2 remove_staging_tag

Boolean attribute, toggles an option to remove run's staging tag,
false by default.

=cut

has q{remove_staging_tag} => (
  isa           => q{Bool},
  is            => q{ro},
  documentation =>
  q{Toggles an option to remove run's staging tag, false by default},
);

=head2 no_s3_archival

Boolean attribute, toggles s3 check, false by default.

=cut

has q{+no_s3_archival} => (
  documentation =>
  q{Toggles an option to check for data in s3, false by default},
);

############## Other public attributes ###########################

#####
# Amend inherited attributes which we do not want to show up as
# scripts' arguments. This is in addition to what is done in the parent class.
has [map {q[+] . $_ }  @NO_SCRIPT_ARG_ATTRS] => (metaclass => 'NoGetopt',);

=head2 archive_path

Attribute inherited from npg_pipeline::base, changed here to return an absolute
path to the archive directory so that paths derived from the archive directory
in different parts of this utility are consistent.

=cut

around 'archive_path' => sub {
  my $orig = shift;
  my $self = shift;
  return abs_path($self->$orig);
};

=head2 irods_destination_collection

Inherited from npg_pipeline::product::release::irods

=cut

has '+irods_destination_collection' => (
  documentation =>
  q{iRODS destination collection, including run identifier},
);

=head2 file_extension

String attribute.
Value set to 'cram if use_cram flag is true.
Inherited from npg_pipeline::validation::common
Example: 'cram'. 

=cut

has q{+file_extension} => ( lazy_build => 1, );
sub _build_file_extension {
  my $self = shift;
  return $self->get_file_extension($self->use_cram);
}

=head2 min_keep_days

Integer attribute, minimum number of days to keep the run folder.

If not set by the caller, the attribute will be computed.
If the run has status 'run cancelled' or 'data discarded', the attribute is
set to 14 days, examination of products is not performed to avoid accessing
non-local sources of LIMS data. For all other run statuses the attribute is
set to the longest deletion time among all applicable products.

=cut

has q{min_keep_days} => (
  isa           => q{NpgTrackingPositiveInt},
  is            => q{ro},
  lazy_build    => 1,
  documentation => q{Minimum number of days to keep the run folder},
);
sub _build_min_keep_days {
  my $self = shift;
  my @delays;
  if (not $self->_is_unconditionally_deletable()) {
    @delays = map  { $self->staging_deletion_delay($_) || $MIN_KEEP_DAYS }
              grep { $self->is_release_data($_) }
              @{$self->products->{'data_products'}};
  }
  return @delays ? max @delays : $MIN_KEEP_DAYS;
}

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
Inherited from npg_pipeline::validation::common

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

=head2 eligible_product_entities

An array of npg_pipeline::validation::entity objects which
were considered eligible for archival by all of product file
archival methods. Inherited from npg_pipeline::validation::common

=cut

has q{+eligible_product_entities}  => (
  metaclass => 'NoGetopt',
);

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
Inherited from npg_pipeline::base, changed here to lazy-build
a database connection.

=cut

has '+qc_schema' => (
  lazy       => 1,
  builder    => '_build_qc_schema',
);
sub _build_qc_schema {
  return npg_qc::Schema->connect();
}

############## Public methods ####################################

=head2 build_eligible_product_entities

Builder method for the eligible_product_entities attribute.
returns an empty array.

=cut

sub build_eligible_product_entities {
  return [];
}

=head2 run

Evaluates whether the run is deletable, returns true if it is and
false if it is not.

If a file or directory named "npg_do_not_delete" is present in the run
folder, false value is returned. No further checks are performed.

Needs access to LIMs data, tries to locate a samplesheet in the current
analysis directory for the run and use it as a source of LIMS data.

If the run folder is deletable and remove_staging_tag flag is set to
true (false by default), unsets staging tag for the run.

=cut

sub run {
  my $self = shift;

  $self->_flagged_as_not_deletable() and return 0;

  my $deletable = $self->_npg_tracking_deletable('unconditional');
  my $vars_set  = 0;

  try {
    $vars_set = $self->_set_vars_from_samplesheet();
    $deletable = $deletable || (
              $self->_npg_tracking_deletable() &&
              $self->_time_limit_deletable()   &&
              $self->_lims_deletable()         &&
              $self->_staging_deletable()      &&
              $self->_irods_seq_deletable()    &&
              $self->_s3_deletable()           &&
              $self->_autoqc_deletable()       &&
              $self->_file_archive_deletable
                               );
  } catch {
    $self->error($_);
  } finally {
    #########
    # unset env variables
    #
    if ($vars_set) {
      for my $var ( npg_pipeline::cache->env_vars() ) {
        ##no critic (RequireLocalizedPunctuationVars)    
        $ENV{$var} = q[];
      }
    }
  };

  if ($deletable && $self->remove_staging_tag) {
    $self->tracking_run->unset_tag($STAGING_TAG);
    $self->info('Staging tag is removed for run ' . $self->id_run);
  }

  return $deletable;
}

############## Private attributes and methods ####################

#####
# A hash reference containing two entries. One, under the key 'seq', is a
# hash reference containing available sequencing files' paths as keys and
# corresponding index files' paths, if available, as values. Another, under
# the key 'ind', contains a hash reference of all found index files, whether
# matching sequencing files or not, where index files' paths are the keys.

has '_staging_files' => (
  isa        => 'HashRef',
  is         => 'ro',
  required   => 0,
  lazy_build => 1,
);
sub _build__staging_files {
  my $self = shift;

  my $files_found  = {};
  my $ext  = $self->file_extension;
  my $iext = $self->index_file_extension;
  my $wanted = sub {
    my $f = $File::Find::name;
    if ($f =~ /[.]$ext\Z/xms) {
      my $i = $self->index_file_path($f);
      # Check for existence rather than for a file in case
      # the files are symbolic links.
      $files_found->{'seq'}->{$f} = (-e $i) ? $i : q[];
    } elsif ($f =~ /[.]$iext\Z/xms) {
      $files_found->{'ind'}->{$f} = 1;
    }
  };
  # Lane directories can be sym-linked, hence the follow option.
  # This option does not take any efect unless the LatestSummary link,
  # which might be present in the archive path, is resolved.
  find({wanted => $wanted, follow => 1, no_chdir => 1}, $self->archive_path);

  return $files_found;
}

has q{_expected_staging_files} => (
  isa        => 'HashRef',
  is         => q{ro},
  lazy_build => 1,
);
sub _build__expected_staging_files {
  my $self = shift;
  my $h = {};
  foreach my $p (@{$self->product_entities}) {
    foreach my $f ($p->staging_files($self->file_extension)) {
      $h->{$f} = $p->target_product()->composition()->freeze2rpt;
    }
  }
  return $h;
}

has q{_run_status_obj} => (
  isa        => q{npg_tracking::Schema::Result::RunStatus},
  is         => q{ro},
  lazy_build => 1,
);
sub _build__run_status_obj {
  my $self = shift;
  my $obj = $self->tracking_run->current_run_status;
  $obj or $self->logcroak('Current run status is undefined.');
  return $obj;
}

has q{_is_unconditionally_deletable} => (
  isa        => q{Bool},
  is         => q{ro},
  lazy_build => 1,
);
sub _build__is_unconditionally_deletable {
  my $self = shift;
  my $crsd = $self->_run_status_obj->description;
  return any { $_ eq $crsd } @NPG_DELETABLE_UNCOND;
}

sub _flagged_as_not_deletable {
  my $self = shift;
  my $test = join q[/], $self->runfolder_path, $DO_NOT_DELETE_NAME;
  my $flagged = -e $test;
  $flagged and $self->info("File or directory '$test' exists, NOT deletable");
  return $flagged;
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
  my $m = sprintf
    'Time limit (min %i): %i last status change was %i days ago, %sdeletable.',
    $self->min_keep_days, $self->id_run, $delta_days, $deletable ?
    q[] : q[NOT ];
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
    $deletable = $self->_is_unconditionally_deletable() &&
                 ($self->ignore_time_limit || $self->_time_limit_deletable());
    if ($deletable) {
      $self->info(qq[$message unconditionally deletable.]);
    }
    return $deletable;
  }

  $deletable = any { $_ eq $crsd } @NPG_DELETABLE_STATES;

  if ($deletable) {
    my $staging_rf = $self->run_folder;
    my $db_rf      = $self->tracking_run->folder_name;
    if ($staging_rf ne $db_rf) {
      $self->logcroak("Runfolder name on staging $staging_rf " .
                      "does not match database runfolder name $db_rf");
    }
  }

  $message .= ($deletable ? q[] : q[NOT ]) . q[deletable.];
  $self->info($message);

  return $deletable;
}

sub _irods_seq_deletable {
  my $self = shift;
  $self->debug('Assessing files in iRODS...');

  if ($self->ignore_irods) {
    $self->info('iRODS check ignored');
    push @{$self->eligible_product_entities}, @{$self->product_entities};
    return 1;
  }

  my $files = {};
  while (my ($f, $rpt_list) = each %{$self->_expected_staging_files}) {
    # Add the sequence file and a correspondign index file.
    push @{$files->{$rpt_list}}, $f, $self->_staging_files->{'seq'}->{$f};
  }

  my $v = npg_pipeline::validation::irods->new(
    irods_destination_collection => $self->irods_destination_collection,
    irods            => $self->irods,
    file_extension   => $self->file_extension,
    product_entities => $self->product_entities,
    staging_files    => $files
  );
  my $deletable = $v->archived_for_deletion();
  push @{$self->eligible_product_entities}, @{$v->eligible_product_entities};

  # Stepping back from a convention to always run every check.
  if ($deletable) {
    $deletable = $self->_irods_seq_pp_deletable();
  }

  my $m = sprintf 'Files in iRODS: run %i %sdeletable',
          $self->id_run , $deletable ? q[] : q[NOT ];
  $self->info($m);

  return $deletable;
}

sub _irods_seq_pp_deletable {
  my $self = shift;
  ######
  # A simplified procedure, which will stop and return 0 (not deletable)
  # as soon as something goes wrong or the first incorrectly archived
  # file is found.
  # No checks are done for products for which no pp was run, these
  # products are considered deletable in the context of this method.
  #

  my $run_collection = $self->_irods_destination_collection4pp();
  my $deletable = 1;
  my $new_re = q[v\d.\d+];
  my $release_type = $npg_pipeline::product::release::IRODS_PP_RELEASE;

  foreach my $p (@{$self->product_entities}) {

    my $product = $p->target_product;
    my $rpt_list = $product->composition()->freeze2rpt;
    $self->is_for_release($product, $release_type) or next;

    my $rel_product_path = $product->dir_path();
    my $irods_root4product = join q[/], $run_collection, $rel_product_path;
    my $staging_root4product = $product->path($self->pp_archive_path);

    my @pp_product_entities = (
      npg_pipeline::validation::entity->new(
        target_product       => $product,
        subsets              => [],
        per_product_archive  => 0,
        staging_archive_root => $staging_root4product
      )
    );

    try {
      my $filters = $self->glob_filters4publisher($product);
      $filters or croak 'Filters not found!';
      my $filter_function = $self->_make_filter_fn(
        $filters->{include}, $filters->{exclude});

      my @files = ();
      find( { wanted => sub { push @files, $_ }, no_chdir => 1 },
            $staging_root4product);
      @files = grep { $filter_function->($_) } grep { -f } @files;

      # Group files by type.
      my $files_by_type = {};
      foreach my $file (@files) {
        ## no critic (RegularExpressions::ProhibitEscapedMetacharacters)
        my ($name,$path,$suffix) = fileparse($file, qr/\.[^.]*/xms);
        ## use critic
        $suffix or croak qq[File $file without suffix];
        push @{$files_by_type->{$suffix}}, $file;
      }

      foreach my $file_type (sort keys %{$files_by_type}) {
        my $v = npg_pipeline::validation::irods->new(
          irods_destination_collection => $irods_root4product,
          irods            => $self->irods,
          file_extension   => $file_type,
          check_md5        => 0,
          product_entities => \@pp_product_entities,
          staging_files    => {$rpt_list => $files_by_type->{$file_type}}
        );
        $deletable = $v->archived_for_deletion();
        $deletable or last;
      }
    } catch {
      $self->error('Error checking pp iRODS archive: ' . $_);
      $deletable = 0;
    };
    $deletable or last;
  }

  return $deletable;
}

sub _irods_destination_collection4pp {
  my $self = shift;

  return __PACKAGE__->new(
    id_run => $self->id_run,
    tracking_run => $self->tracking_run,
    per_product_archive => 1,
    irods_root_collection_ns => $self->irods_pp_root_collection()
  )->irods_destination_collection();
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

  my $m = sprintf 'Autoqc database results: run %i %sdeletable',
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

  foreach my $file (keys %{$self->_expected_staging_files}) {
    if (!-e $file) {
      $self->logwarn("File $file is missing");
      $deletable = 0;
    }
  }

  my $m = sprintf 'Files on staging vs LIMs: run %i %sdeletable',
          $self->id_run , $deletable ? q[] : q[NOT ];
  $self->info($m);

  return $deletable;
}

sub _staging_deletable {
  my $self = shift;
  $self->debug('Examining files on staging');

  $self->debug(join qq{\n}, q{Expected staging files list},
                            (sort keys %{$self->_expected_staging_files}));

  my $deletable = 1;
  foreach my $file (keys %{$self->_staging_files->{'seq'}}) {
    if (!$self->_expected_staging_files->{$file}) {
      $self->logwarn("Staging file $file is not expected");
      $deletable = 0;
    } else {
      if (!$self->_staging_files->{'seq'}->{$file}) {
        $self->logwarn("Staging index file is missing for $file");
        $deletable = 0;
      }
    }
  }

  if ($deletable) {
    my %i_matching = map { $_ => 1 }
                     (values %{$self->_staging_files->{'seq'}});
    foreach my $if (keys %{$self->_staging_files->{'ind'}}) {
      if (!$i_matching{$if}) {
        $self->logwarn("Staging index file $if is not expected");
        $deletable = 0;
      }
    }
  }

  my $m = sprintf 'Files on staging: run %i %sdeletable',
          $self->id_run , $deletable ? q[] : q[NOT ];
  $self->info($m);

  return $deletable;
}

sub _s3_deletable {
  my $self = shift;
  $self->debug('Examining files reported to be in s3');

  if ($self->no_s3_archival) {
    $self->info('s3 check ignored');
    push @{$self->eligible_product_entities}, @{$self->product_entities};
    return 1;
  }

  my $v = npg_pipeline::validation::s3->new(
    product_entities => $self->product_entities,
    file_extension   => $self->file_extension,
    qc_schema        => $self->qc_schema
  );
  my $deletable = $v->fully_archived();
  push @{$self->eligible_product_entities}, @{$v->eligible_product_entities};

  my $m = sprintf 'Files in s3: run %i %sdeletable',
          $self->id_run , $deletable ? q[] : q[NOT ];
  $self->info($m);
  return $deletable;
}

sub _file_archive_deletable {
  my $self = shift;
  $self->debug('Checking that each product is archived in at least ' .
               'one file archive');

  my %product_digests =
    map { $_->target_product->composition->digest => $_->target_product}
    @{$self->product_entities};
  my %archived_product_digest =
    map { $_->target_product->composition->digest => 1}
    @{$self->eligible_product_entities};

  my $deletable = 1;
  for my $original (keys %product_digests) {
    if (!exists $archived_product_digest{$original}) {
      my $tp = $product_digests{$original};
      $self->logwarn('Product not available in any of file archives: ' .
                      $tp->composition->freeze());
      if ($tp->is_tag_zero_product) {
        $self->info('... but it is a tag zero product');
      } elsif ($tp->lims->is_control) {
        $self->info('... but it is a PhiX spike');
      } else {
        $deletable = 0;
      }
    }
  }

  my $m = sprintf
    'Each product is in at least one file archive: run %i %sdeletable',
    $self->id_run , $deletable ? q[] : q[NOT ];
  $self->info($m);

  return $deletable;
}

###########################################################
# The code of this function was copied from
# L<https://github.com/wtsi-npg/npg_irods/blob/63d3485c44cc00e30d8a8ec5bcdc23d2297f0d39/bin/npg_publish_tree.pl#L81> 
# Keith James kdj@sanger.ac.uk is the original author
# Long term this function will have to be moved to npg_seq_common or
# similar NPG Git package
#
sub _make_filter_fn {
  my ($self, $include_a, $exclude_a) = @_;

  my @include_re;
  my @exclude_re;
  my $nerr = 0;
  $include_a ||= [];
  $exclude_a ||= [];

  foreach my $re (@{$include_a}) {
    try {
      push @include_re, qr{$re}msx;
    } catch {
      $self->error("in include regex '$re': $_");
      $nerr++;
    };
  }

  foreach my $re (@{$exclude_a}) {
    try {
      push @exclude_re, qr{$re}msx;
    } catch {
      $self->error("in exclude regex '$re': $_");
      $nerr++;
    };
  }

  if ($nerr > 0) {
    $self->logcroak("$nerr errors in include / exclude filters");
  }

  return sub {
    my ($path) = @_;

    (defined $path and $path ne q[]) or
        croak 'Path argument is required in callback';

    my $include = -f $path;
    if (@include_re) {
      $include = any {$path =~ $_} @include_re;
    }
    if ($include and @exclude_re) {
      $include = not any {$path =~ $_} @exclude_re;
    }

    return $include;
  };
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

=item List::Util

=item Try::Tiny

=item File::Basename

=item Carp

=item WTSI::NPG::iRODS

=item npg_qc::Schema

=item npg_tracking::util::types

=item npg_tracking::util::abs_path

=item npg_tracking::glossary::composition

=item npg_pipeline::base

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

=over

=item Steven Leonard

=item Marina Gourtovaia

=back

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2019,2020,2021,2022 Genome Research Ltd.

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
