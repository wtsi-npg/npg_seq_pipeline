package npg_pipeline::function::pp_archiver;

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;
use Text::CSV qw/csv/;
use File::Spec::Functions;
use File::Basename;
use Readonly;
use Try::Tiny;
use Perl6::Slurp;

use npg_qc::Schema;
use npg_pipeline::function::definition;

extends 'npg_pipeline::base';
with qw/ npg_pipeline::product::release
         npg_pipeline::product::release::portable_pipeline /;
with 'npg_common::roles::software_location' => { tools => [qw/npg_upload2climb npg_climb2mlwh/] };

our $VERSION = '0';

################# Package-level constants ###################

Readonly::Array  our @CSV_PARSER_OPTIONS   => ( strict      => 1,
                                                sep_char    => qq[\t],
                                                eol         => qq[\n],
                                                quote_char  => undef,
                                                escape_char => undef, );
Readonly::Scalar our $SAMPLE_NAME_COLUMN_NAME => q[sample_name];
Readonly::Scalar our $SAP_COLUMN_NAME         => q[staging_archive_path];
Readonly::Scalar our $FILES_GLOB_COLUMN_NAME  => q[files_glob];

##################### Private constants #####################

Readonly::Scalar my $MANIFEST_PATH_ENV_VAR => q[NPG_MANIFEST4PP_FILE];
Readonly::Scalar my $MANIFEST_PREFIX       => q[manifest4pp_upload];

Readonly::Scalar my $PP_NAME               => q[ncov2019-artic-nf];
Readonly::Scalar my $CLIMB_ARCHIVAL_KEY    => q[climb_archival];
Readonly::Array  my @CLIMB_ARCHIVAL_CREDENTIALS_KEYS => qw/user host pkey_file/;
Readonly::Scalar my $PP_DATA_GLOB => q[*_{] .
                q[trimPrimerSequences/*.mapped.bam,makeConsensus/*.fa] .
				       q[}];

Readonly::Array  my @COLUMN_NAMES          => (
                                           $SAMPLE_NAME_COLUMN_NAME,
                                           qw / library_type
                                                primer_panel /,
                                           $FILES_GLOB_COLUMN_NAME,
                                           $SAP_COLUMN_NAME,
                                           qw / product_json
                                                id_product /,
                                              );

=head1 NAME

npg_pipeline::function::pp_archiver

=head1 SYNOPSIS

  my $archiver = npg_pipeline::function::pp_archiver->new(
                 runfolder_path => $my_path);

  # to generate a manifest and create a job definition
  my $definitions = $archiver->create();
  my $d = $definitions->[0];
  print $d->excluded;

  # to generate a manifest
  $archiver->generate_manifest();

=head1 DESCRIPTION

This class provides two public methods, create() and
generate_manifest(), which can serve as callbacks to
for pipeline's functions. Please refer to the methods'
documentation for their functionality.

The class is meant to set the scene for the upload of data,
which are produced by portable pipelines, to third-party archives.

The current implementation is not generic, it focuses on
dealing with data for the ncov2019-artic-nf pipeline.

Both create and generate_manifest methods generate a
manifest, a tab-separated file, containing a header and
sufficient product data for further upload to a third-party
archive.

If the env variable NPG_MANIFEST4PP_FILE is set, the value of
the variable is interpreted as a path of the manifest file.
If the file does not exist, it is created. By default the manifest
is created in the analysis directory of the run folder, its
name starts with 'manifest4pp_upload'. A new file with a unique
name is generated on each execution of either of the methods.

Criteria for products to be included into the  manifest:
the portable pipeline is flagged for external archival and
the library manual QC outcome for this product exists and is
set to 'Accepted Final' and the QC outcome for the lane this
product belongs to should also be 'Accepted Final'.

=cut

=head1 SUBROUTINES/METHODS

=cut

=head2 npg_upload2climb_cmd

An absolute path to the command.

=head2 qc_schema

Lazy-build DBIx schema object for the QC database,
inherited from npg_pipeline::base and changed to
be lazy.

=cut

has '+qc_schema' => (
  lazy       => 1,
  builder    => '_build_qc_schema',
);
sub _build_qc_schema {
  return npg_qc::Schema->connect();
}

=head2 create

Returns an array containing a single npg_pipeline::function::definition
object. When no products are to be archived, the 'excluded' attribute of
the object is set to true, otherwise this attribute is set to false and
the 'command' attribute is set. Running this command should archive all
portable pipeline products to their destinations.

While this method is running, it creates a manifest file, which can be
used by the generated command to infer what products have to be archived
and their location in the run folder.

For now only products belonging to the ncov2019-artic-nf pipeline are
considered.

=cut

sub create {
  my $self = shift;

  my $ref =  {created_by => __PACKAGE__,
              created_on => $self->timestamp(),
              identifier => $self->label};

  # Create and write out the manifest.
  my $num_samples = $self->_generate_manifest4archiver();

  if ($num_samples) {
    $ref->{'job_name'} = join q[_], 'pp_archiver', $self->label();
    $ref->{'command'}  = join q[ ], $self->npg_upload2climb_cmd,
                                    $self->_climb_archival_options(),
                                    '--manifest', $self->_manifest_path,
                                    q[&&], $self->npg_climb2mlwh_cmd,
                                    $self->_climb_archival_options(),
                                    '--run_folder', $self->run_folder;
  } else { # An 'empty' manifest has been generated.
    $self->debug('No pp data to archive, skipping');
    $ref->{'excluded'} = 1;
  }

  return [npg_pipeline::function::definition->new($ref)];
}

=head2 generate_manifest

Returns an array containing a single npg_pipeline::function::definition
object, which has the 'excluded' attribute set to true. No command for 
later execution is generated. While this method is running, it creates
a manifest file, listing the products due to be archived their location
in the run folder.

For now only products belonging to the ncov2019-artic-nf pipeline are
considered.

=cut

sub generate_manifest {
  my $self = shift;

  $self->_generate_manifest4archiver();

  # No need to create a job, the only purpose of this function is
  # to generate the manifest.
  return [ npg_pipeline::function::definition->new(
              created_by => __PACKAGE__,
              created_on => $self->timestamp(),
              identifier => $self->label,
              excluded   => 1) ];
}

sub _generate_manifest4archiver {
  my $self = shift;

  my $num_samples;
  my $path = $ENV{$MANIFEST_PATH_ENV_VAR};

  if ($path and -e $path) {
    $self->info(sprintf 'Existing manifest %s will be used', $path);
    my @lines = slurp $path;
    $num_samples = (scalar @lines) - 1;
    ($num_samples >= 0) or $self->logcroak("No content in $path");
  } else {
    $self->info(sprintf 'A new manifest %s will be created', $self->_manifest_path);

    # In the context of one run the third column of the manifest is redundant.
    # However, this would help to deal with ad-hock situations.

    my @lines = ();
    foreach my $sname (sort keys %{$self->_samples4upload}) {
      my $cached = $self->_samples4upload->{$sname};
      my $c      = $cached->{'product'}->composition;
      my $lims   = $cached->{'product'}->lims;
      push @lines, [$sname,
                    $lims->library_type  || q[],
                    $lims->gbs_plex_name || q[],
                    $cached->{'pp_data_glob'},
                    $self->_staging_archive_path,
                    $c->freeze(),
                    $c->digest()];
    }

    $num_samples = @lines;
    # add header
    unshift @lines, \@COLUMN_NAMES;

    $self->info('Writing manifest to ' . $self->_manifest_path);
    csv(in => \@lines, out => $self->_manifest_path, @CSV_PARSER_OPTIONS);
  }

  $num_samples or $self->warn('Nothing to archive,the manifest is empty');

  return $num_samples;
}

has '_samples4upload' => (
  isa        => q{HashRef},
  is         => q{ro},
  lazy_build => 1,
);
sub _build__samples4upload {
  my $self = shift;

  my $products4archive = {};

  foreach my $product ( @{$self->_products4upload} ) {

    # Skips
    $product->lims->sample_is_control and next;
    $product->lims->sample_consent_withdrawn and next;
    $self->has_qc_for_release($product) or next;

    my $sname = $product->lims->sample_supplier_name;
    $sname or $self->logcroak('Supplier sample name is not set for ' .
      $product->composition->freeze());

    # First come basis for choosing one of the duplicates.
    if ($products4archive->{$sname}) {
      $self->logwarn("Already cached sample $sname for sending, skipping");
      next;
    }

    $products4archive->{$sname}->{'product'} = $product;

    # Where are the files to upload?
    my $dir = $self->pp_archive4product(
                    $product, $self->_pipeline_config, $self->pp_archive_path);
    $products4archive->{$sname}->{'pp_data_glob'} = catdir($dir, $PP_DATA_GLOB);
  }

  return $products4archive;
}

has '_products4upload' => (
  isa        => q{ArrayRef},
  is         => q{ro},
  lazy_build => 1,
);
sub _build__products4upload {
  my $self = shift;

  my @products =  grep { $self->is_release_data($_) }
                  @{$self->products->{data_products}};
  my $pps4archival= {};
  my @products4upload = ();

  foreach my $product (@products) {
    my @pps = grep { $self->pp_enable_external_archival($_) }
              grep { $self->pp_name($_) eq $PP_NAME }
              @{$self->pps_config4product($product)};
    @pps or next;
    foreach my $pp (@pps) {
      $pps4archival->{$self->pp_short_id($pp)} = 1;
    }
    push @products4upload, $product;
  }

  if (keys %{$pps4archival} > 1) {
    $self->logcroak('Multiple external archives are not supported');
  }

  return \@products4upload;
}

has '_pipeline_config' => (
  isa        => q{Maybe[HashRef]},
  is         => q{ro},
  lazy_build => 1,
 );
sub _build__pipeline_config {
  my $self = shift;

  foreach my $product ( @{$self->_products4upload} ) {
    my @pps = @{$self->pps_config4product($product)};
    if (@pps) {
      my $config = $pps[0];
      $self->pp_staging_root($config); # validates
      return $config; # any config will do, we know there is only one
    }
  }

  return;
}

has '_staging_archive_path' => (
  isa        => q{Str},
  is         => q{ro},
  lazy_build => 1,
);
sub _build__staging_archive_path {
  my $self = shift;
  # PP_STAGING_ROOT/RUN_ID/ANALYSIS_DIR_NAME/RUN_FOLDER_NAME
  my ($dir_name) = fileparse($self->analysis_path);
  return catdir($self->pp_staging_root($self->_pipeline_config),
                $self->id_run,
                $dir_name,
		$self->run_folder);
}

has '_manifest_path' => (
  isa        => q{Str},
  is         => q{ro},
  lazy_build => 1,
);
sub _build__manifest_path {
  my $self = shift;

  my $path = $ENV{$MANIFEST_PATH_ENV_VAR};
  if (not defined $path) {
    $path = join q[_], $MANIFEST_PREFIX, $self->label(), $self->random_string();
    $path = catfile($self->analysis_path, $path . q[.tsv]);
  }

  return $path;
}

sub _climb_archival_options {
  my $self = shift;

  my $aconfig = $self->_pipeline_config->{$CLIMB_ARCHIVAL_KEY};
  $aconfig and (ref $aconfig eq q[HASH]) or $self->logcroak(
    "$CLIMB_ARCHIVAL_KEY section of the product config. is absent");

  my @options = ();
  for my $key (@CLIMB_ARCHIVAL_CREDENTIALS_KEYS) {
    $aconfig->{$key} or $self->logcroak(
      "$key entry is absent in product config.");
    push @options, q[--] . $key, $aconfig->{$key};
  }

  return join q[ ], @options;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item MooseX::StrictConstructor

=item namespace::autoclean

=item Text::CSV

=item File::Spec::Functions

=item File::Basename

=item Readonly

=item Try::Tiny

=item Perl6::Slurp

=item npg_qc::Schema

=item npg_common::roles::software_location

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2020,2021 Genome Research Ltd.

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
