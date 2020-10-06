package npg_pipeline::product::release::portable_pipeline;

use Moose::Role;
use Readonly;
use Carp;
use File::Spec::Functions;
use Try::Tiny;

with 'npg_tracking::util::pipeline_config';

Readonly::Scalar my $STUDY_CONFIG_SECTION_NAME => q[portable_pipelines];
Readonly::Scalar my $PP_NAME_KEY               => q[pp_name];
Readonly::Scalar my $PP_VERSION_KEY            => q[pp_version];
Readonly::Scalar my $PP_TYPE_KEY               => q[pp_type];
Readonly::Scalar my $PP_ROOT_KEY               => q[pp_root];
Readonly::Scalar my $PP_ARCHIVAL_FLAG_KEY      => q[pp_archival_flag];
Readonly::Scalar my $PP_STAGING_ROOT_KEY       => q[pp_staging_root];
Readonly::Scalar my $PP_INPUT_GLOB_KEY         => q[pp_input_glob];
Readonly::Scalar my $JOB_NAME_SUBSTR_LENGTH    => 5;

our $VERSION = '0';

=head1 NAME

npg_pipeline::product::release::portable_pipeline

=head1 SYNOPSIS

=head1 DESCRIPTION

Moose role providing utility methods for portable pipelines.
Most of the methods can be used as class methods.

=head1 SUBROUTINES/METHODS

=cut

=head2 pps_config4product

Returns an array of portable pipeline configurations for a product.
This array can be empty. Takes an optional argument, type of the portable
pipeline. If supplied, only the pipelines of this type are returned.

  my $pipelines4type = $obj->pps_config4product($product, 'stage2pp');
  my $all_pipelines  = $obj->pps_config4product($product);

=cut

sub pps_config4product {
  my ($self, $product, $pp_type) = @_;

  $product or croak 'Product attribute should be defined';
  my $pps;
  try {
    $pps = $self->pps_config4lims_entity($product->lims, $pp_type);
  } catch {
    my $err = $_;
    $err = join q[ ],
      "Misconfigured $STUDY_CONFIG_SECTION_NAME section of product config,",
      $err, 'for', $product->composition->freeze();
    croak $err;
  };

  return $pps;
}

=head2 pp_name

Given a configuration hash for the portable pipeline,
returns its name.

=cut

sub pp_name {
  my ($self, $pp_conf) = @_;
  $pp_conf or croak 'pp config should be defined';
  return $pp_conf->{$PP_NAME_KEY};
}

=head2 pp_version

Given a configurationn hash for the portable pipeline,
returns its version.

=cut

sub pp_version {
  my ($self, $pp_conf) = @_;
  $pp_conf or croak 'pp config should be defined';
  return $pp_conf->{$PP_VERSION_KEY};
}

=head2 pps_config4lims_entity

Returns an array of portable pipeline configurations for a lims entiry.
This array can be empty. Takes an optional argument, type of the portable
pipeline. If supplied, only the pipelines of this type are returned.

  my $pipelines4type = $obj->pps_config4product($lims, 'stage2pp');
  my $all_pipelines  = $obj->pps_config4product($lims);

=cut

sub pps_config4lims_entity {
  my ($self, $lims, $pp_type) = @_;

  $lims or croak 'lims attribute should be defined';

  my @pps4type = ();

  my $strict = 1; # disregard the default section
  my $study_config = $self->study_config($lims, $strict);
  if ($study_config and exists $study_config->{$STUDY_CONFIG_SECTION_NAME}) {

    my $pps = $study_config->{$STUDY_CONFIG_SECTION_NAME};
    ($pps and (q[ARRAY] eq ref $pps)) or croak 'array of portable pipelines is expected';

    my $trim = sub {
      my $s = shift;
      $s =~ s/\s+\Z//smx;
      $s =~ s/\A\s+//smx;
      return $s;
    };

    foreach my $pp (@{$pps}) {
      ($pp and (q[HASH] eq ref $pp)) or croak 'portable pipeline config should be a hash';
      $pp->{$PP_NAME_KEY} or croak "$PP_NAME_KEY is missing in a pp config";
      $pp->{$PP_VERSION_KEY} or croak sprintf '%s is missing in a %s pp config',
                                              $PP_VERSION_KEY, $pp->{$PP_NAME_KEY};

      $pp->{$PP_TYPE_KEY} or croak sprintf '%s is missing in a %s pp config',
                                           $PP_TYPE_KEY, $pp->{$PP_NAME_KEY};
      $pp->{$PP_TYPE_KEY} = $trim->($pp->{$PP_TYPE_KEY});
      if ($pp_type && ($pp->{$PP_TYPE_KEY} ne $pp_type)) {
        next;
      }

      $pp->{$PP_NAME_KEY}    =  $trim->($pp->{$PP_NAME_KEY});
      $pp->{$PP_VERSION_KEY} = $trim->($pp->{$PP_VERSION_KEY});
      push @pps4type, $pp;
    }
  }

  return \@pps4type;
}

=head2 pp_archive4product_relative

Relative path to the output of a portable pipeline,
includes its name and version. For example, for a pipeline 'tomato'
version '1.0' this path is 'tomato/1.0'.

Argument - a config hash for a single portable pipeline.
Can be used as a class method.

  my $path = $obj->pps_config4product($pp_conf); 

=cut

sub pp_archive4product_relative {
  my ($self, $pp_conf) = @_;

  $pp_conf or croak 'pp config should be defined';
  return catdir ($self->canonical_name($pp_conf->{$PP_NAME_KEY}),
                 $self->canonical_name($pp_conf->{$PP_VERSION_KEY}));
}

=head2 pp_archive4product

  Returns a product archive for a portable pipeline relative to
  the supplied path. Can be used as a class method.
  
  my $apath = $obj->pp_archive4product($product, $pp_conf, $path);

=cut

sub pp_archive4product {
  my ($self, $product, $pp_conf, $path) = @_;

  ($product and $pp_conf and $path) or croak 'Product object, ' .
    'config hash of a single portable pipeline and a base path ' .
    'should be defined';
  return catdir($product->path($path),
                $self->pp_archive4product_relative($pp_conf));
}

=head2 pp_short_id

Returns a short, no more than 10 characters long id for
a portable pipeline, whose config hash is given as an argument.
This id is not guaranteed to be globally unique, but shoudl be
distinct from other portable pipelines for the same product.
Can be used as a class method.

  my $id = $obj->pp_short_id($pp_conf);

=cut

sub pp_short_id {
  my ($self, $pp_conf) = @_;

  $pp_conf or croak 'pp config should be defined';
  return (substr $pp_conf->{$PP_NAME_KEY}, 0, $JOB_NAME_SUBSTR_LENGTH) .
         (substr $pp_conf->{$PP_VERSION_KEY}, 0, $JOB_NAME_SUBSTR_LENGTH);
}

=head2 pp_deployment_root

Returns the directory root for the portable pipeline deployment.
Can be used as a class method.
Error if the root is not defined or the root directory does not exist.

  my $root = $obj->pp_deployment_root($pp_conf);

=cut

sub pp_deployment_root {
  my ($self, $pp_conf) = @_;

  $pp_conf or croak 'pp config should be defined';
  my $dir = $pp_conf->{$PP_ROOT_KEY};
  $dir or croak "$PP_ROOT_KEY not defined for " . $pp_conf->{$PP_NAME_KEY} ;
  (-d $dir) or croak sprintf '%s directory %s does not exists for %s',
                             $PP_ROOT_KEY, $dir, $pp_conf->{$PP_NAME_KEY};
  return $dir;
}

=head2 pp_deployment_dir

Returns the deployment directory for the portable pipeline.
Can be used as a class method.
Error if the root is not defined or the directory does not exist
or the deployment directory for this pipeline does not exist.

  my $dir = $obj->pp_deployment_dir($pp_conf);

=cut

sub pp_deployment_dir {
  my ($self, $pp_conf) = @_;

  my $pp_dir = catdir($self->pp_deployment_root($pp_conf),
                      $pp_conf->{$PP_NAME_KEY}, $pp_conf->{$PP_VERSION_KEY});
  (-d $pp_dir) or croak "$pp_dir does not exist or is not a directory";
  return $pp_dir;
}

=head2 pp_enable_external_archival

Returns the value of a flag specifying whether the output of this
pipeline is eligible for external archival. Returns false if the flag
is not set for the pipeline. Can be used as a class method.

=cut

sub pp_enable_external_archival {
  my ($self, $pp_conf) = @_;
  $pp_conf or croak 'pp config should be defined';
  return $pp_conf->{$PP_ARCHIVAL_FLAG_KEY};
}

=head2 pp_staging_root

Returns a path to the root of the interim archive for this
portable pipeline. Such an archive can be used to stage the
data before submitting to the external archive.
Error if the value if not defined or the directory specified
in the pipeline config does not exist.
Can be used as a class method.

  my $staging_root = $obj->pp_staging_root($pp_conf);

=cut

sub pp_staging_root {
  my ($self, $pp_conf) = @_;

  $pp_conf or croak 'pp config should be defined';
  my $root = $pp_conf->{$PP_STAGING_ROOT_KEY};
  $root or croak "$PP_STAGING_ROOT_KEY is not defined";
  (-d $root) or croak
    "$PP_STAGING_ROOT_KEY $root does not exist or is not a directory";

  return $root
}

=head2 pp_input_glob

Returns an input glob expression if it is set, undefined
value if it is not set. Can be used as a class method.

  my $staging_root = $obj->pp_input_glob($pp_conf);

=cut

sub pp_input_glob {
  my ($self, $pp_conf) = @_;
  $pp_conf or croak 'pp config should be defined';
  return  $pp_conf->{$PP_INPUT_GLOB_KEY};
}

=head2 canonical_name

Returns a'canonical' version of the argument string.
Anything that is not an alphanumerical character, dot or underscore
is replaced by an underscore. Can be used to convert pipeline name or
version into a string suitable for a directory of file name or a
Perl method subroutine name component.

=cut

sub canonical_name {
  my ($self, $name) = @_;

  my $canonical = $name;
  # Anything that is not an alphanumerical character, dot or underscore
  # is replaced by an underscore.
  ##no critic (RegularExpressions::ProhibitEnumeratedClasses)
  $canonical =~ s/[^0-9a-zA-Z_.]+/_/smxg;
  $canonical =~ /[0-9a-zA-Z]/smx or croak
    sprintf 'Canonical pp name %s for %s - not much left', $canonical, $name;
  ##use critic

  return $canonical;
}

no Moose::Role;

1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose::Role

=item Readonly

=item Carp

=item use File::Spec::Functions

=item Try::Tiny

=item npg_tracking::util::pipeline_config

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2020 Genome Research Ltd.

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
