package npg_pipeline::function::stage2pp;

use namespace::autoclean;
use Moose;
use MooseX::StrictConstructor;
use Readonly;
use Carp;
use Try::Tiny;
use File::Spec::Functions;

use npg_pipeline::function::definition;
use npg_pipeline::cache::reference;

extends 'npg_pipeline::base';
with qw{ npg_pipeline::function::util
         npg_pipeline::product::release };
with 'npg_common::roles::software_location' => { tools => [qw/nextflow/] };

Readonly::Scalar my $FUNCTION_NAME => 'stage2pp';
Readonly::Scalar my $MEMORY        => q{5000}; # memory in megabytes
Readonly::Scalar my $CPUS          => 4;
Readonly::Scalar my $CONFIG_FILE_KEY => join q[_], $FUNCTION_NAME, q[nf];

Readonly::Scalar my $STUDY_CONFIG_SECTION_NAME => q[portable_pipelines];
Readonly::Scalar my $PP_NAME_KEY               => q[pp_name];
Readonly::Scalar my $PP_VERSION_KEY            => q[pp_version];
Readonly::Scalar my $PP_TYPE_KEY               => q[pp_type];
Readonly::Scalar my $PP_ROOT_KEY               => q[pp_root];
Readonly::Scalar my $JOB_NAME_SUBSTR_LENGTH    => 5;

our $VERSION = '0';

=head2 nextflow_cmd

=head2 create

  Arg [1]    : None

  Example    : my $defs = $obj->create
  Description: Create per-product function definitions objects.

  Returntype : ArrayRef[npg_pipeline::function::definition]

=cut

sub create {
  my ($self) = @_;

  my @products = grep { $self->is_release_data($_) }
                 @{$self->products->{data_products}};

  my @definitions = ();

  foreach my $product (@products) {

    my $pps;
    try {
      $pps = $self->_get_pps_config($product);
    } catch {
      my $err = $_;
      $err = join q[ ],
        "Misconfigured $STUDY_CONFIG_SECTION_NAME section of product config,",
        $err, 'for', $product->composition->freeze();
      $self->logcroak($err);
    };

    $pps or next;

    foreach my $pp (@{$pps}) {
      my $method = $pp->{$PP_NAME_KEY};
      $method =~ s/\s+/_/gsmx;
      $method =~ s/[-]+/_/gsmx;
      $method = join q[_], q[], $method, q[create];
      if ($self->can($method)) {
        push @definitions, $self->$method($product, $pp);
      } else {
        $self->error($pp->{$PP_NAME_KEY} . 'portable pipeline is not implemented');
      }
    }
  }

  if (!@definitions) {
    $self->debug('no stage2pp enabled data products, skipping');
    push @definitions, npg_pipeline::function::definition->new(
                         created_by => __PACKAGE__,
                         created_on => $self->timestamp(),
                         identifier => $self->label,
                         excluded   => 1
                       );
  }

  return \@definitions;
}

sub _ncov2019_artic_nf_create {
  my ($self, $product, $pp) = @_;

  my $in_dir_path  = $product->stage1_out_path($self->no_archive_path());
  my $out_dir_path = $product->path($self->archive_path());

  my $ref_cache_instance   = npg_pipeline::cache::reference->instance();
  my $do_gbs_plex_analysis = 0;
  my $ref_path = $ref_cache_instance
                 ->get_path($product, 'bwa0_6', $self->repository, $do_gbs_plex_analysis);
  $ref_path or $self->logcroak(
    'bwa reference is not found for ' . $product->composition->freeze());
  my $bed_file = $ref_cache_instance
                 ->get_primer_panel_bed_file($product, $self->repository);
  $bed_file or $self->logcroak(
    'Bed file is not found for ' . $product->composition->freeze());

  my $pp_id = (substr $pp->{$PP_NAME_KEY}, 0, $JOB_NAME_SUBSTR_LENGTH) .
              (substr $pp->{$PP_VERSION_KEY}, 0, $JOB_NAME_SUBSTR_LENGTH);

  my %job_attrs = ('created_by'  => __PACKAGE__,
                   'created_on'  => $self->timestamp(),
                   'identifier'  => $self->label,
                   'num_cpus'    => [$CPUS],
                   'memory'      => $MEMORY,
                   'composition' => $product->composition(), );

  $pp->{$PP_ROOT_KEY} or $self->logcroak("$PP_ROOT_KEY not defined for " . $pp->{$PP_NAME_KEY});
  (-d $pp->{$PP_ROOT_KEY}) or $self->logcroak(sprintf '%s directory %s does not exists for %s',
                              $PP_ROOT_KEY, $pp->{$PP_ROOT_KEY}, $pp->{$PP_NAME_KEY});
  my $pp_dir = catdir($pp->{$PP_ROOT_KEY}, $pp->{$PP_NAME_KEY}, $pp->{$PP_VERSION_KEY});
  (-d $pp_dir) or $self->logcroak("$pp_dir does not exist or is not a directory");

  # And yes, it's -profile, not --profile!
  my $command = join q[ ], $self->nextflow_cmd(), "run $pp_dir",
                             '-profile singularity,sanger',
                             '--illumina --cram --prefix ' . $self->label,
                             "--ref $ref_path",
                             "--bed $bed_file",
                             "--directory $in_dir_path",
                             "--outdir $out_dir_path";
  $job_attrs{'command'}  = $command;
  $job_attrs{'job_name'} = join q[_], $FUNCTION_NAME, $pp_id, $self->label();

  return npg_pipeline::function::definition->new(\%job_attrs);
}

sub _get_pps_config {
  my ($self, $product) = @_;

  my $strict = 1; # disregard the default section
  my $study_config = $self->study_config($product->lims, $strict);
  ($study_config and exists $study_config->{$STUDY_CONFIG_SECTION_NAME}) or return;

  my $pps = $study_config->{$STUDY_CONFIG_SECTION_NAME};
  ($pps and (q[ARRAY] eq ref $pps)) or croak 'array of portable pipelines is expected';

  my @stage2_pps = ();

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
    if ($pp->{$PP_TYPE_KEY} eq $FUNCTION_NAME) {
      $trim->($pp->{$PP_NAME_KEY});
      $trim->($pp->{$PP_VERSION_KEY});
      push @stage2_pps, $pp;
    }
  }

  return \@stage2_pps;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

npg_pipeline::function::stage2pp

=head1 SYNOPSIS

  my $obj = npg_pipeline::function::stage2pp->new(runfolder_path => $path);

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head1 BUGS AND LIMITATIONS

=head1 INCOMPATIBILITIES

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item namespace::autoclean

=item Moose

=item MooseX::StrictConstructor

=item Readonly

=item Carp

=item Try::Tiny

=item File::Spec::Functions

=item npg_common::roles::software_location

=back

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
