package npg_pipeline::function::stage2pp;

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;
use Readonly;
use Carp;
use Try::Tiny;
use File::Spec::Functions;
use File::Slurp;

use npg_pipeline::cache::reference;
use npg_pipeline::runfolder_scaffold;

extends 'npg_pipeline::base_resource';
with qw{ npg_pipeline::function::util
         npg_pipeline::product::release
         npg_pipeline::product::release::portable_pipeline };
with 'npg_common::roles::software_location' =>
  { tools => [qw/ nextflow samtools /] };
# Not creating above an accessor for the plot-ampliconstats script
# since perl subs cannot have dashes in their names.

Readonly::Scalar my $DEFAULT_PIPELINE_TYPE => q[stage2pp];

Readonly::Array  my @DEFAULT_AMPLICONSTATS_DEPTH  => qw(1 10 20 100);
Readonly::Scalar my $AMPLICONSTATS_OPTIONS        => q[-t 50];

our $VERSION = '0';

=head1 NAME

npg_pipeline::function::stage2pp

=head1 SYNOPSIS

  my $obj = npg_pipeline::function::stage2pp->new(
    runfolder_path => $path,
    pipeline_type  => 'stage2pp');

=head1 DESCRIPTION

This class contains callbacks for arbitrary pipeline functions
that are mapped to the create method of this class. The way
these functions are scheduled is determined by the function
graph.

The create method returns an array of definition for portable
pipelines that have to be invoked for products (samples). Tag
zero and spiked controls are not considered. The nature of the
portable pipelines for each product and their parameters are
defined by the product configuration file. Multiple definitions
can be returned for a single product.

This class is intended for pipelines which use either stage 1
analysis output as input or the output generated during the
stage 2 of the analysis. The output of these pipelines is
normally required at the data archival stage and/or for quality
assessment of the data before the archival step.

=head1 SUBROUTINES/METHODS

=head2 pipeline_type

  Attribute, defaults to 'stage2pp'.

=cut

has 'pipeline_type' => (
  isa      => 'Str',
  is       => 'ro',
  required => 0,
  default  => $DEFAULT_PIPELINE_TYPE,
);

=head2 nextflow_cmd

=head2 samtools_cmd

=head2 create

  Example    : my $defs = $obj->create();
  Description: Create per-product function definitions objects.

  Returntype : ArrayRef[npg_pipeline::function::definition]

=cut

sub create {
  my ($self) = @_;

  my @definitions = ();

  foreach my $product (@{$self->_products}) {

    my $pps;
    try {
      $pps = $self->pps_config4product($product, $self->pipeline_type);
    } catch {
      $self->logcroak($_);
    };

    foreach my $pp (@{$pps}) {
      my $pp_name = $self->pp_name($pp);
      my $cname   = $self->canonical_name($pp_name);
      my $method  = join q[_], q[], $cname, q[create];
      if ($self->can($method)) {
        # Definition factory method might return an undefined
        # value, which will be filtered out later.
        push @definitions, $self->$method($product, $pp);
      } else {
        $self->error(sprintf
          '"%s" portable pipeline is not implemented, method %s is not available',
          $pp_name, $method
        );
      }
    }
  }

  @definitions = grep { $_ } @definitions;

  if (@definitions) {
    # Create directories for all expected outputs.
    $self->info('The following pp output directories will be created:' .
                join qq[\n], q[], @{$self->_output_dirs});
    npg_pipeline::runfolder_scaffold->make_dir(@{$self->_output_dirs});
  } else {
    $self->debug('no stage2pp enabled data products, skipping');
    push @definitions, $self->create_definition({
      identifier => $self->label,
      excluded   => 1
    });
  }

  return \@definitions;
}

=head2 astats_min_depth_array

A class or package method returning an array of the minimum
base depths used to drive the samtools ampliconstats tool.
Can also be invoked on an object instance.

=cut

sub astats_min_depth_array {
  my ($self, $pp) = @_;
  $pp or $self->logcroak('pp argument required');
  my $depth_array = $pp->{'ampliconstats_min_base_depth'}
                    || \@DEFAULT_AMPLICONSTATS_DEPTH;
  return $depth_array;
}

has '_products' => (
  isa        => 'ArrayRef',
  is         => 'ro',
  required   => 0,
  lazy_build => 1,
);
sub _build__products {
  my $self = shift;
  my @products = grep { $self->is_release_data($_) }
                 @{$self->products->{data_products}};
  return \@products;
}

has '_output_dirs' => (
  isa      => 'ArrayRef',
  is       => 'ro',
  required => 0,
  default  => sub { return []; },
);

has '_names_map' => (
  isa      => 'HashRef',
  is       => 'ro',
  required => 0,
  default  => sub { return {}; },
);

sub _canonical_name {
  my ($self, $name) = @_;
  if (!exists $self->_names_map->{$name}) {
    $self->_names_map->{$name} = $self->canonical_name($name);
  }
  return $self->_names_map->{$name};
}

sub _primer_bed_file {
  my ($self,$product) = @_;
  my $bed_file = npg_pipeline::cache::reference->instance()
                 ->get_primer_panel_bed_file($product, $self->repository);
  $bed_file or $self->logcroak(
    'Bed file is not found for ' . $product->composition->freeze());
  return $bed_file;
}

sub _join_commands {
  my ($operator, @commands) = @_;
  return join qq[ $operator ], map { q[(] . $_ . q[)] } @commands;
}

sub _job_name {
  my ($self, $pp) = @_;
  return join q[_], $self->pipeline_type, $self->pp_short_id($pp), $self->label();
}

sub _job_attrs {
  my ($self, $product, $pp) = @_;
  return {'job_name'    => $self->_job_name($pp),
          'composition' => $product->composition()};
}

sub _pp_name_arg {
  my ($self, $pp) = @_;
  return q[--pp_name '] . $self->pp_name($pp) . q['];
}

sub _ncov2019_artic_nf_create {
  my ($self, $product, $pp) = @_;

  my $in_dir_path  = $product->stage1_out_path($self->no_archive_path());
  my $out_dir_path = $self->pp_archive4product($product, $pp, $self->pp_archive_path());
  push @{$self->_output_dirs}, $out_dir_path;

  my $ref_cache_instance   = npg_pipeline::cache::reference->instance();
  my $do_gbs_plex_analysis = 0;
  my $ref_path = $ref_cache_instance
    ->get_path($product, 'bwa0_6', $self->repository, $do_gbs_plex_analysis);
  $ref_path or $self->logcroak(
    'bwa reference is not found for ' . $product->composition->freeze());

  my $job_attrs = $self->_job_attrs($product, $pp);

  $job_attrs->{'command'} = join q[ ],
    $self->nextflow_cmd(), 'run', $self->pp_deployment_dir($pp),
    '-profile singularity,sanger', # It's -profile, not --profile!
    '--illumina --cram --prefix ' . $self->label,
    "--ref $ref_path",
    '--bed ' . $self->_primer_bed_file($product),
    "--directory $in_dir_path",
    "--outdir $out_dir_path";

  return $self->create_definition($job_attrs);
}

has '_lane_counter4ampliconstats' => (
  isa      =>' HashRef',
  is       => 'ro',
  required => 0,
  default  => sub { return {}; },
);

sub _generate_replacement_map {
  my ($self, $lane_product) = @_;

  my $pos = $lane_product->composition->get_component(0)->position;
  my @map = ();
  for my $p (@{$self->_products}) {
    ($p->composition->get_component(0)->position == $pos) or next;
    my $sn = $p->lims->sample_supplier_name || q[unknown];
    $sn =~ s/\s/_/gxms; # No white spaces policy!
    push @map, join qq[\t], $p->file_name, $sn;
  }

  return \@map;
}

sub _ncov2019_artic_nf_ampliconstats_create {
  my ($self, $product, $pp) = @_;

  my $pp_name = $self->pp_name($pp);

  # Can we deal with this product?
  if ($product->composition->num_components > 1) {
    # Not dealing with merges
    $self->warn(qq[$pp_name is for one-component compositions]);
    return;
  }
  # Have we dealt with this lane already?
  my $position = $product->composition->get_component(0)->position;
  if ($self->_lane_counter4ampliconstats->{$position}) { # Yes
    return;
  }

  my $depth_array = $self->astats_min_depth_array($pp);

  my $lane_product = ($product->lanes_as_products)[0];
  my $lane_pp_path = $self->pp_archive4product(
    $lane_product, $pp, $self->pp_archive_path());
  push @{$self->_output_dirs}, $lane_pp_path;
  # Make directory now, we need to create a replacement map file there.
  npg_pipeline::runfolder_scaffold->make_dir($lane_pp_path);
  my $sta_file = join q[/],
    $lane_pp_path, $lane_product->file_name(ext => q[astats]);

  my $file_glob = $self->pp_input_glob($pp);
  $file_glob or $self->logcroak(qq[Input glob is not defined for '$pp_name' pp]);
  my $input_files_glob = join q[/],
    $lane_product->path($self->pp_archive_path()), $file_glob;
  my $lane_archive = $lane_product->path($self->archive_path());
  my $lane_qc_dir = $lane_product->qc_out_path($self->archive_path());

  my $image_dir = join q[/], $lane_qc_dir, q[ampliconstats];
  push @{$self->_output_dirs}, $image_dir;
  my $prefix = join q[/], $image_dir, $lane_product->file_name();

  my $replacement_map_file = join q[/], $lane_pp_path, 'replacement_map.txt';
  write_file($replacement_map_file, join qq[\n],
             @{$self->_generate_replacement_map($lane_product)});

  my $job_attrs = $self->_job_attrs($lane_product, $pp);
  my $num_cpus = $self->_get_massaged_resources()->{num_cpus}[0];
  my $sta_cpus_option = $num_cpus > 1 ? q[-@] . ($num_cpus - 1) : q[];

  # Use samtools to produce ampliconstats - one file per lane.
  my $sta_command = join q[ ], $self->samtools_cmd,
                               'ampliconstats',
                               $sta_cpus_option,
                               $AMPLICONSTATS_OPTIONS,
                               q[-d ] . join(q[,], @{$depth_array}),
                               $self->_primer_bed_file($product),
                               $input_files_glob;
  $sta_command = join q[ > ], $sta_command, $sta_file;

  # Run plot-ampliconstats to produce gnuplot plot files and PNG images
  # for them; prior to this filenames in ampliconstats should be remapped
  # to supplier sample names; tag index part of the file name to be retained.
  my @pa_commands = ();
  ##no critic (ValuesAndExpressions::RequireInterpolationOfMetachars)
  push @pa_commands, join q[ ],
    q[perl -e],
    q['use strict;use warnings;use File::Slurp;],
    q[my%h=map{(split qq(\t))} (read_file shift, chomp=>1);],
    q[map{print}],
    q[map{s/\b(?:\w+_)?(\d+_\d(#\d+))\S*\b/($h{$1} || q{unknown}).$2/e; $_}],
    q[(read_file shift)'],
    $replacement_map_file,
    $sta_file;
  ##use critic
  push @pa_commands, join q[ ], 'plot-ampliconstats', '-page 48', $prefix;
  my $pa_command = join q[ | ], @pa_commands;
  my $ls_command = qq[! ls $input_files_glob];

  # Order of commands in the job:
  #   1. List file glob (covers all samples in a lane), which will be given
  #      to the samtools ampliconstats command; exit normally in case of an
  #      error, which can be caused by the absence of artic output (all
  #      samples in a lane failing).
  #   2. Generate ampliconstats file (lane-level).
  #   3. Using this file, generate plots both on a lane and sample level.
  $job_attrs->{'command'}  = _join_commands(q(||),
    $ls_command,
    _join_commands(q(&&), $sta_command, $pa_command));

  # Set lane flag so that we skip the next product for this lane.
  $self->_lane_counter4ampliconstats->{$position} = 1;

  return $self->create_definition($job_attrs);
}

__PACKAGE__->meta->make_immutable;

1;

__END__

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

=item File::Slurp

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
