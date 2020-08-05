package npg_pipeline::function::stage2pp;

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;
use Readonly;
use Carp;
use Try::Tiny;
use File::Spec::Functions;

use npg_pipeline::function::definition;
use npg_pipeline::cache::reference;
use npg_pipeline::runfolder_scaffold;

extends 'npg_pipeline::base';
with qw{ npg_pipeline::function::util
         npg_pipeline::product::release 
         npg_pipeline::product::release::portable_pipeline };
with 'npg_common::roles::software_location' =>
  { tools => [qw/
                  nextflow
                  npg_simple_robo4artic
                  npg_autoqc_generic4artic
                /] };

Readonly::Scalar my $FUNCTION_NAME => q[stage2pp];
Readonly::Scalar my $MEMORY        => q[5000]; # memory in megabytes
Readonly::Scalar my $CPUS          => 4;

our $VERSION = '0';

=head2 nextflow_cmd

=head2 npg_simple_robo4artic_cmd

=head2 npg_autoqc_generic4artic_cmd

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
      $pps = $self->pps_config4product($product, $FUNCTION_NAME);
    } catch {
      $self->logcroak($_);
    };

    foreach my $pp (@{$pps}) {
      my $pp_name = $self->pp_name($pp);
      my $method = $self->canonical_name($pp_name);
      $method = join q[_], q[], $method, q[create];
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
    (@definitions == @{$self->_output_dirs}) or $self->logcroak(
      sprintf 'Number of definitions %i and output directories %i do not match',
      scalar @definitions, scalar @{$self->_output_dirs}
    );
    # Create directories for all expected outputs.
    npg_pipeline::runfolder_scaffold->make_dir(@{$self->_output_dirs});
  } else {
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

has '_output_dirs' => (
  isa      =>' ArrayRef',
  is       => 'ro',
  required => 0,
  default  => sub { return []; },
);

has '_names_map' => (
  isa      =>' HashRef',
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

sub _ncov2019_artic_nf_create {
  my ($self, $product, $pp) = @_;

  my $pp_version   = $self->pp_version($pp);
  my $in_dir_path  = $product->stage1_out_path($self->no_archive_path());
  my $qc_out_path  = $product->qc_out_path($self->archive_path());
  my $out_dir_path = $self->pp_archive4product($product, $pp, $self->pp_archive_path());
  push @{$self->_output_dirs}, $out_dir_path;

  # Figure out a path to the JSON file with tag metrics results for
  # a lane this product belongs to. 
  my @lane_products = $product->lanes_as_products();
  my $tm_qc_out_path;
  if (@lane_products == 1) {
    $tm_qc_out_path = catfile(
      $lane_products[0]->qc_out_path($self->archive_path()),
      $lane_products[0]->file_name(ext => q[tag_metrics.json]));
  } else {
    $self->warn(
      'Multiple parent lanes for a product, not giving tag metrics path');
  }

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

  my %job_attrs = ('created_by'  => __PACKAGE__,
                   'created_on'  => $self->timestamp(),
                   'identifier'  => $self->label,
                   'num_cpus'    => [$CPUS],
                   'memory'      => $MEMORY,
                   'composition' => $product->composition(), );

  # Run artic
  # And yes, it's -profile, not --profile!
  my $command = join q[ ], $self->nextflow_cmd(), 'run', $self->pp_deployment_dir($pp),
                           '-profile singularity,sanger',
                           '--illumina --cram --prefix ' . $self->label,
                           "--ref $ref_path",
                           "--bed $bed_file",
                           "--directory $in_dir_path",
                           "--outdir $out_dir_path";
  my @commands = ($command);

  my $artic_qc_summary = catfile($out_dir_path, $self->label . '.qc.csv');

  # Check that the artic QC summary exists, fail early if not.
  $command = qq{ ([ -f $artic_qc_summary ] && echo 'Found $artic_qc_summary')} .
             qq{ || (echo 'Not found $artic_qc_summary' && /bin/false) };
  push @commands, $command;

  # Use the summary to create the autoqc review result.
  # The result will not necessary be created, but this would not be an error.
  # The npg_simple_robo4artic will exit early with success exit code if the
  # summary is empty, which can happen in case of zero input reads.

  my $in = join q[ ], 'cat', $artic_qc_summary, q[|];
  $command = join q[ ], $in, $self->npg_simple_robo4artic_cmd(), $qc_out_path;
  push @commands, $command;

  # Use the summary to create the autoqc generic result.
  $command = join q[ ], $in, $self->npg_autoqc_generic4artic_cmd(),
                             q[--qc_out], $qc_out_path;
  if ($tm_qc_out_path) {
    $command = join q[ ], $command,
                          q[--rpt_list], $product->composition->freeze2rpt,
                          q[--tm_json_file], $tm_qc_out_path;
  }
  if ($pp_version) {
    $command = join q[ ], $command, q[--pp_version], $pp_version;
  }
  push @commands, $command;

  $command = join q[ && ], map { q[(] . $_ . q[)] } @commands;

  $job_attrs{'command'}  = $command;
  $job_attrs{'job_name'} = join q[_], $FUNCTION_NAME, $self->pp_short_id($pp), $self->label();

  return npg_pipeline::function::definition->new(\%job_attrs);
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
