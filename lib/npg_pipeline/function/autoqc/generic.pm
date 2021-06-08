package npg_pipeline::function::autoqc::generic;

use Moose;
use MooseX::StrictConstructor;
use namespace::autoclean;
use Readonly;
use File::Spec::Functions qw{catdir catfile};
use Try::Tiny;

use npg_pipeline::function::stage2pp;

extends q{npg_pipeline::base_resource};
with qw{ npg_pipeline::function::util
         npg_pipeline::product::release
         npg_pipeline::product::release::portable_pipeline };
with 'npg_common::roles::software_location' =>
         { tools => [qw/ qc /] };

our $VERSION = '0';

Readonly::Scalar my $AUTOQC_CHECK_NAME => q{generic};

=head1 NAME

npg_pipeline::function::autoqc::generic

=head1 SYNOPSIS

  my $ag = npg_pipeline::function::autoqc::generic->new(
             runfolder_path         => 'my_path',
             label                  => 'my_run',
             spec                   => 'artic',
             portable_pipeline_name => 'ncov2019-artic-nf');
  my @function_definitions = $ag->create();

=head1 DESCRIPTION

This class contains callbacks for pipeline functions that invoke
autoqc generic check. The callbacks are mapped to the create method
of this class. The way these functions are scheduled is determined
by the function graph.

Tag zero and spiked controls are normally not considered. The autoqc
check scan be invoked on any appropriate entity - a product or a pool.

=head1 SUBROUTINES/METHODS

=head2 spec

Generic check's spec, required attribute.

=cut

has 'spec' => (
  isa        => 'Str',
  is         => 'ro',
  required   => 1,
);

=head2 portable_pipeline_name

The name of the portable pipeline which generated input data for the qc check,
required attribute. Multiple portable pipeline can be associated with a product,
this attribute helps to identify the right one.

=cut

has 'portable_pipeline_name' => (
  isa        => 'Str',
  is         => 'ro',
  required   => 0,
);

=head2 create

The qc job definitions are created if the product configuration file contains
a configuration for a portable pipeline given by the prtable_pipeline_name
attribute and this configuration defined the input glob for qc. Error if
multiple portable pipeline satisfy these conditions.

=cut

sub create {
  my $self = shift;

  $self->info('Generating definition for autoqc generic check for ' .
              $self->spec);
  my @products = grep { $self->is_release_data($_) }
                 @{$self->products->{data_products}};
  my @definitions = ();

  for my $p (@products) {

    my $pps;
    try {
      $pps = $self->pps_config4product($p);
    } catch {
      $self->logcroak($_);
    };

    my @pipelines = grep { $self->pp_autoqc_flag($_) }
                    grep { $self->pp_name($_) eq $self->portable_pipeline_name }
                    @{$pps};
    @pipelines or next;
    (@pipelines == 1) or $self->logcroak(
      'Multiple pipelines for ' . $self->portable_pipeline_name);

    my $method = join q[_], q[], q[create], $self->spec;
    if ($self->can($method)) {
      push @definitions, $self->$method($p, $pipelines[0]);
    } else {
      $self->error(sprintf 'autoqc for the "%s" portable pipeline ' .
       'is not implemented, method %s is not available',
        $self->portable_pipeline_name, $method);
    }
  }
  @definitions = grep { $_ } @definitions;

  if (!@definitions) {
    push @definitions, $self->create_excluded_definition();
  }

  return \@definitions;
}

has '_lane_counter' => (
  isa      =>' HashRef',
  is       => 'ro',
  required => 0,
  default  => sub { return {}; },
);

sub _create_artic {
  my ($self, $product, $pp) = @_;

  my $args_generator = sub {
    my $lane_p = shift;

    my $input_file_glob = $self->pp_qc_summary($pp);
    $input_file_glob or $self->logcroak('QC input glob is not defined in ' .
      'pp config for ' . $self->portable_pipeline_name);
    # Our input is sample-level artic QC summaries.
    my $input_dir_glob =
      $self->pp_archive4product($product, $pp, $self->pp_archive_path());
    $input_dir_glob =~ s{/plex\d+/}{/plex*/}smx; # glob over all samples

    return (
      '--tm_json_file',
      $lane_p->file_path($lane_p->qc_out_path(
        $self->archive_path()), ext => q[tag_metrics.json]),
      '--input_files_glob',
      catfile($input_dir_glob, $input_file_glob),
      '--sample_qc_out',
      catdir($lane_p->path($self->archive_path()), 'plex*/qc'),
           );
  };

  return $self->_create_lane_level_definition($product, $pp, $args_generator);
}

sub _create_ampliconstats {
  my ($self, $product, $pp) = @_;

  my $args_generator = sub {
    my $lane_p = shift;

    my @args =
      map { 'FPCOV-' . $_ }
      @{npg_pipeline::function::stage2pp->astats_min_depth_array($pp)};
    unshift @args, 'FREADS';
    @args = map { ('--ampstats_section', $_) } @args;

    push @args, (
      '--input_files',
      $lane_p->file_path($self->pp_archive4product(
        $lane_p, $pp, $self->pp_archive_path()), ext => q[astats]),
      '--qc_out',
      $lane_p->qc_out_path($self->archive_path()),
      '--sample_qc_out',
      catdir($lane_p->path($self->archive_path()), 'plex*/qc'),
                );

    return @args;
  };

  return $self->_create_lane_level_definition($product, $pp, $args_generator);
}

sub _create_lane_level_definition {
  my ($self, $product, $pp, $args_generator) = @_;

  if ($product->composition->num_components > 1) {
    # Not dealing with merges
    $self->warn('One-component compositions only for ' . $self->spec);
    return;
  }
  # Have we dealt with this lane already?
  my $position = $product->composition->get_component(0)->position;
  if ($self->_lane_counter->{$position}) { # Yes
    return;
  }

  $self->_lane_counter->{$position} = 1;

  my $lane_p = ($product->lanes_as_products)[0];
  my @args = map { m{\A--}xms ? $_ : q['] . $_ . q['] }
             $args_generator->($lane_p);
  my $ref = {};
  $ref->{'composition'} = $lane_p->composition();
  $ref->{'job_name'}    = $self->_job_name('artic');
  $ref->{'command'} = $self->_command($lane_p->rpt_list, $pp, @args);

  return $self->create_definition($ref);
}

sub _command {
  my ($self, $rpt_list, $pp, @args) = @_;

  my $quote_me = sub { q['] . shift . q[']};

  return join q{ }, $self->qc_cmd,
    '--check', $AUTOQC_CHECK_NAME,
    '--spec', $self->spec,
    '--rpt_list', $rpt_list,
    '--pp_name', $quote_me->($self->portable_pipeline_name),
    '--pp_version', $quote_me->($self->pp_version($pp)),
    @args;
}

sub _job_name {
  my ($self) = @_;
  return join q{_}, 'qc', 'generic', $self->spec,
                    $self->label(), $self->timestamp();
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

=item Readonly

=item File::Spec::Functions

=item Try::Tiny

=item npg_common::roles::software_location

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
