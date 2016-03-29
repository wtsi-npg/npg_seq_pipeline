package npg_pipeline::daemon::libmerge;

use Moose;
use Carp;
use Readonly;
use Try::Tiny;
use List::MoreUtils qw/uniq/;

use npg_tracking::util::abs_path qw/abs_path/;

extends qw{npg_pipeline::daemon};


our $VERSION = '0';

Readonly::Scalar my $PIPELINE_SCRIPT       => q{run_merge_generator.pl};
Readonly::Scalar my $MERGE_SCRIPT          => q{library_merge.pl};
Readonly::Scalar my $PATH_DELIM            => q{:};


sub build_pipeline_script_name {
  return $PIPELINE_SCRIPT;
}

has 'library_merge_conf' => (
  isa        => q{ArrayRef},
  is         => q{ro},
  lazy_build => 1,
  metaclass  => 'NoGetopt',
  init_arg   => undef,
);
sub _build_library_merge_conf {
  my $self = shift;

  my $config = [];
  try {
    $config = $self->read_config($self->conf_file_path(q{library_merge.yml}));
  } catch {
    $self->logger->warn(qq{Failed to retrieve library merge configuration: $_});
  };

  return $config;
}


has 'run_dir_prefix' => (
  isa        => q{Str},
  is         => q{ro},
  lazy_build => 1,
  metaclass  => 'NoGetopt',
  init_arg   => undef,
);
sub _build_run_dir_prefix {
    my ($self) = @_;

    my $config =  $self->library_merge_conf();
    my $dir;
    foreach my $c (@{$config}){
     	if ($c->{'run_dir'}){ $dir = $c->{'run_dir'} }
    }

    if (!-d $dir) {
     croak qq{Directory '$dir' does not exist};
    }
    return $dir;
}

has 'software' => (
  isa        => q{Str},
  is         => q{ro},
  required   => 0,
  lazy_build => 1,
  metaclass  => 'NoGetopt',
  init_arg   => undef,
);
sub _build_software {
  my ($self) = @_;
  my $config =  $self->library_merge_conf();
  my $software;

  foreach my $c (@{$config}){
	if ($c->{'software'}){ $software = $c->{'software'} }
  }

    if ($software && !-d $software) {
    croak qq{Directory '$software' does not exist};
  }

  return $software ? abs_path($software) : q[];
}


sub study_from_name {
    my ($self,$study_name) = @_;
    if (!$study_name) {
      croak q[Need study name];
    }
    return map {$_->id_study_lims() } $self->mlwh_schema->resultset(q[Study])->search(
         { name =>{'like' => $study_name} },
    )->all();
}


sub run {
  my $self = shift;

 my $config =  $self->library_merge_conf();

 foreach my $c (@{$config}){

    next if ! $c->{'study_name'};

    my $study_name = $c->{'study_name'};
       $study_name =~ s/\\//sxmg;

    if ($study_name =~ /\%/sxm){
       $self->logger->warn(qq{Skipping $study_name as need extra checks for wild card study name query});
       next;
    }
   foreach my $study ($self->study_from_name($study_name) ){
      my $run_dir = join q[/], $self->run_dir_prefix , qq[study_${study}_library_merging];
         $run_dir = abs_path($run_dir);

    try {
       if ( $self->staging_host_match($run_dir)){
            $self->_process_one_study($study,$c,$run_dir);
      }
      else {  $self->logger->warn(qq{Study $study failed on staging_host_match $run_dir }) }
    } catch {
      $self->logger->warn(
        sprintf 'Error processing study %i: %s', $study, $_ );
    };
   }
 }
  return;
}

sub _process_one_study {
  my ($self, $study, $config, $run_dir) = @_;

  if (! -e qq{$run_dir/log}){
     my $cmd = qq{mkdir -p $run_dir/log};
     my $output = qx{$cmd};
  }

  $self->logger->info(qq{Considering study $study $run_dir});
  if ($self->seen->{$study}) {
    $self->logger->info(qq{Already seen study $study, skipping...});
    return;
   }


  my $arg_refs = {};
  $arg_refs->{'id_study_lims'} = $study;
  $arg_refs->{'generator_script'} = $self->pipeline_script_name;
  $arg_refs->{'merge_script'} = $MERGE_SCRIPT;
  $arg_refs->{'run_dir'} = $run_dir;
  $arg_refs->{'minimum_component_count'} = $config->{'minimum_component_count'};
  $arg_refs->{'dry_run'} = $self->dry_run ? 1 : 0;
  $arg_refs->{'software'} = $self->software;

  $self->run_command( $study, $self->_generate_command( $arg_refs ));

  return;
}



##########
# Remove from the PATH the bin the daemon is running from
#
sub _clean_path {
  my ($self, $path) = @_;
  my $bin = $self->local_bin;
  my @path_components  = split /$PATH_DELIM/smx, $path;
  return join $PATH_DELIM, grep { abs_path($_) ne $bin} @path_components;
}

sub _generate_command {
  my ( $self, $arg_refs ) = @_;

  my $cmd = sprintf ' %s --merge_cmd %s --use_lsf --use_irods --log_dir %s --run_dir %s',
             $arg_refs->{'generator_script'},
             $arg_refs->{'merge_script'},
             $arg_refs->{'run_dir'} . q[/log],
             $arg_refs->{'run_dir'};


    if ($arg_refs->{'minimum_component_count'}){
       $cmd .= q{ --minimum_component_count } . $arg_refs->{'minimum_component_count'};
    }
    if ($arg_refs->{'dry_run'}){
       $cmd .= q{ --dry_run };
    }
     $cmd .= q{ --id_study_lims }  . $arg_refs->{'id_study_lims'};

  my $path = join $PATH_DELIM, $self->local_path(), $ENV{'PATH'};

  my $libmerge_path_root = $arg_refs->{'software'};

  if ($libmerge_path_root) {
    $path = join $PATH_DELIM, "${libmerge_path_root}/bin", $self->_clean_path($path);
  }
  my $prefix = $self->daemon_conf()->{'command_prefix'} || q();
  $cmd = qq{export PATH=$path; $prefix$cmd};

  if ($libmerge_path_root) {
    $cmd = join q[; ],
           qq[export PERL5LIB=${libmerge_path_root}/lib/perl5],
           $cmd;
  }
  return $cmd;
}

no Moose;
__PACKAGE__->meta->make_immutable;
1;

__END__

=head1 NAME

npg_pipeline::daemon::libmerge

=head1 SYNOPSIS

  my $runner = npg_pipeline::daemon::libmerge->new();
  $runner->loop();

=head1 DESCRIPTION

Runner for the library merging pipeline.
Inherits most of functionality, including the loop() method,
from npg_pipeline::base.

=head1 SUBROUTINES/METHODS

=head2 build_pipeline_script_name

=head2 library_merge_conf

Returns an array ref of library merge configuration details.
If the configuration file is not found or is not readable,
an empty array is returned.

=head2 run_dir_prefix

Taken from config file

=head2 software

Taken from config file. Optional.

=head2 study_from_name

Returns array of id_study_lims

=head2 run

Invokes the library merging generator script for studies
specified in the library_merge.yml config file

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item Try::Tiny

=item Readonly

=item Carp

=item List::MoreUtils

=item npg_tracking::util::abs_path

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Jillian Durham

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2016 Genome Research Ltd.

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
