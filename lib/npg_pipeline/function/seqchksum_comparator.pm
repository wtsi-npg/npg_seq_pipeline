package npg_pipeline::function::seqchksum_comparator;

use Moose;
use namespace::autoclean;
use File::Spec;
use File::Slurp;
use Readonly;
use Cwd;

use npg_pipeline::function::definition;

extends q{npg_pipeline::base_resource};

our $VERSION = '0';

Readonly::Scalar my $SEQCHKSUM_SCRIPT => q{npg_pipeline_seqchksum_comparator};

=head1 NAME

npg_pipeline::function::seqchksum_comparator

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=cut

=head2 create

Creates and returns a per-run function definition in an array.
The function definition is created as a npg_pipeline::function::definition
type object.

=cut

has 'input_globs' => ( isa        => 'ArrayRef',
                     is         => 'ro',
                     required   => 0,
                     default  => sub {return [];}, # is sub necessary?
                   );

has 'input_fofg_name' => ( isa        => 'Str',
                         is         => 'ro',
                         required   => 0,
                         lazy_build => 1,
                       );
sub _build_input_fofg_name {
  my ( $self ) = @_;

  return sprintf q{%s/%s_input_fofn.txt}, $self->recalibrated_path, $self->id_run;
}

sub create {
  my $self = shift;

  my $job_name = join q{_}, 'seqchksum_comparator', $self->id_run(), $self->timestamp();
  my $command = $SEQCHKSUM_SCRIPT;
  $command .= q{ --id_run=} . $self->id_run();
  $command .= q[ ];
  $command .= join q[ ], (map { qq{--lanes=$_} } ($self->positions));
  $command .= q{ --archive_path=} . $self->archive_path();
  $command .= q{ --bam_basecall_path=} . $self->bam_basecall_path();
  if ($self->verbose() ) {
    $command .= q{ --verbose};
  }

  if($self->input_fofg_name) {
    push my @input_fgs, (map { sprintf q{%s/%s*.cram}, $_->path($self->archive_path), $_->file_name_root } @{$self->products->{data_products}});
    write_file($self->input_fofg_name, (map { "$_\n" } @input_fgs));
    $command .= q{ --input_fofg_name=} . $self->input_fofg_name;
  }
  else {
    for my $dp (@{$self->products->{data_products}}) {
      $command .= sprintf q{ --input_globs=%s/%s*.cram}, $dp->path($self->archive_path), $dp->file_name_root;
    }
  }

  my @definitions = ();

  push @definitions, npg_pipeline::function::definition->new(
    created_by   => __PACKAGE__,
    created_on   => $self->timestamp(),
    identifier   => $self->id_run(),
    job_name     => $job_name,
    command      => $command,
  );

  return \@definitions;
}

=head2 do_comparison

Merge all product seqchksum files together, merge all lane checksum files (from stage1),
and compare the results. Use diff -u rather than cmp and store the files on disk to help
work out what has gone wrong.

=cut

sub do_comparison {
  my ($self) = @_;

  my $wd = getcwd();
  $self->info('Changing to archive directory ', $self->archive_path());
  chdir $self->archive_path() or $self->logcroak('Failed to change directory');

  my $prods_seqchksum_file_name = $self->id_run . '.allprods.seqchksum';
  my $lanes_seqchksum_file_name = $self->id_run . '.alllanes.seqchksum';

  ##############################
  # merge all product seqchksums
  ##############################
  my $cmd = 'seqchksum_merge.pl ';
  my @seqchksums = ();
  # specify product input files via EITHER a fofn (of globs) OR a list of globs in the input_globs attribute
  if($self->input_fofg_name) {
     $cmd .= q[-f ] . $self->input_fofg_name;
  } else {
    my @input_globs = @{$self->input_globs};

    for my $cram_file_name_glob (@input_globs) {
      my @crams = glob $cram_file_name_glob or $self->logcroak("Cannot find any cram files using $cram_file_name_glob");

      ## no critic (RegularExpressions::RequireDotMatchAnything)
      ## no critic (RegularExpressions::RequireExtendedFormatting)
      ## no critic (RegularExpressions::RequireLineBoundaryMatching)
      @seqchksums = map{s/[.]cram$/.seqchksum/r} @crams;
      $self->info("Building .all.seqchksum for product from seqchksum set based on $cram_file_name_glob ...");

      $cmd .= q{ } . join q{ }, @seqchksums;
    }
  }

  $cmd .= qq{ > $prods_seqchksum_file_name};
  $self->info("Running $cmd to generate prods_seqchksum_file_name");
  system(qq[/bin/bash -c "set -o pipefail && $cmd"]) == 0 or $self->logcroak(
    "Failed to run command $cmd");

  #######################################
  # merge all lanes seqchksums (from i2b)
  #######################################
  $cmd = 'seqchksum_merge.pl ';
  @seqchksums = ();
  for my $position ($self->positions) {
    my $fn = $self->id_run . '_' . $position . '.post_i2b.seqchksum';
    my $ffn = File::Spec->catfile($self->bam_basecall_path, $fn);
    push @seqchksums, $ffn;
  }
  $cmd .= q{ } . join q{ }, @seqchksums;
  $cmd .= qq{ > $lanes_seqchksum_file_name};
  $self->info("Running $cmd to generate lanes_seqchksum_file_name");
  system(qq[/bin/bash -c "set -o pipefail && $cmd"]) == 0 or $self->logcroak(
    "Failed to run command $cmd");

  my $compare_cmd = q{diff -u <(grep '.all' } . $prods_seqchksum_file_name . q{ | sort) <(grep '.all' } . $lanes_seqchksum_file_name . q{ | sort)};
  $self->info($compare_cmd);

  my $ret = system qq[/bin/bash -c "$compare_cmd"];
  my $e = qq(seqchksum for post_i2b and product are different, command run "$compare_cmd");
  $ret == 0 or $self->error($e);
  chdir $wd or $self->logcroak("Failed to change back to $wd");
  $ret == 0 or $self->logcroak($e);

  return;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item namespace::autoclean

=item Readonly

=item File::Spec

=item Cwd

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Kate Taylor

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2018 Genome Research Ltd

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

