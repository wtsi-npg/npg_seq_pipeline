package npg_pipeline::archive::file::generation;

use Moose;
use Moose::Meta::Class;
use Carp;
use English qw{-no_match_vars};
use File::Spec;

use npg_common::roles::run::lane::file_names;
use npg_tracking::glossary::tag;
use npg_tracking::glossary::lane;
use npg_tracking::glossary::run;

extends q{npg_pipeline::base};

our $VERSION = '0';

=head1 NAME

npg_pipeline::archive::file::generation

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 create_empty_fastq_files

=cut

sub create_empty_fastq_files {
   my ( $self, $arg_refs ) = @_;

   my $ext = q[fastq];
   my $forward_end = ($self->is_paired_read() || $self->is_indexed()) ? 1 : undef;
    my @files = ();

   my $apath = $self->archive_path;

   foreach my $position ( $self->positions() ) {
     my $generator = Moose::Meta::Class->create_anon_class(
          roles => [qw/npg_common::roles::run::lane::file_names
                       npg_tracking::glossary::tag
                       npg_tracking::glossary::lane
                       npg_tracking::glossary::run/])->new_object(
          {id_run => $self->id_run, position => $position,});
     push @files, File::Spec->catfile($apath,$generator->create_filename($ext, $forward_end));
     if ($self->is_paired_read()) {
       push @files, File::Spec->catfile($apath,$generator->create_filename($ext, 2));
     }
     if ($self->is_indexed() && $self->is_multiplexed_lane($position)) {
       push @files, File::Spec->catfile($apath,$generator->create_filename($ext, q[t]));
       my $lpath = $self->lane_archive_path($position);
       foreach my $tag_index ( @{ $self->get_tag_index_list( $position ) } ) {
         my $pgenerator = Moose::Meta::Class->create_anon_class(
            roles => [qw/npg_common::roles::run::lane::file_names
                         npg_tracking::glossary::tag
                         npg_tracking::glossary::lane
                         npg_tracking::glossary::run/])->new_object(
            {id_run => $self->id_run, position => $position, tag_index => $tag_index,});
         push @files, File::Spec->catfile($lpath,$pgenerator->create_filename($ext, $forward_end));
         if ($self->is_paired_read()) {
           push @files, File::Spec->catfile($lpath,$pgenerator->create_filename($ext, 2));
	 }
       }
     }
   }
   foreach my $file (@files) {
     system "touch $file";
     my $fastqcheck = $file . q[check];
     system "echo '0 sequences, 0 total length' > $fastqcheck";
   }
   return ();
}

no Moose;
__PACKAGE__->meta->make_immutable;
1;
__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Carp

=item English -no_match_vars

=item Moose

=item Moose::Meta::Class

=item File::Spec

=item npg_common::roles::run::lane::file_names

=item npg_tracking::glossary::tag

=item npg_tracking::glossary::lane

=item npg_tracking::glossary::run

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Andy Brown

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2014 Genome Research Ltd

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
