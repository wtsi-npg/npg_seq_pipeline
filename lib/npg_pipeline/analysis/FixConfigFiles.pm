package npg_pipeline::analysis::FixConfigFiles;

use Moose;
use Carp;
use English qw{-no_match_vars};
use File::Slurp;
use XML::LibXML;

extends qw{npg_pipeline::base
           npg_tracking::illumina::run::folder::validation};
with qw{npg_tracking::illumina::run::long_info};

our $VERSION = '0';

=head1 NAME

npg_pipeline::analysis::FixConfigFiles

=head1 SYNOPSIS

  use npg_pipeline::analysis::FixConfigFiles;

  my $fix_config_files = npg_pipeline::analysis::FixConfigFiles->new( {
    id_run => $iIdRun,
  } );

=head1 DESCRIPTION

Got through the config.xml files in the intensity_path and intensity_path/BaseCalls directories, and make sure that
the id_run and runfolder names are correct.

Also check that the last cycle numbers are the same, and the runfolder name is that which NPG tracking expects.
Croak on either of these.

=head1 SUBROUTINES/METHODS

=head2 run

This is the method to run. It will do as the description part to the POD describes.

=cut

sub run {
  my ( $self ) = @_;

  if ( ! $self->runfolder_name_ok() || ! $self->last_cycle_numbers_ok() ) {
    $self->info(
      q{Runfolder name OK: } . ( $self->runfolder_name_ok()     ? q{Y} : q{N} )
    );
    $self->info(
      q{Last Cycles OK: }    . ( $self->last_cycle_numbers_ok() ? q{Y} : q{N} )
    );
    $self->logcroak(q{problem with runfolder_name or last_cycle_numbers});
  };

  eval {
    $self->_correct_config_xmls();
  } or do {
    $self->logcroak(qq{Problem trying to ensure correctness of config files: $EVAL_ERROR});
  };

  return 1;
}

################
# private methods

has q{runfolder_name_ok} => (
  isa => q{Bool},
  is  => q{ro},
  lazy_build => 1,
);

sub _build_runfolder_name_ok {
  my ( $self ) = @_;
  return $self->check() ? 1 : 0;
}

has q{last_cycle_numbers_ok} => (
  isa => q{Bool},
  is  => q{ro},
  lazy_build => 1,
);

sub _build_last_cycle_numbers_ok {
  my ( $self ) = @_;

  my $reads_last_cycle_numbers = {};
  my $number_of_reads = {};

  my $intensity_run_parameters_el = ( $self->data_intensities_config_xml_object()->getElementsByTagName( q{RunParameters} ) )[0];

  my @intensity_imaging_reads = $intensity_run_parameters_el->getElementsByTagName( q{ImagingReads} );
  my @intensity_reads = $intensity_run_parameters_el->getElementsByTagName( q{Reads} );

  foreach my $read ( @intensity_imaging_reads ) {

    my $index = $read->getAttribute( q{Index} );
    $reads_last_cycle_numbers->{ $index }->{intensity_imaging_read} = ( $read->getElementsByTagName( q{LastCycle} ) )[0]->textContent();
    push @{ $number_of_reads->{intensity_imaging_read} }, $index;
  }
  foreach my $read ( @intensity_reads ) {
    my $index = $read->getAttribute( q{Index} );
    $reads_last_cycle_numbers->{ $index }->{intensity_read} = ( $read->getElementsByTagName( q{LastCycle} ) )[0]->textContent();
    push @{ $number_of_reads->{intensity_read} }, $index;
  }

  my $basecalls_run_parameters_el = ( $self->basecalls_xml()->getElementsByTagName( q{RunParameters} ) )[0];

  my @basecalls_imaging_reads = $basecalls_run_parameters_el->getElementsByTagName( q{ImagingReads} );
  my @basecalls_reads = $basecalls_run_parameters_el->getElementsByTagName( q{Reads} );

  foreach my $read ( @basecalls_imaging_reads ) {
    my $index = $read->getAttribute( q{Index} );
    $reads_last_cycle_numbers->{ $index }->{basecalls_imaging_read} = ( $read->getElementsByTagName( q{LastCycle} ) )[0]->textContent();
    push @{ $number_of_reads->{basecalls_imaging_read} }, $index;
  }
  foreach my $read ( @basecalls_reads ) {
    my $index = $read->getAttribute( q{Index} );
    $reads_last_cycle_numbers->{ $index }->{basecalls_read} = ( $read->getElementsByTagName( q{LastCycle} ) )[0]->textContent();
    push @{ $number_of_reads->{basecalls_read} }, $index;
  }

  my $basecalls_parameters_el = ( $self->basecalls_xml()->getElementsByTagName( q{BaseCallParameters} ) )[0];
  my @matrix_reads = $basecalls_parameters_el->getElementsByTagName( q{Matrix} );
  my @phasing_reads = $basecalls_parameters_el->getElementsByTagName( q{Phasing} );
  foreach my $read ( @matrix_reads ) {
    my $index = ( $read->getElementsByTagName( q{Read} ) )[0]->textContent();
    $reads_last_cycle_numbers->{ $index }->{basecalls_matrix_read} = ( $read->getElementsByTagName( q{LastCycle} ) )[0]->textContent();
    push @{ $number_of_reads->{basecalls_matrix_read} }, $index;
  }
  foreach my $read ( @phasing_reads ) {
    my $index = ( $read->getElementsByTagName( q{Read} ) )[0]->textContent();
    $reads_last_cycle_numbers->{ $index }->{basecalls_phasing_read} = ( $read->getElementsByTagName( q{LastCycle} ) )[0]->textContent();
    push @{ $number_of_reads->{basecalls_phasing_read} }, $index;
  }

  my $last_cycle_numbers_ok = 1;
  foreach my $read ( sort keys %{ $reads_last_cycle_numbers } ) {
    $self->info( qq{Last cycle numbers for read $read} );
    my $cycle_number;
    foreach my $read_type ( sort keys %{ $reads_last_cycle_numbers->{ $read } } ) {
      if ( ! $cycle_number ) {
        $cycle_number = $reads_last_cycle_numbers->{ $read }->{ $read_type };
      } else {
        if ( $cycle_number != $reads_last_cycle_numbers->{ $read }->{ $read_type } ) {
          $last_cycle_numbers_ok = 0;
        }
      }
      $self->info(qq[\t$read_type : $reads_last_cycle_numbers->{ $read }->{ $read_type } ]);
    }
  }

  # it is possible that there may be the right number for the last cycle for each read/type,
  # but actually it will be missing some types for some reads - this is just as bad
  my $read_count;
  foreach my $read_type ( sort keys %{ $number_of_reads } ) {
    if ( ! $read_count ) {
      $read_count = scalar @{ $number_of_reads->{ $read_type } };
    }
    if ( scalar @{ $number_of_reads->{ $read_type } } != $read_count ) {
      $last_cycle_numbers_ok = 0;
    }
    $self->info(join q{ }, $read_type, q{reads:}, @{ $number_of_reads->{ $read_type } });
  }

  return $last_cycle_numbers_ok;
}

has q{basecalls_xml} => (
  isa => q{XML::LibXML::Document},
  is  => q{ro},
  init_arg => undef,
  lazy_build => 1,
);

sub _build_basecalls_xml {
  my ( $self ) = @_;

  my $config_file = $self->intensity_path() . q{/BaseCalls/config.xml};

  my $c = read_file( $config_file );
  return XML::LibXML->new()->parse_string( $c );

}

sub _correct_config_xmls {
  my ( $self ) = @_;

  my $return_value;

  eval {
    $self->_copy_original_config_files();

    my @instruments = $self->data_intensities_config_xml_object()->getElementsByTagName( q{Instrument} );
    push @instruments, $self->basecalls_xml()->getElementsByTagName( q{Instrument} );
    foreach my $inst ( @instruments ) {

      if ( $inst->textContent() ne $self->instrument_string() ) {
        ( $inst->childNodes() )[0]->setData( $self->instrument_string() );
      }

    }

    my @runfolders = $self->data_intensities_config_xml_object()->getElementsByTagName( q{RunFolder} );
    push @runfolders, $self->basecalls_xml()->getElementsByTagName( q{RunFolder} );
    foreach my $rf ( @runfolders ) {

      if ( $rf->textContent() ne $self->run_folder() ) {
        ( $rf->childNodes() )[0]->setData( $self->run_folder() );
      }
    }

    my @runfolder_ids = $self->data_intensities_config_xml_object()->getElementsByTagName( q{RunFolderId} );
    push @runfolder_ids, $self->basecalls_xml()->getElementsByTagName( q{RunFolderId} );
    foreach my $r_id ( @runfolder_ids ) {

      if ( $r_id->textContent() ne $self->run_folder() ) {
        ( $r_id->childNodes() )[0]->setData( $self->id_run() );
      }

    }

    write_file ( $self->intensity_path() . q{/config.xml}, $self->data_intensities_config_xml_object()->toString() );
    write_file ( $self->intensity_path() . q{/BaseCalls/config.xml}, $self->basecalls_xml()->toString() );
    $return_value = 1;
  } or do {
    $self->logcroak($EVAL_ERROR);
  };

  return $return_value;
}

sub _copy_original_config_files {
  my ( $self ) = @_;

  my $intensity_config_filename = $self->intensity_path() . q{/config.xml};
  my $basecalls_config_filename = $self->intensity_path() . q{/BaseCalls/config.xml};
  my $output;
  if ( ! -e qq{$intensity_config_filename.ORIG} ) {
    $output = qx{cp $intensity_config_filename $intensity_config_filename.ORIG};
    if ( $CHILD_ERROR ) {
      $self->logcroak(qq{problem making copy of $intensity_config_filename: $output});
    }
  }
  if ( ! -e qq{$basecalls_config_filename.ORIG} ) {
    $output = qx{cp $basecalls_config_filename $basecalls_config_filename.ORIG};
    if ( $CHILD_ERROR ) {
      $self->logcroak(qq{problem making copy of $basecalls_config_filename: $output});
    }
  }

  return 1;
}

no Moose;
__PACKAGE__->meta->make_immutable;
1;
__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item Carp

=item English -no_match_vars

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
