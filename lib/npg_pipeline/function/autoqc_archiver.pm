package npg_pipeline::function::autoqc_archiver;

use Moose;
use namespace::autoclean;
use Readonly;

use npg_pipeline::function::definition;

extends qw{npg_pipeline::base};

our $VERSION = '0';

Readonly::Scalar my $SCRIPT_NAME => q{npg_qc_autoqc_data.pl};

sub create {
  my $self = shift;

  my $job_name = join q[_], qw/autoqc loader/,
                            $self->label(), $self->timestamp();
  my @command = ($SCRIPT_NAME);
  if ($self->has_product_rpt_list) {
    push @command, map { (q{--path}, $_->qc_out_path($self->archive_path)) }
                   @{$self->products->{'data_products'}};
  } else {
    push @command, q{--id_run}, $self->id_run();
    push @command, q{--archive_path}, $self->archive_path;
    push @command, map {  "--lane $_" }
                   map { $_->composition->get_component(0)->position}
                   @{$self->products->{'lanes'}};
  }
  my $command_string = join q{ }, @command;

  my $d = npg_pipeline::function::definition->new(
    created_by    => __PACKAGE__,
    created_on    => $self->timestamp(),
    identifier    => $self->label(),
    job_name      => $job_name,
    command       => $command_string,
    fs_slots_num  => 1,
    queue         =>
      $npg_pipeline::function::definition::LOWLOAD_QUEUE,
  );

  return [$d];
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

npg_pipeline::function::autoqc_archiver

=head1 SYNOPSIS

  my $aaq = npg_pipeline::function::autoqc_archiver->new(
    run_folder => <run_folder>,
  );

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 create

Creates and returns a single function definition as an array.
Function definition is created as a npg_pipeline::function::definition
type object.

  my $definitions = $obj->create(); 

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item namespace::autoclean

=item Readonly

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2019 Genome Research Ltd

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
