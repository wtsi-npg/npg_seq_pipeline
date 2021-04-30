package npg_pipeline::product::heron::upload::climb2mlwh;

use Moose;
use namespace::autoclean;
use MooseX::StrictConstructor;
use English qw(-no_match_vars);
use Carp;
use Readonly;
use Log::Log4perl qw(:easy);
use Try::Tiny;
use DateTime;
use WTSI::DNAP::Warehouse::Schema;
use npg_tracking::glossary::composition::factory::rpt_list;

with qw{MooseX::Getopt};

our $VERSION = '0';

Readonly::Scalar my $REMOTE_COMMAND        => q[wsi-npg-ssh-restricted seqdatalist];
Readonly::Scalar my $SHIFT_EIGHT           => 8;

has 'verbose'  => (
    isa     => q{Bool},
    is      => q{ro},
);

has 'dry_run'  => (
    isa     => q{Bool},
    is      => q{ro},
);

has 'host'  => (
    isa     => q{Str},
    is      => q{ro},
    required    => 1,
);

has 'run_folder'  => (
    isa     => q{Str},
    is      => q{ro},
    required    => 1,
);

has 'user'  => (
    isa     => q{Str},
    is      => q{ro},
    required    => 1,
);

has 'pkey_file'  => (
    isa     => q{Str},
    is      => q{ro},
    required    => 1,
);

has '_schema'    => (
    isa     => q{DBIx::Class::Schema},
    is      => q{ro},
    lazy_build => 1,
);

sub _build__schema {
  my $self = shift;
  my $schema=WTSI::DNAP::Warehouse::Schema->connect();
  return $schema;
}

=head2 run

Run the update

=cut

sub run {
    my $self = shift;

    my $logger = Log::Log4perl->get_logger();
    $logger->level($self->verbose ? $DEBUG : $INFO);

    $self->dry_run and $logger->warn('DRY RUN: not updating database');

    if (not -f $self->pkey_file) {
        $logger->logcroak("Private key file $self->pkey_file does not exist");
    }

    my $remote = join q[@], $self->user, $self->host;
    my @command = ('ssh', '-i', $self->pkey_file, $remote, $REMOTE_COMMAND, $self->run_folder);

    my $e;

    try {
        $self->_update_mlwh($logger, \@command);
    } catch {
        $e = $_;
        $logger->error("Error updating warehouse: $e");
    };

    $logger->info('Exiting');

    return ($e ? 1 : 0);
}


############################ Private functions ##############################

sub _update_mlwh {
  my ($self, $logger, $cmd) = @_;

  my $rs = $self->_schema->resultset(q(IseqHeronProductMetric));

  my $command = join q[ ], @{$cmd};
  $logger->info(qq[Will execute '$command']);

  open my $fh, q[-|], @{$cmd} or $logger->logcroak(qq[Failed to open a handle for '$command']);
  my $samples = {};
  while (my $line= <$fh>) {
    my ($d, $p) = split /\t/smx, $line;
    if (!($d and $p)) { next; }
    $logger->debug(">>> $line");
    my($tp,$s,$rpt)=$p=~m{([^/]+/([^/]+)/(\d+_\d\#\d+))[.]}smx;
    $rpt=~s{[_#]}{:}smxg;
    my $c=npg_tracking::glossary::composition::factory::rpt_list->new(rpt_list=>$rpt)->create_composition();
    if ($self->dry_run) {
      my $msg = q[IseqHeronProductMetric would be updated with: id_iseq_product=>] . $c->digest() .
            q[ path_root=>] . $tp .
            q[ climb_upload=>] . DateTime->from_epoch( epoch => $d) .
            q[ supplier_sample_name=>] . $s;
      $logger->info($msg);
    } else {
      $rs->update_or_create({id_iseq_product=>$c->digest(), path_root=>$tp, climb_upload=>DateTime->from_epoch( epoch => $d), supplier_sample_name=>$s});
    }
  };
  close $fh or $logger->logcroak(qq[Failed to close a handle for '$command']);
  my $child_error = $CHILD_ERROR >> $SHIFT_EIGHT;
  $child_error and $logger->logcroak(qq[Error executing '$command': $child_error]);
  return;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

npg_pipeline::product::heron::upload::climb2mlwh

=head1 VERSION

=head1 CONFIGURATION

=head1 USAGE

npg_climb2mlwh --host climb.com --user me --pkey_file ~/.ssh/climb.pem \
               --run_folder 201225_B12345_666_ABCTKDRXX [--dry_run] [--verbose]

npg_climb2mlwh --help

=head1 SYNOPSIS

This perl script is designed to connect to a remote server and return a list
of files for a particular run folder. It will then parse information from
the filenames and use that information to update the ML warehouse database.

=head1 DESCRIPTION

=head1 OPTIONS

=over

=item --host

The remote host to connect to.

=item --user

The user to connect to the remote host as.

=item --pkey_file

=item --run_folder

=item --dry_run - off by default

=item --verbose - off by default

=item --help

=back

=head1 SUBROUTINES/METHODS

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item strict

=item warnings

=item FindBin

=item lib

=item English

=item Carp

=item Readonly

=item Getopt::Long

=item Pod::Usage

=item Log::Log4perl

=item Try::Tiny

=item DateTime

=item WTSI::DNAP::Warehouse::Schema

=item npg_tracking::glossary::composition::factory::rpt_list;

=back

=head1 INCOMPATIBILITIES

None known

=head1 EXIT STATUS

0 on success, 2 on error in scripts' arguments, 1 on any other error

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Jennifer Liddle E<lt>js10@sanger.ac.ukE<gt>

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2020 Genome Research Ltd.

This file is part of NPG.

NPG is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

=cut
