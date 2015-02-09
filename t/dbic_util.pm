package t::dbic_util;

use Moose;
use File::Temp qw{ tempdir };
use Test::More;
my $DEFAULT_FIXTURE_PATH = './t/data/dbic_fixtures';

with 'npg_testing::db';

has 'fixture_path' => (
    is      => 'ro',
    isa     => 'Str',
    default => $DEFAULT_FIXTURE_PATH,
);

has '_db_temp_dir' => (
    is      => 'ro',
    isa     => 'Str',
    default => sub { tempdir(CLEANUP => 1) },
);

sub _db_file_name {
    my ( $self, $name ) = @_;
    my $db_file_name = join q[/], $self->_db_temp_dir, $name;
    note $db_file_name;
    return $db_file_name;
}

sub test_schema {
    my ($self) = @_;
    return $self->create_test_db(
        'npg_tracking::Schema',
        $self->fixture_path(),
        $self->_db_file_name('npg_tracking')
    );
}

sub test_schema_mlwh {
    my ($self, $fixture_path) = @_;
    return $self->create_test_db(
        'WTSI::DNAP::Warehouse::Schema',
        $fixture_path,
        $self->_db_file_name('mlwh')
    );
}

no Moose;
__PACKAGE__->meta->make_immutable();
1;

