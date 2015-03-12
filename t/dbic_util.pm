package t::dbic_util;

use Moose;
my $DEFAULT_FIXTURE_PATH = './t/data/dbic_fixtures';

with 'npg_testing::db';

has 'fixture_path' => (
    is      => 'ro',
    isa     => 'Str',
    default => $DEFAULT_FIXTURE_PATH,
);

sub test_schema {
    my ($self) = @_;
    return $self->create_test_db(
        'npg_tracking::Schema',
        $self->fixture_path(),
        ':memory:'
    );
}

sub test_schema_mlwh {
    my ($self, $fixture_path) = @_;
    return $self->create_test_db(
        'WTSI::DNAP::Warehouse::Schema',
        $fixture_path,
        ':memory:'
    );
}

sub test_schema_wh {
    my ($self, $fixture_path) = @_;
    return $self->create_test_db(
        'npg_warehouse::Schema',
        $fixture_path,
        ':memory:'
    );
}

no Moose;
__PACKAGE__->meta->make_immutable();
1;

