package t::dbic_util;

use Moose;
use File::Temp qw{ tempdir };
use Test::More;
my $DEFAULT_FIXTURE_PATH = './t/data/dbic_fixtures';

with 'npg_testing::db';

has fixture_path => (
    is      => 'ro',
    isa     => 'Str',
    default => $DEFAULT_FIXTURE_PATH,
);

has db_file_name => (
    is      => 'ro',
    isa     => 'Maybe[Str]',
    lazy_build => 1,
);

sub _build_db_file_name {
  my ( $self ) = @_;
  
  my $db_file_name = tempdir(
    DIR => q{/tmp},
    CLEANUP => 1,
  ) . q{/npg_tracking_dbic};

  note $db_file_name;
  return $db_file_name;
}

sub test_schema {
    my ($self) = @_;

    my $schema = $self->create_test_db(
                    'npg_tracking::Schema',
                    $self->fixture_path(),
                    $self->db_file_name()
    );

    return $schema;
}


no Moose;
__PACKAGE__->meta->make_immutable();
1;

