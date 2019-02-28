use strict;
use warnings;
use Test::More tests => 2;
use Test::Exception;

use_ok('npg_pipeline::validation');

subtest 'create object' => sub {
  plan tests => 1;

  my $v = npg_pipeline::validation->new();
  isa_ok($v, 'npg_pipeline::validation');
};

1;