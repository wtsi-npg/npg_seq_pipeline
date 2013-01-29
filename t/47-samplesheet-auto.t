use strict;
use warnings;
use Test::More tests => 3;
use Test::Exception;

use t::dbic_util;
local $ENV{dev} = q(wibble); # ensure we're not going live anywhere

use_ok('npg::samplesheet::auto');

my $schema = t::dbic_util->new->test_schema();
local $ENV{NPG_WEBSERVICE_CACHE_DIR} = q(t/data/samplesheet);

{
  my $sm;
  lives_ok { $sm = npg::samplesheet::auto->new(npg_tracking_schema=>$schema); } 'miseq monitor object';
  isa_ok($sm, 'npg::samplesheet::auto');
}


1;