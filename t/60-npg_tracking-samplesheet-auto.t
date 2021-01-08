use strict;
use warnings;
use Test::More tests => 9;
use Test::Exception;
use File::Temp qw/ tempdir /;

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

{
  is(npg::samplesheet::auto::_id_run_from_samplesheet('t/data/samplesheet/miseq_default.csv'),
      10262, 'id run retrieved from a samplesheet');
  lives_and { is npg::samplesheet::auto::_id_run_from_samplesheet('some_file'), undef}
      'undef reftuned for a non-exisitng samplesheet';
}

{
  my $dir = tempdir(UNLINK => 1);
  my $file = join q[/], $dir, 'myfile';
  `touch $file`;
  npg::samplesheet::auto::_move_samplesheet($file);
  ok(!-e $file, 'original file does not exist');
  ok(-e $file.'_invalid', 'file has been moved');

  my $sdir = join q[/],  $dir, 'samplesheet';
  mkdir $sdir;
  mkdir $sdir . '_old';
  $file = join q[/], $sdir, 'myfile';
  `touch $file`;
  my $new_file = join q[/], $sdir . '_old', 'myfile_invalid';
  npg::samplesheet::auto::_move_samplesheet($file);
  ok(!-e $file, 'original file does not exist');
  ok(-e $new_file, 'moved file is in samplesheet_old directory');
}


1;