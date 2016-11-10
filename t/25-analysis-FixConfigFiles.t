use strict;
use warnings;
use Test::More tests => 11;
use Test::Exception;
use File::Slurp;
use File::Temp qw{ tempdir };
use Log::Log4perl qw(:levels);

use t::dbic_util;

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], tempdir(CLEANUP => 1), 'logfile'),
                          utf8   => 1});

my $schema = t::dbic_util->new()->test_schema;

use_ok( q{npg_pipeline::analysis::FixConfigFiles} );

sub _hiseq_runfolder_path {
  my $mismatched_cycles = shift;
  my $runfolder_path = tempdir(CLEANUP => 1) .
         q{/nfs/sf45/IL2/analysis/123456_IL2_1234_A_80B5TABXX};
  my $intensity_path = $runfolder_path . q{/Data/Intensities};
  my $basecall_path = $intensity_path . q{/BaseCalls};

  qx{mkdir -p $basecall_path};

  qx{cp t/data/fix_configs/data_intensities_config.xml $intensity_path/config.xml};
  if ( $mismatched_cycles ) {
    qx{cp t/data/fix_configs/data_intensities_basecalls_diff_last_cycle_config.xml $basecall_path/config.xml};
  } else {
    qx{cp t/data/fix_configs/data_intensities_basecalls_config.xml $basecall_path/config.xml};
  }
  return $runfolder_path;
}

my $runfolder_path = _hiseq_runfolder_path();
my $intensity_path = $runfolder_path . q{/Data/Intensities};

{
  my $fix_config_files;
  lives_ok {
    $fix_config_files = npg_pipeline::analysis::FixConfigFiles->new(
      id_run                => 1234,
      npg_tracking_schema   => $schema,
      runfolder_name_ok     => 0,
      last_cycle_numbers_ok => 1,
    );
  } q{new object created ok};

  isa_ok( $fix_config_files, q{npg_pipeline::analysis::FixConfigFiles}, q{$fix_config_files} );

  throws_ok {
    $fix_config_files->run();
  } qr{problem[ ]with[ ]runfolder_name[ ]or[ ]last_cycle_numbers}, q{runfolder_name is not ok, throws error};

  $fix_config_files = npg_pipeline::analysis::FixConfigFiles->new(
    id_run                => 1234,
    npg_tracking_schema   => $schema,
    runfolder_name_ok     => 1,
    last_cycle_numbers_ok => 0,
  );
  throws_ok {
    $fix_config_files->run();
  } qr{problem[ ]with[ ]runfolder_name[ ]or[ ]last_cycle_numbers}, q{last_cycle_numbers are not ok, throws error};

  $fix_config_files = npg_pipeline::analysis::FixConfigFiles->new(
    id_run                => 1234,
    npg_tracking_schema   => $schema,
    runfolder_path        => $runfolder_path,
    last_cycle_numbers_ok => 1,
    intensity_path        => $intensity_path,
  );
  lives_ok {
    $fix_config_files->basecalls_xml();
  } q{BaseCalls/config.xml read in ok};

  lives_ok { $fix_config_files->run(); } q{run ok};

  my $changed_intensities_file = read_file ( $intensity_path . q{/config.xml} );
  my $changed_basecalls_file   = read_file ( $intensity_path . q{/BaseCalls/config.xml} );

  my $expected_intensities_file = read_file ( q{t/data/fix_configs/fixed_intensities_config.xml} );
  my $expected_basecalls_file   = read_file ( q{t/data/fix_configs/fixed_basecalls_config.xml} );

  is( $changed_intensities_file, $expected_intensities_file, q{intensities file has been changed correctly} );
  is( $changed_basecalls_file,   $expected_basecalls_file,   q{basecalls file has been changed correctly} );
}

{
  my $fix_config_files = npg_pipeline::analysis::FixConfigFiles->new(
    id_run                => 1234,
    npg_tracking_schema   => $schema,
    runfolder_path        => $runfolder_path,
    intensity_path        => $intensity_path,
  );

  ok( $fix_config_files->last_cycle_numbers_ok(), q{All last cycle numbers are the same} );

  $runfolder_path = _hiseq_runfolder_path(1);
  $intensity_path = $runfolder_path . q{/Data/Intensities};
  $fix_config_files = npg_pipeline::analysis::FixConfigFiles->new(
    id_run                => 1234,
    npg_tracking_schema   => $schema,
    runfolder_path        => $runfolder_path,
    intensity_path        => $intensity_path,
  );

  ok( ! $fix_config_files->last_cycle_numbers_ok(), q{Not all last cycle numbers are the same} );
}

1;
