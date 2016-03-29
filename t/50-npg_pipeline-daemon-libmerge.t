use strict;
use warnings;
use Test::More tests => 3;
use Test::Exception;
use Cwd;
use File::Path qw{ make_path };
use List::MoreUtils qw{ any };
use Log::Log4perl qw{ :easy };
use English qw{ -no_match_vars };
use IO::File;

use t::util;
use t::dbic_util;

Log::Log4perl->easy_init($INFO);

my $package = 'npg_pipeline::daemon::libmerge';
my $script_name = q[run_merge_generator.pl];
my $config_name = q[library_merge.yml];

use_ok($package);

my $util = t::util->new();
my $temp_directory = $util->temp_directory();
my $script = join q[/],  $temp_directory, $script_name;
qx(touch $script);
qx(chmod +x $script);
my $config_dir = join q[/],  $temp_directory, q[library_merge_conf];
qx(mkdir  $config_dir);
my $config_file  = join q[/],  $config_dir, $config_name;
qx(touch $config_file);
my $rd       = join q[/],  $temp_directory, q[sf46/library_merging]; 
my $rds      = join q[/],  $rd, q[study_2967_merging]; 
qx(mkdir -p $rds);
my $software = join q[/],  $temp_directory, q[lib/perl]; 
qx(mkdir -p $software);

my $fh = IO::File->new($config_file,'>');
  print $fh "- run_dir: $rd\n";
  print $fh "- software: $temp_directory\n";
  print $fh "- study_name: \\%i\\%\n";
  print $fh "- study_name: SEQCAP_Lebanon_LowCov-seq\n";
  print $fh "- study_name: SEQCAP_Lebanon_LowCov-seq\n";
  print $fh "  minimum_component_count: 12\n";
  $fh->close();


my $current_dir = getcwd();
local $ENV{PATH} = join q[:], $temp_directory, $current_dir.'/t/bin', $ENV{PATH};

my $dbic_util = t::dbic_util->new();
my $wh_schema = $dbic_util->test_schema_mlwh('t/data/fixtures/mlwh');

########test class definition start########

package test_libmerge_runner;
use Moose;
extends 'npg_pipeline::daemon::libmerge';

########test class definition end########

package main;

subtest 'retrieve configuration' => sub {
  plan tests => 5;

  my $d = npg_pipeline::daemon::libmerge->new();
  isa_ok( $d->daemon_conf(), q{HASH}, q{$} . qq{base->daemon_conf} );


  $d = npg_pipeline::daemon::libmerge->new(conf_path => $config_dir);
  my $conf = $d->library_merge_conf();
  isa_ok($conf, 'ARRAY', 'ARRAY of study configurations');
  my $first_study = $conf->[2]->{'study_name'};
  is($first_study, '\%i\%', 'study found in config');
  is($d->run_dir_prefix,$rd,'run dir from config');
  is($d->software,$temp_directory,'software from config');
};

subtest 'retrieve lims data' => sub {
 plan tests => 5;
 
  my $runner  = $package->new(
               conf_path => $config_dir,
               mlwh_schema          => $wh_schema,
               dry_run              => 1
  );

  my $study_name1  = $runner->library_merge_conf()->[2]->{'study_name'};
     $study_name1  =~ s/\\//g;

  my $expected = [619,1980];
  my (@aref)       = $runner->study_from_name($study_name1);
     is_deeply(\@aref,$expected,'Correct id_study_lims returned');
  
  my $gscript =$runner->pipeline_script_name();
     is($gscript,$script_name,'script name returned');
  
     lives_ok { $runner->run(); } 'processed o.k.';
 
  my $seen = $runner->seen();  
  is(scalar keys %$seen, '1', '1 study already seen');
  is(exists $seen->{2967}, '1', 'study 2967 already seen');
};



1;
