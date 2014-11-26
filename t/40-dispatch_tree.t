# $Id: 40-dispatch_tree.t 16979 2013-04-08 10:03:18Z mg8 $
use strict;
use warnings;
use Test::More tests => 7;
use Test::Exception;
use JSON;

local $ENV{PATH} = join q[:], q[t/bin], q[t/bin/software/solexa/bin], $ENV{PATH};

use_ok('npg_pipeline::dispatch_tree');

{
  my $tree;
  lives_ok { $tree = npg_pipeline::dispatch_tree->new(); } q{tree object created ok};
  isa_ok($tree, q{npg_pipeline::dispatch_tree}, q{$tree});

  my $test_structure = {};
  $test_structure->{functions} = [];
  my $function_info_one = {
    function          => q{function_one},
    job_ids_launched  => [1,2,3],
    job_dependencies  => q{},
  };
  push @{$test_structure->{functions}}, $function_info_one;
  $tree->append_to_functions($function_info_one);
  is_deeply($tree->_data_structure(), $test_structure, q{returned data structure includes function info});

  my $function_info_two = {
    function          => q{function_one},
    job_ids_launched  => [4,5,6],
    job_dependencies  => q{-w'done(1) && done(2) && done(3)'},
  };
  push @{$test_structure->{functions}}, $function_info_two;
  $tree->append_to_functions($function_info_two);
  my $json_structure = $tree->tree_as_json();
  my $href = from_json($json_structure);
  is_deeply($href, $test_structure, q{json structure is correct});

  my $new_tree;
  lives_ok { $new_tree = npg_pipeline::dispatch_tree->new({ json_structure => $json_structure }) ; } q{tree object created ok with a json_structure};
  is_deeply($tree, $new_tree, q{expected tree object built on construction});
}
1;
