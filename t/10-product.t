use strict;
use warnings;
use Test::More tests => 10;
use Test::Exception;
use List::MoreUtils qw/all/;
use npg_tracking::glossary::composition::component::illumina;

use_ok 'npg_pipeline::product';

subtest 'file names for one-component compositions' => sub {
  plan tests => 20;

  my $p = npg_pipeline::product->new(rpt_list => '26219:1');
  isa_ok ($p, 'npg_pipeline::product');
  is ($p->file_name_root(), '26219_1', 'lane file name root');
  is ($p->file_name(), '26219_1', 'no-options file name');
  is ($p->file_name(ext => 'bam'), '26219_1.bam', 'file name with an extention');
  is ($p->file_name(suffix => 'F0xB00'), '26219_1_F0xB00', 'file name with a suffix');
  is ($p->file_name(ext => 'stats', suffix => 'F0xB00'), '26219_1_F0xB00.stats',
    'file name with both extension and suffix');
  is ($p->file_name(ext => 'stats.md5', suffix => 'F0xB00'), '26219_1_F0xB00.stats.md5',
    'file name with both extension and suffix');

  $p = npg_pipeline::product->new(rpt_list => '26219:1:0');
  is ($p->file_name_root(), '26219_1#0', 'tag zero name root');
  is ($p->file_name(), '26219_1#0', 'no-options file name');
  is ($p->file_name(ext => 'bam'), '26219_1#0.bam', 'file name with an extention');
  is ($p->file_name(suffix => 'F0xB00'), '26219_1#0_F0xB00', 'file name with a suffix');
  is ($p->file_name(ext => 'stats', suffix => 'F0xB00'), '26219_1#0_F0xB00.stats',
    'file name with both extension and suffix');

  $p = npg_pipeline::product->new(rpt_list => '26219:1:3');
  is ($p->file_name_root(), '26219_1#3', 'plex name root');
  is ($p->file_name(ext => 'bam'), '26219_1#3.bam', 'file name with an extention');
  is ($p->file_name(suffix => 'F0xB00'), '26219_1#3_F0xB00', 'file name with a suffix');
  is ($p->file_name(ext => 'stats', suffix => 'F0xB00'), '26219_1#3_F0xB00.stats',
    'file name with both extension and suffix');

  $p = npg_pipeline::product->new(rpt_list => '26219:1:3', selected_lanes => 1);
  is ($p->file_name_root(), '26219_1#3', 'plex name root');
  is ($p->file_name(ext => 'bam'), '26219_1#3.bam', 'file name with an extention');
  is ($p->file_name(suffix => 'F0xB00'), '26219_1#3_F0xB00', 'file name with a suffix');
  is ($p->file_name(ext => 'stats', suffix => 'F0xB00'), '26219_1#3_F0xB00.stats',
    'file name with both extension and suffix');
};

subtest 'file names for merged entities' => sub {
  plan tests => 16;

  my $p = npg_pipeline::product->new(rpt_list => '26219:1:3;26219:2:3;26219:3:3;26219:4:3');
  is ($p->file_name_root(), '26219#3', 'composition of plexes name root');
  is ($p->file_name(ext => 'bam'), '26219#3.bam', 'file name with an extention');
  is ($p->file_name(suffix => 'F0xB00'), '26219#3_F0xB00', 'file name with a suffix');
  is ($p->file_name(ext => 'stats', suffix => 'F0xB00'), '26219#3_F0xB00.stats',
    'file name with both extension and suffix');

  $p = npg_pipeline::product->new(rpt_list => '26219:1;26219:2;26219:3;26219:4');
  is ($p->file_name_root(), '26219', 'composition of lanes name root');
  is ($p->file_name(ext => 'bam'), '26219.bam', 'file name with an extention');
  is ($p->file_name(suffix => 'F0xB00'), '26219_F0xB00', 'file name with a suffix');
  is ($p->file_name(ext => 'stats', suffix => 'F0xB00'), '26219_F0xB00.stats',
    'file name with both extension and suffix');

  $p = npg_pipeline::product->new(rpt_list       => '26219:2:3;26219:4:3;26219:1:3',
                                  selected_lanes => 1);
  is ($p->file_name_root(), '26219_1-2-4#3', 'composition of plexes name root');
  is ($p->file_name(ext => 'bam'), '26219_1-2-4#3.bam', 'file name with an extention');
  is ($p->file_name(suffix => 'F0xB00'), '26219_1-2-4#3_F0xB00', 'file name with a suffix');
  is ($p->file_name(ext => 'stats', suffix => 'F0xB00'), '26219_1-2-4#3_F0xB00.stats',
    'file name with both extension and suffix');

  $p = npg_pipeline::product->new(rpt_list => '26219:2;26219:1;26219:4', selected_lanes => 1);
  is ($p->file_name_root(), '26219_1-2-4', 'composition of lanes name root');
  is ($p->file_name(ext => 'bam'), '26219_1-2-4.bam', 'file name with an extention');
  is ($p->file_name(suffix => 'F0xB00'), '26219_1-2-4_F0xB00', 'file name with a suffix');
  is ($p->file_name(ext => 'stats', suffix => 'F0xB00'), '26219_1-2-4_F0xB00.stats',
    'file name with both extension and suffix');
};

subtest 'file names for entities with subsets' => sub {
  plan tests => 24;

  my $icp = 'npg_tracking::glossary::composition::component::illumina';
  my $c1 = $icp->new(id_run => 26219, position => 1, subset => 'phix');

  my $p = npg_pipeline::product->new(composition =>
    npg_tracking::glossary::composition->new(components => [$c1]));
  is ($p->file_name_root(), '26219_1_phix', 'lane file name root');
  is ($p->file_name(ext => 'bam'), '26219_1_phix.bam', 'file name with an extention');
  is ($p->file_name(suffix => 'F0xB00'), '26219_1_phix_F0xB00', 'file name with a suffix');
  is ($p->file_name(ext => 'stats', suffix => 'F0xB00'), '26219_1_phix_F0xB00.stats',
    'file name with both extension and suffix');

  my $c2 = $icp->new(id_run => 26219, position => 1, tag_index => 4, subset => 'phix');
  $p = npg_pipeline::product->new(composition =>
    npg_tracking::glossary::composition->new(components => [$c2]));
  is ($p->file_name_root(), '26219_1#4_phix', 'lane file name root');
  is ($p->file_name(ext => 'bam'), '26219_1#4_phix.bam', 'file name with an extention');
  is ($p->file_name(suffix => 'F0xB00'), '26219_1#4_phix_F0xB00', 'file name with a suffix');
  is ($p->file_name(ext => 'stats', suffix => 'F0xB00'), '26219_1#4_phix_F0xB00.stats',
    'file name with both extension and suffix');

  my $c3 = $icp->new(id_run => 26219, position => 3, subset => 'phix');
  my $c4 = $icp->new(id_run => 26219, position => 4, subset => 'phix');
  my $comp = npg_tracking::glossary::composition->new(components => [$c4, $c3, $c1]);

  $p = npg_pipeline::product->new(composition => $comp);
  is ($p->file_name_root(), '26219_phix', 'lane file name root');
  is ($p->file_name(ext => 'bam'), '26219_phix.bam', 'file name with an extention');
  is ($p->file_name(suffix => 'F0xB00'), '26219_phix_F0xB00', 'file name with a suffix');
  is ($p->file_name(ext => 'stats', suffix => 'F0xB00'), '26219_phix_F0xB00.stats',
    'file name with both extension and suffix');

  $p = npg_pipeline::product->new(composition => $comp, selected_lanes => 1);
  is ($p->file_name_root(), '26219_1-3-4_phix', 'lane file name root');
  is ($p->file_name(ext => 'bam'), '26219_1-3-4_phix.bam', 'file name with an extention');
  is ($p->file_name(suffix => 'F0xB00'), '26219_1-3-4_phix_F0xB00', 'file name with a suffix');
  is ($p->file_name(ext => 'stats', suffix => 'F0xB00'), '26219_1-3-4_phix_F0xB00.stats',
    'file name with both extension and suffix');

  $c3 = $icp->new(id_run => 26219, position => 3, tag_index => 4, subset => 'phix');
  $c4 = $icp->new(id_run => 26219, position => 4,  tag_index => 4, subset => 'phix');
  $comp = npg_tracking::glossary::composition->new(components => [$c4, $c3, $c2]);

  $p = npg_pipeline::product->new(composition => $comp);
  is ($p->file_name_root(), '26219#4_phix', 'lane file name root');
  is ($p->file_name(ext => 'bam'), '26219#4_phix.bam', 'file name with an extention');
  is ($p->file_name(suffix => 'F0xB00'), '26219#4_phix_F0xB00', 'file name with a suffix');
  is ($p->file_name(ext => 'stats', suffix => 'F0xB00'), '26219#4_phix_F0xB00.stats',
    'file name with both extension and suffix');

  $p = npg_pipeline::product->new(composition => $comp, selected_lanes => 1);
  is ($p->file_name_root(), '26219_1-3-4#4_phix', 'lane file name root');
  is ($p->file_name(ext => 'bam'), '26219_1-3-4#4_phix.bam', 'file name with an extention');
  is ($p->file_name(suffix => 'F0xB00'), '26219_1-3-4#4_phix_F0xB00', 'file name with a suffix');
  is ($p->file_name(ext => 'stats', suffix => 'F0xB00'), '26219_1-3-4#4_phix_F0xB00.stats',
    'file name with both extension and suffix');
};

subtest 'paths for one-component compositions' => sub {
  plan tests => 16;

  my $p = npg_pipeline::product->new(rpt_list => '26219:1');
  is ($p->path('/tmp'), '/tmp/lane1', 'path');
  is ($p->qc_out_path('/tmp'), '/tmp/lane1/qc', 'qc out path');
  is ($p->short_files_cache_path('/tmp'), '/tmp/lane1/.npg_cache_10000',
    'short files cache path');
  is ($p->tileviz_path('/tmp'), '/tmp/lane1/tileviz',
    'tileviz path');

  is ($p->file_path('/tmp', ext => 'bam'), '/tmp/26219_1.bam',
    'file path with an extention');
  is ($p->file_path('/tmp', suffix => 'F0xB00'), '/tmp/26219_1_F0xB00',
    'file path with a suffix');
  is ($p->file_path('/tmp', ext => 'stats', suffix => 'F0xB00'),
    '/tmp/26219_1_F0xB00.stats', 'file path with both extension and suffix');

  $p = npg_pipeline::product->new(rpt_list => '26219:1:0');
  is ($p->path('/tmp'), '/tmp/lane1/plex0', 'path');
  is ($p->qc_out_path('/tmp'), '/tmp/lane1/plex0/qc', 'qc out path');
  is ($p->short_files_cache_path('/tmp'), '/tmp/lane1/plex0/.npg_cache_10000',
    'short files cache path');

  $p = npg_pipeline::product->new(rpt_list => '26219:1:3');
  is ($p->path('/tmp'), '/tmp/lane1/plex3', 'path');
  is ($p->qc_out_path('/tmp'), '/tmp/lane1/plex3/qc', 'qc out path');
  is ($p->short_files_cache_path('/tmp'), '/tmp/lane1/plex3/.npg_cache_10000',
    'short files cache path');

  $p = npg_pipeline::product->new(rpt_list => '26219:1:3', selected_lanes => 1);
  is ($p->path('/tmp'), '/tmp/lane1/plex3', 'path');
  is ($p->qc_out_path('/tmp'), '/tmp/lane1/plex3/qc', 'qc out path');
  is ($p->short_files_cache_path('/tmp'), '/tmp/lane1/plex3/.npg_cache_10000',
    'short files cache path');
};

subtest 'paths for merged entities' => sub {
  plan tests => 12;

  my $p = npg_pipeline::product->new(rpt_list => '26219:1:3;26219:2:3;26219:3:3;26219:4:3');
  is ($p->path('/tmp'), '/tmp/plex3', 'path');
  is ($p->qc_out_path('/tmp'), '/tmp/plex3/qc', 'qc out path');
  is ($p->short_files_cache_path('/tmp'), '/tmp/plex3/.npg_cache_10000',
    'short files cache path'); 

  $p = npg_pipeline::product->new(rpt_list => '26219:1;26219:2;26219:3;26219:4');
  is ($p->path('/tmp'), '/tmp', 'path');
  is ($p->qc_out_path('/tmp'), '/tmp/qc', 'qc out path');
  is ($p->short_files_cache_path('/tmp'), '/tmp/.npg_cache_10000',
    'short files cache path');

  $p = npg_pipeline::product->new(rpt_list       => '26219:2:3;26219:4:3;26219:1:3',
                                  selected_lanes => 1);
  is ($p->path('/tmp'), '/tmp/lane1-2-4/plex3', 'path');
  is ($p->qc_out_path('/tmp'), '/tmp/lane1-2-4/plex3/qc', 'qc out path');
  is ($p->short_files_cache_path('/tmp'), '/tmp/lane1-2-4/plex3/.npg_cache_10000',
    'short files cache path');

  $p = npg_pipeline::product->new(rpt_list => '26219:2;26219:1;26219:4', selected_lanes => 1);
  is ($p->path('/tmp'), '/tmp/lane1-2-4', 'path');
  is ($p->qc_out_path('/tmp'), '/tmp/lane1-2-4/qc', 'qc out path');
  is ($p->short_files_cache_path('/tmp'), '/tmp/lane1-2-4/.npg_cache_10000',
    'short files cache path');
};

subtest 'product subset generation' => sub {
  plan tests => 13;

  my $p = npg_pipeline::product->new(rpt_list => '26219:1:3;26219:2:3;26219:3:3;26219:4:3');
  my $ps = $p->subset_as_product('phix');
  isa_ok ($ps, 'npg_pipeline::product');
  ok (!$ps->selected_lanes, 'selected lanes flag is false');
  is ($ps->composition->num_components, 4, 'for components');
  is ($ps->composition->get_component(0)->subset, 'phix', 'component 0 subset is phix');
  is ($ps->composition->get_component(0)->subset, 'phix', 'component 3 subset is phix');
  is ($ps->file_name_root(), '26219#3_phix', 'subset product file name root');

  $p = npg_pipeline::product->new(rpt_list => '26219:1:3;26219:2:3', selected_lanes => 1);
  $ps = $p->subset_as_product('phix');
  ok ($ps->selected_lanes, 'selected lanes flag is true');
  is ($ps->file_name_root(), '26219_1-2#3_phix', 'subset product file name root');

  $p = npg_pipeline::product->new(rpt_list => '26219:1;26219:2;26219:3;26219:4');
  $ps = $p->subset_as_product('human');
  ok (!$ps->selected_lanes, 'selected lanes flag is false');
  is ($ps->composition->num_components, 4, 'for components');
  is ($ps->composition->get_component(0)->subset, 'human', 'component 0 subset is human');
  is ($ps->composition->get_component(0)->subset, 'human', 'component 3 subset is human');
  is ($ps->file_name_root(), '26219_human', 'subset product file name root');
};

subtest 'generation of product objects for components' => sub {
  plan tests => 17;

  my $p = npg_pipeline::product->new(rpt_list => '26219:1:3');
  my @p_components = $p->components_as_products();
  is (scalar @p_components, 1, 'one-item list for a single component');

  $p = npg_pipeline::product->new(rpt_list => '26219:1:3;26219:2:3;26219:3:3;26219:4:3');
  @p_components = $p->components_as_products();
  is (scalar @p_components, 4, 'four products');
  map { isa_ok ($_, 'npg_pipeline::product') }  @p_components;
  ok ((all { ! $_->selected_lanes } @p_components),
    'selected_lanes flag is false for all objects');
  ok ((all {$_->composition->num_components == 1 }
      @p_components), 'all objects contain a single component');
  ok ((all {$_->composition->get_component(0)->tag_index == 3 }
      @p_components), 'all single components are for tag 3');
  is (join(q[,], map {$_->composition->get_component(0)->position}
      @p_components), '1,2,3,4', 'correct positions');
  is (join(q[-], map {$_->rpt_list} @p_components),
    '26219:1:3-26219:2:3-26219:3:3-26219:4:3', 'correct rpt lists');

  $p = npg_pipeline::product->new(rpt_list => '26219:1;26219:2', selected_lanes => 1);
  @p_components = $p->components_as_products();
  is (scalar @p_components, 2, 'two products');
  ok ((all { $_->selected_lanes } @p_components),
    'selected_lanes flag is true for all objects');
  ok ((all {$_->composition->num_components == 1 } @p_components),
    'all objects contain a single component');
  ok ((all {! defined $_->composition->get_component(0)->tag_index } @p_components),
    'tag index undefined for all single components');
  is (join(q[,], map {$_->composition->get_component(0)->position} @p_components),
    '1,2', 'correct positions');
  is (join(q[-], map {$_->rpt_list} @p_components), '26219:1-26219:2', 'correct rpt lists'); 
};

subtest 'generation of product objects for lanes' => sub {
  plan tests => 16;

  my $p = npg_pipeline::product->new(rpt_list => '26219:1:3');
  my @p_lanes = $p->lanes_as_products();
  is (scalar @p_lanes, 1, 'one products');
  
  $p = npg_pipeline::product->new(rpt_list => '26219:1:3;26219:2:3;26219:3:3;26219:4:3');
  @p_lanes = $p->lanes_as_products();
  map { isa_ok ($_, 'npg_pipeline::product') }  @p_lanes;
  ok ((all { ! $_->selected_lanes } @p_lanes),
    'selected_lanes flag is false for all objects');
  ok ((all {$_->composition->num_components == 1 } @p_lanes),
    'all objects contain a single component');
  ok ((all {! defined $_->composition->get_component(0)->tag_index } @p_lanes),
    'tag index undefined for all single components');
  is (join(q[,], map {$_->composition->get_component(0)->position} @p_lanes),
    '1,2,3,4', 'correct positions');
  is (join(q[-], map {$_->rpt_list} @p_lanes),
    '26219:1-26219:2-26219:3-26219:4', 'correct rpt lists');

  $p = npg_pipeline::product->new(rpt_list => '26219:1;26219:2', selected_lanes => 1);
  @p_lanes = $p->components_as_products();
  is (scalar @p_lanes, 2, 'two products');
  ok ((all { $_->selected_lanes } @p_lanes), 'selected_lanes flag is true for all objects');
  ok ((all {$_->composition->num_components == 1 } @p_lanes),
    'all objects contain a single component');
  ok ((all {! defined $_->composition->get_component(0)->tag_index } @p_lanes),
    'tag index undefined for all single components');
  is (join(q[,], map {$_->composition->get_component(0)->position} @p_lanes),
    '1,2', 'correct positions');
  is (join(q[-], map {$_->rpt_list} @p_lanes), '26219:1-26219:2', 'correct rpt lists');
};

subtest 'tests for simple functions' => sub {
  plan tests => 7;

  my $p = npg_pipeline::product->new(rpt_list => '26219:1:0;26219:2:0;26219:3:0;26219:4:0');
  ok ($p->is_tag_zero_product, 'tag zero product flagged correctly');
  ok ($p->has_multiple_components, 'product has multiple components');
  $p = npg_pipeline::product->new(rpt_list => '26219:1:0;26219:2:1;26219:3:0;26219:4:0');
  ok (!$p->is_tag_zero_product, 'not tag zero product');
  $p = npg_pipeline::product->new(rpt_list => '26219:1;26219:2;26219:3;26219:4');
  ok (!$p->is_tag_zero_product, 'not tag zero product');
  ok ($p->has_multiple_components, 'product has multiple components');
  $p = npg_pipeline::product->new(rpt_list => '26219:1:0');
  ok ($p->is_tag_zero_product, 'tag zero product flagged correctly');
  ok (!$p->has_multiple_components, 'product does not have multiple components');
};

1;
