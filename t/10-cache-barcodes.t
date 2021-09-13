use strict;
use warnings;
use Test::More tests => 71;
use Test::Exception;
use File::Slurp;
use File::Temp qw(tempdir);
use Log::Log4perl qw(:levels);

use st::api::lims; 

my $dir = tempdir( CLEANUP => 1 );

Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $DEBUG,
                          file   => join(q[/], $dir, 'logfile'),
                          utf8   => 1});

use_ok(q{npg_pipeline::cache::barcodes});

{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/barcodes/samplesheet_batch2015.csv];
  
  my $lims = st::api::lims->new(id_run => 1234)->children_ia();
  my $create_lane = npg_pipeline::cache::barcodes->new(
      lane_lims     => $lims->{1},
      index_lengths => [6],
      location      => $dir,
  );
  isa_ok ($create_lane, q{npg_pipeline::cache::barcodes} );
  ok(!$create_lane->generate(), 'lane not a pool - path undefined');

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/barcodes/samplesheet_batch5378.csv];

  $lims = st::api::lims->new(id_run => 1234)->children_ia();
  $create_lane = npg_pipeline::cache::barcodes->new(
      lane_lims     => $lims->{1},
      index_lengths => [6],
      location      => $dir,
  );
  throws_ok { $create_lane->generate(); }
    qr{Lane 1\: no expected tag sequence or index}, q{croak running generate()};

  my $tag_to_trim = {1 => q(actgffce), 2 => q(aacgffcf), 3 => q(aacdffcd)};
  is_deeply($create_lane->_remove_common_suffixes($tag_to_trim,168),
    {1 => q(actgffce), 2 => q(aacgffcf), 3 => q(aacdffcd)}, 'no common suffix');

  $tag_to_trim = {1 => q(actgffc), 2 => q(aacgffc), 3 => q(aacdffcd)};
  throws_ok {$create_lane->_remove_common_suffixes($tag_to_trim,168)}
    qr/The given tags are different in length/, 'different length tags';

  $tag_to_trim = {1 => q(actgffc), 2 => q(aacgffc), 3 => q(aacdffc)};
  is_deeply($create_lane->_remove_common_suffixes($tag_to_trim,168),
    {1 => q(actg), 2 => q(aacg), 3 => q(aacd)}, 'common suffix length 3');

  $tag_to_trim = {1 => q(actgffc), 2 => q(aacgffc), 3 => q(aacdffc), 168 => q(cffgtca)};
  is_deeply($create_lane->_remove_common_suffixes($tag_to_trim,168),
    {1 => q(actg), 2 => q(aacg), 3 => q(aacd), 168 => q(cffg)}, 'common suffix length 3 with phix');

  $tag_to_trim = {1 => q(actgffc)};
  is_deeply($create_lane->_remove_common_suffixes($tag_to_trim,168),
    {1 => q(actgffc)}, 'only one tag');

  $tag_to_trim = {1 => q(actgffc), 168 => q(aacgffc)};
  is_deeply($create_lane->_remove_common_suffixes($tag_to_trim,168),
    {1 => q(actgffc), 168 => q(aacgffc)}, 'only one non-phix tag');

  my $tag_list = [qw(aaa aaa ccc)];
  throws_ok { $create_lane->_check_tag_uniqueness($tag_list) }
    qr{The given tags after trimming are not unique}, q{tags are not unique - throw error};
}

{  
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/barcodes/samplesheet_batch6532.csv];

  my $lims = st::api::lims->new(id_run => 1234)->children_ia;
  my $create_lane = npg_pipeline::cache::barcodes->new(
      lane_lims    => $lims->{1},
      index_lengths=> [7],
      location     => $dir,
  );

  my $i7_tags = {1=> 'TAGCTTGT', 2 => 'CGATGTTT', 3 => 'GCCAATGT',};
  my ($index_list, $tag_seq_list) = $create_lane->_process_tag_list($i7_tags,{},168);
  is_deeply($index_list, [1,2,3], 'correct index list');
  is_deeply($tag_seq_list, [qw(TAGCTTG CGATGTT GCCAATG)], 'correct tag list after trimming');
  
  $i7_tags = {1=> 'TAGCTTGT', 2 => 'CGATGTTT', 3 => 'GCCAATGT', 168 => 'ACAACGCAATC'};
  ($index_list, $tag_seq_list) = $create_lane->_process_tag_list($i7_tags,{},168);
  is_deeply($index_list, [1,168,2,3], 'correct index list');
  is_deeply($tag_seq_list, [qw(TAGCTTG ACAACGC CGATGTT GCCAATG)], 'correct tag list after trimming');
  
  $i7_tags = {'' => 'TAGCTTGT', 2 => 'CGATGTTT', 3 => 'GCCAATGT',};
  ($index_list, $tag_seq_list) = $create_lane->_process_tag_list( $i7_tags,{},168);
  is($index_list, undef, 'no index list available');
  is($tag_seq_list, undef, 'no expected tag sequence available');
  
  $i7_tags = {1 => undef, 2 => 'CGATGTTT', 3 => 'GCCAATGT',};
  ($index_list, $tag_seq_list) = $create_lane->_process_tag_list($i7_tags,{},168);
  is($tag_seq_list, undef, 'no expected tag sequence available');
  
  $i7_tags = {1=> 'TAGCTTGTTGA', 2 => 'TGCGATGTTAA', 3 => 'GGCCAATTTCG',};
  ($index_list, $tag_seq_list) = $create_lane->_process_tag_list($i7_tags,{},168);
  is_deeply($index_list, [1,2,3], 'correct index list');
  is_deeply($tag_seq_list, [qw(TAGCTTG TGCGATG GGCCAAT)],
    'expected tag sequences are correct after trimming to index_length');

  $create_lane = npg_pipeline::cache::barcodes->new(
      lane_lims    => $lims->{1},
      index_lengths=> [8],
      location     => $dir,
  );
  $i7_tags = {1=> 'TAGCTTGTTGA', 2 => 'TGCGATGTTAA', 3 => 'GGCCAATTTCG',};
  ($index_list, $tag_seq_list) = $create_lane->_process_tag_list($i7_tags,{},168);
  is_deeply($index_list, [1,2,3], 'correct index list');
  is_deeply($tag_seq_list, [qw(TAGCTTG TGCGATG GGCCAAT)],
    'expected tag sequences are correct after trimming to index_length and removing common suffix');
}

{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/barcodes/samplesheet_batch6532.csv];  

  my $lims = st::api::lims->new(id_run => 1234)->children_ia;

  my $create_lane = npg_pipeline::cache::barcodes->new(
      lane_lims    => $lims->{1},
      index_lengths=> [8,8],
      location     => $dir,
  );
  my $i7_tags = {1 => 'ACAACGCAATC', 2 => 'TGCGATGT', 3 => 'GGCCAATG',};
  my $i5_tags = {1 => '', 2 => 'TAATTTTT', 3 => 'GGGAAAAA',};
  my ($index_list, $tag_seq_list) = $create_lane->_process_tag_list($i7_tags, $i5_tags, 1);
  is_deeply($index_list, [1,2,3], 'correct index list');
  is_deeply($tag_seq_list, 
    [qw(ACAACGCA-TCTTTCCC TGCGATGT-TAATTTTT GGCCAATG-GGGAAAAA)],
    'single index phix missing i5 tag added');

  $create_lane = npg_pipeline::cache::barcodes->new(
      lane_lims    => $lims->{1},
      index_lengths=> [8,8],
      location     => $dir,
      i5opposite   => 1,
  );
  $i7_tags = {1 => 'ACAACGCAATC', 2 => 'TGCGATGT', 3 => 'GGCCAATG',};
  $i5_tags = {1 => '', 2 => 'TAATTTTT', 3 => 'GGGAAAAA',};
  ($index_list, $tag_seq_list) = $create_lane->_process_tag_list($i7_tags, $i5_tags, 1);
  is_deeply($index_list, [1,2,3], 'correct index list');
  is_deeply($tag_seq_list, 
    [qw(ACAACGCA-AGATCTCG TGCGATGT-TAATTTTT GGCCAATG-GGGAAAAA)],
    'i5opposite single index phix missing i5 tag added');

  $create_lane = npg_pipeline::cache::barcodes->new(
      lane_lims    => $lims->{1},
      index_lengths=> [12],
      location     => $dir,
  );
  $i7_tags = {1 => 'ACAACGCAATC', 2 => 'TGCGATGTTAA', 3 => 'GGCCAATGGGGC',};
  ($index_list, $tag_seq_list) = $create_lane->_process_tag_list($i7_tags, {}, 1);
  is_deeply($index_list, [1,2,3], 'correct index list');
  is_deeply($tag_seq_list, 
    [qw(ACAACGCAATCT TGCGATGTTAAA GGCCAATGGGGC)],
    'single index phix and 1 sample is padded correctly');

  $create_lane = npg_pipeline::cache::barcodes->new(
      lane_lims    => $lims->{1},
      index_lengths=> [8,8],
      location     => $dir,
  );
  $i7_tags = {1 => 'GTGGATCAAA', 2 => 'ATAGGGCGAG', 3 => 'AGCAAGAAGC',};
  $i5_tags = {1 => 'GCCAACCCTG', 2 => 'TGCATCGAGT', 3 => 'TTGTGTTTCT',};
  ($index_list, $tag_seq_list) = $create_lane->_process_tag_list($i7_tags, $i5_tags, 1);
  is_deeply($index_list, [1,2,3], 'correct index list');
  is_deeply($tag_seq_list, 
    [qw(GTGGATCA-GCCAACCC ATAGGGCG-TGCATCGA AGCAAGAA-TTGTGTTT)],
    'both tags truncated correctly');

  $create_lane = npg_pipeline::cache::barcodes->new(
      lane_lims    => $lims->{1},
      index_lengths=> [10,10],
      location     => $dir,
  );
  $i7_tags = {1 => 'GTGGATCAAA', 2 => 'ATAGGGCGAG', 3 => 'AGCAAGAAGC', 888 => 'TGTGCAGC'};
  $i5_tags = {1 => 'GCCAACCCTG', 2 => 'TGCATCGAGT', 3 => 'TTGTGTTTCT', 888 => 'ACTGATGT'};
  ($index_list, $tag_seq_list) = $create_lane->_process_tag_list($i7_tags, $i5_tags, 888);
  is_deeply($index_list, [1,2,3,888], 'correct index list');
  is_deeply($tag_seq_list, 
    [qw(GTGGATCAAA-GCCAACCCTG ATAGGGCGAG-TGCATCGAGT AGCAAGAAGC-TTGTGTTTCT TGTGCAGCAT-ACTGATGTAC)],
    'dual index phix both tags padded correctly');

  $create_lane = npg_pipeline::cache::barcodes->new(
      lane_lims    => $lims->{1},
      index_lengths=> [8],
      location     => $dir,
  );
  $i7_tags = {1 => 'GTGGATCAAA', 2 => 'GTGGATCAAA', 3 => 'AGCAAGAAGC'};
  $i5_tags = {1 => 'GCCAACCCTG', 2 => 'TGCATCGAGT', 3 => 'ACTGATGTAC'};
  throws_ok { $create_lane->_process_tag_list($i7_tags, $i5_tags, 888) }
    qr{The given tags after trimming are not unique}, q{Dual tags but single index read with non-unique i7 tags};

  $create_lane = npg_pipeline::cache::barcodes->new(
      lane_lims    => $lims->{1},
      index_lengths=> [10,10],
      location     => $dir,
  );
  $i7_tags = {1 => 'GTGGATCAAA', 2 => 'GTGGATCAAA', 3 => 'AGCAAGAAGC'};
  $i5_tags = {1 => 'GCCAACCCTG', 2 => 'TGCATCGAGT', 3 => 'TTGTGTTTCT'};
  ($index_list, $tag_seq_list) = $create_lane->_process_tag_list($i7_tags, $i5_tags, 888);
  is_deeply($index_list, [1,2,3], 'correct index list');
  is_deeply($tag_seq_list, 
    [qw(GTGGATCAAA-GCCAACCCTG GTGGATCAAA-TGCATCGAGT AGCAAGAAGC-TTGTGTTTCT)],
    'dual index i7 tags not unique');

  $create_lane = npg_pipeline::cache::barcodes->new(
      lane_lims    => $lims->{1},
      index_lengths=> [10,10],
      location     => $dir,
  );
  $i7_tags = {1 => 'GTGGATCAAA', 2 => 'ATAGGGCGAG', 3 => 'AGCAAGAAGC'};
  $i5_tags = {1 => 'GCCAACCCTG', 2 => 'GCCAACCCTG', 3 => 'TTGTGTTTCT'};
  ($index_list, $tag_seq_list) = $create_lane->_process_tag_list($i7_tags, $i5_tags, 888);
  is_deeply($index_list, [1,2,3], 'correct index list');
  is_deeply($tag_seq_list, 
    [qw(GTGGATCAAA-GCCAACCCTG ATAGGGCGAG-GCCAACCCTG AGCAAGAAGC-TTGTGTTTCT)],
    'dual index i5 tags not unique');

  $create_lane = npg_pipeline::cache::barcodes->new(
      lane_lims    => $lims->{1},
      index_lengths=> [10,10],
      location     => $dir,
  );
  $i7_tags = {1 => 'GTGGAT',   2 => 'ATAGGGCG', 3 => 'AGCAAGAAGC', 888 => 'TGTGCAGC'};
  $i5_tags = {1 => 'GCCAACCC', 2 => 'TGCATCGA', 3 => 'TTGTGTTTCT', 888 => 'ACTGATGT'};
  ($index_list, $tag_seq_list) = $create_lane->_process_tag_list($i7_tags, $i5_tags, 888);
  is_deeply($index_list, [1,2,3,888], 'correct index list');
  is_deeply($tag_seq_list, 
    [qw(GTGGATATCT-GCCAACCCAC ATAGGGCGAT-TGCATCGAAC AGCAAGAAGC-TTGTGTTTCT TGTGCAGCAT-ACTGATGTAC)],
    'dual phix mixed tags lengths padded correctly');

  $create_lane = npg_pipeline::cache::barcodes->new(
      lane_lims    => $lims->{1},
      index_lengths=> [10,10],
      location     => $dir,
      i5opposite   => 1,
  );
  $i7_tags = {1 => 'GTGGAT', 2 => 'ATAGGGCG', 3 => 'TAACGCGTGA', 888 => 'TGTGCAGC'};
  $i5_tags = {1 => 'GCCAAC', 2 => 'TGCATCGA', 3 => 'CCCTAACTTC', 888 => 'ACTGATGT'};
  ($index_list, $tag_seq_list) = $create_lane->_process_tag_list($i7_tags, $i5_tags, 888);
  is_deeply($index_list, [1,2,3,888], 'correct index list');
  is_deeply($tag_seq_list, 
    [qw(GTGGATATCT-GCCAACGTGT ATAGGGCGAT-TGCATCGAGT TAACGCGTGA-CCCTAACTTC TGTGCAGCAT-ACTGATGTGT)],
    'i5oppsite dual phix mixed tag lengths padded correctly');

  $create_lane = npg_pipeline::cache::barcodes->new(
      lane_lims    => $lims->{1},
      index_lengths=> [10,10],
      location     => $dir,
      i5opposite   => 0,
  );
  $i7_tags = {1 => 'GTGGAT', 2 => 'ATAGGGCG', 3 => 'AGCAAGAAGC'};
  $i5_tags = {1 => 'GCCAAC', 2 => 'TGCATCGA', 3 => 'CCCTAACTTC'};
  throws_ok { $create_lane->_process_tag_list($i7_tags, $i5_tags, 888) }
    qr{Cannot extend for more bases than in padding sequence}, q{mixed tag lengths i5 pad too short};

  $create_lane = npg_pipeline::cache::barcodes->new(
      lane_lims    => $lims->{1},
      index_lengths=> [13],
      location     => $dir,
  );
  $i7_tags = {1 => 'GTGGATNNNNNNN', 2 => 'ATAGGGNNNNNNN', 3 => 'TAACGCNNNNNNN', 888 => 'TGTGCAGC'};
  ($index_list, $tag_seq_list) = $create_lane->_process_tag_list($i7_tags, $i5_tags, 888);
  is_deeply($index_list, [1,2,3,888], 'correct index list');
  is_deeply($tag_seq_list, 
    [qw(GTGGAT ATAGGG TAACGC TGTGCA)],
    'single-end haplotagging phix i7 tag padded correctly then a common N suffix is removed');

  # the following two test should be changed once we have the full 5-base i5 pads
  # we still don't have the 5-base i5 pad but we do have the 5-base i5opposite pad

  $create_lane = npg_pipeline::cache::barcodes->new(
      lane_lims    => $lims->{1},
      index_lengths=> [13,13],
      location     => $dir,
  );
  $i7_tags = {1 => 'GTGGATNNNNNNN', 2 => 'ATAGGGNNNNNNN', 3 => 'TAACGCNNNNNNN', 888 => 'TGTGCAGC'};
  $i5_tags = {1 => 'GCCAACNNNNNNN', 2 => 'TGCATCNNNNNNN', 3 => 'CCCTAANNNNNNN', 888 => 'ACTGATGT'};
  throws_ok { $create_lane->_process_tag_list($i7_tags, $i5_tags, 888) }
    qr{Cannot extend for more bases than in padding sequence}, q{dual-end haplotagging phix i5 pad too short};

  $create_lane = npg_pipeline::cache::barcodes->new(
      lane_lims    => $lims->{1},
      index_lengths=> [13,13],
      location     => $dir,
      i5opposite   => 1,
  );
  $i7_tags = {1 => 'GTGGATNNNNNNN', 2 => 'ATAGGGNNNNNNN', 3 => 'TAACGCNNNNNNN', 888 => 'TGTGCAGC'};
  $i5_tags = {1 => 'GCCAACNNNNNNN', 2 => 'TGCATCNNNNNNN', 3 => 'CCCTAANNNNNNN', 888 => 'ACTGATGT'};
  ($index_list, $tag_seq_list) = $create_lane->_process_tag_list($i7_tags, $i5_tags, 888);
  is_deeply($index_list, [1,2,3,888], 'correct index list');
  is_deeply($tag_seq_list,
    [qw(GTGGAT-GCCAAC ATAGGG-TGCATC TAACGC-CCCTAA TGTGCA-ACTGAT)],
    'i5opposite dual-end haplotagging phix i5 tag padded correctly then a common N suffix is removed');
}

{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/barcodes/samplesheet_batch6713.csv];

  my $lims = st::api::lims->new(id_run => 1234)->children_ia;
  my $create_lane  = npg_pipeline::cache::barcodes->new(
      lane_lims     => $lims->{1},
      index_lengths=> [6],
      location     => $dir,
  );

  my $tag_list;
  lives_ok {
    $tag_list = $create_lane->generate();
  } q{no croak running generate() for batch 6713};

  is($tag_list, "$dir/lane_1.taglist", 'tag list file path');
  my $file_contents;
  lives_ok {$file_contents = read_file($tag_list);} 'reading tag list file';
  my $expected = qq[barcode_sequence\tbarcode_name\tlibrary_name\tsample_name\tdescription\nATCACG\t1\t214599\tYeast\tRapid high-resolution QTL mapping in yeast: Sequencing DNA from an entire pool of segregants before and after a selection step to map alleles responsible for increased growth in a restrictive condition.\nCGATGT\t2\t214599\tYeast\tRapid high-resolution QTL mapping in yeast: Sequencing DNA from an entire pool of segregants before and after a selection step to map alleles responsible for increased growth in a restrictive condition.];
  is($file_contents, $expected, 'tag list file contents as expected');
}

{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/barcodes/samplesheet_batch42225.csv];

  my $lims = st::api::lims->new(id_run => 18124)->children_ia;
  my $create_lane  = npg_pipeline::cache::barcodes->new(
      lane_lims     => $lims->{1},
      index_lengths => [8,8],
      location      => $dir,
      i5opposite    => 1,
  );

  my $tag_list;
  lives_ok {
    $tag_list = $create_lane->generate();
  } q{i5opposite dual index no croak running generate() for batch 42225};

  is($tag_list, "$dir/lane_1.taglist", 'i5opposite dual index tag list file path');
  my $file_contents;
  lives_ok {$file_contents = read_file($tag_list);} 'i5opposite dual index reading tag list file';
  my $expected = qq[barcode_sequence\tbarcode_name\tlibrary_name\tsample_name\tdescription\nATTACTCG-AGGCTATA\t1\t15144164\t3165STDY6250498\tHX Test Plan: Development of sequencing and library prep protocols using Human DNA \nACAACGCA-AGATCTCG\t888\t12172503\tphiX_for_spiked_buffers\tIllumina Controls: SPIKED_CONTROL];
  is($file_contents, $expected, 'i5opposite dual index tag list file contents as expected');
}

{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/barcodes/samplesheet_batch42225_amended1.csv];

  my $expected_tag_list = "$dir/lane_1.taglist";
  unlink  $expected_tag_list;

  my $lims = st::api::lims->new(id_run => 18124)->children_ia;
  my $create_lane  = npg_pipeline::cache::barcodes->new(
      lane_lims     => $lims->{1},
      index_lengths => [6,8],
      location      => $dir,
      i5opposite    => 1,
  );

  my $tag_list;
  lives_ok {
    $tag_list = $create_lane->generate();
  } q{i5opposite dual index no croak running generate()};

  is($tag_list, $expected_tag_list, 'i5opposite dual index tag list file path');
  my $file_contents;
  lives_ok {$file_contents = read_file($tag_list);} 'i5opposite dual index reading tag list file';
  my $expected = qq[barcode_sequence\tbarcode_name\tlibrary_name\tsample_name\tdescription\nATTACT-AGGCTATA\t1\t15144164\t3165STDY6250498\tHX Test Plan: Development of sequencing and library prep protocols using Human DNA \nACAACG-AGATCTCG\t888\t12172503\tphiX_for_spiked_buffers\tIllumina Controls: SPIKED_CONTROL];
  is($file_contents, $expected, 'i5opposite dual index tag list file contents as expected');
}

{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/barcodes/samplesheet_batch42225_amended2.csv];

  my $expected_tag_list = "$dir/lane_1.taglist";
  unlink  $expected_tag_list;

  my $lims = st::api::lims->new(id_run => 18124)->children_ia;
  my $create_lane  = npg_pipeline::cache::barcodes->new(
      lane_lims     => $lims->{1},
      index_lengths=> [6,8],
      location     => $dir,
      i5opposite   => 1,
  );

  my $tag_list;
  lives_ok {
    $tag_list = $create_lane->generate();
  } q{i5opposite dual index no croak running generate() for batch 42227};

  is($tag_list, $expected_tag_list, 'i5opposite dual index tag list file path');
  my $file_contents;
  lives_ok {$file_contents = read_file($tag_list);} 'i5opposite dual index reading tag list file';
  my $expected = qq[barcode_sequence\tbarcode_name\tlibrary_name\tsample_name\tdescription\nATTACT-AGGCTATA\t1\t15144164\t3165STDY6250498\tHX Test Plan: Development of sequencing and library prep protocols using Human DNA \nACAACG-AGATCTCG\t888\t12172503\tphiX_for_spiked_buffers\tIllumina Controls: SPIKED_CONTROL];
  is($file_contents, $expected, 'i5opposite dual index tag list file contents as expected');
}

{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/barcodes/samplesheet_batch85331_amended1.csv];

  my $expected_tag_list = "$dir/lane_1.taglist";
  unlink  $expected_tag_list;

  my $lims = st::api::lims->new(id_run => 18124)->children_ia;
  my $create_lane  = npg_pipeline::cache::barcodes->new(
      lane_lims     => $lims->{1},
      index_lengths=> [6,8],
      location     => $dir,
      i5opposite   => 0,
  );

  my $tag_list;
  lives_ok {
    $tag_list = $create_lane->generate();
  } q{dual index common i7 tag no croak running generate() for batch 85331};

  is($tag_list, $expected_tag_list, 'dual index common i7 tag list file path');
  my $file_contents;
  lives_ok {$file_contents = read_file($tag_list);} 'i5opposite dual index reading tag list file';
  my $expected = qq[barcode_sequence\tbarcode_name\tlibrary_name\tsample_name\tdescription\n-CCATCCAA\t1\t42539892\tPD5847h_lo0097_WGMS\tEGAS00001005210: Whole genome methylation sequencing of colonies of patients with myeloproliferative neoplasms. \n-ACACCCAG\t2\t42539893\tPD5847h_lo0098_WGMS\tEGAS00001005210: Whole genome methylation sequencing of colonies of patients with myeloproliferative neoplasms. \n-ACTGATGT\t888\t27409532\tphiX_for_spiked_buffers\tIllumina Controls: SPIKED_CONTROL];
  is($file_contents, $expected, 'i5opposite dual index tag list file contents as expected');
}

{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/barcodes/samplesheet_batch85331_amended2.csv];

  my $expected_tag_list = "$dir/lane_1.taglist";
  unlink  $expected_tag_list;

  my $lims = st::api::lims->new(id_run => 18124)->children_ia;
  my $create_lane  = npg_pipeline::cache::barcodes->new(
      lane_lims     => $lims->{1},
      index_lengths=> [8,6],
      location     => $dir,
      i5opposite   => 1,
  );

  my $tag_list;
  lives_ok {
    $tag_list = $create_lane->generate();
  } q{i5opposite dual index common i5 tag no croak running generate() for batch 85331};

  is($tag_list, $expected_tag_list, 'i5opposite dual index common i5 tag list file path');
  my $file_contents;
  lives_ok {$file_contents = read_file($tag_list);} 'i5opposite dual index reading tag list file';
  my $expected = qq[barcode_sequence\tbarcode_name\tlibrary_name\tsample_name\tdescription\nGGGATCCT-\t1\t42539892\tPD5847h_lo0097_WGMS\tEGAS00001005210: Whole genome methylation sequencing of colonies of patients with myeloproliferative neoplasms. \nGATACTCC-\t2\t42539893\tPD5847h_lo0098_WGMS\tEGAS00001005210: Whole genome methylation sequencing of colonies of patients with myeloproliferative neoplasms. \nTGTGCAGC-\t888\t27409532\tphiX_for_spiked_buffers	Illumina Controls: SPIKED_CONTROL];
  is($file_contents, $expected, 'i5opposite dual index tag list file contents as expected');
}

1;


