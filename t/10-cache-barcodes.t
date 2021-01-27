use strict;
use warnings;
use Test::More tests => 74;
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

  my $tag_to_trim = [qw(actgffc aacgffc aacdffcd)];
  is($create_lane->_tag_common_suffix_length($tag_to_trim), 0, 'tag no common suffix'); 
  
  $tag_to_trim = [qw(actgffc aacgffc aacdffc)];
  is($create_lane->_tag_common_suffix_length($tag_to_trim), 3, 'tag common suffix length 3');

  $tag_to_trim = [qw(aacdffc aacdffc aacdffc)];
  is($create_lane->_tag_common_suffix_length($tag_to_trim), 7, 'all tags are the same'); 

  $tag_to_trim = [qw(actgffce aacgffcf aacdffcd)];
  my $index_to_trim = [qw(1 2 3)];
  is_deeply($create_lane->_trim_tag_common_suffix($tag_to_trim,$index_to_trim,168),
    [qw(actgffce aacgffcf aacdffcd)], 'no common suffix');

  $tag_to_trim = [qw(actgffc aacgffc aacdffcd)];
  throws_ok {$create_lane->_trim_tag_common_suffix($tag_to_trim,$index_to_trim,168)}
    qr/The given tags are different in length/, 'different length tags';

  $tag_to_trim = [qw(actgffc aacgffc aacdffc)];
  is_deeply($create_lane->_trim_tag_common_suffix($tag_to_trim,$index_to_trim,168),
    [qw(actg aacg aacd)], 'common suffix length 3');

  $tag_to_trim = [qw(aacdffc aacdffc aacdffc)];
  throws_ok {$create_lane->_trim_tag_common_suffix($tag_to_trim,$index_to_trim,168)}
    qr{All tags are the same}, 'all tags are the same'; 

  $tag_to_trim = [qw(actgffc aacgffc aacdffc cffgtca )];
  $index_to_trim = [qw(1 2 3 168)];
  is_deeply($create_lane->_trim_tag_common_suffix($tag_to_trim,$index_to_trim,168),
    [qw(actg aacg aacd cffg)], 'common suffix length 3 with phix');

  $tag_to_trim = [qw(actgffc)];
  $index_to_trim = [qw(1)];
  is_deeply($create_lane->_trim_tag_common_suffix($tag_to_trim,$index_to_trim,168),
    [qw(actgffc)], 'only one tag');

  $tag_to_trim = [qw(actgffc aacgffc)];
  $index_to_trim = [qw(1 168)];
  is_deeply($create_lane->_trim_tag_common_suffix($tag_to_trim,$index_to_trim,168),
    [qw(actgffc aacgffc)], 'only one non-phix tag');

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

  my $tag_list_lane1 = {1=> 'TAGCTTGT', 2 => 'CGATGTTT', 3 => 'GCCAATGT',};
  my ($index_list, $tag_seq_list) = $create_lane->_process_tag_list($tag_list_lane1,168);
  is_deeply($index_list, [1,2,3], 'correct index list');
  is_deeply($tag_seq_list, [qw(TAGCTTG CGATGTT GCCAATG)], 'correct tag list after trimming');
  
  $tag_list_lane1 = {1=> 'TAGCTTGT', 2 => 'CGATGTTT', 3 => 'GCCAATGT', 168 => 'ACAACGCA'};
  ($index_list, $tag_seq_list) = $create_lane->_process_tag_list($tag_list_lane1,168);
  is_deeply($index_list, [1,168,2,3], 'correct index list');
  is_deeply($tag_seq_list, [qw(TAGCTTG ACAACGC CGATGTT GCCAATG)], 'correct tag list after trimming');
  
  $tag_list_lane1 = {'' => 'TAGCTTGT', 2 => 'CGATGTTT', 3 => 'GCCAATGT',};
  ($index_list, $tag_seq_list) = $create_lane->_process_tag_list( $tag_list_lane1,168);
  is($index_list, undef, 'no index list available');
  is($tag_seq_list, undef, 'no expected tag sequence available');
  
  $tag_list_lane1 = {1 => undef, 2 => 'CGATGTTT', 3 => 'GCCAATGT',};
  ($index_list, $tag_seq_list) = $create_lane->_process_tag_list($tag_list_lane1,168);
  is($tag_seq_list, undef, 'no expected tag sequence available');
  
  $tag_list_lane1 = {1=> 'TAGCTTGT', 2 => 'CGATGTTT', 3 => 'GCCAATGT',};
  ($index_list, $tag_seq_list) = $create_lane->_process_tag_list($tag_list_lane1,168);
  is_deeply($create_lane->_check_tag_length($tag_seq_list,$index_list,168),
    [qw(TAGCTTG CGATGTT GCCAATG)],
    'expected tag sequence length are the same as the index_length after trimming common suffix');

  $create_lane = npg_pipeline::cache::barcodes->new(
      lane_lims    => $lims->{1},
      index_lengths=> [8],
      location     => $dir,
  );
  $tag_list_lane1 = {1=> 'TAGCTTGTTGA', 2 => 'TGCGATGTTAA', 3 => 'GGCCAATGGGG',};
  ($index_list, $tag_seq_list) = $create_lane->_process_tag_list($tag_list_lane1,168);
  is_deeply($create_lane->_check_tag_length($tag_seq_list,$index_list,168),
    [qw(TAGCTTGT TGCGATGT GGCCAATG)],
    'expected tag sequence length are the same as the index_length after trimming common suffix');
  lives_ok {
    $create_lane->generate();
  } q{no croak running generate()};
}

{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/barcodes/samplesheet_batch6532.csv];

  my $lims = st::api::lims->new(id_run => 1234)->children_ia;
  my $create_lane = npg_pipeline::cache::barcodes->new(
      lane_lims    => $lims->{1},
      index_lengths=> [7,7],
      location     => $dir,
  );

  my $tag_list_lane = {1=> 'actgffc-actcfga', 2 => 'aacggfc-aacgfga'};
  my ($index_list, $tag_seq_list) = $create_lane->_process_tag_list($tag_list_lane,168);
  is_deeply($index_list, [1,2], 'dual index');
  is_deeply($tag_seq_list, [qw(actgf-actc aacgg-aacg)], 'correct dual tag list after trimming');
}

{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/barcodes/samplesheet_batch6532.csv];

  my $lims = st::api::lims->new(id_run => 1234)->children_ia;
  my $create_lane = npg_pipeline::cache::barcodes->new(
      lane_lims    => $lims->{1},
      index_lengths=> [7,7],
      location     => $dir,
  );

  my $tag_list_lane = {1=> 'actgffc-actcfga', 2 => 'actgffc-aacgfga'};
  throws_ok { $create_lane->_process_tag_list($tag_list_lane,168) }
    qr{All tags are the same actgffc actgffc}, q{dual tags are not unique - throw error};
}

{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/barcodes/samplesheet_batch6532.csv];

  my $lims = st::api::lims->new(id_run => 1234)->children_ia;
  my $create_lane = npg_pipeline::cache::barcodes->new(
      lane_lims    => $lims->{1},
      index_lengths=> [7,7],
      location     => $dir,
  );

  my $tag_list_lane = {1=> 'nnnnnnn-actcfga', 2 => 'nnnnnnn-aacgfga'};
  my ($index_list, $tag_seq_list) = $create_lane->_process_tag_list($tag_list_lane,168);
  is_deeply($index_list, [1,2], 'dual index');
  is_deeply($tag_seq_list, [qw(-actc -aacg)], 'correct dual tag list after trimming');
}

{
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/barcodes/samplesheet_batch6532.csv];

  my $lims = st::api::lims->new(id_run => 1234)->children_ia;
  my $create_lane = npg_pipeline::cache::barcodes->new(
      lane_lims    => $lims->{1},
      index_lengths=> [7,7],
      location     => $dir,
  );

  my $tag_list_lane = {1=> 'actcfga-nnnnnnn', 2 => 'aacgfga-nnnnnnn'};
  my ($index_list, $tag_seq_list) = $create_lane->_process_tag_list($tag_list_lane,168);
  is_deeply($index_list, [1,2], 'dual index');
  is_deeply($tag_seq_list, [qw(actc- aacg-)], 'correct dual tag list after trimming');
}

 {
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/barcodes/samplesheet_batch6532.csv];  

  my $lims = st::api::lims->new(id_run => 1234)->children_ia;

  my $create_lane = npg_pipeline::cache::barcodes->new(
      lane_lims    => $lims->{1},
      index_lengths=> [8,8],
      location     => $dir,
  );
  my $tag_list_lane_init = {1=> 'ACAACGCAATC', 2 => 'TGCGATGT-TAATTTTT', 3 => 'GGCCAATG-GGGAAAAA',};
  my ($index_list, $tag_seq_list) = $create_lane->_process_tag_list($tag_list_lane_init, 1);
  is_deeply($create_lane->_check_tag_length($tag_seq_list, $index_list, 3),
    [qw(ACAACGCA-TCTTTCCC TGCGATGT-TAATTTTT GGCCAATG-GGGAAAAA)],
    'short spiked phix tag is padded');

  $create_lane = npg_pipeline::cache::barcodes->new(
      lane_lims    => $lims->{1},
      index_lengths=> [14],
      location     => $dir,
  );
  $tag_list_lane_init = {1=> 'ACAACGCAATC', 2 => 'TGCGATGTTAAT', 3 => 'GGCCAATGGGGA',};
  throws_ok { $create_lane->_process_tag_list($tag_list_lane_init, 1) }
    qr/It looks likes the padded sequence for spiked PhiX ACAACGCAATCAT is too short/,
    'error when spiked phix tag is not long enough';  
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

  my $tags_length_checked;
  $create_lane  = npg_pipeline::cache::barcodes->new(
      lane_lims     => $lims->{1},
      index_lengths=> [5],
      location     => $dir,
  );
  my $expected_tag_results = [ qw{ AAAAA AAAAA AAAAA AAAAA } ];
  lives_ok {
    $tags_length_checked = $create_lane->_check_tag_length( [
      qw{ AAAAA AAAAA AAAAA AAAAA }
    ] , [ qw{1 2 3 4} ] , 168 );
  } q{tags returned ok};
  is_deeply( $tags_length_checked, $expected_tag_results, q{tags all same size, same as index length} );
  lives_ok {
    $tags_length_checked = $create_lane->_check_tag_length( [
      qw{ AAAAAC AAAAAC AAAAAC AAAAAC }
    ] , [ qw{1 2 3 4} ] , 168 );
  } q{tags returned ok};
  is_deeply( $tags_length_checked, $expected_tag_results, q{tags all same size, longer than index length} );
  lives_ok {
    $tags_length_checked = $create_lane->_check_tag_length( [
      qw{ AAAAA AAAAA AAAAA AAAAACCCC }
    ] , [ qw{1 2 3 4} ] , 168 );
  } q{tags returned ok};
  is_deeply( $tags_length_checked, $expected_tag_results, q{short tags equal to index length, one tag longer than index length} );

  $create_lane  = npg_pipeline::cache::barcodes->new(
      lane_lims     => $lims->{1},
      index_lengths=> [6],
      location     => $dir,
  );
  lives_ok {
    $tags_length_checked = $create_lane->_check_tag_length( [
      qw{ AAAAA AAAAA AAAAA AAAAACCCC }
    ] , [ qw{1 2 3 4} ] , 168 );
  } q{tags returned ok};
  is_deeply( $tags_length_checked, [qw{AAAAAA AAAAAA AAAAAA AAAAAC}], q{short tags shorter than index length, one tag longer than index length});
  lives_ok {
    $tags_length_checked = $create_lane->_check_tag_length( [
      qw{ AAAAA AAAAA AAAAA AAAAACCCC }
    ] , [ qw{1 2 3 168} ] , 168 );
  } q{tags returned ok};
  is_deeply( $tags_length_checked, [qw{AAAAAA AAAAAA AAAAAA AAAAAC}], q{short tags shorter than index length, one tag longer than index length is PhiX} );
  lives_ok {
    $tags_length_checked = $create_lane->_check_tag_length( [
      qw{ AAAAA AAAAA AAAAACCCC AAAAACCCC }
    ] , [ qw{1 2 168 168} ] , 168 ) ;
  } q{tags returned ok};
  is_deeply( $tags_length_checked, [ qw{AAAAAA AAAAAA AAAAAC AAAAAC}], q{short tags shorter than index length, multiple tags longer than index length all PhiX});

  $create_lane  = npg_pipeline::cache::barcodes->new(
      lane_lims     => $lims->{1},
      index_lengths=> [12],
      location     => $dir,
  );
  throws_ok {
    $tags_length_checked = $create_lane->_check_tag_length( [
      qw{ AAAAA AAAAA AAAAA AAAAACCCC }
    ] , [ qw{1 2 3 4} ] , 168 ) ;
  } qr{AAAAAAT:AAAAAAT:AAAAAAT:AAAAACCCCAT}, q{2 different lengths, longest shorter than the index_length};
  lives_ok {
    $tags_length_checked = $create_lane->_check_tag_length( [
      qw{ AAAAAAT AAAAAAT AAAAAAT AAAAACCCCAT }
    ] , [ qw{1 2 3 168} ] , 168 ) ;
  } q{tags returned ok};
  is_deeply( $tags_length_checked, [qw{AAAAAATAT AAAAAATAT AAAAAATAT AAAAACCCC}], q{2 different lengths, one longer tag shorter than the index_length is PhiX} );
  throws_ok {
    $tags_length_checked = $create_lane->_check_tag_length( [
      qw{ AAAAAAT AAAAACCCCAT AAAAAAT AAAAACCCCAT }
    ] , [ qw{1 168 3 168} ] , 168 ) ;
  } qr{AAAAAATAT:AAAAACCCCATA:AAAAAATAT:AAAAACCCCATA}, q{2 different lengths, multiple longer tags shorter than the index_length all PhiX};

  $create_lane  = npg_pipeline::cache::barcodes->new(
      lane_lims    => $lims->{1},
      index_lengths=> [9],
      location     => $dir,
  );
  throws_ok {
    $tags_length_checked = $create_lane->_check_tag_length( [
      qw{ AAAAA AAAAACCCC AAAAA AAAAACCCC }
    ] , [ qw{1 2 3 4} ] , 168 ) ;
  } qr{AAAAAAT:AAAAACCCC:AAAAAAT:AAAAACCCC}, q{2 different lengths, more than one longest};

  $create_lane  = npg_pipeline::cache::barcodes->new(
      lane_lims    => $lims->{1},
      index_lengths=> [9],
      location     => $dir,
  );
  throws_ok {
    $tags_length_checked = $create_lane->_check_tag_length( [
      qw{ AAAAA AAAAA AAAAA AAAA }
    ] , [ qw{1 2 3 4} ] , 168 ) ;
  } qr{AAAAAAT:AAAAAAT:AAAAAAT:AAAAAT}, q{2 different lengths, only one shortest};
  throws_ok {
    $tags_length_checked = $create_lane->_check_tag_length( [
      qw{ AAAAA AAAAACCCC AAAAA AAAAACCCC }
    ] , [ qw{168 2 168 4} ] , 168 ) ;
  } qr{AAAAAAT:AAAAACCCC:AAAAAAT:AAAAACCCC}, q{2 different lengths, multiple shortest all PhiX};
  throws_ok {
    $tags_length_checked = $create_lane->_check_tag_length( [
      qw{ AAAAAC AAAAA AAAAA AAAA }
    ] , [ qw{1 2 3 4} ] , 168 ) ;
  } qr{AAAAACAT:AAAAAAT:AAAAAAT:AAAAAT}, q{3 different lengths};

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
  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q[t/data/barcodes/samplesheet_batch42225_amended3.csv];

  my $expected_tag_list = "$dir/lane_1.taglist";
  unlink  $expected_tag_list;

  my $lims = st::api::lims->new(id_run => 18124)->children_ia;
  my $create_lane  = npg_pipeline::cache::barcodes->new(
      lane_lims     => $lims->{1},
      index_lengths=> [6,10],
      location     => $dir,
      i5opposite   => 0,
  );

  my $tag_list;
  lives_ok {
    $tag_list = $create_lane->generate();
  } q{i5opposite dual index no croak running generate() for batch 42227};

  is($tag_list, $expected_tag_list, 'i5opposite dual index tag list file path');
  my $file_contents;
  lives_ok {$file_contents = read_file($tag_list);} 'i5opposite dual index reading tag list file';
  my $expected = qq[barcode_sequence\tbarcode_name\tlibrary_name\tsample_name\tdescription\nATTACT-TATAGCCTAC\t1\t15144164\t3165STDY6250498\tHX Test Plan: Development of sequencing and library prep protocols using Human DNA \nACAACG-TCTTTCCCTA\t888\t12172503\tphiX_for_spiked_buffers\tIllumina Controls: SPIKED_CONTROL];
  is($file_contents, $expected, 'i5opposite dual index tag list file contents as expected');
}

1;

