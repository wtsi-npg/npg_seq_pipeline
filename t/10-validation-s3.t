use strict;
use warnings;
use Test::More tests => 11;
use Test::Exception;
use Test::Warn;
use Test::Trap qw/ :warn/;
use Log::Log4perl qw/ :levels/;
use File::Copy;
use File::Slurp qw/read_file/;
use File::Which;
use File::Path qw/make_path/;
use Moose::Meta::Class;
use YAML qw/LoadFile Dump/;
use IO::Compress::Bzip2;

use st::api::lims;
use t::util;

use_ok('npg_pipeline::product');
use_ok('npg_pipeline::validation::entity');
use_ok ('npg_pipeline::validation::s3');

my $util = t::util->new();
my $dir  = $util->temp_directory();
Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                          level  => $WARN,
                          file   => join(q[/], $dir, 'logfile'),
                          utf8   => 1});
my $logger = Log::Log4perl->get_logger(q[]);

my $qc_schema = Moose::Meta::Class->create_anon_class(
                  roles => [qw/npg_testing::db/])->new_object()
                ->create_test_db(q[npg_qc::Schema], 't/data/qc_outcomes/fixtures');

my $config_dir = join q[/], $dir, 'config';
my $pr_file = 't/data/release/config/archive_on/product_release.yml';
mkdir $config_dir or die "Failed to create $config_dir";
copy $pr_file, $config_dir;
copy 'data/config_files/general_values.ini', $config_dir;

my $receipts_dir = join q[/], $dir, 'receipts';
mkdir $receipts_dir or die "Failed to create $receipts_dir";
my $cache_dir = join q[/], $dir, 'cache';
mkdir $cache_dir or die "Failed to create $cache_dir";

subtest 'run with no s3 archival' => sub {
  plan tests => 2;

  my $pconfig_content = read_file join(q[/], $config_dir, 'product_release.yml');
  my $study_id = 3573;
  ok ($pconfig_content !~ /study_id: \"$study_id\"/xms,
    'no product release config for this run study');

  local $ENV{NPG_CACHED_SAMPLESHEET_FILE} = q{t/data/miseq/samplesheet_16850.csv};

  my @ets = map {
    npg_pipeline::validation::entity->new(
      staging_archive_root => q[t],
      target_product => npg_pipeline::product->new(
        rpt_list => $_,
        lims     => st::api::lims->new(rpt_list => $_)
      )
    )
  } map { qq[16850:1:$_] } (0 .. 2);

  my $v = npg_pipeline::validation::s3->new(
    product_entities  => \@ets,
    logger            => $logger,
    file_extension    => 'cram',
    conf_path         => $config_dir,
    qc_schema         => $qc_schema
  );
  ok ($v->fully_archived, 'nothing in a run is archivable to s3 - archived');
};

local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
  q{t/data/novaseq/180709_A00538_0010_BH3FCMDRXX/Data/Intensities/} .
  q{BAM_basecalls_20180805-013153/metadata_cache_26291/samplesheet_26291.csv};

my @ets = map {
  npg_pipeline::validation::entity->new(
    staging_archive_root => q[t],
    target_product => npg_pipeline::product->new(
      rpt_list => $_,
      lims     => st::api::lims->new(rpt_list => $_)
    )
  )
} map { qq[26291:1:$_;26291:2:$_] } (0 .. 12,888);

subtest 'checking receipts - errors and boundary cases' => sub {
  plan tests => 6;

  my $v = npg_pipeline::validation::s3->new(
    product_entities  => \@ets,
    logger            => $logger,
    file_extension    => 'cram',
    conf_path         => $config_dir,
    qc_schema         => $qc_schema
  );
  throws_ok { $v->fully_archived }
    qr/Failed to retrieve receipts location/,
    'error if no receipt location configured';

  copy 't/data/release/config/default_s3_archival/product_release.yml',
       $config_dir;
  $v = npg_pipeline::validation::s3->new(
    product_entities  => \@ets,
    logger            => $logger,
    file_extension    => 'cram',
    conf_path         => $config_dir,
    qc_schema         => $qc_schema
  );
  throws_ok { $v->fully_archived }
    qr/Failed to open receipts directory \/tmp\/receipts\/does\/not\/exist for reading/,
   'error if receipts location does not exist';

  my $h = LoadFile($pr_file);
  $h->{study}->[0]->{s3}->{receipts} = $receipts_dir;
  my $file = join q[/], $config_dir, 'product_release.yml';
  open my $fh, '>', $file or die "Failed to open file $file for writing";
  print $fh (Dump $h) or die "Failed to write to $file";
  close $fh or warn "Failed to close file handle to $file";
  
  $v = npg_pipeline::validation::s3->new(
    product_entities  => \@ets,
    logger            => $logger,
    file_extension    => 'cram',
    conf_path         => $config_dir,
    qc_schema         => $qc_schema
  );
  lives_ok {$v->_cached_receipts} 'no error with no receipts';
  is (keys %{$v->_cached_receipts}, 0, 'no receipts cached');
  my $received = 1;
  my $product = $ets[1]->target_product;

  warning_like {$received = $v->_received_by_customer($product)}
    qr/No receipt for file 26291#1.cram/, 'no receipt - warning';
  ok(!$received, 'not received');
};

subtest 'reading receipts' => sub {
  plan tests => 17;

  my $v = npg_pipeline::validation::s3->new(
    product_entities  => \@ets,
    logger            => $logger,
    file_extension    => 'cram',
    conf_path         => $config_dir,
    qc_schema         => $qc_schema
  );
  
  my @paths = map { "$dir/$_" }
              qw/ returned_20-02-04.txt.bz2
                  2019-02-04.txt.bz2
                  returned_2019-02-04.txt
                  returned_2019-02-1.txt.bz2
                  returned_2019-02.txt.bz2 /;
  for my $p (@paths) {
    my $r; 
    warning_like { $r = $v->_read_receipt_file($p) }
      qr/not a receipt/, 'warning about unexpected file name';
    ok (($r and !@{$r}), 'empty array returned'); 
  }
  
  my $p = "$dir/returned_2019-02-04.txt.bz2";
  my $r;
  warning_like { $r = $v->_read_receipt_file($p) }
    qr/Failed to read/, 'failure to read a file warning';
  ok (($r and !@{$r}), 'empty array returned');

  SKIP: {
    skip 'bzcat executable not available', 5 unless which('bzcat');

    open my $fh, '>', $p or die "Failed to open $p for writing";
    print $fh 'some text' or die "Failed to write to $p";
    close $fh or warn "Failed to close a file handle to $p\n";
    warning_like { $r = $v->_read_receipt_file($p) } 
      qr/Failed to read/, 'failure to read a file warning';
    ok (($r and !@{$r}), 'empty array returned');

    my $z = new IO::Compress::Bzip2 $p, AutoClose => 1;
    my $header = 'Some Bucket key     WSI_MD5 SBG_MD5 Match?';
    $z->print($header);
    $z->close();
    warnings_like { $r = $v->_read_receipt_file($p) }
      [qr/Unexpected header $header/, qr/Only header/],
      'warning about a mismatched and only header';
    ok (($r and !@{$r}), 'empty array returned');

    my $content = "Bucket key     WSI_MD5 SBG_MD5 Match?\n" .
                  "First line \n Second line";
    $z = new IO::Compress::Bzip2 $p, AutoClose => 1;
    $z->print($content);
    $z->close();
    is_deeply ($v->_read_receipt_file($p),
      ["First line \n",' Second line'], 'file read correctly');
  }
};

subtest 'parsing receipts' => sub {
  plan tests => 12;

  my $v = npg_pipeline::validation::s3->new(
    product_entities  => \@ets,
    logger            => $logger,
    file_extension    => 'cram',
    conf_path         => $config_dir,
    qc_schema         => $qc_schema
  );

  my $r = " sample/file \t4567  4567\t ";
  throws_ok {$v->_parse_receipt($r)}
    qr/Missing columns or data in '$r'/, 'error parsing receipt';
  $r = " sample/file\t\t4567";
  throws_ok {$v->_parse_receipt($r)}
    qr/Missing columns or data in '$r'/, 'error parsing receipt';

  $r = "27773#9.cram\tf2eb\tf2eb\tcorrect\n";
  throws_ok {$v->_parse_receipt($r)}
    qr/Failed to get sample and file name from '$r'/,
    'error parsing receipt';

  $r = "sample1/27773#9.cram\tf2eb\tf2eb\tcorrect\n";
  my @a = $v->_parse_receipt($r);
  ok ((@a and @a==2), 'two-member list returned');
  is ($a[0], '27773#9.cram', 'file name returned');
  is_deeply ($a[1], {sample => 'sample1', flag => 1},
    'sample and flag returned');

  $r = "sample1/27773#9.cram\tf2eb\tb2eb\tnot correct";
  @a = $v->_parse_receipt($r);
  ok ((@a and @a==2), 'two-member list returned');
  is ($a[0], '27773#9.cram', 'file name returned');
  is_deeply ($a[1], {sample => 'sample1', flag => 0},
    'sample and flag returned');
  
  $r = "sample1/27773#9.cram\tf2eb\tmissing\n";
  @a = $v->_parse_receipt($r);
  ok ((@a and @a==2), 'two-member list returned');
  is ($a[0], '27773#9.cram', 'file name returned');
  is_deeply ($a[1], {sample => 'sample1', flag => 0},
    'sample and flag returned');
};

subtest 'caching receipts' => sub {
  plan tests => 5;

  my $content1 = <<'END_CONTENT1';
Bucket key	WSI_MD5	SBG_MD5	Match?
6005537/27998#20.cram	b60e062281b1f17da4056c9073884856	b60e062281b1f17da4056c9073884856	correct
6007303/27977#12.cram	36a28b5039e54393fcaad5c24f990838	36a28b5039e54393fcaad5c24f990838	correct
6010298/28029#3.cram	a428c5abd61fa7fb0256d2c9fad0e36f	a428c5abd61fa7fb0256d2c9fad0e36f	correct
6011309/28013#18.cram	43d67e73d228e6ad524800b9a498cb20	43d67e73d228e6ad524800b9a498cb20	correct
6012496/27772#17.cram	dba4995012539011b4076a14232a91e0	dba4995012539011b4076a14232a91e0	correct
END_CONTENT1
 
  my $p1 = "$receipts_dir/returned_2019-02-04.txt.bz2";
  my $z = new IO::Compress::Bzip2 $p1, AutoClose => 1;
  $z->print($content1);
  $z->close();

  my $content2 = <<'END_CONTENT2';
Bucket key	WSI_MD5	SBG_MD5	Match?
xxx/27772#17.cram	dba4995012539011b4076a14232a91e0	dba4995012539011b4076a14232a91e0	correct
6013647/28032#8.cram	31d9b2c1a0bad8e556cf92a671d89ccd	31d9b2c1a0bad8e556cf92a671d89ccd	correct
6016940/27772#19.cram	8be03f3e8bd2e921b81bae34b6def980	8be03f3e8bd2e921b81bae34b6def980	correct
6020632/28011#24.cram	0c7e9fc4aef271c179b4acf13a8c6a54	0c7e9fc4aef271c179b4acf13a8c6a54	correct
6021330/27826#7.cram	bd86af24d6dc3adc1194afdda0e5ca51	bd86af24d6dc3adc1194afdda0e5ca51	correct
6022249/28062#16.cram	c91275b3e68e6a1fc5711a1215ac87c6	c91275b3e68e6a1fc5711a1215ac87c6	correct
END_CONTENT2

  my $p2 = "$receipts_dir/returned_2019-02-05.txt.bz2";
  $z = new IO::Compress::Bzip2 $p2, AutoClose => 1;
  $z->print($content2);
  $z->close();

  my $v = npg_pipeline::validation::s3->new(
    product_entities  => \@ets,
    logger            => $logger,
    file_extension    => 'cram',
    conf_path         => $config_dir,
    qc_schema         => $qc_schema
  );
  throws_ok { $v->_cached_receipts() }
    qr/Mismatching sample names for 27772\#17\.cram/,
    'mismatching sample names - error';

  $content2 = <<'END_CONTENT2';
Bucket key	WSI_MD5	SBG_MD5	Match?
6012496/27772#17.cram	dba4995012539011b4076a14232a91e0	dba4995012539011b4076a14232a91e0	not correct
6013647/28032#8.cram	31d9b2c1a0bad8e556cf92a671d89ccd	31d9b2c1a0bad8e556cf92a671d89ccd	correct
6016940/27772#19.cram	8be03f3e8bd2e921b81bae34b6def980	8be03f3e8bd2e921b81bae34b6def980	correct
6020632/28011#24.cram	0c7e9fc4aef271c179b4acf13a8c6a54	0c7e9fc4aef271c179b4acf13a8c6a54	correct
6021330/27826#7.cram	bd86af24d6dc3adc1194afdda0e5ca51	bd86af24d6dc3adc1194afdda0e5ca51	correct
6022249/28062#16.cram	c91275b3e68e6a1fc5711a1215ac87c6	c91275b3e68e6a1fc5711a1215ac87c6	correct
END_CONTENT2

  $z = new IO::Compress::Bzip2 $p2, AutoClose => 1;
  $z->print($content2);
  $z->close();

  $v = npg_pipeline::validation::s3->new(
    product_entities  => \@ets,
    logger            => $logger,
    file_extension    => 'cram',
    conf_path         => $config_dir,
    qc_schema         => $qc_schema
  );
  throws_ok { $v->_cached_receipts() }
    qr/Mismatching flags for 27772\#17\.cram/, 'mismatching flags - error';

  $content2 = <<'END_CONTENT2';
Bucket key	WSI_MD5	SBG_MD5	Match?
6012496/27772#17.cram	dba4995012539011b4076a14232a91e0	dba4995012539011b4076a14232a91e0	correct
6013647/28032#8.cram	31d9b2c1a0bad8e556cf92a671d89ccd	31d9b2c1a0bad8e556cf92a671d89ccd	correct
6016940/27772#19.cram	8be03f3e8bd2e921b81bae34b6def980	8be03f3e8bd2e921b81bae34b6def980	correct
6020632/28011#24.cram	0c7e9fc4aef271c179b4acf13a8c6a54	0c7e9fc4aef271c179b4acf13a8c6a54	correct
6021330/27826#7.cram	bd86af24d6dc3adc1194afdda0e5ca51	missing
6022249/28062#16.cram	c91275b3e68e6a1fc5711a1215ac87c6	missing
END_CONTENT2

  $z = new IO::Compress::Bzip2 $p2, AutoClose => 1;
  $z->print($content2);
  $z->close();

  $v = npg_pipeline::validation::s3->new(
    product_entities  => \@ets,
    logger            => $logger,
    file_extension    => 'cram',
    conf_path         => $config_dir,
    qc_schema         => $qc_schema
  );

  my $expected = {
    '27772#17.cram' => {sample => 6012496, flag => 1},
    '27998#20.cram' => {sample => 6005537, flag => 1},
    '27977#12.cram' => {sample => 6007303, flag => 1},
    '28029#3.cram'  => {sample => 6010298, flag => 1},
    '28013#18.cram' => {sample => 6011309, flag => 1},
    '28032#8.cram'  => {sample => 6013647, flag => 1},
    '27772#19.cram' => {sample => 6016940, flag => 1}, 
    '28011#24.cram' => {sample => 6020632, flag => 1}, 
    '27826#7.cram'  => {sample => 6021330, flag => 0},
    '28062#16.cram' => {sample => 6022249, flag => 0}, 
  };
  
  my $received;
  warning_like { $received = $v->_cached_receipts() }
    qr/Duplicate record for 27772\#17\.cram/, 'Duplicate record warning';
  is_deeply ($received, $expected, 'correct cache created');

  $content2 = <<'END_CONTENT2';
Bucket key	WSI_MD5	SBG_MD5	Match?
6013647/28032#8.cram	31d9b2c1a0bad8e556cf92a671d89ccd	31d9b2c1a0bad8e556cf92a671d89ccd	correct
6016940/27772#19.cram	8be03f3e8bd2e921b81bae34b6def980	8be03f3e8bd2e921b81bae34b6def980	correct
6020632/28011#24.cram	0c7e9fc4aef271c179b4acf13a8c6a54	0c7e9fc4aef271c179b4acf13a8c6a54	correct
6021330/27826#7.cram	bd86af24d6dc3adc1194afdda0e5ca51	bd86af24d6dc3adc1194afdda0e5ca51	not correct
6022249/28062#16.cram	c91275b3e68e6a1fc5711a1215ac87c6	c91275b3e68e6a1fc5711a1215ac87c6	not correct
END_CONTENT2

  $z = new IO::Compress::Bzip2 $p2, AutoClose => 1;
  $z->print($content2);
  $z->close();

  $v = npg_pipeline::validation::s3->new(
    product_entities  => \@ets,
    logger            => $logger,
    file_extension    => 'cram',
    conf_path         => $config_dir,
    qc_schema         => $qc_schema
  );
  $received = $v->_cached_receipts();
  is_deeply ($received, $expected, 'correct cache created');
};

subtest 'checking receipts' => sub {
  plan tests => 2;

  my $content = <<'END_CONTENT';
Bucket key	WSI_MD5	SBG_MD5	Match?
HG00614_1_9uM/26291#1.cram	31d9b2c1a0bad8e556cf92a671d89ccd	31d9b2c1a0bad8e556cf92a671d89ccd	correct
HG00683_1_25uM/26291#2.cram	8be03f3e8bd2e921b81bae34b6def980	missing
END_CONTENT

  my $p = "$receipts_dir/returned_2019-03-05.txt.bz2";
  my $z = new IO::Compress::Bzip2 $p, AutoClose => 1;
  $z->print($content);
  $z->close();

  my $v = npg_pipeline::validation::s3->new(
    product_entities  => \@ets,
    logger            => $logger,
    file_extension    => 'cram',
    conf_path         => $config_dir,
    qc_schema         => $qc_schema
  );

  my $product = $ets[1]->target_product;
  ok ($v->_received_by_customer($product), '26291#1.cram received');
  $product = $ets[2]->target_product;
  ok (!$v->_received_by_customer($product), '26291#2.cram not received');

  unlink $p;
};

subtest 'product failed mqc' => sub {
  plan tests => 9;

  my $v = npg_pipeline::validation::s3->new(
    product_entities  => \@ets,
    logger            => $logger,
    file_extension    => 'cram',
    conf_path         => $config_dir,
    qc_schema         => $qc_schema
  );

  my @ps = ();
  my $p = $ets[10]->target_product; # plex 10, Rejected final
  push @ps, $p;
  my $failed;
   warning_like { $failed = $v->_failed_mqc($p) } qr/failed QC/,
    'warning about failing qc';
  ok ($failed, 'failed mqc with lib qc "Rejected final"');

  $p = $ets[9]->target_product; # plex 9, Rejected final
  push @ps, $p;
  warning_like { $failed = $v->_failed_mqc($p) } qr/did not fail QC/,
    'warning about not failing qc';
  ok (!$failed, 'did not fail mqc with lib qc "Accepted final"');

  $p = $ets[6]->target_product; # plex 6, Undecided final
  push @ps, $p;
  warning_like { $failed = $v->_failed_mqc($p) } qr/did not fail QC/,
    'warning about not failing qc';
  ok (!$failed, 'did not fail mqc with lib qc "Undecided final"');

  $p = $ets[2]->target_product; # plex 2, Rejected preliminary
  push @ps, $p;
  throws_ok { $v->_failed_mqc($p) } qr/not final lib QC outcome/,
    'not final library qc outcome - error';

  my $lane2 = $qc_schema->resultset('MqcOutcomeEnt')->search({id_run => 26291, position => 2});
  $lane2->update({id_mqc_outcome => 4}); # Rejected final
  ok ((4 == scalar grep { $_ } map { trap { $v->_failed_mqc($_) } } @ps), 
   'all products now considered as failed mqc');
  $lane2->update({id_mqc_outcome => 2}); # Rejected preliminary
  throws_ok { $v->_failed_mqc($ps[0]) } qr/not all final seq QC outcomes/,
   'not all lanes have final seq outcome - error';
};

subtest 's3 fully archived' => sub {
  plan tests => 12;
    SKIP: {
      skip 'bzcat executable not available', 12 unless which('bzcat');
  my $content = <<'END_CONTENT';
Bucket key	WSI_MD5	SBG_MD5	Match?
HG00614_1_9uM/26291#1.cram	31d9b2c1a0bad8e556cf92a671d89ccd	31d9b2c1a0bad8e556cf92a671d89ccd	correct
HG00683_1_25uM/26291#2.cram	8be03f3e8bd2e921b81bae34b6def980	8be03f3e8bd2e921b81bae34b6def980	correct
NA19240_F12_25uM/26291#3.cram	3704e9447532487fcb00e61fe52378e2	3704e9447532487fcb00e61fe52378e2	correct
NA12892_E4_25uM/26291#4.cram	242a93ef2f8d1fc6856e79935e0061cd	242a93ef2f8d1fc6856e79935e0061cd	correct
NA12878_N7_25uM/26291#5.cram	b1b23810910e74fef8e79109a1c9acb5	b1b23810910e74fef8e79109a1c9acb5	correct
NA19238_E2_25uM/26291#6.cram	3a639e356dace78bd8f9e2dc4283d076	3a639e356dace78bd8f9e2dc4283d076	correct
NA12891_F1_25uM/26291#7.cram	d17535538147e3ad8b149f1726f497d7	d17535538147e3ad8b149f1726f497d7	correct
NA19239_C1_25uM/26291#8.cram	1d5189f7f0f9a762bd8c71d7307c7669	1d5189f7f0f9a762bd8c71d7307c7669	correct
HG00614_1_25uM/26291#9.cram	57f40585da201b0b40ebb0dee457abc6	57f40585da201b0b40ebb0dee457abc6	correct
NA19240_F12_25uM/26291#11.cram	81747881edfc68f85e64ab6b20b776d4	81747881edfc68f85e64ab6b20b776d4	correct
NA12892_E4_25uM/26291#12.cram	c78cd8149ba10969e8572bf6e3d2dc20	c78cd8149ba10969e8572bf6e3d2dc20	correct
HG00683_1_25uM/26291#10.cram	867cc0e4631e56f0d142bac64adffc5f	867cc0e4631e56f0d142bac64adffc5f	correct
END_CONTENT

  my $path = "$receipts_dir/returned_2019-03-05.txt.bz2";
  my $z = new IO::Compress::Bzip2 $path, AutoClose => 1;
  $z->print($content);
  $z->close();

  $qc_schema->resultset('MqcOutcomeEnt')->search({id_run => 26291})
            ->update({id_mqc_outcome => 3}); # make al lanes Accepted final

  my $v = npg_pipeline::validation::s3->new(
    product_entities  => \@ets,
    logger            => $logger,
    file_extension    => 'cram',
    conf_path         => $config_dir,
    qc_schema         => $qc_schema
  );
  is (@{$v->product_entities} - @{$v->eligible_product_entities}, 2,
    'eligible product entities contain fewer objects');
  ok($v->fully_archived(), 'archived - all products acknowledged');

  my @lines = split "\n", $content;
  pop @lines; # remove record for tag 10;
  $z = new IO::Compress::Bzip2 $path, AutoClose => 1;
  $z->print(join qq[\n], @lines);
  $z->close();

  my $file = join q[/], $config_dir, 'product_release.yml';
  my $h = LoadFile($file);
  delete $h->{study}->[0]->{merge}->{component_cache_dir};
  open my $fh, '>', $file or die "Failed to open file $file for writing";
  print $fh (Dump $h) or die "Failed to write to $file";
  close $fh or warn "Failed to close file handle to $file";

  $v = npg_pipeline::validation::s3->new(
    product_entities  => \@ets,
    logger            => $logger,
    file_extension    => 'cram',
    conf_path         => $config_dir,
    qc_schema         => $qc_schema
  );
  my $archived;
  warnings_like { $archived = $v->fully_archived() }
    [ qr/No receipt for file 26291\#10\.cram/, qr/failed QC/ ],
    'warning about no receipt and failed qc';
  ok($archived, 'archived - one failed mqc, others are acknowledged');

  my $p = $ets[10]->target_product; # plex 10, Rejected final
  # update lib outcome to final undecided
  $qc_schema->resultset('MqcLibraryOutcomeEnt')
    ->search_via_composition([$p->composition])->update({id_mqc_outcome => 6});
  
  ok (!$v->merge_component_cache_dir($p), 'merge_component_cache_dir is not set');
  warnings_like { $archived = $v->fully_archived() }
    [ qr/No receipt for file 26291\#10\.cram/,
      qr/did not fail QC/,
      qr/product cache directory not configured/ ],
    'warnings about no receipt, not failin qc, no product cache dir';
  ok (!$archived,
    'not archived - is not acknowledged, undecided, no top-up dir configured');

  $file = join q[/], $config_dir, 'product_release.yml';
  $h = LoadFile($file);
  $h->{study}->[0]->{merge}->{component_cache_dir} = $cache_dir;
  open $fh, '>', $file or die "Failed to open file $file for writing";
  print $fh (Dump $h) or die "Failed to write to $file";
  close $fh or warn "Failed to close file handle to $file";

  $v = npg_pipeline::validation::s3->new(
    product_entities  => \@ets,
    logger            => $logger,
    file_extension    => 'cram',
    conf_path         => $config_dir,
    qc_schema         => $qc_schema
  );
  my $product_cache = $v->merge_component_cache_dir($p);
  make_path $product_cache or die "failed to create drectory $product_cache";
  ok ($product_cache, 'merge_component_cache_dir is set');

  $file = join q[/], $product_cache, '26292#10.cram';
  open my $fh1, '>', $file or die "Failed to open file $file for writing";
  print $fh1 'test cram file' or die "Failed to write to $file";
  close $fh1 or warn "Failed to close file handle to $file";  
  warnings_like { $archived = $v->fully_archived() }
    [ qr/No receipt for file 26291\#10\.cram/,
      qr/did not fail QC/,
      qr/cached file $product_cache\/26291\#10\.cram not found/ ],
    'warnings about no receipt, not failin qc, not found in cache dir';
  ok (!$archived, 'not archived - is not acknowledged, undecided, not in top-up');

  $file = join q[/], $product_cache, '26291#10.cram';
  open my $fh2, '>', $file or die "Failed to open file $file for writing";
  print $fh2 'test cram file' or die "Failed to write to $file";
  close $fh2 or warn "Failed to close file handle to $file";

  warnings_like { $archived = $v->fully_archived() }
    [ qr/No receipt for file 26291\#10\.cram/,
      qr/did not fail QC/,
      qr/cached file $product_cache\/26291\#10\.cram found/ ],
    'warnings about no receipt, not failin qc, found in cache dir';
  ok ($archived, 'archived - found in top-up cache');
    }
};

1;
