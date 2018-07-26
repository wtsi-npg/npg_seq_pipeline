use strict;
use warnings;

use Digest::MD5;
use File::Copy;
use File::Path qw[make_path];
use File::Temp;
use Log::Log4perl qw[:easy];
use Test::More tests => 4;
use Test::Exception;
use t::util;

Log::Log4perl->easy_init($ERROR);

local $ENV{NPG_CACHED_SAMPLESHEET_FILE} =
  't/data/novaseq/180709_A00538_0010_BH3FCMDRXX/' .
  'Data/Intensities/BAM_basecalls_20180723-111241/' .
  'metadata_cache_26291/samplesheet_26291.csv';

my $pkg = 'npg_pipeline::function::product_delivery_notifier';
use_ok($pkg);

my $runfolder_path = 't/data/novaseq/180709_A00538_0010_BH3FCMDRXX';
my $timestamp      = '20180701-123456';
my $customer       = 'test_customer';

my $msg_host      = 'test_msg_host';
my $msg_port      = 5672;
my $msg_vhost     = 'test_msg_vhost';
my $msg_exchange  = 'test_msg_exchange';


subtest 'create' => sub {
  plan tests => 2;

  my $notifier;
  lives_ok {
    $notifier = $pkg->new
      (customer_name    => $customer,
       runfolder_path   => $runfolder_path,
       timestamp        => $timestamp,
       message_host     => $msg_host,
       message_port     => $msg_port,
       message_vhost    => $msg_vhost,
       message_exchange => $msg_exchange);
  } 'notifier created ok';

  my $defs = $notifier->create;
  my $num_defs_observed = scalar @{$defs};
  my $num_defs_expected = 12;
  cmp_ok($num_defs_observed, '==', $num_defs_expected,
         "create returns $num_defs_expected definitions");
};

subtest 'commands' => sub {
  plan tests => 60;

  my $notifier = $pkg->new
    (customer_name    => $customer,
     runfolder_path   => $runfolder_path,
     timestamp        => $timestamp,
     message_host     => $msg_host,
     message_port     => $msg_port,
     message_vhost    => $msg_vhost,
     message_exchange => $msg_exchange);

  my $defs = $notifier->create;

  my $i   = 0;
  my $tag = 1;
  foreach my $def (@{$defs}) {
    is($def->created_by, $pkg, "created_by $i is correct");
    is($def->created_on, $timestamp, "created_on $i is correct");
    is($def->identifier, 26291, "identifier $i is set correctl");
    is($def->job_name, "notify_product_delivery_26291_$i",
       "job_name $i is correct");

    is($def->command,
       "notify_delivery.py --host $msg_host --port $msg_port " .
       "--vhost $msg_vhost --exchange $msg_exchange " .
       "$runfolder_path/messages/26291#$tag.msg.json",
       "command $i is correct");
    $i++;
    $tag++;
  }
};

subtest 'no_archival' => sub {
  plan tests => 1;

  my $notifier = $pkg->new
    (customer_name    => $customer,
     runfolder_path   => $runfolder_path,
     timestamp        => $timestamp,
     message_host     => $msg_host,
     message_port     => $msg_port,
     message_vhost    => $msg_vhost,
     message_exchange => $msg_exchange,
     no_s3_archival   => 1);

  my $defs = $notifier->create;
  my $num_defs_observed = scalar @{$defs};
  my $num_defs_expected = 1;
  cmp_ok($num_defs_observed, '==', $num_defs_expected,
         "create returns $num_defs_expected definition");
};
