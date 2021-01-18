#!/usr/bin/env perl
use Test::More tests => 20;
use strict;
use warnings;
use DateTime;
use Getopt::Long;
use FindBin qw($Bin);
use t::dbic_util;
use lib ( -d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib" ); 
use_ok('npg_pipeline::product::heron::majora', qw/ get_table_info_for_id_run
                                             get_majora_data
                                             json_to_structure
                                             update_metadata
                                             get_id_runs_missing_data
                                             get_id_runs_missing_data_in_last_days/);
#getting simplified json output from file
my $short_json_string;
my $path = 't/data/majora/simplified_majora_output.json';
open(my $fh, '<', $path) or die "can't open file $path";
{
  local $/;
  $short_json_string = <$fh>;
}
close($fh);

#example run with test schema
my $id_run = 35340;

my $npg_tracking_schema=t::dbic_util->new()->test_schema('t/data/dbic_fixtures/');
my $schema_for_fn=t::dbic_util->new()->test_schema_mlwh('t/data/fixtures/mlwh-majora');

my ($fn,$rs) = get_table_info_for_id_run($id_run,$npg_tracking_schema,$schema_for_fn);

ok($fn eq "201102_A00950_0194_AHTJJKDRXX", "folder name is correct");
is($rs, 20, "correct number of rows in result set");

my %ds = json_to_structure($short_json_string,$fn);
my $ds_ref = \%ds;
ok(defined $ds_ref, "structure is produced");
#expected structure from simplified json output
my $short_ds= ({NT1648725O=> {
                          'MILK-AB8E21' =>  {
                                                  central_sample_id =>'MILK-AB8E21',
                                                  submission_org => '1'
                                                 },
                          'MILK-AB7F7A' => {
                                                  central_sample_id =>'MILK-AB7F7A',
                                                  submission_org => '1'
                                                }
                                },
                    NT1648726P=>{
                          'MILK-AB7D8F' =>  {
                                                  central_sample_id => 'MILK-AB7D8F',
                                                  submission_org => undef
                                                },
                          'MILK-AB7772' => {
                                                  central_sample_id => 'MILK-AB7772',
                                                  submission_org => undef
                                                }
                              }
                   });
is_deeply( $ds_ref ,$short_ds,"Data structure is correct format");    

#testing update_metadata method 
#using compact json
my $test_schema_for_json=t::dbic_util->new()->test_schema_mlwh('t/data/fixtures/mlwh-majora');
my $test_rs_for_json =$test_schema_for_json->resultset("IseqProductMetric")->search(
                                                      {"me.id_run" => $id_run},
                                                      {join=>{"iseq_flowcell" => "sample"}});
 
my @cog_val_before_json = (map{$_->iseq_heron_product_metric->cog_sample_meta}$test_rs_for_json->all());
is_deeply([(3)x20],\@cog_val_before_json,"values before update json");

update_metadata($test_rs_for_json,$ds_ref);

my @cog_val_after_json = (map{$_->iseq_heron_product_metric->cog_sample_meta}$test_rs_for_json->all());
is_deeply([(1)x2,(undef)x8,(0)x2,(undef)x8],\@cog_val_after_json,"values after update json");

#updating with no biosample
my %no_biosample_ds = (NT1648725O=> {},NT1648726P=>{});
my $no_biosample_ref = \%no_biosample_ds;
my $test_schema_no_bio=t::dbic_util->new()->test_schema_mlwh('t/data/fixtures/mlwh-majora');
my $test_rs_no_bio =$test_schema_no_bio->resultset("IseqProductMetric")->search(
                                                  {"id_run" => $id_run},
                                                  {join=>{"iseq_flowcell" => "sample"}}); 

my @cog_val_before_no_bio = (map{$_->iseq_heron_product_metric->cog_sample_meta}$test_rs_no_bio->all());
is_deeply([(3)x20],\@cog_val_before_no_bio,"values before update no biosample");

update_metadata($test_rs_no_bio,$no_biosample_ref);

my @cog_val_after_no_bio = (map{$_->iseq_heron_product_metric->cog_sample_meta}$test_rs_no_bio->all());
is_deeply([(undef)x20],\@cog_val_after_no_bio,"values after update no biosample");

#update with empty data structure
my %empty_ds = ();
my $empty_ref = \%empty_ds;
my $test_schema_for_empty=t::dbic_util->new()->test_schema_mlwh('t/data/fixtures/mlwh-majora');
my $test_rs_for_empty =$test_schema_for_empty->resultset("IseqProductMetric")->search(
                                                        {"id_run" => $id_run},
                                                        {join=>{"iseq_flowcell" => "sample"}}); 

my @cog_val_before_empty = (map{$_->iseq_heron_product_metric->cog_sample_meta}$test_rs_for_empty->all());
is_deeply([(3)x20],\@cog_val_before_empty,"values before update empty data structure");

update_metadata($test_rs_for_empty,$empty_ref);

my @cog_val_after_empty = (map{$_->iseq_heron_product_metric->cog_sample_meta}$test_rs_for_empty->all());
is_deeply([(undef)x20],\@cog_val_after_empty,"values after update empty data structure");


#testing the get_id_runs_missing_data for id_runs missing data
my $schema_ids_without_data=t::dbic_util->new()->test_schema_mlwh('t/data/fixtures/mlwh-majora');
my $checking_missing_data_rs = $schema_ids_without_data->resultset('IseqHeronProductMetric')->search({},{join=>q(iseq_product_metric)});

#id_runs with cog_sample_meta = 1 AND climb_upload set
$checking_missing_data_rs->update({cog_sample_meta=>1});

my @ids_cog_not_zero = get_id_runs_missing_data($schema_ids_without_data);
my @empty;

is_deeply(\@ids_cog_not_zero,\@empty, "no id_runs returned when cog_sample_meta is not 0");

#id_runs when _some_ cog_sample_meta=0 or is NULL AND corresponding climb_upload set
## one run all 0
$checking_missing_data_rs->search({'iseq_product_metric.id_run'=>35356})->update({cog_sample_meta=>0});
## one run mix of 0 and NULL
$checking_missing_data_rs->search({'iseq_product_metric.id_run'=>35355, 'iseq_product_metric.position'=>1})->update({cog_sample_meta=>0});
$checking_missing_data_rs->search({'iseq_product_metric.id_run'=>35355, 'iseq_product_metric.position'=>2})->update({cog_sample_meta=>undef});
## one run half NULL, half 1
$checking_missing_data_rs->search({'iseq_product_metric.id_run'=>35348, 'iseq_product_metric.position'=>2, tag_index=>{q(>)=>2}})->update({cog_sample_meta=>undef});
## leaving run 35340 all 1

my @id_zero_set = get_id_runs_missing_data($schema_ids_without_data);
is_deeply(\@id_zero_set,[35348,35355,35356], "id_runs with cog_sample_meta:0 or NULL and climb_upload set returned");
is_deeply([get_id_runs_missing_data($schema_ids_without_data,[0])],[35355,35356], "id_runs with cog_sample_meta:0 and climb_upload set returned");
is_deeply([get_id_runs_missing_data($schema_ids_without_data,[undef])],[35348,35355], "id_runs with cog_sample_meta:NULL and climb_upload set returned");
is_deeply([get_id_runs_missing_data($schema_ids_without_data,[1])],[35340,35348], "id_runs with cog_sample_meta:1 and climb_upload set returned");

# the date in the fixtures is fixed, so we need to work back from "now" when the test is run, to figure out how many days it has been since then
my$ndays = DateTime->now()->delta_days( DateTime->new({year=>2020, month=>11, day=>3}))->in_units('days');
is_deeply([get_id_runs_missing_data_in_last_days($schema_ids_without_data,$ndays)],[35348,35355,35356], "id_runs with cog_sample_meta:0 or NULL and climb_upload in last $ndays");
$ndays-=4;
is_deeply([get_id_runs_missing_data_in_last_days($schema_ids_without_data,$ndays)],[], "id_runs with cog_sample_meta:0 or NULL and climb_upload in last $ndays");



#id_runs with cog_sample_meta = 0 and climb_upload =undef
$checking_missing_data_rs->update({cog_sample_meta=>0});
$checking_missing_data_rs->update({climb_upload=>undef});

my @ids_climb_undef = get_id_runs_missing_data($schema_ids_without_data);

is_deeply(\@ids_climb_undef,\@empty, "no id_runs returned when climb_upload is undef");

#id_runs with cog_sample_meta = 1 and climb_upload = undef
$checking_missing_data_rs->update({cog_sample_meta=>1});

my @ids_climb_undef_cog_set = get_id_runs_missing_data($schema_ids_without_data);

is_deeply(\@ids_climb_undef_cog_set,\@empty, "no id_runs returned when climb_upload is undef and cog_sample_meta is 1");

done_testing();

