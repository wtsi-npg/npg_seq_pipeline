use strict;
use warnings;
use Test::More tests => 67;
use Test::Warn;
use JSON;
use t::dbic_util;
use Test::Mock::LWP::UserAgent;
use Test::Mock::HTTP::Response;
use Test::Mock::HTTP::Request;
use Log::Log4perl qw(:easy);
use Test::Exception;
use List::MoreUtils qw(uniq);
use_ok('npg_pipeline::product::heron::majora');

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

#setting $ndays variable for getting id_runs from the last 'ndays'
#the date in the fixtures is fixed, so we need to work back from "now" when the test is run,
# to figure out how many days it has been since then
my $ndays = DateTime->now()->delta_days( DateTime->new({year=>2020, month=>11, day=>3}))->in_units('days');


#Adding test schemas
my $npg_tracking_schema=t::dbic_util->new()->test_schema('t/data/dbic_fixtures/');
my $schema_for_fn=t::dbic_util->new()->test_schema_mlwh('t/data/fixtures/mlwh-majora');

## adding MAJORA_DOMAIN environment variable
local $ENV{MAJORA_DOMAIN} = 'https://covid.majora.ironowl.it/';
local $ENV{MAJORA_USER} = 'DUMMYUSER';
local $ENV{MAJORA_TOKEN} = 'DUMMYTOKEN';

#testing input options

lives_ok { npg_pipeline::product::heron::majora->new(update=>1, dry_run=>1) }
  'no error when both update and dry_run attrs are set';

throws_ok { npg_pipeline::product::heron::majora->new(days=>0) }  qr/'days' attribute value should be a positive number/,
'error when days attribute is set to 0';

throws_ok { npg_pipeline::product::heron::majora->new(id_runs=>[1,2], days=>4) }  qr/'id_runs' and 'days' attributes cannot be set at the same time/,
'error when both id_runs and days attrs are defined';

throws_ok { npg_pipeline::product::heron::majora->new(id_runs=>[]) }  qr/'id_runs' attribute cannot be set to an empty array/,
'error when id_runs attribute is  defined as empty array';

my $majora = npg_pipeline::product::heron::majora->new(update=>0);
is_deeply($majora->_majora_update_runs,{}, "Majora_update_runs is empty when update isn't set");

$majora = npg_pipeline::product::heron::majora->new(update=>1,id_runs=>[1,2,3]);
is_deeply($majora->_majora_update_runs,{1=>1,2=>1,3=>1}, "hash of id_runs set to 1 when update is set");

#checking values of id_runs and _majora_update_runs when --update and --days are set
my $init = {
  _npg_tracking_schema       => $npg_tracking_schema,
  _mlwh_schema               => $schema_for_fn,
  days                       => $ndays,
  update                     => 1,
 };

my $rs_runs_missing_data = $schema_for_fn->resultset('IseqHeronProductMetric')->search({},{join=>q(iseq_product_metric)});
$rs_runs_missing_data->update({cog_sample_meta=>undef});

# setting object up with new schema with ndays in
$majora = npg_pipeline::product::heron::majora->new($init);

is_deeply($majora->_majora_update_runs,{35340 =>1,35355=>1,35348=>1,35356=>1},'id_runs for Majora update when days not set is correct');
is_deeply(sort $majora->id_runs,[35340,35348,35355,35356], "id_runs with --update are correct");

#checking values of id_runs and _majora_update_runs when --update is set and days is NOT set
$init = {
  _npg_tracking_schema       => $npg_tracking_schema,
  _mlwh_schema               => $schema_for_fn,
  update                     => 1,
 };
# setting cog_sample meta values to undef
$rs_runs_missing_data = $schema_for_fn->resultset('IseqHeronProductMetric')->search({},{join=>q(iseq_product_metric)});
$rs_runs_missing_data->update({cog_sample_meta=>undef});

# setting object up with new schema with ndays in
$majora = npg_pipeline::product::heron::majora->new($init);
is_deeply($majora->_majora_update_runs,{35340 =>1,35355=>1,35348=>1,35356=>1}, 'id_runs for Majora when only update is set is correct');
is_deeply(sort $majora->id_runs,[35340,35348,35355,35356], "id_runs with only update is set, are correct");

#JSON returned when no folder name is found
my $mock_json_response = qq({"errors": 0, "warnings": 1, "messages": [], "tasks": [], "new": [],"updated": [], "ignored": ["2021FolderNameNotFound"],"request": "1a89b394-d0a4-48c9-84ed-0060fb425f5c", "get": {}, "success": true});

#setting Mock request and response
$Mock_request->mock( as_string    => sub { return q();} );
$Mock_resp->mock( content         => sub {$mock_json_response});
$Mock_resp->mock( decoded_content => sub {$mock_json_response});
$Mock_resp->mock( code            => sub { 200 } );
$Mock_resp->mock( is_error        => sub { return; } );
$init = {
  _npg_tracking_schema    => $npg_tracking_schema,
  _mlwh_schema            => $schema_for_fn,
  user_agent              => $Mock_ua,
};
$majora = npg_pipeline::product::heron::majora->new($init);


my ($fn,$rs) = $majora->get_table_info_for_id_run($id_run);
ok($fn eq "201102_A00950_0194_AHTJJKDRXX", "folder name is correct");
is($rs, 20, "correct number of rows in result set");

my $json_string_no_fn = $majora->get_majora_data('2021FolderNameNotFound');
my $Mock_args = $Mock_request->new_args;
#expected args to pass
my $request = 'HTTP::Request';
my $method = 'POST';
my $url = '/api/v2/process/sequencing/get/';
my $encoded_data = {"run_name"=>["2021FolderNameNotFound"],"token"=>"DUMMYTOKEN","username"=>"DUMMYUSER"};
my $data_to_encode = {%{$encoded_data}};

my $header = [q(Content-Type) => q(application/json; charset=UTF-8)];

# checking args passed are correct
is ($Mock_args->[0],$request, 'First argument is HTTP::Request');
is ($Mock_args->[1],$method, 'method is POST');
is ($Mock_args->[2],$ENV{MAJORA_DOMAIN}.$url, 'URL is correct');
is_deeply($Mock_args->[3],$header, 'Header is correct');
my $Mock_decoded_data = decode_json($Mock_args->[4]); 
is_deeply($Mock_decoded_data->{run_name},$encoded_data->{run_name}, 'run_name is passed correctly');
is($Mock_decoded_data->{username},$encoded_data->{username}, 'username is passed correctly');
is($Mock_decoded_data->{token},$encoded_data->{token}, 'token is passed correctly');

my %ds_no_fn = $majora->json_to_structure($json_string_no_fn,'2021FolderNameNotFound');
my $ds_ref_no_fn = \%ds_no_fn;
is_deeply($ds_ref_no_fn,{}, "When no run found, data structure produced is empty");

my %ds = $majora->json_to_structure($short_json_string,$fn);
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
Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n'});
my $logger = Log::Log4perl->get_logger();
$logger->level($INFO);

#using compact json
my $test_schema_for_json=t::dbic_util->new()->test_schema_mlwh('t/data/fixtures/mlwh-majora');
my $test_rs_for_json =$test_schema_for_json->resultset("IseqProductMetric")->search(
                                                      {"me.id_run" => $id_run},
                                                      {join=>{"iseq_flowcell" => "sample"}});
 
my @cog_val_before_json = (map{$_->iseq_heron_product_metric->cog_sample_meta}$test_rs_for_json->all());
is_deeply([(3)x20],\@cog_val_before_json,"values before update json");

$majora->update_metadata($test_rs_for_json,$ds_ref);

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

$majora->update_metadata($test_rs_no_bio,$no_biosample_ref);

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

$majora->update_metadata($test_rs_for_empty,$empty_ref);

my @cog_val_after_empty = (map{$_->iseq_heron_product_metric->cog_sample_meta}$test_rs_for_empty->all());
is_deeply([(undef)x20],\@cog_val_after_empty,"values after update empty data structure");


#testing the get_id_runs_missing_data for id_runs missing data
my $schema_ids_without_data=t::dbic_util->new()->test_schema_mlwh('t/data/fixtures/mlwh-majora');
$init = {
  _npg_tracking_schema       => $npg_tracking_schema,
  _mlwh_schema               => $schema_ids_without_data,
};
# setting object up with new schema
$majora = npg_pipeline::product::heron::majora->new($init);
my $checking_missing_data_rs = $schema_ids_without_data->resultset('IseqHeronProductMetric')->search({},{join=>q(iseq_product_metric)});

#id_runs with cog_sample_meta = 1 AND climb_upload set
$checking_missing_data_rs->update({cog_sample_meta=>1});

my @ids_cog_not_zero = $majora->get_id_runs_missing_data();
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

my @id_zero_set = $majora->get_id_runs_missing_data();
is_deeply(\@id_zero_set,[35348,35355,35356], "id_runs with cog_sample_meta:0 or NULL and climb_upload set returned");
is_deeply([$majora->get_id_runs_missing_data([0])],[35355,35356], "id_runs with cog_sample_meta:0 and climb_upload set returned");
is_deeply([$majora->get_id_runs_missing_data([undef])],[35348,35355], "id_runs with cog_sample_meta:NULL and climb_upload set returned");
is_deeply([$majora->get_id_runs_missing_data([1])],[35340,35348], "id_runs with cog_sample_meta:1 and climb_upload set returned");

$init = {
  _npg_tracking_schema       => $npg_tracking_schema,
  _mlwh_schema               => $schema_ids_without_data,
  days                       => $ndays
};
# setting object up with new schema with ndays in
$majora = npg_pipeline::product::heron::majora->new($init);
is_deeply([$majora->get_id_runs_missing_data_in_last_days()],[35348,35355,35356], "id_runs with cog_sample_meta:0 or NULL and climb_upload in last $ndays");

$ndays-=4;
$init = {
  _npg_tracking_schema       => $npg_tracking_schema,
  _mlwh_schema               => $schema_ids_without_data,
  days                       => $ndays
};
# setting object up with new schema with new value for ndays
$majora = npg_pipeline::product::heron::majora->new($init);
is_deeply([$majora->get_id_runs_missing_data_in_last_days()],[], "id_runs with cog_sample_meta:0 or NULL and climb_upload in last $ndays");

#id_runs with cog_sample_meta = 0 and climb_upload =undef
$checking_missing_data_rs->update({cog_sample_meta=>0});
$checking_missing_data_rs->update({climb_upload=>undef});

my @ids_climb_undef = $majora->get_id_runs_missing_data();

is_deeply(\@ids_climb_undef,\@empty, "no id_runs returned when climb_upload is undef");

#id_runs with cog_sample_meta = 1 and climb_upload = undef
$checking_missing_data_rs->update({cog_sample_meta=>1});

my @ids_climb_undef_cog_set = $majora->get_id_runs_missing_data();

is_deeply(\@ids_climb_undef_cog_set,\@empty, "no id_runs returned when climb_upload is undef and cog_sample_meta is 1");

#Tests for Majora update sequence runs
$init = {
         _npg_tracking_schema    => $npg_tracking_schema,
         _mlwh_schema            => $schema_for_fn,
         user_agent              => $Mock_ua,
        };

$majora = npg_pipeline::product::heron::majora->new($init);

#majora update --dummy response
$Mock_resp->mock( content  => sub {});
$Mock_resp->mock( code     => sub { 200 } );
$Mock_resp->mock( is_error => sub { return; } );

#updating Majora data for id_run (35340)
my $r35340sub = $majora->update_majora($id_run);
$Mock_args = $Mock_request->new_args; # only catches last use of LWP...

is(scalar @$r35340sub, 4, '4 web requests are made for run 35430');
is_deeply([map{$_->[0]}@$r35340sub], [('POST') x 4], 'all 4 are POSTs');
is_deeply([map{$_->[1]}@$r35340sub], [(q(api/v2/artifact/library/add/)) x 2, (q(api/v2/process/sequencing/add/)) x 2], '2 to library endpoint, 2 to sequencing endpoint');
my@r35340libs = map{$_->[2]}@$r35340sub[0,1];
cmp_ok($r35340libs[0]->{library_layout_config}, 'eq', 'PAIRED', 'library layout is PAIRED');
cmp_ok($r35340libs[0]->{library_seq_kit}, 'eq', 'NEB ULTRA II', 'library kit is NEB ULTRA II');
cmp_ok($r35340libs[0]->{library_seq_protocol}, 'eq', 'TAILING', 'library protocol is TAILING');
is_deeply([sort map{$_->{library_name}}@r35340libs],[qw(NT1648725O NT1648726P)], 'correct lane library names');
cmp_ok((uniq map{$_->{library_primers}} map{@{$_->{biosamples}}} @r35340libs)[0], 'eq', '3', 'artic primer version is 3');
cmp_ok((uniq map{$_->{library_selection}} map{@{$_->{biosamples}}} @r35340libs)[0], 'eq', 'PCR', 'library selecion is PCR');
cmp_ok((uniq map{$_->{library_source}} map{@{$_->{biosamples}}} @r35340libs)[0], 'eq', 'VIRAL_RNA', 'library source is VIRAL_RNA');
cmp_ok((uniq map{$_->{library_strategy}} map{@{$_->{biosamples}}} @r35340libs)[0], 'eq', 'AMPLICON', 'library strategy is AMPLICON');

#expected args
$request = 'HTTP::Request';
$method = 'POST';
$url = 'api/v2/process/sequencing/add/';
$encoded_data = {
                    library_name => 'LIBRARY NAME TEST',
                    runs => [{
                              run_name => '201102_A00950_0194_AHTJJKDRXX',
                              instrument_make => 'ILLUMINA',
                              instrument_model => 'NovaSeq',
                              bioinfo_pipe_version => 'v0.10.0',
                              bioinfo_pipe_name => 'ncov2019-artic-nf',
                            }],
                    token =>"DUMMYTOKEN",
                    username => "DUMMYUSER",
                   };

# alter DB to change primer version so we have a mix of primer versions which we don't currently deal with
my$rs_to_alter_primer_ver=$schema_for_fn->resultset(q(IseqFlowcell))->search({id_pool_lims=>[qw(NT1648725O NT1648726P)]});
$rs_to_alter_primer_ver->first->update({primer_panel=>q(nCoV-2019/V4.1alt)});
throws_ok { $majora->update_majora($id_run) } qr/multiple primer panels/, 'Bail out when multiple primer types.';

# alter DB to change primer version to 4.1alt and /B variant of 3 which shoudl not be represented.
$rs_to_alter_primer_ver=$schema_for_fn->resultset(q(IseqFlowcell))->search({id_pool_lims=>[qw(NT1648726P)]});
$rs_to_alter_primer_ver->update({primer_panel=>q(nCoV-2019/V4.1alt)});
$rs_to_alter_primer_ver=$schema_for_fn->resultset(q(IseqFlowcell))->search({id_pool_lims=>[qw(NT1648725O)]});
$rs_to_alter_primer_ver->update({primer_panel=>q(nCoV-2019/V3)});
my $r35340newsub;
lives_ok { $r35340newsub=$majora->update_majora($id_run) } 'Continue when single primer type per lane/library.';
my($r35340libNT1648726P) = grep{$_->{library_name} eq q(NT1648726P)} map{$_->[2]}@$r35340newsub[0,1];
cmp_ok((uniq map{$_->{library_primers}} @{$r35340libNT1648726P->{biosamples}})[0], 'eq', '4.1alt', 'artic primer version is 4.1alt');

$data_to_encode = {%{$encoded_data}};
# checking args passed are correct
is ($Mock_args->[0],$request, 'First argument is HTTP::Request');
is ($Mock_args->[1],$method, 'method is POST');
is ($Mock_args->[2],$ENV{MAJORA_DOMAIN}.$url, 'URL is correct');
is_deeply($Mock_args->[3],$header, 'Header is correct');
$Mock_decoded_data = decode_json($Mock_args->[4]);
is_deeply($Mock_decoded_data->{runs},$encoded_data->{runs}, 'run_name is passed correctly');
is($Mock_decoded_data->{username},$encoded_data->{username}, 'username is passed correctly');
is($Mock_decoded_data->{token},$encoded_data->{token}, 'token is passed correctly');




#Warning is returned when different values for pp_name and pp_version
warning_like { $majora->update_majora(35348) }
  qr/Different values found for pp_name and pp_version. Passing empty value/,
  'warning of undef pp_name or pp_version values';
$Mock_args = $Mock_request->new_args;

#bioinfo_pipe_version and bioinfo_pipe_name should both have empty value when multiple values
#found for version and analysis
$encoded_data = {
                    library_name => 'LIBRARY NAME TEST',
                    runs => [{
                              run_name => '201103_A00968_0145_AHTJMFDRXX',
                              instrument_make => 'ILLUMINA',
                              instrument_model => 'NovaSeq',
                              bioinfo_pipe_version => '',
                              bioinfo_pipe_name => '',
                            }],
                    token =>"DUMMYTOKEN",
                    username => "DUMMYUSER",
                   };

$data_to_encode = {%{$encoded_data}};
# checking args passed are correct
is ($Mock_args->[0],$request, 'First argument is HTTP::Request');
is ($Mock_args->[1],$method, 'method is POST');
is ($Mock_args->[2],$ENV{MAJORA_DOMAIN}.$url, 'URL is correct');
is_deeply($Mock_args->[3],$header, 'Header is correct');
$Mock_decoded_data = decode_json($Mock_args->[4]);
is_deeply($Mock_decoded_data->{runs},$encoded_data->{runs}, 'run_name is passed correctly');

is($Mock_decoded_data->{username},$encoded_data->{username}, 'username is passed correctly');
is($Mock_decoded_data->{token},$encoded_data->{token}, 'token is passed correctly');
1;
