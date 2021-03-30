use strict;
use warnings;
use Test::More tests => 5;
use FindBin '$Bin';

use Perl6::Slurp;
use JSON;
use JSON::Validator;


my $schema_file = $Bin.'/../data/json-graph-schema.json';
my $schema = decode_json slurp $schema_file;
my $validator = JSON::Validator->new();
$validator->schema($schema);
ok($validator, 'Instantiated validator with JGF schema');
my $graph = decode_json slurp $Bin.'/../data/config_files/function_list_central.json';
ok($graph, 'Loaded central pipeline def');
my @errors = $validator->validate($graph);

cmp_ok(@errors, '==', 0, 'Central pipeline graph validated');

$graph = decode_json slurp $Bin.'/../data/config_files/function_list_post_qc_review.json';
ok($graph, 'Post-QC review graph loaded');
@errors = $validator->validate($graph);
cmp_ok(@errors, '==', 0, 'Post-QC review graph validated');
