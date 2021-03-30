use strict;
use warnings;
use Test::More tests => 5;
use FindBin '$Bin';

use Perl6::Slurp;
use JSON::Schema;


my $schema_file = $Bin.'/../data/json-graph-schema.json';
my $schema = slurp $schema_file;
my $validator = JSON::Schema->new($schema);
ok($validator, 'Instantiated validator with JGF schema');

my $graph = slurp $Bin.'/../data/config_files/function_list_central.json';
ok($graph, 'Loaded central pipeline def');
my $result = $validator->validate($graph);

ok($result, 'Central pipeline graph validated');

$graph = slurp $Bin.'/../data/config_files/function_list_post_qc_review.json';
ok($graph, 'Post-QC review graph loaded');
$result = $validator->validate($graph);
ok($result, 'Post-QC review graph validated');
