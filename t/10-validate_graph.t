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

my @pipeline_graphs = (
  $Bin.'/../data/config_files/function_list_central.json',
  $Bin.'/../data/config_files/function_list_post_qc_review.json'
);

foreach my $file (@pipeline_graphs) {
  my $graph = decode_json slurp $file;
  ok($graph, 'Loaded pipeline def');
  my @errors = $validator->validate($graph);
  cmp_ok(@errors, '==', 0, "$file pipeline graph validated");
  if (@errors) {
    diag (join "\n", $file, @errors);
  }
}
