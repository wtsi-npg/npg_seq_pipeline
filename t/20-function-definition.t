use strict;
use warnings;
use Test::More tests => 3;
use Test::Exception;
use JSON;

use_ok ('npg_pipeline::function::definition');

subtest 'accessors' => sub {
  plan tests => 1;

  my $d = npg_pipeline::function::definition->new(
            created_by   => 'module',
            created_on   => 'June 25th',
            job_name     => 'name',
            identifier   => 2345,
            command      => 'command',
            log_file_dir => '/some/dir'
          );
  isa_ok ($d, 'npg_pipeline::function::definition');
};

subtest 'serialization to JSON' => sub {
  plan tests => 10;

  my $ref = {
              created_by   => 'module',
              created_on   => 'June 25th',
              job_name     => 'name',
              identifier   => 2345,
              command      => 'command',
              log_file_dir => '/some/dir'
            };
  my $d = npg_pipeline::function::definition->new($ref);

  for my $m (qw/pack freeze TO_JSON/) {
    ok ($d->can($m), "'$m' method available");
    lives_ok { $d->$m } "'$m' method can be envoked";
  }

  my $json = JSON->new->convert_blessed;
  my $js;
  lives_ok {$js = $json->pretty->encode({ 'a' => [$d]}) }
    'object can be serialized to JSON as a part of a complex data structure';
  my $h;
  lives_ok { $h = $json->decode($js) }
    'JSON serialization can be converted back to Perl data structure';

  my $d2;
  lives_ok { $d2 = npg_pipeline::function::definition->thaw(
                     $json->encode($h->{'a'}->[0]))}
    'the hash representing our object can be converted into an object';
  isa_ok($d2, 'npg_pipeline::function::definition');
};

1