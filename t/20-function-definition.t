use strict;
use warnings;
use Test::More tests => 3;
use Test::Exception;
use JSON;

use_ok ('npg_pipeline::function::definition');

subtest 'constructor and accessors' => sub {
  plan tests => 16;

  my %init = ( created_by   => 'module',
               created_on   => 'June 25th',
               job_name     => 'name',
               identifier   => 2345,
               command      => 'command'
             );

  my $d = npg_pipeline::function::definition->new(\%init);
  isa_ok ($d, 'npg_pipeline::function::definition');
  is ($d->queue(), 'default', 'default queue is set');

  for my $must_have ( qw/job_name identifier command/ ) {
  
    my %i = %init;
    delete $i{$must_have};
    throws_ok {npg_pipeline::function::definition->new(\%i)}
      qr/'$must_have' should be defined/,
      'error if must have attr is not given';
    $i{'excluded'} = 1;
    my $d;
    lives_ok {$d = npg_pipeline::function::definition->new(\%i)}
      'no restriction if function is excluded';
    ok (!$d->has_queue(), 'queue attr is not set');
  }
  
  my %i = %init;
  $i{'array_cpu_limit'} = 4;
  throws_ok {npg_pipeline::function::definition->new(\%i)}
    qr/array_cpu_limit is set, apply_array_cpu_limit should be set to true/,
    'error if apply_array_cpu_limit is false and array_cpu_limit is set';
  $i{'apply_array_cpu_limit'} = 1;
  $i{'queue'} = 'small';
  lives_ok {$d = npg_pipeline::function::definition->new(\%i)}
    'no error if cpu limt attrs are set correctly';
  is ($d->queue(), 'small', 'queue as set');

  $i{'queue'} = 'large';
  throws_ok {npg_pipeline::function::definition->new(\%i)}
    qr/Unrecognised queue \'large\'/,
    'error if queue value is not recognised';
  $i{'queue'} = q[];
  throws_ok {npg_pipeline::function::definition->new(\%i)}
    qr/Unrecognised queue \'\'/,
    'error if queue value is not recognised';
};

subtest 'serialization to JSON' => sub {
  plan tests => 10;

  my $ref = {
              created_by   => 'module',
              created_on   => 'June 25th',
              job_name     => 'name',
              identifier   => 2345,
              command      => 'command'
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
