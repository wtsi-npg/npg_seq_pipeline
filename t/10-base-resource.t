use strict;
use warnings;
use Test::More tests => 3;
use Test::Exception;

use_ok(q{npg_pipeline::base_resource});

subtest 'basics' => sub {
  plan tests => 4;
  my $function = npg_pipeline::base_resource->new(
    resource => {
      default => {
        memory => 10
      }
    }
  );

  my $resources = $function->get_resources;
  is_deeply(
    $resources,
    {
      low_cpu => 1,
      memory => 10
    },
    'Default values set by constructor are merged with global defaults'
  );

  $function = npg_pipeline::base_resource->new(
    resource => {
      default => {
        memory => 3,
        high_cpu => 2
      },
      special => {
        memory => 5
      }
    }
  );
  $resources = $function->get_resources;
  is_deeply(
    $resources,
    {
      low_cpu => 1,
      high_cpu => 2,
      memory => 3
    },
    'Ensure special resource specs do not pollute regular defaults'
  );

  $resources = $function->get_resources('special');
  is_deeply(
    $resources,
    {
      low_cpu => 1,
      high_cpu => 2,
      memory => 5
    },
    'Check special resource request contains both defaults'
  );
  throws_ok {
    $function->get_resources('fanciful')
  } qr/Tried to get resource spec "fanciful"/,
  'Getting an undefined resource specialisation causes an error';

};

subtest 'Definition creation' => sub {
  plan tests => 6;

  my $function = npg_pipeline::base_resource->new();
  my $definition = $function->create_definition({
    command => 'echo',
    job_name => 'test',
    identifier => '1234'
  });

  ok($definition, 'Default resources produced a meaningful definition');
  ok($definition->created_on, 'Defaults are set');
  is($definition->command, 'echo', 'Pass through of options');
  is_deeply($definition->num_cpus, [1], 'Default cpu option flattened to single value');
  cmp_ok($definition->memory, '==', 2000, 'Default memory is converted from GB to MB');

  $definition = $function->create_definition({
    command => 'sleep 1',
    job_name => 'test2',
    identifier => '2345',
    memory => 15
  });

  cmp_ok($definition->memory, '==', 15000, 'Resource override from calling code operates');
};
