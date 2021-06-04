use strict;
use warnings;
use Test::More tests => 4;
use Test::Exception;

use_ok(q{npg_pipeline::base_resource});

subtest 'basics' => sub {
  plan tests => 4;
  my $function = npg_pipeline::base_resource->new(
    resource => {
      default => {
        minimum_cpu => 1,
        memory => 10
      }
    }
  );

  my $resources = $function->get_resources;
  is_deeply(
    $resources,
    {
      minimum_cpu => 1,
      memory => 10
    },
    'Default values set by constructor are merged with global defaults'
  );

  $function = npg_pipeline::base_resource->new(
    resource => {
      default => {
        memory => 3,
        minimum_cpu => 1,
        maximum_cpu => 2
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
      minimum_cpu => 1,
      maximum_cpu => 2,
      memory => 3
    },
    'Ensure special resource specs do not pollute regular defaults'
  );

  $resources = $function->get_resources('special');
  is_deeply(
    $resources,
    {
      minimum_cpu => 1,
      maximum_cpu => 2,
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
  plan tests => 20;

  my $function = npg_pipeline::base_resource->new(
    resource => {
      default => {
        minimum_cpu => 1,
        memory => 2
      }
    },
    id_run => 26291
  );
  my $definition = $function->create_definition({
    command => 'echo',
    job_name => 'test',
    identifier => '1234',
  });

  ok($definition, 'Default resources produced a meaningful definition');
  ok($definition->created_on, 'Defaults are set');
  is($definition->command, 'echo', 'Pass through of options');
  is_deeply($definition->num_cpus, [1], 'Default cpu option flattened to single value');
  cmp_ok($definition->memory, '==', 2000, 'Default memory is converted from GB to MB');
  is($definition->identifier, '1234', 'identifier override');

  $definition = $function->create_definition({
    command => 'sleep 1',
    job_name => 'test2',
    memory => 15,
  });

  cmp_ok($definition->memory, '==', 15000, 'Resource override from calling code operates');
  is($definition->identifier, '26291', 'default identifier from run id');

  $definition = $function->create_definition({
    minimum_cpu => 2,
    memory => 15,
    excluded => 1,
  });
  ok (!defined $definition->memory, 'memory is not defined');
  ok (!defined $definition->num_cpus, 'number of cpus is not defined');
  ok ($definition->excluded, 'job is excluded');
  is($definition->identifier, '26291', 'default identifier from run id');

  $definition = $function->create_definition({
    excluded => 1,
  });
  ok (!defined $definition->memory, 'memory is not defined');
  ok (!defined $definition->num_cpus, 'number of cpus is not defined');
  ok ($definition->excluded, 'job is excluded');
  is($definition->identifier, '26291', 'default identifier from run id');

  $definition = $function->create_excluded_definition();
  ok (!defined $definition->memory, 'memory is not defined');
  ok (!defined $definition->num_cpus, 'number of cpus is not defined');
  ok ($definition->excluded, 'job is excluded');
  is($definition->identifier, '26291', 'default identifier from run id');
};

subtest 'Multithread definition creation' => sub {
  plan tests => 2;

  my $function = npg_pipeline::base_resource->new(
    resource => {
      default =>{
        minimum_cpu => 2,
        maximum_cpu => 4,
        memory => 2
      }
    },
    id_run => 26291
  );

  my $definition = $function->create_definition({
    command => 'echo "again"',
    job_name => 'test_host_localisation',
    identifier => '3456',
  });

  is_deeply($definition->num_cpus, [2,4], 'Multithread cpu resources');
  cmp_ok($definition->num_hosts, '==', 1, 'Single host encouraged automatically');
};
