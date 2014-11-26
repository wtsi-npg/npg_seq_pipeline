# $Id: 21-references_adapters.t 16979 2013-04-08 10:03:18Z mg8 $
use strict;
use warnings;
use English qw{-no_match_vars};
use Test::More tests => 3;
use npg_pipeline::base;
use t::util;

local $ENV{PATH} = join q[:], q[t/bin], q[t/bin/software/solexa/bin], $ENV{PATH};

{
  my $ref_adapt = npg_pipeline::base->new({
      repository => q{t/data/sequence},
      conf_path => t::util->new()->conf_path(),
      domain => q{test},
    });
  is( $ref_adapt->ref_adapter_pre_exec_string(), q{-E 'npg_pipeline_preexec_references --repository t/data/sequence'}, q{correct ref_adapter_pre_exec_string} );

}

{
  `bin/npg_pipeline_preexec_references --repository t/data/sequence/refs 2>/dev/null`;
  ok( $CHILD_ERROR, qq{failed as could not locate references directory - $CHILD_ERROR} );
}

{
  qx{bin/npg_pipeline_preexec_references --repository t/data/sequence};
  ok( ! $CHILD_ERROR, q{script runs OK} );
}
1;
