use 5.010000;
use strict;
use warnings;
use Test::More;



eval "use Test::CheckManifest 0.9";

plan skip_all => "Test::CheckManifest 0.9 required" if $@;

ok_manifest();
