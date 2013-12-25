use 5.010001;
use strict;
use warnings;
use Test::More;


use Test::Version 1.001001 qw( version_all_ok ), {
    is_strict   => 1,
    has_version => 1,
};

version_all_ok( 'lib' );

done_testing;