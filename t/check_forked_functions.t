#!perl -T

use 5.10.1;
use strict;
use warnings;
use Test::More;

unless ( $ENV{RELEASE_TESTING} ) {
    plan( skip_all => "Author tests not required for installation" );
}
else {
    plan tests => 1;
    
}


my @unicode_cut;
my @unicode_sprintf;


open my $fh, '<', 'lib/Term/Choose.pm' or die $!;
while ( readline $fh ) {
    if ( /\Asub _unicode_cut/ .. /\A\}/ ) {
        push @unicode_cut, $_;
    }    
    if ( /\Asub _unicode_sprintf/ .. /\A\}/ ) {
        push @unicode_sprintf, $_;
    }    
}
close $fh or die $!;



my @c = map { s/\A\s+//; s/\$arg->\{length_longest\}/---/; s/\$arg->\{maxcols\}/---/; s/\$length/---/; $_ } @unicode_cut[8..27];
my @s = map { s/\A\s+//; s/\$arg->\{length_longest\}/---/; s/\$arg->\{maxcols\}/---/; s/\$length/---/; $_ } @unicode_sprintf[7..26];

ok( @c ~~ @s, '_unicode... cut ok' );
