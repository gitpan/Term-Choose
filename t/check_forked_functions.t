use 5.010000;
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
close $fh;


my @c = map { s/\A\s+//; s/\$available_terminal_width/---/;        s/ terminal / --- /; $_ } @unicode_cut[7..25];
my @s = map { s/\A\s+//; s/\$arg->\{available_column_width\}/---/; s/ column / --- /;   $_ } @unicode_sprintf[6..24];

ok( @c ~~ @s, '_unicode... cut ok' );
