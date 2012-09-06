#!perl -T

use 5.10.1;
use strict;
use warnings;
use Test::More;

unless ( $ENV{RELEASE_TESTING} ) {
    plan( skip_all => "Author tests not required for installation" );
}
else {
    plan tests => 4;
    
}

my $Choose_GC = 'lib/Term/Choose/GC.pm';
my @prepare_promptline_GC;
my @wr_cell_GC;
my @size_and_layout_GC;
my @unicode_cut;
my @unicode_sprintf;

open my $fh, '<', $Choose_GC or die $!;
while ( readline $fh ) {
    if ( /\Asub Term::Choose::_prepare_promptline/ .. /\A\}/ ) {
        push @prepare_promptline_GC, $_;
    }    
    if ( /\Asub Term::Choose::_wr_cell/ .. /\A\}/ ) {
        push @wr_cell_GC, $_;
    }    
    if ( /\Asub Term::Choose::_size_and_layout/ .. /\A\}/ ) {
        push @size_and_layout_GC, $_;
    }
    if ( /\Asub _unicode_cut/ .. /\A\}/ ) {
        push @unicode_cut, $_;
    }    
    if ( /\Asub _unicode_sprintf/ .. /\A\}/ ) {
        push @unicode_sprintf, $_;
    }    
}
close $fh or die $!;


my $Choose = 'lib/Term/Choose.pm';
my @prepare_promptline;
my @wr_cell;
my @size_and_layout;

open $fh, '<', $Choose or die $!;
while ( readline $fh ) {
    if ( /\Asub _prepare_promptline/ .. /\A\}/ ) {
        push @prepare_promptline, $_;
    }    
    if ( /\Asub _wr_cell/ .. /\A\}/ ) {
        push @wr_cell, $_;
    }    
    if ( /\Asub _size_and_layout/ .. /\A\}/ ) {
        push @size_and_layout, $_;
    }    
}
close $fh or die $!;


### wr_cell

splice( @wr_cell, 0, 1 );
splice( @wr_cell, 5, 1 );
splice( @wr_cell, 8, 1 );
splice( @wr_cell, 13, 1 );
splice( @wr_cell, 15, 2 );

splice( @wr_cell_GC, 0, 1 );
splice( @wr_cell_GC, 5, 7 );
splice( @wr_cell_GC, 8, 1 );
splice( @wr_cell_GC, 13, 1 );
splice( @wr_cell_GC, 15, 1 );

#for my $i ( 0 .. $#wr_cell ) {
#    if ( $wr_cell[$i] ne $wr_cell_GC[$i] ) {
#        print "$i no : ", $wr_cell[$i];
#        print "$i gc : ", $wr_cell_GC[$i];
#    }
#}

ok( @wr_cell ~~ @wr_cell_GC, 'frok: wr_cell ok' );


### prepare_promptline

splice( @prepare_promptline, 0, 1 );
splice( @prepare_promptline, 7, 1 );
splice( @prepare_promptline, 12, 3 );

splice( @prepare_promptline_GC, 0, 1 );
splice( @prepare_promptline_GC, 7, 9 );
splice( @prepare_promptline_GC, 12, 11 );

#for my $i ( 0 .. $#prepare_promptline ) {
#    if ( $prepare_promptline[$i] ne $prepare_promptline_GC[$i] ) {
#        print "$i no : ", $prepare_promptline[$i];
#        print "$i gc : ", $prepare_promptline_GC[$i];
#    }
#}

ok( @prepare_promptline ~~ @prepare_promptline_GC, 'fork: prepare_promptline ok' );

### size_and_layout

splice( @size_and_layout, 0, 1 );
splice( @size_and_layout, 16, 1 );
splice( @size_and_layout, 28, 3 );

splice( @size_and_layout_GC, 0, 1 );
splice( @size_and_layout_GC, 16, 9 );
splice( @size_and_layout_GC, 28, 1 );

#for my $i ( 0 .. $#size_and_layout ) {
#    if ( $size_and_layout[$i] ne $size_and_layout_GC[$i] ) {
#        print "$i no : ", $size_and_layout[$i];
#        print "$i gc : ", $size_and_layout_GC[$i];
#    }
#}

ok( @size_and_layout ~~ @size_and_layout_GC, 'fork: size_and_layout ok' );

### _unicode_cut

my @c = map { s/\A\s+//; s/\$arg->\{length_longest\}/---/; s/\$arg->\{maxcols\}/---/; s/\$length/---/; $_ } @unicode_cut[8..27];
my @s = map { s/\A\s+//; s/\$arg->\{length_longest\}/---/; s/\$arg->\{maxcols\}/---/; s/\$length/---/; $_ } @unicode_sprintf[7..26];

ok( @c ~~ @s, '_unicode... cut ok' );
