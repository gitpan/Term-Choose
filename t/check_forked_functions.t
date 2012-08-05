#!perl -T

use 5.006;
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
my @print_firstline_GC;
my @wr_cell_GC;
my @size_and_layout_GC;
my @unicode_cut;
my @unicode_sprintf;

open my $fh, '<', $Choose_GC or die $!;
while ( readline $fh ) {
    if ( /\Asub Term::Choose::_print_firstline/ .. /\A\}/ ) {
        push @print_firstline_GC, $_;
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
my @print_firstline;
my @wr_cell;
my @size_and_layout;

open $fh, '<', $Choose or die $!;
while ( readline $fh ) {
    if ( /\Asub _print_firstline/ .. /\A\}/ ) {
        push @print_firstline, $_;
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


### print_firstline

splice( @print_firstline, 0, 1 );
splice( @print_firstline, 7, 1 );
splice( @print_firstline, 12, 3 );

splice( @print_firstline_GC, 0, 1 );
splice( @print_firstline_GC, 7, 9 );
splice( @print_firstline_GC, 12, 12 );

#for my $i ( 0 .. $#print_firstline ) {
#    if ( $print_firstline[$i] ne $print_firstline_GC[$i] ) {
#        print "$i no : ", $print_firstline[$i];
#        print "$i gc : ", $print_firstline_GC[$i];
#    }
#}

ok( @print_firstline ~~ @print_firstline_GC, 'fork: print_firstline ok' );

### size_and_layout

splice( @size_and_layout, 0, 1 );
splice( @size_and_layout, 17, 1 );
splice( @size_and_layout, 29, 3 );

splice( @size_and_layout_GC, 0, 1 );
splice( @size_and_layout_GC, 17, 9 );
splice( @size_and_layout_GC, 29, 11 );

#for my $i ( 0 .. $#size_and_layout ) {
#    if ( $size_and_layout[$i] ne $size_and_layout_GC[$i] ) {
#        print "$i no : ", $size_and_layout[$i];
#        print "$i gc : ", $size_and_layout_GC[$i];
#    }
#}

ok( @size_and_layout ~~ @size_and_layout_GC, 'fork: size_and_layout ok' );

### _unicode_cut

my @c = map { s/\A\s+//; s/\$arg->\{length_longest\}/---/; s/\$arg->\{maxcols\}/---/; s/\$length/---/; $_ } @unicode_cut[4..20];
my @s = map { s/\A\s+//; s/\$arg->\{length_longest\}/---/; s/\$arg->\{maxcols\}/---/; s/\$length/---/; $_ }  @unicode_sprintf[7..23];

ok( @c ~~ @s, '_unicode... cut ok' );
