#!/usr/bin/env perl
use warnings;
use strict;
use 5.10.1;
use Test::More;
use Cwd qw(abs_path);


my $unix = 'lib/Term/Choose.pm';
my $win  = '../Term-Choose-Win32/lib/Term/Choose/Win32.pm';

my ( $hash_unix, $hash_win, $fh );
my @equal = ( qw( _beep _handle_mouse ) );
my @subs  = ( @equal, qw( choose _write_first_screen  _wr_cell _wr_screen ) );

diag( "\n", abs_path( $unix ) );
diag( abs_path( $win ), "\n" );

plan tests => @subs + 1;
plan skip_all => "Could not find 'Term/Choose/Win32.pm'" if ! -f $win;


### equal

for my $sub ( @equal ) {
    open $fh, '<', $unix or die $!;
    while ( <$fh> ) {
        chomp;
        if ( /^sub\s(\Q$sub\E)/ .. /^\}/ ) {
            push @{$hash_unix->{$sub}}, $. . '|' . $_;
        }
    }
    close $fh;
}

for my $sub ( @equal ) {
    open $fh, '<', $win or die $!;
    while ( <$fh> ) {
        chomp;
        if ( /^sub\s(\Q$sub\E)/ .. /^\}/ ) {
            push @{$hash_win->{$sub}}, $. . '|' . $_;
        }
    }
    close $fh;
}


### _write_first_screen

my $open = 0;
open $fh, '<', $unix or die $!;
while ( <$fh> ) {
    chomp;
    if ( /^sub\s(_write_first_screen)/ .. /^\}/ ) {
        next if $_ eq q/        print CLEAR_SCREEN;/;
        next if $_ eq q/        print GO_TO_TOP_LEFT;/;
        next if $_ eq q/        $arg->{abs_cursor_x} = 0;/; #
        next if $_ eq q/        $arg->{abs_cursor_y} = 0;/; #
        next if $_ eq q/        print GET_CURSOR_POSITION;/;
        push @{$hash_unix->{_write_first_screen}}, $. . '|' . $_;
    }
}
close $fh;

open $fh, '<', $win or die $!;
while ( <$fh> ) {
    chomp;
    if ( /^sub\s(_write_first_screen)/ .. /^\}/ ) {
        next if $_ eq q/    $arg->{screen_col} = 0;/;
        next if $_ eq q/        print NL x $arg->{term_height};/;
        next if $_ eq q/        print UP x $arg->{term_height};/;
        next if $_ eq q/        ( $arg->{abs_cursor_x}, $arg->{abs_cursor_y} ) = Cursor();/;
        next if $_ eq q/        #$arg->{abs_cursor_x}--;/;
        next if $_ eq q/        $arg->{abs_cursor_y}--;/;
        s/_get_term_size\(/GetTerminalSize(/;
        s/Term::Choose::_/_/g;
        push @{$hash_win->{_write_first_screen}}, $. . '|' . $_;
    }
}
close $fh;


### choose

open $fh, '<', $unix or die $!;
while ( <$fh> ) {
    chomp;
    if ( /^sub\s(choose)/ .. /^\}/ ) {
        next if $_ eq q/            print CR, UP x ( $arg->{screen_row} + $arg->{nr_prompt_lines} );/;
        push @{$hash_unix->{choose}}, $. . '|' . $_;
    }
}
close $fh;

open $fh, '<', $win or die $!;
while ( <$fh> ) {
    chomp;
    if ( /^sub\s(choose)/ .. /^\}/ ) {
        next if $_ eq q/            print LEFT x $arg->{screen_col}, UP x ( $arg->{screen_row} + $arg->{nr_prompt_lines} );/;
        s/Term::Choose::Win32/Term::Choose/g;
        s/_get_term_size\(/GetTerminalSize(/;
        s/Term::Choose::_/_/g;
        push @{$hash_win->{choose}}, $. . '|' . $_;
    }
}
close $fh;


### _wr_cell

open $fh, '<', $unix or die $!;
while ( <$fh> ) {
    chomp;
    if ( /^sub\s(_wr_cell)/ .. /^\}/ ) {
        push @{$hash_unix->{_wr_cell}}, $. . '|' . $_;
    }
}
close $fh;

open $fh, '<', $win or die $!;
while ( <$fh> ) {
    chomp;
    if ( /^sub\s(_wr_cell)/ .. /^\}/ ) {
        next if $_ eq q/        my $gcs_element = Unicode::GCString->new( $arg->{list}[$arg->{rc2idx}[$row][$col]] );/;
        next if $_ eq q/        $arg->{screen_col} += $gcs_element->columns();/;
        next if $_ eq q/        $arg->{screen_col} += $arg->{length_longest};/;
        s/Term::Choose::_/_/g;
        push @{$hash_win->{_wr_cell}}, $. . '|' . $_;
    }
}
close $fh;


### _wr_screen

open $fh, '<', $unix or die $!;
while ( <$fh> ) {
    chomp;
    if ( /^sub\s(_wr_screen)/ .. /^\}/ ) {
        push @{$hash_unix->{_wr_screen}}, $. . '|' . $_;
    }
}
close $fh;

open $fh, '<', $win or die $!;
while ( <$fh> ) {
    chomp;
    if ( /^sub\s(_wr_screen)/ .. /^\}/ ) {
next if $_ eq q&            $arg->{screen_col} += length sprintf $arg->{pp_printf_fmt}, $arg->{width_pp}, int( $arg->{row_on_top} / $arg->{avail_height} ) + 1, $arg->{pp};&;
next if $_ eq q&            $arg->{screen_col} += length sprintf $arg->{pp_printf_fmt}, $arg->{width_pp}, $arg->{width_pp}, int( $arg->{row_on_top} / $arg->{avail_height} ) + 1;&;
        s/Term::Choose::_/_/g;
        push @{$hash_win->{_wr_screen}}, $. . '|' . $_;
    }
}
close $fh;


### test subs

my $diag = "";
my $error = 0;
for my $sub ( @subs ) {
    for my $i ( 0 .. ( @{$hash_unix->{$sub}} > @{$hash_win->{$sub}} ? $#{$hash_unix->{$sub}} : $#{$hash_unix->{$sub}} ) ) {
       my ( $unix_nr, $unix_row ) = split '\|', $hash_unix->{$sub}[$i];
       my ( $win_nr, $win_row ) = split '\|', $hash_win->{$sub}[$i];
       if ( $unix_row ne $win_row ) {
           #$diag .= sprintf( "unix - %4d: %s\n", $unix_nr, $unix_row ) . sprintf( "win  - %4d: %s\n\n", $win_nr, $win_row );
           $error++;
       }
    }
    ok( $error == 0, $sub ) or diag( $diag );
    $error = 0;
    $diag = "";
}


### POD

my @unix_pod;
my @win_pod;

my $empty = 0;
my $pod = 0;
open $fh, '<', $unix or die $!;
while ( <$fh> ) {
    chomp;
    $pod = 1 if /^=pod/;
    next if ! $pod;
    if ( /^\z/ ) {
        die "two empty lines: unix - $." if $empty;
        $empty++;
        next;
    }
    else {
        $empty = 0;
    }
    if ( /^=head1 SYNOPSIS/ .. /^=head2 Modules/ ) {
        next if /^For OS 'MSWin32' see L<Term::Choose::Win32>\.\z/;
        next if m&L</MOTIVATION>&;
        next if /^1 - mouse mode 1003 enabled\z/;
        next if /^2 - mouse mode 1003 enabled; the output width is limited to 223 print-columns and the height to 223 rows \(mouse mode 1003 doesn't work above 223\)\z/;
        next if /^3 - extended mouse mode \(1005\) - uses utf8\z/;
        next if /^4 - extended SGR mouse mode \(1006\)\z/;
        next if /^If a mouse mode is enabled layers for STDIN are changed. Then before leaving I<choose> as a cleanup STDIN is marked as UTF-8 with ":encoding\(UTF-8\)"\.\z/;
        push @unix_pod, $. . '|' . $_;
    }
    if ( /^=head2 Monospaced font/ .. /^=head2 Escape sequences/ ) {
        push @unix_pod, $. . '|' . $_;
    }
    if ( /^    "\e\[A"      Cursor Up/  .. /^    "\e\[\?25h"   Show Cursor/ ) {
        push @unix_pod, $. . '|' . $_;
    }
    if ( /^=head1 SUPPORT/ .. /^=cut/ ) {
        push @unix_pod, $. . '|' . $_;
    }
}
close $fh;

$empty = 0;
$pod = 0;
open $fh, '<', $win or die $!;
while ( <$fh> ) {
    chomp;
    $pod = 1 if /^=pod/;
    next if ! $pod;
    if ( /^\z/ ) {
        die "two empty lines: win - $." if $empty;
        $empty++;
        next;
    }
    else {
        $empty = 0;
    }
    if ( /^=head1 SYNOPSIS/ .. /^=head2 Modules/ ) {
        next if /^L<Term::Choose::Win32> is intended for 'MSWin32' operating systems\. For other operating system see L<Term::Choose>\.\z/;
        next if m&L<Term::choose/MOTIVATION\|https://metacpan.org/module/Term::Choose#MOTIVATION>&;
        next if /^1 - mouse mode enabled\z/;
        next if /^2 - mouse mode enabled; the output width is limited to 223 print-columns and the height to 223 rows\z/;
        next if /^Mouse mode 3 and 4 behave like mouse mode 1\.\z/;
        s/Term::Choose::Win32/Term::Choose/g;
        push @win_pod, $. . '|' . $_;
    }
    if ( /^=head2 Monospaced font/ .. /^=head2 Escape sequences/ ) {
        s/Term::Choose::Win32/Term::Choose/g;
        push @win_pod, $. . '|' . $_;
    }
    if ( /^    "\e\[A"      Cursor Up/  .. /^    "\e\[\?25h"   Show Cursor/ ) {
        s/Term::Choose::Win32/Term::Choose/g;
        push @win_pod, $. . '|' . $_;
    }
    if ( /^=head1 SUPPORT/ .. /^=cut/ ) {
        s/Term::Choose::Win32/Term::Choose/g;
        s/2013-2014/2012-2014/;
        push @win_pod, $. . '|' . $_;
    }
}
close $fh;


### test POD

$diag = "\n";
$error = 0;
for my $i ( 0 .. ( @unix_pod > @win_pod ? $#unix_pod : $#win_pod ) ) {
    my ( $unix_nr, $unix_row ) = split '\|', $unix_pod[$i];
    my ( $win_nr, $win_row ) = split '\|', $win_pod[$i];
    if ( $unix_row ne $win_row ) {
        $diag .= sprintf( "unix - %4d: %s\n", $unix_nr, $unix_row ) . sprintf( "win  - %4d: %s\n\n", $win_nr, $win_row );
        $error++;
    }
}

ok( $error == 0, "POD" ) or diag( $diag );
diag( "\n" );






__DATA__
