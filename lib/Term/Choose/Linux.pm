package Term::Choose::Linux;

use warnings;
use strict;
use 5.10.1;

our $VERSION = '1.074_01';

use Exporter qw(import);
our @EXPORT_OK = qw( __init_term __get_key __get_term_size __term_cursor_position __reset_term );

use Term::ReadKey qw( GetTerminalSize ReadKey ReadMode );

use constant {
    UP                              => "\e[A",
    CR                              => "\r",
    GET_CURSOR_POSITION             => "\e[6n",

    HIDE_CURSOR                     => "\e[?25l",
    SHOW_CURSOR                     => "\e[?25h",

    SET_ANY_EVENT_MOUSE_1003        => "\e[?1003h",
    SET_EXT_MODE_MOUSE_1005         => "\e[?1005h",
    SET_SGR_EXT_MODE_MOUSE_1006     => "\e[?1006h",
    UNSET_ANY_EVENT_MOUSE_1003      => "\e[?1003l",
    UNSET_EXT_MODE_MOUSE_1005       => "\e[?1005l",
    UNSET_SGR_EXT_MODE_MOUSE_1006   => "\e[?1006l",

    MAX_ROW_MOUSE_1003              => 223,
    MAX_COL_MOUSE_1003              => 223,

    CLEAR_TO_END_OF_SCREEN          => "\e[0J",
    RESET                           => "\e[0m",
};

use constant {
    NEXT_get_key => -1,
    KEY_BTAB     => 0x08,
    KEY_ESC      => 0x1b,
};

use constant {
    VK_PAGE_UP   => 33,
    VK_PAGE_DOWN => 34,
    VK_END       => 35,
    VK_HOME      => 36,
    VK_LEFT      => 37,
    VK_UP        => 38,
    VK_RIGHT     => 39,
    VK_DOWN      => 40,
    VK_INSERT    => 45,
    VK_DELETE    => 46,
};


sub __get_key {
    my ( $self ) = @_;
    my $c1 = ReadKey( 0 );
    return if ! defined $c1;
    if ( $c1 eq "\e" ) {
        my $c2 = ReadKey( 0.10 );
        if ( ! defined $c2 ) { return KEY_ESC; } # unused
        elsif ( $c2 eq '[' ) {
            my $c3 = ReadKey( 0 );
               if ( $c3 eq 'A' ) { return VK_UP; }
            elsif ( $c3 eq 'B' ) { return VK_DOWN; }
            elsif ( $c3 eq 'C' ) { return VK_RIGHT; }
            elsif ( $c3 eq 'D' ) { return VK_LEFT; }
            elsif ( $c3 eq 'F' ) { return VK_END; }
            elsif ( $c3 eq 'H' ) { return VK_HOME; }
            elsif ( $c3 eq 'Z' ) { return KEY_BTAB; }
            elsif ( $c3 =~ /^[0-9]$/ ) {
                my $c4 = ReadKey( 0 );
                if ( $c4 eq '~' ) {
                       if ( $c3 eq '2' ) { return VK_INSERT; } # unused
                    elsif ( $c3 eq '3' ) { return VK_DELETE; } # unused
                    elsif ( $c3 eq '5' ) { return VK_PAGE_UP; }
                    elsif ( $c3 eq '6' ) { return VK_PAGE_DOWN; }
                    else {
                        return NEXT_get_key;
                    }
                }
                elsif ( $c4 =~ /^[;0-9]$/ ) { # response to "\e[6n"
                    my $abs_curs_y = $c3;
                    my $ry = $c4;
                    while ( $ry =~ m/^[0-9]$/ ) {
                        $abs_curs_y .= $ry;
                        $ry = ReadKey( 0 );
                    }
                    return NEXT_get_key if $ry ne ';';
                    my $abs_curs_x = '';
                    my $rx = ReadKey( 0 );
                    while ( $rx =~ m/^[0-9]$/ ) {
                        $abs_curs_x .= $rx;
                        $rx = ReadKey( 0 );
                    }
                    if ( $rx eq 'R' ) {
                        $self->{abs_cursor_y} = $abs_curs_y;
                        $self->{abs_cursor_x} = $abs_curs_x; # unused
                    }
                    return NEXT_get_key;
                }
                else {
                    return NEXT_get_key;
                }
            }
            # http://invisible-island.net/xterm/ctlseqs/ctlseqs.html
            elsif ( $c3 eq 'M' && $self->{mouse} ) {
                my $event_type = ord( ReadKey( 0 ) ) - 32;
                my $x          = ord( ReadKey( 0 ) ) - 32;
                my $y          = ord( ReadKey( 0 ) ) - 32;
                return $self->__handle_mouse( $event_type, $x, $y );
            }
            elsif ( $c3 eq '<' && $self->{mouse} ) {  # SGR 1006
                my $event_type = '';
                my $m1;
                while ( ( $m1 = ReadKey( 0 ) ) =~ m/^[0-9]$/ ) {
                    $event_type .= $m1;
                }
                return NEXT_get_key if $m1 ne ';';
                my $x = '';
                my $m2;
                while ( ( $m2 = ReadKey( 0 ) ) =~ m/^[0-9]$/ ) {
                    $x .= $m2;
                }
                return NEXT_get_key if $m2 ne ';';
                my $y = '';
                my $m3;
                while ( ( $m3 = ReadKey( 0 ) ) =~ m/^[0-9]$/ ) {
                    $y .= $m3;
                }
                return NEXT_get_key if $m3 !~ /^[mM]$/;
                my $button_released = $m3 eq 'm' ? 1 : 0;
                return NEXT_get_key if $button_released;
                return $self->__handle_mouse( $event_type, $x, $y );
            }
            else {
                return NEXT_get_key;
            }
        }
        else {
            return NEXT_get_key;
        }
    }
    else {
        return ord $c1;
    }
};

sub __init_term {
    my ( $self ) = @_;
    $self->{old_handle} = select( $self->{handle_out} );
    $self->{backup_flush} = $|;
    $| = 1;
    if ( $self->{mouse} ) {
        if ( $self->{mouse} == 3 ) {
            my $return = binmode STDIN, ':utf8';
            if ( $return ) {
                print SET_ANY_EVENT_MOUSE_1003;
                print SET_EXT_MODE_MOUSE_1005;
            }
            else {
                $self->{mouse} = 0;
                warn "binmode STDIN, :utf8: $!\n";
                warn "mouse-mode disabled\n";
            }
        }
        elsif ( $self->{mouse} == 4 ) {
            my $return = binmode STDIN, ':raw';
            if ( $return ) {
                print SET_ANY_EVENT_MOUSE_1003;
                print SET_SGR_EXT_MODE_MOUSE_1006;
            }
            else {
                $self->{mouse} = 0;
                warn "binmode STDIN, :raw: $!\n";
                warn "mouse-mode disabled\n";
            }
        }
        else {
            my $return = binmode STDIN, ':raw';
            if ( $return ) {
                print SET_ANY_EVENT_MOUSE_1003;
            }
            else {
                $self->{mouse} = 0;
                warn "binmode STDIN, :raw: $!\n";
                warn "mouse-mode disabled\n";
            }
        }
    }
    print HIDE_CURSOR if $self->{hide_cursor};
    Term::ReadKey::ReadMode( 'ultra-raw' );
};

sub __reset_term {
    my ( $self, $from_choose ) = @_;
    if ( $from_choose ) {
        print CR, UP x ( $self->{screen_row} + $self->{nr_prompt_lines} );
        print CLEAR_TO_END_OF_SCREEN;
    }
    print RESET;
    if ( $self->{mouse} ) {
        binmode STDIN, ':encoding(UTF-8)' or warn "binmode STDIN, :encoding(UTF-8): $!\n";
        print UNSET_EXT_MODE_MOUSE_1005     if $self->{mouse} == 3;
        print UNSET_SGR_EXT_MODE_MOUSE_1006 if $self->{mouse} == 4;
        print UNSET_ANY_EVENT_MOUSE_1003;
    }
    print SHOW_CURSOR if $self->{hide_cursor};
    $| = $self->{backup_flush};
    Term::ReadKey::ReadMode( 'restore' );
    select( $self->{old_handle} );
    if ( $self->{backup_opt} ) {
        my $backup_opt = delete $self->{backup_opt};
        for my $key ( keys %$backup_opt ) {
            $self->{$key} = $backup_opt->{$key};
        }
    }
}


sub __term_cursor_position {
    my ( $self ) = @_;
    $self->{abs_cursor_x} = 0;
    $self->{abs_cursor_y} = 0;
    print GET_CURSOR_POSITION;
    $self->{cursor_row} = $self->{screen_row};
}


sub __get_term_size {
    my ( $self ) = @_;
    return GetTerminalSize( $self->{handle_out} );
}



1;

__END__


=pod

=encoding UTF-8

=head1 NAME

Term::Choose::Linux

=head1 VERSION

Version 1.074_01

=head1 DESCRIPTION

This module is not expected to be directly used by any module other than L<Term::Choose>.

=head1 SEE ALSO

L<Term::Choose>

=head1 AUTHORS

Matthäus Kiem <cuer2s@gmail.com>

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2012-2014 Matthäus Kiem.

This library is free software; you can redistribute it and/or modify it under the same terms as Perl 5.10.0. For
details, see the full text of the licenses in the file LICENSE.

=cut
