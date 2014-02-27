package Term::Choose::Win32;

use warnings;
use strict;
use 5.10.1;

our $VERSION = '1.075_01';

use Exporter qw(import);
our @EXPORT_OK = qw( __init_term __get_key __get_term_size __term_cursor_position __reset_term );

use Term::Size::Win32    qw( chars );
use Win32::Console       qw( STD_INPUT_HANDLE ENABLE_MOUSE_INPUT ENABLE_PROCESSED_INPUT
                             RIGHT_ALT_PRESSED LEFT_ALT_PRESSED RIGHT_CTRL_PRESSED LEFT_CTRL_PRESSED SHIFT_PRESSED );
use Win32::Console::ANSI qw( :func );

use constant {
    UP                              => "\e[A",
    CR                              => "\r",

    HIDE_CURSOR                     => "\e[?25l",
    SHOW_CURSOR                     => "\e[?25h",

    CLEAR_TO_END_OF_SCREEN          => "\e[0J",
    RESET                           => "\e[0m",
};

use constant {
    NEXT_get_key    => -1,
    CONTROL_SPACE   => 0x00,
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

use constant {
    MOUSE_WHEELED                => 0x0004,
    LEFTMOST_BUTTON_PRESSED      => 0x0001,
    RIGHTMOST_BUTTON_PRESSED     => 0x0002,
    FROM_LEFT_2ND_BUTTON_PRESSED => 0x0004,
};

use constant SHIFTED_MASK => RIGHT_ALT_PRESSED  | LEFT_ALT_PRESSED  |
                             RIGHT_CTRL_PRESSED | LEFT_CTRL_PRESSED |
                             SHIFT_PRESSED;


INIT {
    # MSWin32: ordinary   print "\e(U";
    # causes the the 00-load test to fail
    # workaround:
    print "\e(U";
}


sub __get_key {
    my ( $self ) = @_;
    my @event = $self->{input}->Input;
    my $event_type = shift @event;
    return NEXT_get_key if ! defined $event_type;
    if ( $event_type == 1 ) {
        my ( $key_down, $repeat_count, $v_key_code, $v_scan_code, $char, $ctrl_key_state ) = @event;
        return NEXT_get_key if ! $key_down;
        if ( $char ) {
            if ( $char == 32 && $ctrl_key_state & ( RIGHT_CTRL_PRESSED | LEFT_CTRL_PRESSED ) ) {
                return CONTROL_SPACE;
            }
            else {
                return $char;
            }
        }
        else{
            if ( $ctrl_key_state & SHIFTED_MASK ) {
                return NEXT_get_key;
            }
            elsif ( $v_key_code == VK_PAGE_UP )   { return VK_PAGE_UP }
            elsif ( $v_key_code == VK_PAGE_DOWN ) { return VK_PAGE_DOWN }
            elsif ( $v_key_code == VK_END )       { return VK_END }
            elsif ( $v_key_code == VK_HOME )      { return VK_HOME }
            elsif ( $v_key_code == VK_LEFT )      { return VK_LEFT }
            elsif ( $v_key_code == VK_UP )        { return VK_UP }
            elsif ( $v_key_code == VK_RIGHT )     { return VK_RIGHT }
            elsif ( $v_key_code == VK_DOWN )      { return VK_DOWN }
            elsif ( $v_key_code == VK_INSERT )    { return VK_INSERT }
            elsif ( $v_key_code == VK_DELETE )    { return VK_DELETE }
            else                                  { return NEXT_get_key }
        }
    }
    elsif ( $self->{mouse} && $event_type == 2 ) {
        my( $x, $y, $button_state, $control_key, $event_flags ) = @event;
        my $compat_event_type;
        if ( ! $event_flags ) {
            if ( $button_state & LEFTMOST_BUTTON_PRESSED ) {
                $compat_event_type = 0b0000000; # 1
            }
            elsif ( $button_state & RIGHTMOST_BUTTON_PRESSED ) {
                $compat_event_type = 0b0000010; # 3
            }
            elsif ( $button_state & FROM_LEFT_2ND_BUTTON_PRESSED ) {
                $compat_event_type = 0b0000001; # 2
            }
            else {
                return NEXT_get_key;
            }
        }
        elsif ( $event_flags & MOUSE_WHEELED ) {
            if ( $button_state >> 24 ) {
                $compat_event_type = 0b1000001; # 5
            }
            else {
                $compat_event_type = 0b1000000; # 4
            }
        }
        else {
            return NEXT_get_key;
        }
        return $self->__handle_mouse( $compat_event_type, $x, $y );
    }
    else {
        return NEXT_get_key;
    }
}


sub __init_term {
    my ( $self ) = @_;
    $self->{old_handle} = select( $self->{handle_out} );
    $self->{backup_flush} = $|;
    $| = 1;
    $self->{input} = Win32::Console->new( STD_INPUT_HANDLE );
    $self->{old_in_mode} = $self->{input}->Mode();
    $self->{input}->Mode( !ENABLE_PROCESSED_INPUT )                    if ! $self->{mouse};
    $self->{input}->Mode( !ENABLE_PROCESSED_INPUT|ENABLE_MOUSE_INPUT ) if   $self->{mouse};
    print HIDE_CURSOR if $self->{hide_cursor};
}


sub __reset_term {
    my ( $self, $from_choose ) = @_;
    if ( $from_choose ) {
        #print LEFT x $self->{screen_col}, UP x ( $self->{screen_row} + $self->{nr_prompt_lines} );
        print CR, UP x ( $self->{screen_row} + $self->{nr_prompt_lines} );
        print CLEAR_TO_END_OF_SCREEN;
    }
    print RESET;
    $self->{input}->Mode( $self->{old_in_mode} );
    $self->{input}->Flush;
    # workaround Bug #33513:
    $self->{input}{handle} = undef;
    #
    print SHOW_CURSOR if $self->{hide_cursor};
    $| = $self->{backup_flush};
    select( $self->{old_handle} );
}


sub __get_term_size {
    my ( $self ) = @_;
    my ( $term_width, $term_height ) = chars( $self->{handle_out} );
    return $term_width - 1, $term_height;
}


sub __term_cursor_position {
    my ( $self ) = @_;
    ( $self->{abs_cursor_x}, $self->{abs_cursor_y} ) = Cursor();
    #$self->{abs_cursor_x}--;
    $self->{abs_cursor_y}--;
    $self->{cursor_row} = $self->{screen_row};
}




1;

__END__



=pod

=encoding UTF-8

=head1 NAME

Term::Choose::Win32

=head1 VERSION

Version 1.075_01

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
