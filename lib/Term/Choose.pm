use warnings;
use strict;
use 5.10.1;
use utf8;
package Term::Choose;

our $VERSION = '0.7.12';
use Exporter 'import';
our @EXPORT_OK = qw(choose);

use Carp;
use Scalar::Util qw(reftype);
use Signals::XSIG;
use Term::ReadKey;

#use warnings FATAL => qw(all);
#use Log::Log4perl qw(get_logger);
#my $log = get_logger("Term::Choose");


use constant {
    ROW         => 0,
    COL         => 1,
};

use constant {
    UP                                 => "\e[A",
    DOWN                               => "\n",
    RIGHT                              => "\e[C",
    CR                                 => "\r",
    GET_CURSOR_POSITION                => "\e[6n",

    HIDE_CURSOR                        => "\e[?25l",
    SHOW_CURSOR                        => "\e[?25h",

    SET_ANY_EVENT_MOUSE_1003           => "\e[?1003h",
    SET_EXT_MODE_MOUSE_1005            => "\e[?1005h",
    SET_SGR_EXT_MODE_MOUSE_1006        => "\e[?1006h",
    UNSET_ANY_EVENT_MOUSE_1003         => "\e[?1003l",
    UNSET_EXT_MODE_MOUSE_1005          => "\e[?1005l",
    UNSET_SGR_EXT_MODE_MOUSE_1006      => "\e[?1006l",

    MAX_MOUSE_1003_ROW                 => 224,
    MAX_MOUSE_1003_COL                 => 224,

    BEEP                               => "\07",
    CLEAR_SCREEN                       => "\e[2J",
    GO_TO_TOP_LEFT                     => "\e[1;1H",
    CLEAR_EOS                          => "\e[0J",
    RESET                              => "\e[0m",
    UNDERLINE                          => "\e[4m",
    REVERSE                            => "\e[7m",
    BOLD                               => "\e[1m",
};

use constant {
    BIT_MASK_xxxxxx11    => 0x03,
    BIT_MASK_xx1xxxxx    => 0x20,
    BIT_MASK_x1xxxxxx    => 0x40,
};

use constant {
    NEXT_getch          => -1,

    CONTROL_c           => 0x03,
    KEY_TAB             => 0x09,
    KEY_ENTER           => 0x0d,
    KEY_ESC             => 0x1b,
    KEY_SPACE           => 0x20,
    KEY_e               => 0x65,
    KEY_h               => 0x68,
    KEY_j               => 0x6a,
    KEY_k               => 0x6b,
    KEY_l               => 0x6c,
    KEY_q               => 0x71,
    KEY_Tilde           => 0x7e,
    KEY_BSPACE          => 0x7f,

    KEY_UP              => 0x1b5b41,
    KEY_DOWN            => 0x1b5b42,
    KEY_RIGHT           => 0x1b5b43,
    KEY_LEFT            => 0x1b5b44,
    KEY_BTAB            => 0x1b5b5a,
};


sub _getch {
    my ( $arg ) = @_;
    my $c = ReadKey 0;
    if ( $c eq "\e" ) {
        my $c = ReadKey 0.10;
        if ( ! defined $c ) { return KEY_ESC; }
        elsif ( $c eq 'A' ) { return KEY_UP; }
        elsif ( $c eq 'B' ) { return KEY_DOWN; }
        elsif ( $c eq 'C' ) { return KEY_RIGHT; }
        elsif ( $c eq 'D' ) { return KEY_LEFT; }
        elsif ( $c eq 'Z' ) { return KEY_BTAB; }
        elsif ( $c eq '[' ) {
            my $c = ReadKey 0;
               if ( $c eq 'A' ) { return KEY_UP; }
            elsif ( $c eq 'B' ) { return KEY_DOWN; }
            elsif ( $c eq 'C' ) { return KEY_RIGHT; }
            elsif ( $c eq 'D' ) { return KEY_LEFT; }
            elsif ( $c eq 'Z' ) { return KEY_BTAB; }
            elsif ( $c eq 'M' && $arg->{mouse_mode} ) {
                # http://invisible-island.net/xterm/ctlseqs/ctlseqs.html
                # http://leonerds-code.blogspot.co.uk/2012/04/wide-mouse-support-in-libvterm.html
                my $event_type  = ord( ReadKey 0 ) - 32;        # byte 4
                my $x           = ord( ReadKey 0 ) - 32;        # byte 5
                my $y           = ord( ReadKey 0 ) - 32;        # byte 6
                my $button_drag = ( $event_type & BIT_MASK_xx1xxxxx ) >> 5;
                my $button_pressed;
                my $low_2_bits = $event_type & BIT_MASK_xxxxxx11;
                if ( $low_2_bits == 3 ) {
                    $button_pressed = 0;
                }
                else {
                    if ( $event_type & BIT_MASK_x1xxxxxx ) {
                        $button_pressed = $low_2_bits + 4; # button 4, 5
                    }
                    else {
                        $button_pressed = $low_2_bits + 1; # button 1, 2, 3
                    }
                }
                return _handle_mouse( $x, $y, $button_pressed, $button_drag, $arg );
            }
            elsif ( $c =~ /\d/ ) {
                my $c1 = ReadKey 0;
                if ( $c1 =~ /[;\d]/ ) {   # cursor-position report, response to \e[6n
                    my $abs_curs_Y = 0 + $c;
                    while ( 1 ) {
                        last if $c1 eq ';';
                        $abs_curs_Y = 10 * $abs_curs_Y + $c1;
                        $c1 = ReadKey 0;
                    }
                    my $abs_curs_X = 0;
                    while ( 1 ) {
                        $c1 = ReadKey 0;
                        last if $c1 !~ /\d/;
                        $abs_curs_X = 10 * $abs_curs_X + $c1;
                    }
                    if ( $c1 eq 'R' ) {
                        $arg->{abs_curs_Y} = $abs_curs_Y;
                        $arg->{abs_curs_X} = $abs_curs_X;
                    }
                    return NEXT_getch;
                }
                # elsif ( $c1 eq '~' ) {
                # }
                else {
                    return NEXT_getch;
                }
            }
            else {
                return NEXT_getch;
            }
        }
        else {
            return NEXT_getch;
        }
    }
    else {
        return ord $c;
    }
}


sub _init_scr {
    my ( $arg ) = @_;
    $arg->{old_handle} = select( $arg->{handle_out} );
    $|++;
    if ( $arg->{mouse_mode} ) {
        if ( $arg->{mouse_mode} == 3 ) {
            my $return = binmode STDIN, ':utf8';
            if ( $return ) {
                print SET_ANY_EVENT_MOUSE_1003;
                print SET_EXT_MODE_MOUSE_1005;
            }
            else {
                $arg->{mouse_mode} = 0;
                warn "binmode STDIN, :utf8: $!\n";
                warn "mouse-mode disabled\n";
            }
        }
        elsif ( $arg->{mouse_mode} == 4 ) {
            my $return = binmode STDIN, ':raw';
            if ( $return ) {
                print SET_ANY_EVENT_MOUSE_1003;
                print SET_SGR_EXT_MODE_MOUSE_1006;
            }
            else {
                $arg->{mouse_mode} = 0;
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
                $arg->{mouse_mode} = 0;
                warn "binmode STDIN, :raw: $!\n";
                warn "mouse-mode disabled\n";
            }
        }
    }
    print HIDE_CURSOR if $arg->{hide_cursor};
    Term::ReadKey::ReadMode 'ultra-raw';
}


sub _end_win {
    my ( $arg ) = @_;
    print CR, UP x ( $arg->{this_cell}[ROW] + $arg->{head} );
    _clear_to_end_of_screen( $arg );
    print RESET;
    if ( $arg->{mouse_mode} ) {
        binmode STDIN, ':encoding(UTF-8)' or warn "binmode STDIN, :encoding(UTF-8): $!\n";
        print UNSET_EXT_MODE_MOUSE_1005         if $arg->{mouse_mode} == 3;
        print UNSET_SGR_EXT_MODE_MOUSE_1006     if $arg->{mouse_mode} == 4;
        print UNSET_ANY_EVENT_MOUSE_1003;
    }
    Term::ReadKey::ReadMode 'restore';
    print SHOW_CURSOR if $arg->{hide_cursor};
    select( $arg->{old_handle} );
}


sub _length_longest {
    my ( $list ) = @_;
    my $longest = length $list->[0];
    for my $str ( @{$list} ) {
        if ( length $str > $longest ) {
            $longest = length $str;
        }
    }
    return $longest;
}


sub _print_firstline {
    my ( $arg ) = @_;
    $arg->{prompt} =~ s/\p{Space}/ /g;
    $arg->{prompt} =~ s/\p{Cntrl}//g;
    $arg->{firstline} = $arg->{prompt};
    if ( defined $arg->{wantarray} && $arg->{wantarray} ) {
        if ( $arg->{prompt} ) {
            $arg->{firstline} = $arg->{prompt} . '  (multiple choice with spacebar)';
            $arg->{firstline} = $arg->{prompt} . ' (multiple choice)' if length $arg->{firstline} > $arg->{maxcols};
        }
        else {
            $arg->{firstline} = '';
        }
    }
    if ( length $arg->{firstline} > $arg->{maxcols} ) {
        $arg->{firstline} = substr( $arg->{prompt}, 0, $arg->{maxcols} );
    }
    print $arg->{firstline};
    $arg->{head} = 1;
}


sub _write_first_screen {
    my ( $arg ) = @_;
    if ( $arg->{clear_screen} ) {
        print CLEAR_SCREEN;
        print GO_TO_TOP_LEFT;
    }
    ( $arg->{maxcols}, $arg->{maxrows} ) = GetTerminalSize( $arg->{handle_out} );
    if ( $arg->{screen_width} ) {
        $arg->{maxcols} = int( ( $arg->{maxcols} / 100 ) * $arg->{screen_width} );
    }
    if ( $arg->{mouse_mode} == 2 ) {
	$arg->{maxcols} = MAX_MOUSE_1003_COL if $arg->{maxcols} > MAX_MOUSE_1003_COL;
        $arg->{maxrows} = MAX_MOUSE_1003_ROW if $arg->{maxrows} > MAX_MOUSE_1003_ROW;
    }
    $arg->{head} = 0;
    $arg->{marked} = [];
    _goto( $arg, $arg->{head}, 0 );
    _clear_to_end_of_screen( $arg );
    _print_firstline( $arg )if $arg->{prompt} ne '0';
    $arg->{maxrows} = $arg->{maxrows} - $arg->{head};
    _size_and_layout( $arg );
    $arg->{maxrows_index} = $arg->{maxrows} - 1;
    $arg->{maxrows_index} = 0 if $arg->{maxrows_index} < 0;
    $arg->{begin_page} = 0;
    $arg->{end_page} = $arg->{maxrows_index};
    $arg->{end_page} = $#{$arg->{rowcol2list}} if $arg->{maxrows_index} > $#{$arg->{rowcol2list}};
    $arg->{page} = 0;
    _wr_screen( $arg );
    print GET_CURSOR_POSITION if $arg->{mouse_mode};  # in: $arg->{abs_curs_X}, $arg->{abs_curs_Y}
    $arg->{size_changed} = 0;
}


sub _copy_orig_list {
    my ( $arg ) = @_;
    if ( defined $arg->{list_to_long} && $arg->{list_to_long} ) {
        return [ map {
            my $copy = $_;
            $copy = ( ! defined $copy ) ? $arg->{undef}         : $copy;
            $copy = ( $copy eq '' )     ? $arg->{empty_string}  : $copy;
            $copy =~ s/\p{Space}/ /g;
            $copy =~ s/\p{Cntrl}//g;
            $copy; # " $copy ";
        } @{$arg->{orig_list}}[ 0 .. $arg->{max_list} - 1 ] ];
    }
    return [ map {
        my $copy = $_;
        $copy = ( ! defined $copy ) ? $arg->{undef}         : $copy;
        $copy = ( $copy eq '' )     ? $arg->{empty_string}  : $copy;
        $copy =~ s/\p{Space}/ /g;
        $copy =~ s/\p{Cntrl}//g;
        $copy; # " $copy ";
    } @{$arg->{orig_list}} ];
}


sub _validate_option {
    my ( $config ) = @_;
    my %validate = (
        prompt           => '',
        right_justify    => qr/\A[01]\z/,
        layout           => qr/\A[0123]\z/,
        vertical         => qr/\A[01]\z/,
        length_longest   => qr/\A[1-9][0-9]{0,8}\z/,
        clear_screen     => qr/\A[01]\z/,
        mouse_mode       => qr/\A[01234]\z/,
        pad              => qr/\A[0-9][0-9]?\z/,
        pad_one_row      => qr/\A[0-9][0-9]?\z/,
        extra_key        => qr/\A[01]\z/,
        beep             => qr/\A[01]\z/,
        empty_string     => '',
        undef            => '',
        max_list         => qr/\A[1-9][0-9]{0,8}\z/,
        screen_width     => qr/\A[1-9][0-9]\z/,
        hide_cursor      => qr/\A[01]\z/,
    );
    my $warn = 0;
    for my $key ( keys %$config ) {
        if ( $validate{$key} ) {
            if ( defined $config->{$key} && $config->{$key} !~ $validate{$key} ) {
                carp "choose: \"$config->{$key}\" not a valid value for option \"$key\". Falling back to default value.";
                $config->{$key} = undef;
                ++$warn;
            }
        }
        elsif ( ! exists $validate{$key} ) {
            carp "choose: \"$key\": no such option";
            delete $config->{$key};
            ++$warn;
        }
    }
    if ( $warn ) {
        print "Press a key to continue";
        my $dummy = <STDIN>;
    }
    return $config;
}


sub _set_layout {
    my ( $wantarray, $config ) = @_;
    my $prompt = ( defined $wantarray ) ? 'Your choice:' : 'Close with ENTER';
    $config = _validate_option( $config // {} );
    $config->{prompt}           //= $prompt;
    $config->{right_justify}    //= 0;
    $config->{layout}           //= 1;
    $config->{vertical}         //= 1;
    $config->{length_longest}   //= undef;
    $config->{clear_screen}     //= 0;
    $config->{mouse_mode}       //= 0;
    $config->{pad}              //= 2;
    $config->{pad_one_row}      //= 3;
    $config->{extra_key}        //= 1;
    $config->{beep}             //= 0;
    $config->{empty_string}     //= '<empty>';
    $config->{undef}            //= '<undef>';
    $config->{max_list}         //= 100_000;
    $config->{screen_width}     //= undef; # 100
    $config->{hide_cursor}      //= 1;
    return $config;
}


sub choose {
    my ( $orig_list, $config ) = @_;
    croak "choose: First argument is not a ARRAY reference" if ! defined $orig_list;
    croak "choose: First argument is not a ARRAY reference" if ! reftype( $orig_list );
    croak "choose: First argument is not a ARRAY reference" if reftype( $orig_list ) ne 'ARRAY';
    if ( defined $config ) {
        croak "choose: Second argument is not a HASH reference." if ! reftype( $config );
        croak "choose: Second argument is not a HASH reference." if reftype( $config ) ne 'HASH'
    }
    if ( ! @$orig_list ) {
        carp "choose: First argument refers to an empty list!";
        return;
    }
    my $wantarray;
    $wantarray = wantarray ? 1 : 0 if defined wantarray;
    my $arg = _set_layout( $wantarray, $config );
    if ( @$orig_list > $arg->{max_list} ) {
        my $list_length = scalar @$orig_list;
        carp "choose: List has $list_length items.\nchoose: \"max_list\" is set to $arg->{max_list} items!\nchoose: The first $arg->{max_list} itmes are used by choose.";
        $arg->{list_to_long} = 1;
        print "Press a key to continue";
        my $dummy = <STDIN>;
    }
    $arg->{orig_list} = $orig_list;
    $arg->{handle_out} = -t \*STDOUT ? \*STDOUT : \*STDERR;
    $arg->{list} = _copy_orig_list( $arg );
    $arg->{length_longest} = _length_longest( $arg->{list} ) if ! defined $arg->{length_longest};
    $arg->{col_width} = $arg->{length_longest} + $arg->{pad};
    $arg->{wantarray} = $wantarray;
    # $arg->{LastEventWasPress} = 0;  # in order to ignore left-over button-ups # orig comment
    $arg->{abs_curs_X} = 0;
    $arg->{abs_curs_Y} = 0;
    $arg->{screen_row} = 0;
    $arg->{this_cell} = [];
    _init_scr( $arg );
    _write_first_screen( $arg );
    $XSIG{WINCH}[5] = sub { $arg->{size_changed} = 1; };
    while ( 1 ) {
        my $c = _getch( $arg );
        next if $c == NEXT_getch;
        next if $c == KEY_Tilde;
        if ( $arg->{size_changed} ) {
            $arg->{list} = _copy_orig_list( $arg );
            _write_first_screen( $arg );
            next;
        }
        # $arg->{rowcol2list} holds the new list (AoA) formated in "_size_and_layout" appropirate to the choosen layout.
        # $arg->{rowcol2list} does not hold the values dircetly but the respective list indexes from the original list.
        # If the original list would be ( 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h' ) and the new formated list should be
        #     a d g
        #     b e h
        #     c f
        # then the $arg->{rowcol2list} would look like this
        #     0 3 6
        #     1 4 7
        #     2 5
        # so e.g. to get the second value in the second row: $arg->{list}[ $arg->{rowcol2list}[1][1] ]
        # $#{$arg->{rowcol2list}} would be the index of the last row of the new list
        # and the index for the last column in the first row should be $#{$arg->{rowcol2list}[0]}.
        given ( $c ) {
            when ( $c == KEY_j || $c == KEY_DOWN ) {
                if ( $#{$arg->{rowcol2list}} == 0 || ! ( $arg->{rowcol2list}[$arg->{this_cell}[ROW]+1] && $arg->{rowcol2list}[$arg->{this_cell}[ROW]+1][$arg->{this_cell}[COL]] ) ) {
                    _beep( $arg );
                }
                else {
                    $arg->{this_cell}[ROW]++;
                    if ( $arg->{this_cell}[ROW] <= $arg->{end_page} ) {
                        _wr_cell( $arg, $arg->{this_cell}[ROW] - 1, $arg->{this_cell}[COL] );
                        _wr_cell( $arg, $arg->{this_cell}[ROW],     $arg->{this_cell}[COL] );
                    }
                    else {
                        $arg->{page} = $arg->{this_cell}[ROW];
                        $arg->{end_page}++;
                        $arg->{begin_page} = $arg->{end_page};
                        $arg->{end_page} = $arg->{end_page} + $arg->{maxrows_index};
                        $arg->{end_page} = $#{$arg->{rowcol2list}} if $arg->{end_page} > $#{$arg->{rowcol2list}};
                        _wr_screen( $arg );
                    }
                }
            }
            when ( $c == KEY_k || $c == KEY_UP ) {
                if ( $arg->{this_cell}[ROW] == 0 ) {
                    _beep( $arg );
                }
                else {
                    $arg->{this_cell}[ROW]--;
                    if ( $arg->{this_cell}[ROW] >= $arg->{begin_page} ) {
                        _wr_cell( $arg, $arg->{this_cell}[ROW] + 1, $arg->{this_cell}[COL] );
                        _wr_cell( $arg, $arg->{this_cell}[ROW],     $arg->{this_cell}[COL] );
                    }
                    else {
                        $arg->{page} = $arg->{this_cell}[ROW] - $arg->{maxrows_index};
                        $arg->{begin_page}--;
                        $arg->{end_page} = $arg->{begin_page};
                        $arg->{begin_page} = $arg->{begin_page} - $arg->{maxrows_index};
                        $arg->{begin_page} = 0 if $arg->{begin_page} < 0;
                        _wr_screen( $arg );
                    }
                }
            }
            when ( $c == KEY_TAB ) {
                if ( $arg->{this_cell}[COL] == $#{$arg->{rowcol2list}[$arg->{this_cell}[ROW]]} && $arg->{this_cell}[ROW] == $#{$arg->{rowcol2list}} ) {
                    _beep( $arg );
                }
                else {
                    if ( $arg->{this_cell}[COL] < $#{$arg->{rowcol2list}[$arg->{this_cell}[ROW]]} ) {
                        $arg->{this_cell}[COL]++;
                        _wr_cell( $arg, $arg->{this_cell}[ROW], $arg->{this_cell}[COL] - 1 );
                        _wr_cell( $arg, $arg->{this_cell}[ROW], $arg->{this_cell}[COL] );
                    }
                    else {
                        $arg->{this_cell}[ROW]++;
                        if ( $arg->{this_cell}[ROW] <= $arg->{end_page} ) {
                            $arg->{this_cell}[COL] = 0;
                            _wr_cell( $arg, $arg->{this_cell}[ROW] - 1, $#{$arg->{rowcol2list}[$arg->{this_cell}[ROW] - 1]} );
                            _wr_cell( $arg, $arg->{this_cell}[ROW],     $arg->{this_cell}[COL] );
                        }
                        else {
                            $arg->{page} = $arg->{this_cell}[ROW];
                            $arg->{end_page}++;
                            $arg->{begin_page} = $arg->{end_page};
                            $arg->{end_page} = $arg->{end_page} + $arg->{maxrows_index};
                            $arg->{end_page} = $#{$arg->{rowcol2list}} if $arg->{end_page} > $#{$arg->{rowcol2list}};
                            $arg->{this_cell}[COL] = 0;
                            _wr_screen( $arg );
                        }
                    }
                }
            }
            when ( ( $c == KEY_BSPACE || $c == KEY_BTAB ) && ( $arg->{this_cell} > 0 ) ) {
                if ( $arg->{this_cell}[COL] == 0 && $arg->{this_cell}[ROW] == 0 ) {
                    _beep( $arg );
                }
                else {
                    if ( $arg->{this_cell}[COL] > 0 ) {
                        $arg->{this_cell}[COL]--;
                        _wr_cell( $arg, $arg->{this_cell}[ROW], $arg->{this_cell}[COL] + 1 );
                        _wr_cell( $arg, $arg->{this_cell}[ROW], $arg->{this_cell}[COL] );
                    }
                    else {
                        $arg->{this_cell}[ROW]--;
                        if ( $arg->{this_cell}[ROW] >= $arg->{begin_page} ) {
                            $arg->{this_cell}[COL] = $#{$arg->{rowcol2list}[$arg->{this_cell}[ROW]]};
                            _wr_cell( $arg, $arg->{this_cell}[ROW] + 1, 0 );
                            _wr_cell( $arg, $arg->{this_cell}[ROW],     $arg->{this_cell}[COL] );
                        }
                        else {
                            $arg->{page} = $arg->{this_cell}[ROW] - $arg->{maxrows_index};
                            $arg->{begin_page}--;
                            $arg->{end_page} = $arg->{begin_page};
                            $arg->{begin_page} = $arg->{begin_page} - $arg->{maxrows_index};
                            $arg->{begin_page} = 0 if $arg->{begin_page} < 0;
                            $arg->{this_cell}[COL] = $#{$arg->{rowcol2list}[$arg->{this_cell}[ROW]]};
                            _wr_screen( $arg );
                        }
                    }
                }
            }
            when ( $c == KEY_l || $c == KEY_RIGHT ) {
                if ( $arg->{this_cell}[COL] == $#{$arg->{rowcol2list}[$arg->{this_cell}[ROW]]} ) {
                    _beep( $arg );
                }
                else {
                    $arg->{this_cell}[COL]++;
                    _wr_cell( $arg, $arg->{this_cell}[ROW], $arg->{this_cell}[COL] - 1 );
                    _wr_cell( $arg, $arg->{this_cell}[ROW], $arg->{this_cell}[COL] );
                }
            }
            when ( $c == KEY_h || $c == KEY_LEFT ) {
                if ( $arg->{this_cell}[COL] == 0 ) {
                    _beep( $arg );
                }
                else {
                    $arg->{this_cell}[COL]--;
                    _wr_cell( $arg, $arg->{this_cell}[ROW], $arg->{this_cell}[COL] + 1 );
                    _wr_cell( $arg, $arg->{this_cell}[ROW], $arg->{this_cell}[COL] );
                }
            }
            when ( $c == KEY_q ) {
                _end_win( $arg );
                return;
            }
            when ( $c == KEY_e ) {
                if ( $arg->{extra_key} ) {
                    _end_win( $arg );
                    exit;
                }
                else {
                    _beep( $arg );
                }
            }
            when ( $c == CONTROL_c ) {
                _end_win( $arg );
                print "^C";
                kill( 'INT', $$ );
                return;
            }
            when ( $c == KEY_ENTER ) {
                my @chosen;
                _end_win( $arg );
                return if ! defined $arg->{wantarray};
                if ( $arg->{wantarray} ) {
                    if ( $arg->{vertical} ) {
                        for my $col ( 0 .. $#{$arg->{rowcol2list}[0]} ) {
                            for my $row ( 0 .. $#{$arg->{rowcol2list}} ) {
                                if ( $arg->{marked}[$row][$col] || [ $row, $col ] ~~ $arg->{this_cell} ) {
                                    my $i = $arg->{rowcol2list}[$row][$col];
                                    #$i //= $row; # ? layout
                                    push @chosen, $arg->{orig_list}[$i];
                                }
                            }
                        }
                    }
                    else {
                         for my $row ( 0 .. $#{$arg->{rowcol2list}} ) {
                            for my $col ( 0 .. $#{$arg->{rowcol2list}[$row]} ) {
                                if ( $arg->{marked}[$row][$col] || [ $row, $col ] ~~ $arg->{this_cell} ) {
                                    my $i = $arg->{rowcol2list}[$row][$col];
                                    #$i //= $row; # ? layout
                                    push @chosen, $arg->{orig_list}[$i];
                                }
                            }
                        }
                    }
                    return @chosen;
                }
                else {
                    my $i = $arg->{rowcol2list}[$arg->{this_cell}[ROW]][$arg->{this_cell}[COL]];
                    return $arg->{orig_list}[$i];
                }
            }
            when ( $c == KEY_SPACE ) {
                if ( defined $arg->{wantarray} && $arg->{wantarray} ) {
                    if ( ! $arg->{marked}[$arg->{this_cell}[ROW]][$arg->{this_cell}[COL]] ) {
                        $arg->{marked}[$arg->{this_cell}[ROW]][$arg->{this_cell}[COL]] = 1;
                    }
                    else {
                        $arg->{marked}[$arg->{this_cell}[ROW]][$arg->{this_cell}[COL]] = 0;
                    }
                    _wr_cell( $arg, $arg->{this_cell}[ROW], $arg->{this_cell}[COL] );
                }
            }
            default {
                _beep( $arg );
            }
        }
    }
    _end_win( $arg );
    warn "choose: shouldn't reach here ...\n";
}


sub _beep {
    my ( $arg ) = @_;
    print BEEP if $arg->{beep};
}


sub _clear_to_end_of_screen {
    my ( $arg ) = @_;
    print CLEAR_EOS;
}


sub _goto {
    my ( $arg, $newrow, $newcol ) = @_;
    print CR, RIGHT x $newcol;
    if ( $newrow > $arg->{screen_row} ) {
        print DOWN x ( $newrow - $arg->{screen_row} );
        $arg->{screen_row} += ( $newrow - $arg->{screen_row} );
    }
    elsif ( $newrow < $arg->{screen_row} ) {
        print UP x ( $arg->{screen_row} - $newrow );
        $arg->{screen_row} -= ( $arg->{screen_row} - $newrow );
    }
}


sub _wr_screen {
    my $arg = shift;
    _goto( $arg, $arg->{head}, 0 );
    _clear_to_end_of_screen( $arg );
    for my $row ( $arg->{begin_page} .. $arg->{end_page} ) {
        for my $col ( 0 .. $#{$arg->{rowcol2list}[$row]} ) {
            _wr_cell( $arg, $row, $col ); # unless [ $row, $col ] ~~ $this_cell;
        }
    }
    _wr_cell( $arg, $arg->{this_cell}[ROW], $arg->{this_cell}[COL] );
}


sub _wr_cell {
    my( $arg, $row, $col ) = @_;
    if ( $#{$arg->{rowcol2list}} == 0 ) {
        my $lngth = 0;
        if ( $col > 0 ) {
            for my $cl ( 0 .. $col - 1 ) {
                $lngth += length $arg->{list}[$arg->{rowcol2list}[$row][$cl]];
                $lngth += $arg->{pad_one_row} // 0;
            }
        }
        _goto( $arg, $row + $arg->{head} - $arg->{page}, $lngth );
        print BOLD, UNDERLINE if $arg->{marked}[$row][$col];
        print REVERSE if [ $row, $col ] ~~ $arg->{this_cell};
        print $arg->{list}[$arg->{rowcol2list}[$row][$col]];
    }
    else {
        _goto( $arg, $row + $arg->{head} - $arg->{page}, $col * $arg->{col_width} );
        print BOLD, UNDERLINE if $arg->{marked}[$row][$col];
        print REVERSE if [ $row, $col ] ~~ $arg->{this_cell};
        printf "%*.*s",  $arg->{length_longest}, $arg->{length_longest}, $arg->{list}[$arg->{rowcol2list}[$row][$col]] if   $arg->{right_justify};
        printf "%-*.*s", $arg->{length_longest}, $arg->{length_longest}, $arg->{list}[$arg->{rowcol2list}[$row][$col]] if ! $arg->{right_justify};
    }
    print RESET if $arg->{marked}[$row][$col] || [ $row, $col ] ~~ $arg->{this_cell};
}


sub _size_and_layout {
    my ( $arg ) = @_;
    my $layout = $arg->{layout};
    $arg->{rowcol2list} = [];
    $arg->{all_in_first_row} = 0;
    if ( $arg->{length_longest} > $arg->{maxcols} ) {
        $arg->{length_longest} = $arg->{maxcols};
        $layout = 3;
    }
    ### layout
    $arg->{this_cell} = [ 0, 0 ];
    my $all_in_first_row;
    if ( $layout == 2 ) {
        $layout = 3 if scalar @{$arg->{list}} <= $arg->{maxrows};
    }
    elsif ( $layout < 2 ) {
        for my $idx ( 0 .. $#{$arg->{list}} ) {
            $all_in_first_row .= $arg->{list}[$idx];
            if ( length $all_in_first_row > $arg->{maxcols} ) {
                $all_in_first_row = '';
                last;
            }
            $all_in_first_row .= ' ' x $arg->{pad_one_row} if $idx < $#{$arg->{list}};
        }
    }
    if ( $all_in_first_row ) {
        $arg->{all_in_first_row} = 1;
        $arg->{rowcol2list}[0] = [ 0 .. $#{$arg->{list}} ];
    }
    elsif ( $layout == 3 ) {
        for my $idx ( 0 .. $#{$arg->{list}} ) {
            if ( length $arg->{list}[$idx] > $arg->{length_longest} ) {
                $arg->{list}[$idx] = substr( $arg->{list}[$idx], 0, $arg->{length_longest} - 3 ) . '...';
            }
            $arg->{rowcol2list}[$idx][0] = $idx;
        }
    }
    else {
        # auto_format
        my $maxcls = $arg->{maxcols};
        if ( ( $arg->{layout} == 1 || $arg->{layout} == 2 ) && $arg->{maxrows} > 0 ) {
            my $tmc = int( @{$arg->{list}} / $arg->{maxrows} );
            $tmc++ if @{$arg->{list}} % $arg->{maxrows};
            $tmc *= $arg->{col_width};
            if ( $tmc < $maxcls ) {
                $tmc = int( $tmc + ( ( $maxcls - $tmc ) / 2 ) ) if $arg->{layout} == 1;
                $tmc = int( $tmc + ( ( $maxcls - $tmc ) / 6 ) ) if $arg->{layout} == 2;
                $maxcls = $tmc;
            }
        }
    ### vertical
        my $cols_per_row = int( $maxcls / $arg->{col_width} );
        $cols_per_row = 1 if $cols_per_row < 1;
        my $rows = int( ( $#{$arg->{list}} + $cols_per_row ) / $cols_per_row );
        $arg->{rest} = @{$arg->{list}} % $cols_per_row;
        if ( $arg->{vertical} ) {
            my @rearranged_idx;
            my $begin = 0;
            my $end = $rows - 1;
            for my $c ( 0 .. $cols_per_row - 1 ) {
                --$end if $arg->{rest} && $c >= $arg->{rest};
                $rearranged_idx[$c]  = [ $begin .. $end ];
                $begin = $end + 1;
                $end = $begin + $rows - 1;
            }
            for my $r ( 0 .. $rows - 1 ) {
                my @temp_idx;
                for my $c ( 0 .. $cols_per_row - 1 ) {
                    next if $arg->{rest} && $r == $rows - 1 && $c >= $arg->{rest};
                    push @temp_idx, $rearranged_idx[$c][$r];
                }
                push @{$arg->{rowcol2list}}, \@temp_idx;
            }
        }
        else {
            my $begin = 0;
            my $end = $cols_per_row - 1;
            while ( @{$arg->{list}}[$begin..$end] ) { ###
                push @{$arg->{rowcol2list}}, [ $begin .. $end ];
                $begin = $end + 1;
                $end = $begin + $cols_per_row - 1;
                $end = $#{$arg->{list}} if $end > $#{$arg->{list}};
            }
        }
    }
}


sub _handle_mouse {
    my ( $x, $y, $button_pressed, $button_drag, $arg ) = @_;
    return NEXT_getch if $button_drag;
    my $top_row = $arg->{abs_curs_Y}; # $arg->{abs_curs_Y} - $arg->{cursor_row_begin}; # history ?
    if ( $button_pressed == 4 ) {
        return KEY_UP;
    }
    elsif ( $button_pressed == 5 ) {
        return KEY_DOWN;
    }
#    if ( $arg->{LastEventWasPress} ) {
#        $arg->{LastEventWasPress} = 0;
#        return NEXT_getch;
#    }
    return NEXT_getch if $y < $top_row;
    my $mouse_row = $y - $top_row;
    my $mouse_col = $x;
    my( $found_row, $found_col );
    my $found = 0;
    for my $row ( 0 .. @{$arg->{list}} ) {
	if ( $row == $mouse_row ) {
            for my $col ( 0 .. $#{$arg->{rowcol2list}[$row]} ) {
                if ( ( $col * $arg->{col_width} < $mouse_col ) && ( ( $col + 1 ) * $arg->{col_width} >= $mouse_col ) ) {
                    $found = 1;
                    $found_row = $row + $arg->{page};
                    $found_col = $col;
                    last;
                }
            }
        }
    }
    return NEXT_getch if ! $found;
    # if xterm doesn't receive a button-up event it thinks it's dragging # orig comment
    my $return_char = '';
    if ( $button_pressed == 1 ) {
        # $arg->{LastEventWasPress} = 1;
        $return_char = KEY_ENTER;
    }
    elsif ( $button_pressed == 3  ) {
        # $arg->{LastEventWasPress} = 1;
        $return_char = KEY_SPACE;
    }
    else {
        return NEXT_getch; # xterm
    }
    if ( ! ( [ $found_row, $found_col ] ~~ $arg->{this_cell} ) ) {
        my $t = $arg->{this_cell};
        $arg->{this_cell} = [ $found_row, $found_col ];
        _wr_cell( $arg, $t->[0], $t->[1] );
        _wr_cell( $arg, $arg->{this_cell}[ROW], $arg->{this_cell}[COL] );
    }
    return $return_char;
}


1;

__END__


=pod

=encoding utf8

=head1 NAME

Term::Choose - Choose items from a list.

=head1 VERSION

Version 0.7.12

=cut

=head1 SYNOPSIS

    use 5.10.1;
    use Term::Choose qw(choose);

    my $list = [ qw( one two three four five ) ];

    my $choice = choose( $list );                                 # single choice
    say $choice;

    my @choices = choose( [ 1 .. 100 ], { right_justify => 1 } ); # multiple choice
    say "@choices";

    choose( [ 'Press ENTER to continue' ], { prompt => 0 } );     # no choice


=head1 DESCRIPTION

Choose from a list of elements.

Requires Perl Version 5.10.1 or greater.

Based on the I<choose> function from L<Term::Clui> module.

Differences between L<Term::Clui> and L<Term::Choose>:

L<Term::Clui>'s I<choose> expects a I<question> as the first argument, and then the list of items. With L<Term::Choose> the first argument is the list of items passed as an array reference. Options can be passed with a hash reference as an optional second argument. The I<question> can be passed as an option (I<prompt>).

The reason for writing L<Term::Choose> was to get a nicer output. If the list does not fit in one row, I<choose> from L<Term::Clui> puts the elements on the screen without ordering the items in columns. L<Term::Choose> arranges the elements in columns which makes it easier for me to find elements and easier to navigate on the screen.

Another difference is how lists which don't find on the screen are handled. L<Term::Clui::choose|http://search.cpan.org/perldoc?Term::Clui#SUBROUTINES> asks the user to enter a substring as a clue. As soon as the matching items will fit, they are displayed as normal. I<choose> from L<Term::Choose> skips - when scrolling and reaching the end (resp. the begin) of the screen - to the next (resp. previous) page.

Strings where the number of characters are not equal to the number of columns on the screen break the output from L<Term::Clui> and L<Term::Choose>. L<Term::Choose::GC> tries to get along with such situations - see L</"BUGS AND LIMITATIONS">.

L<Term::Clui>'s I<choose> prints and returns the chosen items, while I<choose> from L<Term::Choose> only returns the chosen items.

L<Term::Clui> disables the mouse mode if the environment variable I<CLUI_MOUSE> is set to I<off>. In L<Term::Choose> the mouse mode is set with the option I<mouse_mode>.

Only in L<Term::Clui>:

L<Term::Clui> provides a speaking interface, offers a bundle of functions and has a fallback to work when only Perl core modules are available.

The I<choose> function from L<Term::Clui> can remember choices made in scalar context and allows multiline question - the first line is put on the top, the subsequent lines are displayed below the list.

These differences refer to L<Term::Clui> version 1.65. For a more precise description of L<Term::Clui> consult its own documentation.

=head1 EXPORT

Nothing by default.

    use Term::Choose qw(choose);

=head1 SUBROUTINES/METHODS

=head2 choose

    $scalar = choose( $array_ref [, \%options] );

    @array =  choose( $array_ref [, \%options] );

              choose( $array_ref [, \%options] );

I<choose> expects as first argument an array reference which passes the list elements available for selection (in void context no selection can be made).

Options can be passed with a hash reference as a second (optional) argument.

=head3 Usage and return values

=over

=item

If I<choose> is called in a I<scalar context>, the user can choose an item by using the "move-around-keys" and "Return".

I<choose> then returns the chosen item.

=item

If I<choose> is called in an I<list context>, the user can also mark an item with the "SpaceBar".

I<choose> then returns the list of marked items, (including the item highlight when "Return" was pressed).

=item

If I<choose> is called in an I<void context>, the user can move around but mark nothing; the output shown by I<choose> can be closed with "Return".

Called in void context I<choose> returns nothing.

=back

If the items of the list don't fit in the screen, the user can scroll to the next (previous) page(s).

If the window size is changed, then as soon as the user enters a keystroke I<choose> rewrites the screen. In list context marked items are reset.

The "q" key returns I<undef> or an empty list in list context.

With a I<mouse_mode> enabled (and if supported by the terminal) the element can be chosen with the left mouse key, in list context the right mouse key can be used instead the "SpaceBar" key.


If the option I<extra_key> is enabled pressing "e" calls I<exit()>.


Keys to move around: arrow keys (or hjkl), Tab, BackSpace, Shift-Tab.

=head3 Modifications for the output

For the output on the screen the list elements are modified:

=over

=item *

if a list element is not defined the value from the option I<undef> is assigned to the element.

=item *

if a list element holds an empty string the value from the option I<empty_string> is assigned to the element.

=item *

white-spaces in list elements are replaced with simple spaces.

    $element =~ s/\p{Space}/ /g;

=item *

control characters are removed.

    $element =~ s/\p{Cntrl}//g;

=item *

if the length of a list element is greater than the width of the screen the element is cut.


    $element = substr( $element, 0, $allowed_length - 3 ) . '...';

=back

All these modifications are made on a copy of the original list so I<choose> returns the chosen elements as they were passed to the function without modifications.

=head3 Options

All options are optional.

Defaults may change in a future release.

=head4 prompt

If I<prompt> is undefined default prompt-string will be shown.

If I<prompt> is 0 no prompt-line will be shown.

default in list and scalar context: 'Your choice:'

default in void context: 'Close with ENTER'

=head4 right_justify

0 - columns are left justified (default)

1 - columns are right justified

=head4 layout (modified)

From broad to narrow: 0 > 1 > 2 > 3

=over

=item

0 - layout off

 .----------------------.   .----------------------.   .----------------------.   .----------------------.
 | .. .. .. .. .. .. .. |   | .. .. .. .. .. .. .. |   | .. .. .. .. .. .. .. |   | .. .. .. .. .. .. .. |
 |                      |   | .. .. .. .. .. .. .. |   | .. .. .. .. .. .. .. |   | .. .. .. .. .. .. .. |
 |                      |   |                      |   | .. .. .. .. ..       |   | .. .. .. .. .. .. .. |
 |                      |   |                      |   |                      |   | .. .. .. .. .. .. .. |
 |                      |   |                      |   |                      |   | .. .. .. .. .. .. .. |
 |                      |   |                      |   |                      |   | .. .. .. .. .. .. .. |
 '----------------------'   '----------------------'   '----------------------'   '----------------------'

=item

1 - layout "H" (default)

 .----------------------.   .----------------------.   .----------------------.   .----------------------.
 | .. .. .. .. .. .. .. |   | .. .. .. ..          |   | .. .. .. .. ..       |   | .. .. .. .. .. .. .. |
 |                      |   | .. .. .. ..          |   | .. .. .. .. ..       |   | .. .. .. .. .. .. .. |
 |                      |   | .. ..                |   | .. .. .. .. ..       |   | .. .. .. .. .. .. .. |
 |                      |   |                      |   | .. .. .. ..          |   | .. .. .. .. .. .. .. |
 |                      |   |                      |   |                      |   | .. .. .. .. .. .. .. |
 |                      |   |                      |   |                      |   | .. .. .. .. .. .. .. |
 '----------------------'   '----------------------'   '----------------------'   '----------------------'


=item

2 - layout "V"

 .----------------------.   .----------------------.   .----------------------.   .----------------------.
 | ..                   |   | .. ..                |   | .. .. ..             |   | .. .. .. .. .. .. .. |
 | ..                   |   | .. ..                |   | .. .. ..             |   | .. .. .. .. .. .. .. |
 | ..                   |   | .. ..                |   | .. .. ..             |   | .. .. .. .. .. .. .. |
 | ..                   |   | ..                   |   | .. .. ..             |   | .. .. .. .. .. .. .. |
 | ..                   |   |                      |   | .. ..                |   | .. .. .. .. .. .. .. |
 | ..                   |   |                      |   |                      |   | .. .. .. .. .. .. .. |
 '----------------------'   '----------------------'   '----------------------'   '----------------------'

=item

3 - all in a single column

 .----------------------.   .----------------------.   .----------------------.   .----------------------.
 | ..                   |   | ..                   |   | ..                   |   | ..                   |
 | ..                   |   | ..                   |   | ..                   |   | ..                   |
 | ..                   |   | ..                   |   | ..                   |   | ..                   |
 |                      |   | ..                   |   | ..                   |   | ..                   |
 |                      |   |                      |   | ..                   |   | ..                   |
 |                      |   |                      |   |                      |   | ..                   |
 '----------------------'   '----------------------'   '----------------------'   '----------------------'

=back

=head4 screen_width

If set, restricts the screen width to I<screen_width> percentage of the effective screen width.

If not defined all the screen width is used.

Allowed values: 10 - 99

(default: undef)

=head4 vertical (replaces I<vertical_order>)

0 - items ordered horizontally

1 - items ordered vertically (default)

=head4 length_longest (new)

If the length of the longest element of the list is known before calling I<choose> it can be passed with this option.

If I<length_longest> is set, then I<choose> doesn't calculate the length of the longest element itself but uses the value passed with this option.

If I<length_longest> is set to a value less than the length of the longest element, then all elements which a length greater as this value will be cut.

A higher value than the length of the longest element wastes space on the screen.

Allowed values: 1 - 999_999_999

(default: undef)

=head4 clear_screen

0 - off (default)

1 - clears the screen before printing the choices

=head4 mouse_mode

0 - no mouse mode (default)

1 - mouse mode 1003 enabled

2 - mouse mode 1003 enabled; maxcols/maxrows limited to 224 (mouse mode 1003 doesn't work above 224)

3 - extended mouse mode (1005) - uses utf8

4 - extended SGR mouse mode (1006); mouse mode 1003 if mouse mode 1006 is not supported

=head4 pad

space between columns (default: 2)

allowed values: 0 - 99

=head4 pad_one_row

space between items if we have only one row (default: 3)

allowed values: 0 - 99

=head4 extra_key

0 - off

1 - on: pressing key "e" calls I<exit()> (default)

=head4 beep

0 - off (default)

1 - on

=head4 hide_cursor

0 - off

1 - on (default)

=head4 undef

string displayed on the screen instead a undefined list element

default: '<undef>'

=head4 empty_string

string displayed on the screen instead an empty string

default: '<empty>'

=head4 max_list

maximal allowed length of the list referred by the first argument (default: 100_000)

allowed values: 1 - 999_999_999

=head3 Error handling

=over

=item * With no arguments I<choose> dies.

=item * If the first argument is not a array reference I<choose> dies.

=item * If the list referred by the first argument is empty I<choose> returns  I<undef> resp. an empty list and issues a warning.

=item * If the list referred by the first argument has more than I<max_list> items (default 100_000) I<choose> warns and uses the first I<max_list> list items.

=item * If the (optional) second argument is not a hash reference I<choose> dies.

=item * If an option does not exist I<choose> warns.

=item * If an option value is not valid  I<choose> warns an falls back to the default value.

=back

=head1 REQUIREMENTS

=head2 Perl Version

Requires Perl Version 5.10.1 or greater.

=head2 Modules

Used modules not provided as core modules:

=over

=item

L<Signals::XSIG>

=item

L<Term::ReadKey>

=back

=head2 Escape sequences

The Terminal needs to understand the following ANSI escape sequences:

    "\e[A"      Cursor Up

    "\e[C"      Cursor Forward

    "\e[0J"     Clear to  End of Screen (Erase Data)

    "\e[0m"     Normal/Reset (SGR)

    "\e[1m"     Bold (SGR)

    "\e[4m"     Underline (SGR)

    "\e[7m"     Inverse (SGR)


If option "hide_cursor" is enabled:

    "\e[?25l"   Hide Cursor (DECTCEM)

    "\e[?25h"   Show Cursor (DECTCEM)

If option "clear_screen" is enabled:

    "\e[2J"     Clear Screen (Erase Data)

    "\e[1;1H"   Go to Top Left (Cursor Position)

If option "mouse_mode" is set:

    "\e[6n"     Get Cursor Position (Device Status Report)

Mouse Tracking: The escape sequences

    "\e[?1003h", "\e[?1005h", "\e[?1006h"

and

    "\e[?1003l", "\e[?1005l", "\e[?1006l"

are used to enable/disable the different mouse modes.

=head1 BUGS AND LIMITATIONS

=head2 Unicode

This modules uses the Perl builtin functions I<length> to determine the length of strings, I<substr> to cut strings and I<sprintf> widths to justify strings. Therefore strings with characters that take more or less than one print column will break the layout. Using L<Term::Choose::GC> instead improves the layout in such conditions. It determines the string length by using the I<columns> method from L<Unicode::GCString> module.

    use Term::Choose:GC qw(choose);

Usage and options are the same as for L<Term::Choose>.

The use of L<Term::Choose::GC> needs additionally the L<Unicode::GCString> module to be installed.

Known drawbacks:

L<Term::Choose::GC>'s I<choose> is probably slower than I<choose> from L<Term::Choose>.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Term::Choose

=head1 AUTHOR

Kürbis cuer2s@gmail.com

=head1 CREDITS

Based on and inspired by the I<choose> function from L<Term::Clui> module.

Thanks to the L<http://www.perl-community.de> and the people form L<http://stackoverflow.com> for the help.

=head1 LICENSE AND COPYRIGHT

Copyright 2012 Kürbis.

This library is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

=cut

# End of Term::Choose
