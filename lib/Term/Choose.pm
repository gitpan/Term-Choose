use warnings;
use strict;
use 5.10.1;
use utf8;
package Term::Choose;

our $VERSION = '1.019';
use Exporter 'import';
our @EXPORT_OK = qw(choose);

use Carp;
use Scalar::Util qw(reftype);
use Term::ReadKey;
use Unicode::GCString;

#use warnings FATAL => qw(all);
#use Log::Log4perl qw(get_logger);
#my $log = get_logger("Term::Choose");

use constant {
    ROW     => 0,
    COL     => 1,

    MIN     => 0,
    MAX     => 1,
};

use constant {
    UP                              => "\e[A",
    DOWN                            => "\n",
    RIGHT                           => "\e[C",
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

    MAX_MOUSE_1003_ROW              => 224,
    MAX_MOUSE_1003_COL              => 224,

    BEEP                            => "\07",
    CLEAR_SCREEN                    => "\e[2J",
    GO_TO_TOP_LEFT                  => "\e[1;1H",
    CLEAR_EOS                       => "\e[0J",
    RESET                           => "\e[0m",
    UNDERLINE                       => "\e[4m",
    REVERSE                         => "\e[7m",
    BOLD                            => "\e[1m",
};

use constant {
    BIT_MASK_xxxxxx11   => 0x03,
    BIT_MASK_xx1xxxxx   => 0x20,
    BIT_MASK_x1xxxxxx   => 0x40,
};

use constant {
    NEXT_getch      => -1,

    CONTROL_B       => 0x02,
    CONTROL_C       => 0x03,
    CONTROL_D       => 0x04,
    CONTROL_F       => 0x06,
    CONTROL_H       => 0x08,
    KEY_TAB         => 0x09,
    KEY_ENTER       => 0x0d,
    KEY_ESC         => 0x1b,
    KEY_SPACE       => 0x20,
    KEY_h           => 0x68,
    KEY_j           => 0x6a,
    KEY_k           => 0x6b,
    KEY_l           => 0x6c,
    KEY_q           => 0x71,
    KEY_Tilde       => 0x7e,
    KEY_BSPACE      => 0x7f,

    KEY_UP          => 0x1b5b41,
    KEY_DOWN        => 0x1b5b42,
    KEY_RIGHT       => 0x1b5b43,
    KEY_LEFT        => 0x1b5b44,
    KEY_BTAB        => 0x1b5b5a,
    KEY_PAGE_UP     => 0x1b5b35,
    KEY_PAGE_DOWN   => 0x1b5b36,
};


sub _getch {
    my ( $arg ) = @_;
    my $c = ReadKey 0;
    return if ! defined $c;
    if ( $c eq "\e" ) {
        my $c = ReadKey 0.10;
        if ( ! defined $c ) { return KEY_ESC; }
        elsif ( $c eq 'A' ) { return KEY_UP; }
        elsif ( $c eq 'B' ) { return KEY_DOWN; }
        elsif ( $c eq 'C' ) { return KEY_RIGHT; }
        elsif ( $c eq 'D' ) { return KEY_LEFT; }
        elsif ( $c eq 'Z' ) { return KEY_BTAB; }
        elsif ( $c eq '5' ) { return KEY_PAGE_UP; }
        elsif ( $c eq '6' ) { return KEY_PAGE_DOWN; }
        elsif ( $c eq '[' ) {
            my $c = ReadKey 0;
               if ( $c eq 'A' ) { return KEY_UP; }
            elsif ( $c eq 'B' ) { return KEY_DOWN; }
            elsif ( $c eq 'C' ) { return KEY_RIGHT; }
            elsif ( $c eq 'D' ) { return KEY_LEFT; }
            elsif ( $c eq 'Z' ) { return KEY_BTAB; }
            elsif ( $c =~ /\d/ ) {
                my $c1 = ReadKey 0;
                if ( $c1 eq '~' ) {
                       if ( $c eq '5' ) { return KEY_PAGE_UP; }
                    elsif ( $c eq '6' ) { return KEY_PAGE_DOWN; }
                }
                elsif ( $c1 =~ /[;\d]/ ) {   # cursor-position report, response to \e[6n
                    my $abs_curs_Y = 0 + $c;
                    while ( 1 ) {
                        last if $c1 eq ';';
                        $abs_curs_Y = 10 * $abs_curs_Y + $c1;
                        $c1 = ReadKey 0;
                    }
                    my $abs_curs_X = 0; # $arg->{abs_curs_X} never used
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
                else {
                    return NEXT_getch;
                }
            }
            elsif ( $c eq '5' ) { return KEY_PAGE_UP; }
            elsif ( $c eq '6' ) { return KEY_PAGE_DOWN; }
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
        print UNSET_EXT_MODE_MOUSE_1005     if $arg->{mouse_mode} == 3;
        print UNSET_SGR_EXT_MODE_MOUSE_1006 if $arg->{mouse_mode} == 4;
        print UNSET_ANY_EVENT_MOUSE_1003;
    }
    Term::ReadKey::ReadMode 'restore';
    print SHOW_CURSOR if $arg->{hide_cursor};
    select( $arg->{old_handle} );
}


sub _length_longest {
    my ( $list ) = @_;
    utf8::upgrade( $list->[0] );
    my $gcs = Unicode::GCString->new( $list->[0] );
    my $longest = $gcs->columns();
    for my $str ( @{$list} ) {
        utf8::upgrade( $str );
        my $gcs = Unicode::GCString->new( $str );
        my $length = $gcs->columns();
        $longest = $length if $length > $longest;
    }
    return $longest;
}


sub _copy_orig_list {
    my ( $arg ) = @_;
    if ( $arg->{list_to_long} ) {
        return [ map {
            my $copy = $_;
            $copy = ( ! defined $copy ) ? $arg->{undef}        : $copy;
            $copy = ( $copy eq '' )     ? $arg->{empty_string} : $copy;
            $copy =~ s/\p{Space}/ /g;
            $copy =~ s/\p{Cntrl}//g;
            $copy;
        } @{$arg->{orig_list}}[ 0 .. $arg->{limit} - 1 ] ];
    }
    return [ map {
        my $copy = $_;
        $copy = ( ! defined $copy ) ? $arg->{undef}        : $copy;
        $copy = ( $copy eq '' )     ? $arg->{empty_string} : $copy;
        $copy =~ s/\p{Space}/ /g;
        $copy =~ s/\p{Cntrl}//g;
        $copy;
    } @{$arg->{orig_list}} ];
}


sub _validate_option {
    my ( $config ) = @_;
    my $limit = 1_000_000_000;
    my $validate = {    #   min      max
        beep            => [ 0,       1 ],
        clear_screen    => [ 0,       1 ],
        default         => [ 0,  $limit ],
        empty_string    => '',
        hide_cursor     => [ 0,       1 ],
        
        justify         => [ 0,       2 ], # NOTE: NEW -> replaces "right_justify"
        
        layout          => [ 0,       3 ],
        length_longest  => [ 1,  $limit ],
        limit           => [ 1,  $limit ],
        mouse_mode      => [ 0,       4 ],
        
        order           => [ 0,       1 ], # NOTE: NEW -> replaces "vertical"
        
        pad             => [ 0,  $limit ],
        pad_one_row     => [ 0,  $limit ],
        page            => [ 0,       1 ],
        prompt          => '',
        
        right_justify   => [ 0,       1 ], # NOTE: deprecated -> replaced by "justify"
   
        screen_width    => [ 1 ,    100 ],
        undef           => '',
        
        vertical        => [ 0,       1 ], # NOTE: deprecated -> replaced by "order"
        
    };
    my $warn = 0;
    for my $key ( keys %$config ) {
        if ( $validate->{$key} ) {
            if ( defined $config->{$key} && ( $config->{$key} !~ m/\A\d+\z/ || $config->{$key} < $validate->{$key}[MIN] || $config->{$key} > $validate->{$key}[MAX] ) ) {
                carp "choose: \"$config->{$key}\" not a valid value for option \"$key\". Falling back to default value.";
                $config->{$key} = undef;
                ++$warn;
            }
        }
        elsif ( ! exists $validate->{$key} ) {
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
    
    # ### #####
    if ( defined $config->{right_justify} && ! defined $config->{justify} ) {
        $config->{justify} = $config->{right_justify};
    }   
    if ( defined $config->{vertical} && ! defined $config->{order} ) {
        $config->{order} = $config->{vertical};
    }
    # ### #####
    
    $config = _validate_option( $config // {} );
    $config->{beep}             //= 0;
    $config->{clear_screen}     //= 0;
    #$config->{default}         //= undef;
    $config->{empty_string}     //= '<empty>';
    $config->{hide_cursor}      //= 1;
    
    $config->{justify}          //= 0; # NOTE: NEW -> replaces "right_justify"
    
    $config->{layout}           //= 1;
    #$config->{length_longest}  //= undef;
    $config->{limit}            //= 100_000;
    $config->{mouse_mode}       //= 0;
    
    $config->{order}            //= 1; # NOTE: NEW -> replaces "vertical"
    
    $config->{pad}              //= 2;
    $config->{pad_one_row}      //= 3;
    $config->{page}             //= 1;
    $config->{prompt}           //= $prompt;
    
    ## $config->{right_justify}    //= 0; # NOTE: deprecated -> replaced by "justify"
    
    #$config->{screen_width}    //= undef;
    $config->{undef}            //= '<undef>';
    
    ## $config->{vertical}         //= 1; # NOTE: deprecated -> replaced by "order"
    
    return $config;
}


sub _set_this_cell {
    my ( $arg ) = @_;
    $arg->{tmp_this_cell} = [ 0, 0 ];
    LOOP: for my $i ( 0 .. $#{$arg->{rowcol2list}} ) {
        # if ( $arg->{default} ~~ @{$arg->{rowcol2list}[$i]} ) {
            for my $j ( 0 .. $#{$arg->{rowcol2list}[$i]} ) {
                if ( $arg->{default} == $arg->{rowcol2list}[$i][$j] ) {
                    $arg->{tmp_this_cell} = [ $i, $j ];
                    last LOOP;
                }
            }
        # }
    }
    while ( $arg->{tmp_this_cell}[ROW] > $arg->{end_page} ) {
        $arg->{top_listrow} = $arg->{maxrows} * ( int( $arg->{this_cell}[ROW] / $arg->{maxrows} ) + 1 );
        $arg->{this_cell}[ROW] = $arg->{top_listrow};
        $arg->{begin_page} = $arg->{top_listrow};
        $arg->{end_page} = $arg->{begin_page} + $arg->{maxrows} - 1;
        $arg->{end_page} = $#{$arg->{rowcol2list}} if $arg->{end_page} > $#{$arg->{rowcol2list}};
    }
    $arg->{this_cell} = $arg->{tmp_this_cell};
}


sub _prepare_page_number {
    my ( $arg ) = @_;
    $arg->{total_pages} = int( $#{$arg->{rowcol2list}} / ( $arg->{maxrows} + $arg->{tail} ) ) + 1;
    if ( $arg->{total_pages} > 1 ) {
        $arg->{total_pages} = int( $#{$arg->{rowcol2list}} / ( $arg->{maxrows} ) ) + 1;
        $arg->{length_total_pages} = length $arg->{total_pages};
        $arg->{prompt_printf_template} = "--- Page %0*d/%d ---";
        $arg->{prompt_arguments} = 0;
        if ( length sprintf( $arg->{prompt_printf_template}, $arg->{length_total_pages}, $arg->{total_pages}, $arg->{total_pages} )  > $arg->{maxcols} ) {
            $arg->{prompt_printf_template} = "%0*d/%d";
            if ( length sprintf( $arg->{prompt_printf_template}, $arg->{length_total_pages}, $arg->{total_pages}, $arg->{total_pages} )  > $arg->{maxcols} ) {
                $arg->{length_total_pages} = ( $arg->{length_total_pages} > $arg->{maxcols} ) ? $arg->{maxcols} : $arg->{length_total_pages};
                $arg->{prompt_printf_template} = "%0*.*s";
                $arg->{prompt_arguments} = 1;
            }
        }
    }
    else {
        $arg->{maxrows} += $arg->{tail};
        $arg->{tail} = 0;
    }
}


sub _prepare_promptline {
    my ( $arg ) = @_;
    $arg->{prompt} =~ s/\p{Space}/ /g;
    $arg->{prompt} =~ s/\p{Cntrl}//g;
    $arg->{prompt_line} = $arg->{prompt};
    if ( defined $arg->{wantarray} && $arg->{wantarray} ) {
        if ( $arg->{prompt} ) {
            $arg->{prompt_line} = $arg->{prompt} . '  (multiple choice with spacebar)';
            my $prompt_length;
            utf8::upgrade( $arg->{prompt_line} );
            my $gcs = Unicode::GCString->new( $arg->{prompt_line} );
            $prompt_length = $gcs->columns();
            $arg->{prompt_line} = $arg->{prompt} . ' (multiple choice)' if $prompt_length > $arg->{maxcols};
        }
        else {
            $arg->{prompt_line} = '';
        }
    }
    my $prompt_length;
    utf8::upgrade( $arg->{prompt_line} );
    my $gcs = Unicode::GCString->new( $arg->{prompt_line} );
    $prompt_length = $gcs->columns();
    if ( $prompt_length > $arg->{maxcols} ) {
        $arg->{prompt_line} = _unicode_cut( $arg, $arg->{prompt} );
    }
}


sub _write_first_screen {
    my ( $arg ) = @_;
#    if ( $arg->{clear_screen} ) {
#        print CLEAR_SCREEN;
#        print GO_TO_TOP_LEFT;
#    }
    ( $arg->{maxcols}, $arg->{maxrows} ) = GetTerminalSize( $arg->{handle_out} );
    if ( $arg->{screen_width} ) {
        $arg->{maxcols} = int( ( $arg->{maxcols} / 100 ) * $arg->{screen_width} );
        $arg->{maxcols} = 1 if $arg->{maxcols} == 0;
    }
    if ( $arg->{mouse_mode} == 2 ) {
        $arg->{maxcols} = MAX_MOUSE_1003_COL if $arg->{maxcols} > MAX_MOUSE_1003_COL;
        $arg->{maxrows} = MAX_MOUSE_1003_ROW if $arg->{maxrows} > MAX_MOUSE_1003_ROW;
    }
    $arg->{head} = 0;
    $arg->{tail} = 0;
    $arg->{head} = 1 if $arg->{prompt} ne '0';
    $arg->{tail} = 1 if $arg->{page};
    $arg->{maxrows} -= $arg->{head} + $arg->{tail};
    _size_and_layout( $arg );
    _prepare_promptline( $arg ) if $arg->{prompt} ne '0';
    _prepare_page_number( $arg ) if $arg->{page};
    $arg->{maxrows_index} = $arg->{maxrows} - 1;
    $arg->{maxrows_index} = 0 if $arg->{maxrows_index} < 0;
    $arg->{begin_page} = 0;
    $arg->{end_page} = $arg->{maxrows_index};
    $arg->{end_page} = $#{$arg->{rowcol2list}} if $arg->{maxrows_index} > $#{$arg->{rowcol2list}};
    $arg->{top_listrow} = 0;
    $arg->{marked} = [];
    $arg->{screen_row} = 0;
    $arg->{this_cell} = [ 0, 0 ];
    _set_this_cell( $arg ) if defined $arg->{default} && $arg->{default} <= $#{$arg->{list}};
    # No printing before clear_screen!
    if ( $arg->{clear_screen} ) {
        print CLEAR_SCREEN;
        print GO_TO_TOP_LEFT;
    }
    _wr_screen( $arg );
    $arg->{abs_curs_X} = 0;
    $arg->{abs_curs_Y} = 0;
    print GET_CURSOR_POSITION if $arg->{mouse_mode};        # in: $arg->{abs_curs_X}, $arg->{abs_curs_Y}
    $arg->{cursor_row} = $arg->{screen_row} - $arg->{head}; # needed by handle_mouse
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
    if ( @$orig_list > $arg->{limit} ) {
        my $list_length = scalar @$orig_list;
        carp "choose: List has $list_length items.\nchoose: \"limit\" is set to $arg->{limit} items!\nchoose: The first $arg->{limit} itmes are used by choose.";
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
    _init_scr( $arg );
    $arg->{size_changed} = 0;
    my $orig_sigwinch = $SIG{'WINCH'};
    local $SIG{'WINCH'} = sub {
        $orig_sigwinch->() if $orig_sigwinch && ref $orig_sigwinch eq 'CODE';
        $arg->{size_changed} = 1;
    };
    _write_first_screen( $arg );

    while ( 1 ) {
        my $c = _getch( $arg );
        if ( ! defined $c ) {
            _end_win( $arg );
            warn "EOT";
            return;
        }
        next if $c == NEXT_getch;
        next if $c == KEY_Tilde;
        if ( $arg->{size_changed} ) {
            $arg->{list} = _copy_orig_list( $arg );
            print CR, UP x ( $arg->{this_cell}[ROW] + $arg->{head} );
            _write_first_screen( $arg );
            $arg->{size_changed} = 0;
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
        # So e.g. the second value in the second row of the new list would be $arg->{list}[ $arg->{rowcol2list}[1][1] ].
        # On the other hand the index of the last row of the new list would be $#{$arg->{rowcol2list}}
        # or the index of the last column in the first row would be $#{$arg->{rowcol2list}[0]}.
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
                        $arg->{top_listrow} = $arg->{this_cell}[ROW];
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
                    if ( defined $arg->{backup_col} ) {
                        $arg->{this_cell}[COL] = $arg->{backup_col};
                        $arg->{backup_col}     = undef;
                    }
                    if ( $arg->{this_cell}[ROW] >= $arg->{begin_page} ) {
                        _wr_cell( $arg, $arg->{this_cell}[ROW] + 1, $arg->{this_cell}[COL] );
                        _wr_cell( $arg, $arg->{this_cell}[ROW],     $arg->{this_cell}[COL] );
                    }
                    else {
                        $arg->{top_listrow} = $arg->{this_cell}[ROW] - $arg->{maxrows_index};
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
                            $arg->{top_listrow} = $arg->{this_cell}[ROW];
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
            when ( $c == KEY_BSPACE || $c == CONTROL_H || $c == KEY_BTAB ) {
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
                            $arg->{top_listrow} = $arg->{this_cell}[ROW] - $arg->{maxrows_index};
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
                    $arg->{backup_col} = undef if defined $arg->{backup_col}; # don't remember col if col is changed deliberately
                }
            }
            when ( $c == CONTROL_B || $c == KEY_PAGE_UP ) {
                if ( $arg->{begin_page} <= 0 ) {
                    _beep( $arg );
                }
                else {
                    $arg->{top_listrow} = $arg->{maxrows} * ( int( $arg->{this_cell}[ROW] / $arg->{maxrows} ) -1 );
                    $arg->{this_cell}[ROW] = $arg->{top_listrow};
                    if ( defined $arg->{backup_col} ) {
                        $arg->{this_cell}[COL] = $arg->{backup_col};
                        $arg->{backup_col}     = undef;
                    }
                    $arg->{begin_page} = $arg->{top_listrow};
                    $arg->{end_page}   = $arg->{begin_page} + $arg->{maxrows} - 1;
                    _wr_screen( $arg );
                }
            }
            when ( $c == CONTROL_F || $c == KEY_PAGE_DOWN ) {
                if ( $arg->{end_page} >= $#{$arg->{rowcol2list}} ) {
                    _beep( $arg );
                }
                else {
                    $arg->{top_listrow} = $arg->{maxrows} * ( int( $arg->{this_cell}[ROW] / $arg->{maxrows} ) + 1 );
                    $arg->{this_cell}[ROW] = $arg->{top_listrow};
                    # if it remains only the last row (which is then also the first row) for the last page
                    # and the column in use doesn't exist in the last row, then backup col
                    if ( $arg->{top_listrow} == $#{$arg->{rowcol2list}} && $arg->{rest} && $arg->{this_cell}[COL] >= $arg->{rest}) {
                        $arg->{backup_col}     = $arg->{this_cell}[COL];
                        $arg->{this_cell}[COL] = $#{$arg->{rowcol2list}[$arg->{this_cell}[ROW]]};
                    }
                    $arg->{begin_page} = $arg->{top_listrow};
                    $arg->{end_page}   = $arg->{begin_page} + $arg->{maxrows} - 1;
                    $arg->{end_page}   = $#{$arg->{rowcol2list}} if $arg->{end_page} > $#{$arg->{rowcol2list}};
                    _wr_screen( $arg );
                }
            }
            when ( $c == KEY_q || $c == CONTROL_D ) {
                _end_win( $arg );
                return;
            }
            when ( $c == CONTROL_C ) {
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
                    #if ( $arg->{vertical} ) {
                    if ( $arg->{order} ) {                    
                        for my $col ( 0 .. $#{$arg->{rowcol2list}[0]} ) {
                            for my $row ( 0 .. $#{$arg->{rowcol2list}} ) {
                                if ( $arg->{marked}[$row][$col] || $row == $arg->{this_cell}[ROW] && $col == $arg->{this_cell}[COL] ) {
                                    my $i = $arg->{rowcol2list}[$row][$col];
                                    push @chosen, $arg->{orig_list}[$i];
                                }
                            }
                        }
                    }
                    else {
                         for my $row ( 0 .. $#{$arg->{rowcol2list}} ) {
                            for my $col ( 0 .. $#{$arg->{rowcol2list}[$row]} ) {
                                if ( $arg->{marked}[$row][$col] || $row == $arg->{this_cell}[ROW] && $col == $arg->{this_cell}[COL] ) {
                                    my $i = $arg->{rowcol2list}[$row][$col];
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
    my ( $arg ) = @_;
    _goto( $arg, 0, 0 );
    _clear_to_end_of_screen( $arg );
    if ( $arg->{prompt} ne '0' ) {
        print $arg->{prompt_line};
        _goto( $arg, $arg->{head}, 0 );
    }
    if ( $arg->{page} && $arg->{total_pages} > 1 ) {
        _goto( $arg, $arg->{maxrows_index} + $arg->{head} + $arg->{tail}, 0 );
        if ( $arg->{prompt_arguments} == 0 ) {
            printf $arg->{prompt_printf_template}, $arg->{length_total_pages}, int($arg->{top_listrow} / $arg->{maxrows}) + 1, $arg->{total_pages};
        }
        elsif ( $arg->{prompt_arguments} == 1 ) {
            printf $arg->{prompt_printf_template}, $arg->{length_total_pages}, $arg->{length_total_pages}, int($arg->{top_listrow} / $arg->{maxrows}) + 1;
        }
     }
    for my $row ( $arg->{begin_page} .. $arg->{end_page} ) {
        for my $col ( 0 .. $#{$arg->{rowcol2list}[$row]} ) {
            _wr_cell( $arg, $row, $col ); # unless $row == $arg->{this_cell}[ROW] && $col == $arg->{this_cell}[COL];
        }
    }
    _wr_cell( $arg, $arg->{this_cell}[ROW], $arg->{this_cell}[COL] );
}


sub _wr_cell {
    my( $arg, $row, $col ) = @_;
    if ( $arg->{all_in_first_row} ) {
        my $lngth = 0;
        if ( $col > 0 ) {
            for my $cl ( 0 .. $col - 1 ) {
                utf8::upgrade( $arg->{list}[$arg->{rowcol2list}[$row][$cl]] );
                my $gcs = Unicode::GCString->new( $arg->{list}[$arg->{rowcol2list}[$row][$cl]] );
                $lngth += $gcs->columns();
                $lngth += $arg->{pad_one_row} // 0;
            }
        }
        _goto( $arg, $row + $arg->{head} - $arg->{top_listrow}, $lngth );
        print BOLD, UNDERLINE if $arg->{marked}[$row][$col];
        print REVERSE if $row == $arg->{this_cell}[ROW] && $col == $arg->{this_cell}[COL];
        print $arg->{list}[$arg->{rowcol2list}[$row][$col]];
    }
    else {
        _goto( $arg, $row + $arg->{head} - $arg->{top_listrow}, $col * $arg->{col_width} );
        print BOLD, UNDERLINE if $arg->{marked}[$row][$col];
        print REVERSE if $row == $arg->{this_cell}[ROW] && $col == $arg->{this_cell}[COL];
        print _unicode_sprintf( $arg, $arg->{list}[$arg->{rowcol2list}[$row][$col]] );
    }
    print RESET if $arg->{marked}[$row][$col] || $row == $arg->{this_cell}[ROW] && $col == $arg->{this_cell}[COL];
}


sub _size_and_layout {
    my ( $arg ) = @_;
    my $layout = $arg->{layout};
    $arg->{rowcol2list} = [];
    $arg->{all_in_first_row} = 0;
    if ( $arg->{length_longest} > $arg->{maxcols} ) {
        $arg->{length_longest} = $arg->{maxcols}; # needed for _unicode_sprintf
        $layout = 3;
    }
    ### layout
    my $all_in_first_row;
    if ( $layout == 2 ) {
        $layout = 3 if scalar @{$arg->{list}} <= $arg->{maxrows};
    }
    elsif ( $layout < 2 ) {
        for my $idx ( 0 .. $#{$arg->{list}} ) {
            $all_in_first_row .= $arg->{list}[$idx];
            $all_in_first_row .= ' ' x $arg->{pad_one_row} if $idx < $#{$arg->{list}};
            my $length_first_row;
            utf8::upgrade( $all_in_first_row );
            my $gcs = Unicode::GCString->new( $all_in_first_row );
            $length_first_row = $gcs->columns();
            if ( $length_first_row > $arg->{maxcols} ) {
                $all_in_first_row = '';
                last;
            }
        }
    }
    if ( $all_in_first_row ) {
        $arg->{all_in_first_row} = 1;
        $arg->{rowcol2list}[0] = [ 0 .. $#{$arg->{list}} ];
    }
    elsif ( $layout == 3 ) {
        for my $idx ( 0 .. $#{$arg->{list}} ) {
            $arg->{list}[$idx] = _unicode_cut( $arg, $arg->{list}[$idx] );
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
                #$tmc = int( $tmc + ( ( $maxcls - $tmc ) / 2 ) ) if $arg->{layout} == 1;
                $tmc = int( $tmc + ( ( $maxcls - $tmc ) / 1.5 ) ) if $arg->{layout} == 1;
                $tmc = int( $tmc + ( ( $maxcls - $tmc ) / 6 ) ) if $arg->{layout} == 2;
                $maxcls = $tmc;
            }
        }
    ### order
        my $cols_per_row = int( $maxcls / $arg->{col_width} );
        $cols_per_row = 1 if $cols_per_row < 1;
        my $rows = int( ( $#{$arg->{list}} + $cols_per_row ) / $cols_per_row );
        $arg->{rest} = @{$arg->{list}} % $cols_per_row;
        #if ( $arg->{vertical} ) {
        if ( $arg->{order} ) {        
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
            $end = $#{$arg->{list}} if $end > $#{$arg->{list}};
            push @{$arg->{rowcol2list}}, [ $begin .. $end ];
            while ( $end < $#{$arg->{list}} ) {
                $begin += $cols_per_row;
                $end   += $cols_per_row;
                $end = $#{$arg->{list}} if $end > $#{$arg->{list}};
                push @{$arg->{rowcol2list}}, [ $begin .. $end ];
            }
        }
    }
}


sub _unicode_cut {
    my ( $arg, $unicode ) = @_;
    utf8::upgrade( $unicode );
    my $gcs = Unicode::GCString->new( $unicode );
    my $colwidth = $gcs->columns();
    if ( $colwidth > $arg->{maxcols} ) {
        my $length = $arg->{maxcols} - 3;
        my $max_length = int( $length / 2 ) + 1;
        while ( 1 ) {
            #my( $tmp ) = $unicode =~ /\A(\X{0,$max_length})/;
            my $tmp = substr( $unicode, 0, $max_length );
            my $gcs = Unicode::GCString->new( $tmp );
            $colwidth = $gcs->columns();
            if ( $colwidth > $length ) {
                # As soon as the string is longer than length_longest again:
                $unicode = $tmp;
                last;
            }
            $max_length += 10;
        }
        while ( $colwidth > $length ) {
            $unicode =~ s/\X\z//;
            my $gcs = Unicode::GCString->new( $unicode );
            $colwidth = $gcs->columns();
        }
        $unicode .= ' ' if $colwidth < $length;
        $unicode .= '...';
    }
    return $unicode;
}


#sub _unicode_sprintf {
#    my ( $arg, $unicode ) = @_;
#    utf8::upgrade( $unicode );
#    my $gcs = Unicode::GCString->new( $unicode );
#    my $colwidth = $gcs->columns();
#    if ( $colwidth > $arg->{length_longest} ) {
#        my $max_length = int( $arg->{length_longest} / 2 ) + 1;
#        while ( 1 ) {
#            #my( $tmp ) = $unicode =~ /\A(\X{0,$max_length})/;
#            my $tmp = substr( $unicode, 0, $max_length );
#            my $gcs = Unicode::GCString->new( $tmp );
#            $colwidth = $gcs->columns();
#            if ( $colwidth > $arg->{length_longest} ) {
#                # As soon as the string is longer than length_longest again:
#                $unicode = $tmp;
#                last;
#            }
#            $max_length += 10;
#        }
#        while ( $colwidth > $arg->{length_longest} ) {
#            $unicode =~ s/\X\z//;
#            my $gcs = Unicode::GCString->new( $unicode );
#            $colwidth = $gcs->columns();
#        }
#        $unicode .= ' ' if $colwidth < $arg->{length_longest};
#    }
#    elsif ( $colwidth < $arg->{length_longest} ) {
#        if ( $arg->{right_justify} ) {
#            $unicode = " " x ( $arg->{length_longest} - $colwidth ) . $unicode;
#        }
#        else {
#            $unicode = $unicode . " " x ( $arg->{length_longest} - $colwidth );
#        }
#    }
#    return $unicode;
#}


sub _unicode_sprintf {
    my ( $arg, $unicode ) = @_;
    utf8::upgrade( $unicode );
    my $gcs = Unicode::GCString->new( $unicode );
    my $colwidth = $gcs->columns();
    if ( $colwidth > $arg->{length_longest} ) {
        my $max_length = int( $arg->{length_longest} / 2 ) + 1;
        while ( 1 ) {
            #my( $tmp ) = $unicode =~ /\A(\X{0,$max_length})/;
            my $tmp = substr( $unicode, 0, $max_length );
            my $gcs = Unicode::GCString->new( $tmp );
            $colwidth = $gcs->columns();
            if ( $colwidth > $arg->{length_longest} ) {
                # As soon as the string is longer than length_longest again:
                $unicode = $tmp;
                last;
            }
            $max_length += 10;
        }
        while ( $colwidth > $arg->{length_longest} ) {
            $unicode =~ s/\X\z//;
            my $gcs = Unicode::GCString->new( $unicode );
            $colwidth = $gcs->columns();
        }
        $unicode .= ' ' if $colwidth < $arg->{length_longest};
    }
    elsif ( $colwidth < $arg->{length_longest} ) {
        if ( $arg->{justify} == 0 ) {
            $unicode = $unicode . " " x ( $arg->{length_longest} - $colwidth );
        }
        elsif ( $arg->{justify} == 1 ) {
            $unicode = " " x ( $arg->{length_longest} - $colwidth ) . $unicode;
        }
        elsif ( $arg->{justify} == 2 ) {
            my $all = $arg->{length_longest} - $colwidth;
            my $half = int( $all / 2 ); 
            $unicode = " " x $half . $unicode . " " x ( $all - $half );
        }
        
    }
    return $unicode;
}



sub _handle_mouse {
    my ( $x, $y, $button_pressed, $button_drag, $arg ) = @_;
    return NEXT_getch if $button_drag;
    my $top_row = $arg->{abs_curs_Y} - $arg->{cursor_row};
    # abs_curs_Y: on which row (one  based index) on the            screen is the cursor after _write_first_screen
    # cursor_row: on which row (zero based index) of the printed list rows is the cursor after _write_first_screen
    # top_row   : which mouse row corresponds to the first list row of the printed list rows
    if ( $button_pressed == 4 ) {
        return KEY_UP;
    }
    elsif ( $button_pressed == 5 ) {
        return KEY_DOWN;
    }
    return NEXT_getch if $y < $top_row;
    my $mouse_row = $y - $top_row;
    my $mouse_col = $x;
    my( $found_row, $found_col );
    my $found = 0;
    for my $row ( 0 .. $#{$arg->{rowcol2list}} ) {
        if ( $row == $mouse_row ) {
            for my $col ( 0 .. $#{$arg->{rowcol2list}[$row]} ) {
                if ( ( $col * $arg->{col_width} < $mouse_col ) && ( ( $col + 1 ) * $arg->{col_width} >= $mouse_col ) ) {
                    $found = 1;
                    $found_row = $row + $arg->{top_listrow};
                    $found_col = $col;
                    last;
                }
            }
        }
    }
    return NEXT_getch if ! $found;
    my $return_char = '';
    if ( $button_pressed == 1 ) {
        $return_char = KEY_ENTER;
    }
    elsif ( $button_pressed == 3  ) {
        $return_char = KEY_SPACE;
    }
    else {
        return NEXT_getch; # xterm
    }
    if ( $found_row != $arg->{this_cell}[ROW] || $found_col != $arg->{this_cell}[COL] ) {
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

Version 1.019

=cut

=head1 SYNOPSIS

    use 5.10.1;
    use Term::Choose qw(choose);

    my $array_ref = [ qw( one two three four five ) ];

    my $choice = choose( $array_ref );                            # single choice
    say $choice;

    my @choices = choose( [ 1 .. 100 ], { right_justify => 1 } ); # multiple choice
    say "@choices";

    choose( [ 'Press ENTER to continue' ], { prompt => 0 } );     # no choice


=head1 DESCRIPTION

Choose from a list of items.

Based on the I<choose> function from the L<Term::Clui> module - for more details see L</MOTIVATION>.

=head1 EXPORT

Nothing by default.

    use Term::Choose qw(choose);

=head1 SUBROUTINES

=head2 choose

    $scalar = choose( $array_ref [, \%options] );

    @array =  choose( $array_ref [, \%options] );

              choose( $array_ref [, \%options] );

I<choose> expects as a first argument an array reference. The array the reference refers to holds the list items available for selection (in void context no selection can be made).

The array the reference - passed with the first argument - refers to is called in the documentation simply array resp. elements (of the array).

Options can be passed with a hash reference as a second (optional) argument.

=head3 Usage and return values

=over

=item

If I<choose> is called in a I<scalar context>, the user can choose an item by using the "move-around-keys" and confirming with "Return".

I<choose> then returns the chosen item.

=item

If I<choose> is called in an I<list context>, the user can also mark an item with the "SpaceBar".

I<choose> then returns - when "Return" is pressed - the list of marked items including the highlighted item.

=item

If I<choose> is called in an I<void context>, the user can move around but mark nothing; the output shown by I<choose> can be closed with "Return".

Called in void context I<choose> returns nothing.

=back

If the items of the list don't fit on the screen, the user can scroll to the next (previous) page(s).

If the window size is changed, then as soon as the user enters a keystroke I<choose> rewrites the screen. In list context marked items are reset.

The "q" key returns I<undef> or an empty list in list context.

With a I<mouse_mode> enabled (and if supported by the terminal) the item can be chosen with the left mouse key, in list context the right mouse key can be used instead the "SpaceBar" key.

Keys to move around: arrow keys (or hjkl), Tab, BackSpace (or Shift-Tab or Ctrl-H), PageUp and PageDown (or Ctrl+B/Ctrl+F).

=head3 Modifications for the output

For the output on the screen the array elements are modified:

=over

=item *

if an element is not defined the value from the option I<undef> is assigned to the element.

=item *

if an element holds an empty string the value from the option I<empty_string> is assigned to the element.

=item *

white-spaces in elements are replaced with simple spaces.

    $element =~ s/\p{Space}/ /g;

=item *

control characters are removed.

    $element =~ s/\p{Cntrl}//g;

=item *

if the length of an element is greater than the width of the screen the element is cut.

    $element = substr( $element, 0, $allowed_length - 3 ) . '...';*

* L<Term::Choose> uses its own function to cut strings which uses print columns for the arithmetic.

=back

All these modifications are made on a copy of the original array so I<choose> returns the chosen elements as they were passed to the function without modifications.

=head3 Options

All options are optional.

Defaults may change in a future release.

Options which expect a number as their value expect integers.

There is a general upper limit of 1_000_000_000 for options which expect a number as their value and where no upper limit is mentioned.

=head4 prompt

If I<prompt> is undefined a default prompt-string will be shown.

If I<prompt> is 0 no prompt-line will be shown.

default in list and scalar context: 'Your choice:'

default in void context: 'Close with ENTER'

=head4 layout

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
 | .. .. .. .. .. .. .. |   | .. .. .. .. ..       |   | .. .. .. .. .. ..    |   | .. .. .. .. .. .. .. |
 |                      |   | .. .. .. .. ..       |   | .. .. .. .. .. ..    |   | .. .. .. .. .. .. .. |
 |                      |   | .. ..                |   | .. .. .. .. .. ..    |   | .. .. .. .. .. .. .. |
 |                      |   |                      |   | .. .. .. .. .. ..    |   | .. .. .. .. .. .. .. |
 |                      |   |                      |   | .. .. ..             |   | .. .. .. .. .. .. .. |
 |                      |   |                      |   |                      |   | .. .. .. .. .. .. .. |
 '----------------------'   '----------------------'   '----------------------'   '----------------------'


=item

2 - layout "V"

 .----------------------.   .----------------------.   .----------------------.   .----------------------.
 | ..                   |   | .. ..                |   | .. .. .. ..          |   | .. .. .. .. .. .. .. |
 | ..                   |   | .. ..                |   | .. .. .. ..          |   | .. .. .. .. .. .. .. |
 | ..                   |   | .. ..                |   | .. .. .. ..          |   | .. .. .. .. .. .. .. |
 | ..                   |   | ..                   |   | .. .. ..             |   | .. .. .. .. .. .. .. |
 | ..                   |   |                      |   | .. .. ..             |   | .. .. .. .. .. .. .. |
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

If set, restricts the screen width to the integer value of I<screen_width> percentage of the effective screen width.

If the integer value of I<screen_width> percentage of the screen width is zero the virtual screen width is set to one screen column.

If not defined all the screen width is used.

Allowed values: from 1 to 100

(default: undef)

=head4 vertical DEPRECATED

This option will be removed - use I<order> instead.

If the output has more than one row and more than one column:

0 - elements are ordered horizontally

1 - elements are ordered vertically (default)

=head4 order

If the output has more than one row and more than one column:

0 - elements are ordered horizontally

1 - elements are ordered vertically (default)

=head4 right_justify DEPRECATED

This option will be removed - use I<justify> instead.

0 - elements ordered in columns are left justified (default)

1 - elements ordered in columns are right justified

=head4 justify

0 - elements ordered in columns are left justified (default)

1 - elements ordered in columns are right justified

2 - elements ordered in columns are centered

=head4 pad

Sets the number of whitespaces between columns. (default: 2)

Allowed values:  0 or greater

=head4 pad_one_row

Sets the number of whitespaces between elements if we have only one row. (default: 3)

Allowed values:  0 or greater

=head4 clear_screen

0 - off (default)

1 - clears the screen before printing the choices

=head4 length_longest

If the length* of the element with the largest length is known before calling I<choose> it can be passed with this option.

If I<length_longest> is set, then I<choose> doesn't calculate the length of the longest element itself but uses the value passed with this option.

If I<length_longest> is set to a value less than the length of the longest element all elements which a length greater than this value will be cut.

A larger value than the length of the longest element wastes space on the screen.

If the value of I<length_longest> is greater than the screen width I<length_longest> will be set to the screen width.

* length means the number of print columns the element will use on the terminal.

Allowed values: 1 or greater

(default: undef)

=head4 default

With the option I<default> can be selected an element, which will be highlighted as the default instead of the first element.

I<default> expects a zero indexed value, so e.g. to highlight the third element the value would be I<2>.

If the passed value is greater than the index of the last array element the first element is highlighted.

Allowed values:  0 or greater

(default: undef)

=head4 page

0 - off

1 - print the page number on the bottom of the screen if there is more then one page. (default)

=head4 mouse_mode

0 - no mouse mode (default)

1 - mouse mode 1003 enabled

2 - mouse mode 1003 enabled; maxcols/maxrows limited to 224 (mouse mode 1003 doesn't work above 224)

3 - extended mouse mode (1005) - uses utf8

4 - extended SGR mouse mode (1006) - mouse mode 1003 is used if mouse mode 1006 is not supported

=head4 undef

Sets the string displayed on the screen instead an undefined element.

default: '<undef>'

=head4 empty_string

Sets the string displayed on the screen instead an empty string.

default: '<empty>'

=head4 beep

0 - off (default)

1 - on

=head4 hide_cursor

0 - keep the terminals highlighting of the cursor position

1 - hide the terminals highlighting of the cursor position (default)

=head4 limit

Sets the maximal allowed length of the array. (default: 100_000)

Allowed values:  1 or greater

=head3 Error handling

=over

=item * With no arguments I<choose> dies.

=item * If the first argument is not a array reference I<choose> dies.

=item * If the array referred by the first argument is empty I<choose> returns  I<undef> resp. an empty list and issues a warning.

=item * If the array referred by the first argument has more than I<limit> elements (default 100_000) I<choose> warns and uses the first I<limit> array elements.

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

L<Term::ReadKey>

=item

L<Unicode::GCString>

=back

=head2 Decoded strings

I<choose> expects decoded strings as array elements.

=head2 Monospaced font

It is needed a terminal that uses a monospaced font.

=head2 SIGWINCH

L<Term::Choose> makes use of the Perl signal handling as described in L<perlipc/Signals|http://search.cpan.org/perldoc?perlipc#Signals>. It is needed an operating system which knows the WINCH signal: I<choose> uses SIGWINCH to check if the windows size has changed.

=head2 Escape sequences

The Terminal needs to understand the following ANSI escape sequences:

    "\e[A"      Cursor Up

    "\e[C"      Cursor Forward

    "\e[0J"     Clear to  End of Screen (Erase Data)

    "\e[0m"     Normal/Reset

    "\e[1m"     Bold

    "\e[4m"     Underline

    "\e[7m"     Inverse


If the option "hide_cursor" is enabled:

    "\e[?25l"   Hide Cursor

    "\e[?25h"   Show Cursor

If the option "clear_screen" is enabled:

    "\e[2J"     Clear Screen (Erase Data)

    "\e[1;1H"   Go to Top Left (Cursor Position)

If a "mouse_mode" is enabled:

    "\e[6n"     Get Cursor Position (Device Status Report)

Mouse Tracking: The escape sequences

    "\e[?1003h", "\e[?1005h", "\e[?1006h"

and

    "\e[?1003l", "\e[?1005l", "\e[?1006l"

are used to enable/disable the different mouse modes.

=head1 MOTIVATION

The reason for writing L<Term::Choose> was to get something like L<Term::Clui::choose|http://search.cpan.org/perldoc?Term%3A%3AClui#SUBROUTINES> but with a nicer output in the case the list doesn't fit in one row.

If the list does not fit in one row, I<choose> from L<Term::Clui> puts the items on the screen without ordering the items in columns. L<Term::Choose> arranges the items in columns which makes it easier for me to find items and easier to navigate on the screen.

=over

=item Differences between L<Term::Clui> and L<Term::Choose>

L<Term::Clui>'s I<choose> expects a I<question> as the first argument, and then the list of items. With L<Term::Choose> the available choices are passed with an array reference as first argument. Options can be passed with a hash reference as an optional second argument. The I<question> can be passed with the option I<prompt>.

As mentioned above I<choose> from L<Term::Clui> does not order the elements in columns if there is more than one row on the screen while L<Term::Choose> arranges the elements in such situations in columns.

Another difference is how lists which don't fit on the screen are handled. L<Term::Clui::choose|http://search.cpan.org/perldoc?Term::Clui#SUBROUTINES> asks the user to enter a substring as a clue. As soon as the matching items will fit, they are displayed as normal. I<choose> from L<Term::Choose> skips - when scrolling and reaching the end (resp. the begin) of the screen - to the next (resp. previous) page.

Strings with characters where I<length(>characterI<)>* is not equal to the number of print columns of the respective character might break the output from L<Term::Clui>. To make L<Term::Choose>'s I<choose> function work with such kind of Unicode strings it uses the method I<columns> from L<Unicode::GCString> to determine the string length.

* Perl builtin function I<length>.

L<Term::Clui>'s I<choose> prints and returns the chosen items while I<choose> from L<Term::Choose> only returns the chosen items.

L<Term::Clui> disables the mouse mode if the environment variable I<CLUI_MOUSE> is set to I<off>. In L<Term::Choose> the mouse mode is set with the option I<mouse_mode>.

=item Only in L<Term::Clui>

L<Term::Clui> provides a speaking interface, offers a bundle of functions and has a fallback to work when only Perl core modules are available.

The I<choose> function from L<Term::Clui> can remember choices made in scalar context and allows multiline question - the first line is put on the top, the subsequent lines are displayed below the list.

=back

These differences refer to L<Term::Clui> version 1.66. For a more precise description of L<Term::Clui> consult its own documentation.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Term::Choose

=head1 AUTHOR

Matthäus Kiem <cuer2s@gmail.com>

=head1 CREDITS

Based on and inspired by the I<choose> function from the L<Term::Clui> module.

Thanks to the L<Perl-Community.de|http://www.perl-community.de> and the people form L<stackoverflow|http://stackoverflow.com> for the help.

=head1 LICENSE AND COPYRIGHT

Copyright 2012 Matthäus Kiem.

This library is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

=cut

