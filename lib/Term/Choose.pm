package Term::Choose;

use 5.10.0;
use strict;

our $VERSION = '1.061';
use Exporter 'import';
our @EXPORT_OK = qw(choose);

use Carp qw(croak carp);
use Term::ReadKey qw(GetTerminalSize ReadKey);
use Text::LineFold;
use Unicode::GCString;

#use warnings FATAL => qw(all);
#use Log::Log4perl qw(get_logger);
#my $log = get_logger( 'Term::Choose' );

use constant {
    ROW     => 0,
    COL     => 1,
    MIN     => 0,
    MAX     => 1,
};

use constant {
    UP                              => "\e[A",
    RIGHT                           => "\e[C",
    LF                              => "\n",
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

    BEEP                            => "\07",
    CLEAR_SCREEN                    => "\e[2J",
    GO_TO_TOP_LEFT                  => "\e[1;1H",
    CLEAR_TO_END_OF_SCREEN          => "\e[0J",
    RESET                           => "\e[0m",
    UNDERLINE                       => "\e[4m",
    REVERSE                         => "\e[7m",
    BOLD                            => "\e[1m",
};

use constant {
    NEXT_read_key      => -1,

    CONTROL_SPACE   => 0x00,
    CONTROL_A       => 0x01,
    CONTROL_B       => 0x02,
    CONTROL_C       => 0x03,
    CONTROL_D       => 0x04,
    CONTROL_E       => 0x05,
    CONTROL_F       => 0x06,
    CONTROL_H       => 0x08,
    KEY_BTAB        => 0x08,
    CONTROL_I       => 0x09,
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
    KEY_PAGE_UP     => 0x1b5b35,
    KEY_PAGE_DOWN   => 0x1b5b36,
    KEY_HOME        => 0x1b5b48,
    KEY_END         => 0x1b5b46,
    KEY_INSERT      => 0x1b5b32,
    KEY_DELETE      => 0x1b5b33,
};


sub _read_key {
    my ( $arg ) = @_;
    my $c1 = ReadKey 0;
    return if ! defined $c1;
    if ( $c1 eq "\e" ) {
        my $c2 = ReadKey 0.10;
        if ( ! defined $c2 ) { return KEY_ESC; } # unused
        elsif ( $c2 eq '[' ) {
            my $c3 = ReadKey 0;
               if ( $c3 eq 'A' ) { return KEY_UP; }
            elsif ( $c3 eq 'B' ) { return KEY_DOWN; }
            elsif ( $c3 eq 'C' ) { return KEY_RIGHT; }
            elsif ( $c3 eq 'D' ) { return KEY_LEFT; }
            elsif ( $c3 eq 'F' ) { return KEY_END; }
            elsif ( $c3 eq 'H' ) { return KEY_HOME; }
            elsif ( $c3 eq 'Z' ) { return KEY_BTAB; }
            elsif ( $c3 =~ /^[0-9]$/ ) {
                my $c4 = ReadKey 0;
                   if ( $c4 eq '~' ) {
                       if ( $c3 eq '2' ) { return KEY_INSERT; } # unused
                    elsif ( $c3 eq '3' ) { return KEY_DELETE; } # unused
                    elsif ( $c3 eq '5' ) { return KEY_PAGE_UP; }
                    elsif ( $c3 eq '6' ) { return KEY_PAGE_DOWN; }
                    else {
                        return NEXT_read_key;
                    }
                }
                elsif ( $c4 =~ /^[;0-9]$/ ) { # response to "\e[6n"
                    my $abs_curs_y = $c3;
                    my $ry = $c4;
                    while ( $ry =~ m/^[0-9]$/ ) {
                        $abs_curs_y .= $ry;
                        $ry = ReadKey 0;
                    }
                    return NEXT_read_key if $ry ne ';';
                    my $abs_curs_x = '';
                    my $rx = ReadKey 0;
                    while ( $rx =~ m/^[0-9]$/ ) {
                        $abs_curs_x .= $rx;
                        $rx = ReadKey 0;
                    }
                    if ( $rx eq 'R' ) {
                        $arg->{abs_cursor_y} = $abs_curs_y;
                        $arg->{abs_cursor_x} = $abs_curs_x; # unused
                    }
                    return NEXT_read_key;
                }
                else {
                    return NEXT_read_key;
                }
            }
            # http://invisible-island.net/xterm/ctlseqs/ctlseqs.html
            elsif ( $c3 eq 'M' && $arg->{mouse} ) {
                my $event_type = ord( ReadKey 0 ) - 32;
                my $x          = ord( ReadKey 0 ) - 32;
                my $y          = ord( ReadKey 0 ) - 32;
                return _handle_mouse( $arg, $event_type, $x, $y );
            }
            elsif ( $c3 eq '<' && $arg->{mouse} ) {  # SGR 1006
                my $event_type = '';
                my $m1;
                while ( ( $m1 = ReadKey 0 ) =~ m/^[0-9]$/ ) {
                    $event_type .= $m1;
                }
                return NEXT_read_key if $m1 ne ';';
                my $x = '';
                my $m2;
                while ( ( $m2 = ReadKey 0 ) =~ m/^[0-9]$/ ) {
                    $x .= $m2;
                }
                return NEXT_read_key if $m2 ne ';';
                my $y = '';
                my $m3;
                while ( ( $m3 = ReadKey 0 ) =~ m/^[0-9]$/ ) {
                    $y .= $m3;
                }
                return NEXT_read_key if $m3 !~ /^[mM]$/;
                my $button_released = $m3 eq 'm' ? 1 : 0;
                return NEXT_read_key if $button_released;
                return _handle_mouse( $arg, $event_type, $x, $y );
            }
            else {
                return NEXT_read_key;
            }
        }
        else {
            return NEXT_read_key;
        }
    }
    else {
        return ord $c1;
    }
}


sub _init_scr {
    # OO so DESTROY does the cleanup.
    my $class = shift;
    my ( $arg ) = @_;
    my $self = bless $arg, $class;
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
    Term::ReadKey::ReadMode 'ultra-raw';
    return $self;
}

sub DESTROY {
    my $self = shift;
    print CR, UP x ( $self->{screen_row} + $self->{nr_prompt_lines} );
    print CLEAR_TO_END_OF_SCREEN;
    print RESET;
    if ( $self->{mouse} ) {
        binmode STDIN, ':encoding(UTF-8)' or warn "binmode STDIN, :encoding(UTF-8): $!\n";
        print UNSET_EXT_MODE_MOUSE_1005     if $self->{mouse} == 3;
        print UNSET_SGR_EXT_MODE_MOUSE_1006 if $self->{mouse} == 4;
        print UNSET_ANY_EVENT_MOUSE_1003;
    }
    print SHOW_CURSOR if $self->{hide_cursor};
    $| = $self->{backup_flush};
    Term::ReadKey::ReadMode 'restore';
    select( $self->{old_handle} );
    carp "EOT: $!"      if $self->{EOT};
    print STDERR "^C\n" if $self->{cC};
}


sub _length_longest {
    my ( $arg ) = @_;
    if ( defined $arg->{ll} ) {
        $arg->{length_longest} = $arg->{ll};
        $arg->{length} = [];
    }
    else {
        my $list = $arg->{list};
        my $len;
        my $longest = 0;
        for my $i ( 0 .. $#$list ) {
            my $gcs = Unicode::GCString->new( $list->[$i] );
            $len->[$i] = $gcs->columns();
            $longest = $len->[$i] if $len->[$i] > $longest;
        }
        $arg->{length_longest} = $longest;
        $arg->{length} = $len;
    }
}


sub _copy_orig_list {
    my ( $arg ) = @_;
    if ( $arg->{ll} ) {
        if ( $arg->{list_to_long} ) {
            return [ map {
                my $copy = $_;
                if ( ! $copy ) {
                    $copy = $arg->{undef} if ! defined $copy;
                    $copy = $arg->{empty} if $copy eq '';
                }
                $copy;
            } @{$arg->{orig_list}}[ 0 .. $arg->{limit} - 1 ] ];
        }
        return [ map {
            my $copy = $_;
            if ( ! $copy ) {
                $copy = $arg->{undef} if ! defined $copy;
                $copy = $arg->{empty} if $copy eq '';
            }
            $copy;
        } @{$arg->{orig_list}} ];
    }
    else {
        if ( $arg->{list_to_long} ) {
            return [ map {
                my $copy = $_;
                if ( ! $copy ) {
                    $copy = $arg->{undef} if ! defined $copy;
                    $copy = $arg->{empty} if $copy eq '';
                }
                utf8::upgrade( $copy );
                $copy =~ s/\p{Space}/ /g;  # replace, but don't squash sequences of spaces
                $copy =~ s/\P{Print}/\x{fffd}/g;  # (�)
                $copy;
            } @{$arg->{orig_list}}[ 0 .. $arg->{limit} - 1 ] ];
        }
        return [ map {
            my $copy = $_;
            if ( ! $copy ) {
                $copy = $arg->{undef} if ! defined $copy;
                $copy = $arg->{empty} if $copy eq '';
            }
            utf8::upgrade( $copy );
            $copy =~ s/\p{Space}/ /g;
            $copy =~ s/\P{Print}/\x{fffd}/g;
            $copy;
        } @{$arg->{orig_list}} ];
    }
}


sub _set_defaults {
    my ( $config, $wantarray ) = @_;
    my $prompt = defined $wantarray ? 'Your choice:' : 'Close with ENTER';
    $config->{beep}             //= 0;
    $config->{clear_screen}     //= 0;
    #$config->{default}         //= undef;
    $config->{empty}            //= '<empty>';
    $config->{hide_cursor}      //= 1;
    $config->{index}            //= 0;
    $config->{justify}          //= 0;
    $config->{keep}             //= 5;
    $config->{layout}           //= 1;
    #$config->{lf}              //= undef;
    #$config->{ll}              //= undef;
    $config->{limit}            //= 100_000;
    #$config->{max_height}      //= undef;
    #$config->{max_width}       //= undef;
    $config->{mouse}            //= 0;
    $config->{order}            //= 1;
    $config->{pad}              //= 2;
    $config->{pad_one_row}      //= $config->{pad};
    $config->{page}             //= 1;
    $config->{prompt}           //= $prompt;
    $config->{undef}            //= '<undef>';
    return $config;
}


sub _validate_options {
    my ( $config, $wantarray, $list_length ) = @_;
    my $limit = 1_000_000_000;
    my $validate = {    #   min      max
        beep            => [ 0,       1 ],
        clear_screen    => [ 0,       1 ],
        default         => [ 0,  $limit ],
        empty           => '',
        hide_cursor     => [ 0,       1 ],
        index           => [ 0,       1 ],
        justify         => [ 0,       2 ],
        keep            => [ 1,  $limit ],
        layout          => [ 0,       3 ],
        lf              => 'ARRAY',
        ll              => [ 1,  $limit ],
        limit           => [ 1,  $limit ],
        max_height      => [ 1,  $limit ],
        max_width       => [ 1,  $limit ],
        mouse           => [ 0,       4 ],
        order           => [ 0,       1 ],
        pad             => [ 0,  $limit ],
        pad_one_row     => [ 0,  $limit ],
        page            => [ 0,       1 ],
        prompt          => '',
        screen_width    => [ 1,  $limit ], # DEPRECATED
        undef           => '',
    };
    # ###
    if ( defined $config->{screen_width} && ! defined $config->{max_width} ) {
        $config->{max_width} = $config->{screen_width};
    }
    # ###
    my $warn = 0;
    for my $key ( keys %$config ) {
        if ( ! exists $validate->{$key} ) {
            carp "choose: \"$key\" is not a valid option";
            delete $config->{$key};
            $warn++;
        }
        elsif ( $validate->{$key} ) {
            next if ! defined $config->{$key};
            if ( $key eq 'lf' ) {
                my $warn_str;
                if ( ref( $config->{$key} ) ne 'ARRAY' ) {
                    $warn_str = "choose: the value for the option \"$key\" is not a ARRAY reference.";
                }
                elsif ( @{$config->{$key}} != 2 ) {
                    $warn_str = "choose: the value for the option \"$key\" is not a reference to an array with 2 elements.";
                }
                else {
                    for my $i ( 0 .. $#{$config->{$key}} ) {
                        if ( defined $config->{$key}[$i] && $config->{$key}[$i] !~ m/^[0-9]+\z/ ) {
                            my $sep = $warn_str ? "\n" : "";
                            $warn_str .= "choose: option \"$key\": ";
                            $warn_str .= "'$config->{$key}[$i]' is not a valid value.$sep";
                        }
                    }
                }
                if ( $warn_str ) {
                    $warn_str .= " Falling back to the default value.";
                    carp $warn_str;
                    $config->{$key} = undef;
                    $warn++;
                }
            }
            elsif (   $config->{$key} !~ m/^[0-9]+\z/
                   || $config->{$key} < $validate->{$key}[MIN]
                   || $config->{$key} > $validate->{$key}[MAX]
            ) {
                carp "choose: \"$config->{$key}\" is not a valid value for the option \"$key\"."
                    . " Falling back to the default value.";
                $config->{$key} = undef;
                $warn++;
            }
        }
        # elsif ( $validate->{$key} eq '' ) {
        #     nothing to do;
        # }
    }
    $config = _set_defaults( $config, $wantarray );
    if ( $list_length > $config->{limit} ) {
        carp "choose: The list has $list_length items. Option \"limit\" is set to $config->{limit}. "
           . "The first $config->{limit} itmes are used by choose.";
        $config->{list_to_long} = 1;
        print "Press a key to continue";
        $warn++;
    }
    if ( $warn ) {
        print "Press a key to continue";
        my $dummy = <STDIN>;
    }
    $config->{wantarray} = $wantarray;
    return $config;
}


sub _set_default_cell {
    my ( $arg ) = @_;
    $arg->{tmp_cursor} = [ 0, 0 ];
    LOOP: for my $i ( 0 .. $#{$arg->{rc2idx}} ) {
        # if ( $arg->{default} ~~ @{$arg->{rc2idx}[$i]} ) {
            for my $j ( 0 .. $#{$arg->{rc2idx}[$i]} ) {
                if ( $arg->{default} == $arg->{rc2idx}[$i][$j] ) {
                    $arg->{tmp_cursor} = [ $i, $j ];
                    last LOOP;
                }
            }
        # }
    }
    while ( $arg->{tmp_cursor}[ROW] > $arg->{p_end} ) {
        $arg->{row_on_top} = $arg->{avail_height} * ( int( $arg->{cursor}[ROW] / $arg->{avail_height} ) + 1 );
        $arg->{cursor}[ROW] = $arg->{row_on_top};
        $arg->{p_begin} = $arg->{row_on_top};
        $arg->{p_end} = $arg->{p_begin} + $arg->{avail_height} - 1;
        $arg->{p_end} = $#{$arg->{rc2idx}} if $arg->{p_end} > $#{$arg->{rc2idx}};
    }
    $arg->{cursor} = $arg->{tmp_cursor};
}


sub _prepare_page_number {
    my ( $arg ) = @_;
    $arg->{pp} = int( $#{$arg->{rc2idx}} / ( $arg->{avail_height} + $arg->{tail} ) ) + 1;
    if ( $arg->{pp} > 1 ) {
        $arg->{pp} = int( $#{$arg->{rc2idx}} / $arg->{avail_height} ) + 1;
        $arg->{width_pp} = length $arg->{pp};
        $arg->{pp_printf_fmt} = "--- Page %0*d/%d ---";
        $arg->{pp_printf_type} = 0;
        if ( length sprintf( $arg->{pp_printf_fmt}, $arg->{width_pp}, $arg->{pp}, $arg->{pp} )  > $arg->{avail_width} ) {
            $arg->{pp_printf_fmt} = "%0*d/%d";
            if ( length sprintf( $arg->{pp_printf_fmt}, $arg->{width_pp}, $arg->{pp}, $arg->{pp} )  > $arg->{avail_width} ) {
                $arg->{width_pp} = $arg->{avail_width} if $arg->{width_pp} > $arg->{avail_width};
                $arg->{pp_printf_fmt} = "%0*.*s";
                $arg->{pp_printf_type} = 1;
            }
        }
    }
    else {
        $arg->{avail_height} += $arg->{tail};
        $arg->{tail} = 0;
    }
}


sub _prepare_promptline {
    my ( $arg ) = @_;
    $arg->{prompt} =~ s/[^\n\P{Space}]/ /g;
    $arg->{prompt} =~ s/[^\n\p{Print}]/\x{fffd}/g;
    utf8::upgrade( $arg->{prompt} );
    my $gcs_prompt = Unicode::GCString->new( $arg->{prompt} );
    if ( $arg->{prompt} !~ /\n/ && $gcs_prompt->columns() <= $arg->{avail_width} ) {
        $arg->{nr_prompt_lines} = 1;
        $arg->{prompt_copy} = $arg->{prompt} . "\n\r";
    }
    else {
        my $line_fold = Text::LineFold->new(
            Charset=> 'utf-8',
            ColMax => $arg->{avail_width},
            OutputCharset => '_UNICODE_',
            Urgent => 'FORCE'
        );
        if ( defined $arg->{lf} ) {
            $arg->{prompt_copy} = $line_fold->fold( ' ' x $arg->{lf}[0] // 0, ' ' x $arg->{lf}[1] // 0, $arg->{prompt} );
        }
        else {
            $arg->{prompt_copy} = $line_fold->fold( $arg->{prompt}, 'PLAIN' );
        }
        $arg->{nr_prompt_lines} = $arg->{prompt_copy} =~ s/\n/\n\r/g;
    }
}


sub _write_first_screen {
    my ( $arg ) = @_;
    ( $arg->{avail_width}, $arg->{avail_height} ) = GetTerminalSize( $arg->{handle_out} );
    if ( $arg->{max_width} && $arg->{avail_width} > $arg->{max_width} ) {
        $arg->{avail_width} = $arg->{max_width};
    }
    if ( $arg->{mouse} == 2 ) {
        $arg->{avail_width}  = MAX_COL_MOUSE_1003 if $arg->{avail_width}  > MAX_COL_MOUSE_1003;
        $arg->{avail_height} = MAX_ROW_MOUSE_1003 if $arg->{avail_height} > MAX_ROW_MOUSE_1003;
    }
    $arg->{avail_width} = 1 if $arg->{avail_width}  < 1;
    if ( $arg->{prompt} eq '' ) {
        $arg->{nr_prompt_lines} = 0;
    }
    else {
        _prepare_promptline( $arg );
    }
    $arg->{tail} = $arg->{page} ? 1 : 0;
    $arg->{avail_height} -= $arg->{nr_prompt_lines} + $arg->{tail};
    if ( $arg->{avail_height} < $arg->{keep} ) {
        my $height = ( GetTerminalSize( $arg->{handle_out} ) )[1];
        $arg->{avail_height} = $height >= $arg->{keep} ? $arg->{keep} : $height;
        $arg->{avail_height} = 1 if $arg->{avail_height} < 1;
    }
    $arg->{avail_height} = $arg->{max_height} if $arg->{max_height} && $arg->{max_height} < $arg->{avail_height};
    _size_and_layout( $arg );
    _prepare_page_number( $arg ) if $arg->{page};
    $arg->{avail_height_idx} = $arg->{avail_height} - 1;
    $arg->{p_begin}    = 0;
    $arg->{p_end}      = $arg->{avail_height_idx};
    $arg->{p_end}      = $#{$arg->{rc2idx}} if $arg->{avail_height_idx} > $#{$arg->{rc2idx}};
    $arg->{marked}     = [];
    $arg->{row_on_top} = 0;
    $arg->{screen_row} = 0;
    $arg->{cursor}     = [ 0, 0 ];
    _set_default_cell( $arg ) if defined $arg->{default} && $arg->{default} <= $#{$arg->{list}};
    if ( $arg->{clear_screen} ) {
        print CLEAR_SCREEN;
        print GO_TO_TOP_LEFT;
    }
    print $arg->{prompt_copy} if $arg->{prompt} ne '';
    _wr_screen( $arg );
    if ( $arg->{mouse} ) {
        $arg->{abs_cursor_x} = 0;
        $arg->{abs_cursor_y} = 0;
        print GET_CURSOR_POSITION;
        $arg->{cursor_row} = $arg->{screen_row};
    }
}


sub choose {
    my ( $orig_list_ref, $config ) = @_;
    croak "choose: called without arguments. 'choose' expects 1 or 2 arguments." if @_ < 1;
    croak "choose: called with " . scalar @_ . " arguments. 'choose' expects 1 or 2 arguments." if @_ > 2;
    croak "choose: The first argument is not defined. "
        . "The first argument has to be an ARRAY reference." if ! defined $orig_list_ref;
    croak "choose: The first argument is not a reference. "
        . "The first argument has to be an ARRAY reference." if ref( $orig_list_ref ) eq '';
    croak "choose: The first argument is not an ARRAY reference. "
        . "The first argument has to be an ARRAY reference." if ref( $orig_list_ref ) ne 'ARRAY';
    if ( defined $config ) {
        croak "choose: The second argument is not a reference. "
            . "The (optional) second argument has to be a HASH reference." if ref( $config ) eq '';
        croak "choose: The second argument is not a HASH reference. "
            . "The (optional) second argument has to be a HASH reference." if ref( $config ) ne 'HASH';
    }
    if ( ! @$orig_list_ref ) {
        carp "choose: The first argument refers to an empty list!";
        return;
    }
    local $\ = undef;
    local $, = undef;
    my $arg = _validate_options( $config // {}, wantarray, scalar @$orig_list_ref );
    $arg->{orig_list}  = $orig_list_ref;
    $arg->{handle_out} = -t \*STDOUT ? \*STDOUT : \*STDERR;
    $arg->{list}       = _copy_orig_list( $arg );
    _length_longest( $arg );
    $arg->{col_width} = $arg->{length_longest} + $arg->{pad};
    local $SIG{'INT'} = sub {
        my $signame = shift;
        exit( 1 );
    };
    my $init = Term::Choose->_init_scr( $arg );
    $arg->{size_changed} = 0;
    my $orig_sigwinch = $SIG{'WINCH'};
    local $SIG{'WINCH'} = sub {
        $orig_sigwinch->() if $orig_sigwinch && ref $orig_sigwinch eq 'CODE';
        $arg->{size_changed} = 1;
    };
    _write_first_screen( $arg );

    while ( 1 ) {
        my $key = _read_key( $arg );
        if ( ! defined $key ) {
            $arg->{EOT} = 1;
            return;
        }
        if ( $arg->{size_changed} ) {
            $arg->{list} = _copy_orig_list( $arg );
            print CR, UP x ( $arg->{screen_row} + $arg->{nr_prompt_lines} );
            print CLEAR_TO_END_OF_SCREEN;
            _write_first_screen( $arg );
            $arg->{size_changed} = 0;
            next;
        }
        next if $key == NEXT_read_key;
        next if $key == KEY_Tilde;

        # $arg->{rc2idx} holds the new list (AoA) formated in "_size_and_layout" appropirate to the choosen layout.
        # $arg->{rc2idx} does not hold the values dircetly but the respective list indexes from the original list.
        # If the original list would be ( 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h' ) and the new formated list should be
        #     a d g
        #     b e h
        #     c f
        # then the $arg->{rc2idx} would look like this
        #     0 3 6
        #     1 4 7
        #     2 5
        # So e.g. the second value in the second row of the new list would be $arg->{list}[ $arg->{rc2idx}[1][1] ].
        # On the other hand the index of the last row of the new list would be $#{$arg->{rc2idx}}
        # or the index of the last column in the first row would be $#{$arg->{rc2idx}[0]}.

        if ( $key == KEY_j || $key == KEY_DOWN ) {
            if ( $#{$arg->{rc2idx}} == 0 || ! (    $arg->{rc2idx}[$arg->{cursor}[ROW]+1]
                                                && $arg->{rc2idx}[$arg->{cursor}[ROW]+1][$arg->{cursor}[COL]] )
            ) {
                _beep( $arg );
            }
            else {
                $arg->{cursor}[ROW]++;
                if ( $arg->{cursor}[ROW] <= $arg->{p_end} ) {
                    _wr_cell( $arg, $arg->{cursor}[ROW] - 1, $arg->{cursor}[COL] );
                    _wr_cell( $arg, $arg->{cursor}[ROW],     $arg->{cursor}[COL] );
                }
                else {
                    $arg->{row_on_top} = $arg->{cursor}[ROW];
                    $arg->{p_begin} = $arg->{p_end} + 1;
                    $arg->{p_end}   = $arg->{p_end} + $arg->{avail_height};
                    $arg->{p_end}   = $#{$arg->{rc2idx}} if $arg->{p_end} > $#{$arg->{rc2idx}};
                    _wr_screen( $arg );
                }
            }
        }
        elsif ( $key == KEY_k || $key == KEY_UP ) {
            if ( $arg->{cursor}[ROW] == 0 ) {
                _beep( $arg );
            }
            else {
                $arg->{cursor}[ROW]--;
                if ( defined $arg->{backup_col} ) {
                    $arg->{cursor}[COL] = $arg->{backup_col};
                    $arg->{backup_col} = undef;
                }
                if ( $arg->{cursor}[ROW] >= $arg->{p_begin} ) {
                    _wr_cell( $arg, $arg->{cursor}[ROW] + 1, $arg->{cursor}[COL] );
                    _wr_cell( $arg, $arg->{cursor}[ROW],     $arg->{cursor}[COL] );
                }
                else {
                    $arg->{row_on_top} = $arg->{cursor}[ROW] - ( $arg->{avail_height} - 1 );
                    $arg->{p_end}   = $arg->{p_begin} - 1;
                    $arg->{p_begin} = $arg->{p_begin} - $arg->{avail_height};
                    $arg->{p_begin} = 0 if $arg->{p_begin} < 0;
                    _wr_screen( $arg );
                }
            }
        }
        elsif ( $key == KEY_TAB || $key == CONTROL_I ) {
            if (    $arg->{cursor}[ROW] == $#{$arg->{rc2idx}}
                 && $arg->{cursor}[COL] == $#{$arg->{rc2idx}[$arg->{cursor}[ROW]]}
            ) {
                _beep( $arg );
            }
            else {
                if ( $arg->{cursor}[COL] < $#{$arg->{rc2idx}[$arg->{cursor}[ROW]]} ) {
                    $arg->{cursor}[COL]++;
                    _wr_cell( $arg, $arg->{cursor}[ROW], $arg->{cursor}[COL] - 1 );
                    _wr_cell( $arg, $arg->{cursor}[ROW], $arg->{cursor}[COL] );
                }
                else {
                    $arg->{cursor}[ROW]++;
                    if ( $arg->{cursor}[ROW] <= $arg->{p_end} ) {
                        $arg->{cursor}[COL] = 0;
                        _wr_cell( $arg, $arg->{cursor}[ROW] - 1, $#{$arg->{rc2idx}[$arg->{cursor}[ROW] - 1]} );
                        _wr_cell( $arg, $arg->{cursor}[ROW],     $arg->{cursor}[COL] );
                    }
                    else {
                        $arg->{row_on_top} = $arg->{cursor}[ROW];
                        $arg->{p_begin} = $arg->{p_end} + 1;
                        $arg->{p_end}   = $arg->{p_end} + $arg->{avail_height};
                        $arg->{p_end}   = $#{$arg->{rc2idx}} if $arg->{p_end} > $#{$arg->{rc2idx}};
                        $arg->{cursor}[COL] = 0;
                        _wr_screen( $arg );
                    }
                }
            }
        }
        elsif ( $key == KEY_BSPACE || $key == CONTROL_H || $key == KEY_BTAB ) {
            if ( $arg->{cursor}[COL] == 0 && $arg->{cursor}[ROW] == 0 ) {
                _beep( $arg );
            }
            else {
                if ( $arg->{cursor}[COL] > 0 ) {
                    $arg->{cursor}[COL]--;
                    _wr_cell( $arg, $arg->{cursor}[ROW], $arg->{cursor}[COL] + 1 );
                    _wr_cell( $arg, $arg->{cursor}[ROW], $arg->{cursor}[COL] );
                }
                else {
                    $arg->{cursor}[ROW]--;
                    if ( $arg->{cursor}[ROW] >= $arg->{p_begin} ) {
                        $arg->{cursor}[COL] = $#{$arg->{rc2idx}[$arg->{cursor}[ROW]]};
                        _wr_cell( $arg, $arg->{cursor}[ROW] + 1, 0 );
                        _wr_cell( $arg, $arg->{cursor}[ROW],     $arg->{cursor}[COL] );
                    }
                    else {
                        $arg->{row_on_top} = $arg->{cursor}[ROW] - ( $arg->{avail_height} - 1 );
                        $arg->{p_end}   = $arg->{p_begin} - 1;
                        $arg->{p_begin} = $arg->{p_begin} - $arg->{avail_height};
                        $arg->{p_begin} = 0 if $arg->{p_begin} < 0;
                        $arg->{cursor}[COL] = $#{$arg->{rc2idx}[$arg->{cursor}[ROW]]};
                        _wr_screen( $arg );
                    }
                }
            }
        }
        elsif ( $key == KEY_l || $key == KEY_RIGHT ) {
            if ( $arg->{cursor}[COL] == $#{$arg->{rc2idx}[$arg->{cursor}[ROW]]} ) {
                _beep( $arg );
            }
            else {
                $arg->{cursor}[COL]++;
                _wr_cell( $arg, $arg->{cursor}[ROW], $arg->{cursor}[COL] - 1 );
                _wr_cell( $arg, $arg->{cursor}[ROW], $arg->{cursor}[COL] );
            }
        }
        elsif ( $key == KEY_h || $key == KEY_LEFT ) {
            if ( $arg->{cursor}[COL] == 0 ) {
                _beep( $arg );
            }
            else {
                $arg->{cursor}[COL]--;
                _wr_cell( $arg, $arg->{cursor}[ROW], $arg->{cursor}[COL] + 1 );
                _wr_cell( $arg, $arg->{cursor}[ROW], $arg->{cursor}[COL] );
                # since col has been changed deliberately:
                $arg->{backup_col} = undef if defined $arg->{backup_col};
            }
        }
        elsif ( $key == CONTROL_B || $key == KEY_PAGE_UP ) {
            if ( $arg->{p_begin} <= 0 ) {
                _beep( $arg );
            }
            else {
                $arg->{row_on_top} = $arg->{avail_height} * ( int( $arg->{cursor}[ROW] / $arg->{avail_height} ) - 1 );
                $arg->{cursor}[ROW] = $arg->{row_on_top};
                if ( defined $arg->{backup_col} ) {
                    $arg->{cursor}[COL] = $arg->{backup_col};
                    $arg->{backup_col} = undef;
                }
                $arg->{p_begin} = $arg->{row_on_top};
                $arg->{p_end}   = $arg->{p_begin} + $arg->{avail_height} - 1;
                _wr_screen( $arg );
            }
        }
        elsif ( $key == CONTROL_F || $key == KEY_PAGE_DOWN ) {
            if ( $arg->{p_end} >= $#{$arg->{rc2idx}} ) {
                _beep( $arg );
            }
            else {
                $arg->{row_on_top} = $arg->{avail_height} * ( int( $arg->{cursor}[ROW] / $arg->{avail_height} ) + 1 );
                $arg->{cursor}[ROW] = $arg->{row_on_top};
                if (    $arg->{rest}
                     && $arg->{cursor}[ROW] == $#{$arg->{rc2idx}}
                     && $arg->{cursor}[COL] > $arg->{rest} - 1
                ) {
                    $arg->{backup_col} = $arg->{cursor}[COL];
                    $arg->{cursor}[COL] = $#{$arg->{rc2idx}[$arg->{cursor}[ROW]]};
                }
                $arg->{p_begin} = $arg->{row_on_top};
                $arg->{p_end}   = $arg->{p_begin} + $arg->{avail_height} - 1;
                $arg->{p_end}   = $#{$arg->{rc2idx}} if $arg->{p_end} > $#{$arg->{rc2idx}};
                _wr_screen( $arg );
            }
        }
        elsif ( $key == CONTROL_A || $key == KEY_HOME ) {
            if ( $arg->{cursor}[COL] == 0 && $arg->{cursor}[ROW] == 0 ) {
                _beep( $arg );
            }
            else {
                $arg->{row_on_top} = 0;
                $arg->{cursor}[ROW] = $arg->{row_on_top};
                $arg->{cursor}[COL] = 0;
                $arg->{p_begin} = $arg->{row_on_top};
                $arg->{p_end}   = $arg->{p_begin} + $arg->{avail_height} - 1;
                $arg->{p_end}   = $#{$arg->{rc2idx}} if $arg->{p_end} > $#{$arg->{rc2idx}};
                _wr_screen( $arg );
            }
        }
        elsif ( $key == CONTROL_E || $key == KEY_END ) {
            if ( $arg->{order} == 1 && $arg->{rest} ) {
                if (    $arg->{cursor}[ROW] == $#{$arg->{rc2idx}} - 1
                     && $arg->{cursor}[COL] == $#{$arg->{rc2idx}[$arg->{cursor}[ROW]]}
                ) {
                    _beep( $arg );
                }
                else {
                    $arg->{row_on_top} = @{$arg->{rc2idx}} - ( @{$arg->{rc2idx}} % $arg->{avail_height} || $arg->{avail_height} );
                    $arg->{cursor}[ROW] = $#{$arg->{rc2idx}} - 1;
                    $arg->{cursor}[COL] = $#{$arg->{rc2idx}[$arg->{cursor}[ROW]]};
                    if ( $arg->{row_on_top} == $#{$arg->{rc2idx}} ) {
                        $arg->{row_on_top} = $arg->{row_on_top} - $arg->{avail_height};
                        $arg->{p_begin} = $arg->{row_on_top};
                        $arg->{p_end}   = $arg->{p_begin} + $arg->{avail_height} - 1;
                    }
                    else {
                        $arg->{p_begin} = $arg->{row_on_top};
                        $arg->{p_end}   = $#{$arg->{rc2idx}};
                    }
                    _wr_screen( $arg );
                }
            }
            else {
                if (    $arg->{cursor}[ROW] == $#{$arg->{rc2idx}}
                     && $arg->{cursor}[COL] == $#{$arg->{rc2idx}[$arg->{cursor}[ROW]]}
                ) {
                    _beep( $arg );
                }
                else {
                    $arg->{row_on_top} = @{$arg->{rc2idx}} - ( @{$arg->{rc2idx}} % $arg->{avail_height} || $arg->{avail_height} );
                    $arg->{cursor}[ROW] = $#{$arg->{rc2idx}};
                    $arg->{cursor}[COL] = $#{$arg->{rc2idx}[$arg->{cursor}[ROW]]};
                    $arg->{p_begin} = $arg->{row_on_top};
                    $arg->{p_end}   = $#{$arg->{rc2idx}};
                    _wr_screen( $arg );
                }
            }
        }
        elsif ( $key == CONTROL_SPACE ) {
            if ( defined $arg->{wantarray} && $arg->{wantarray} ) {
                for my $i ( 0 .. $#{$arg->{rc2idx}} ) {
                    for my $j ( 0 .. $#{$arg->{rc2idx}[$i]} ) {
                        $arg->{marked}[$i][$j] = $arg->{marked}[$i][$j] ? 0 : 1;
                    }
                }
                _wr_screen( $arg );
            }
        }
        elsif ( $key == KEY_q || $key == CONTROL_D ) {
            return;
        }
        elsif ( $key == CONTROL_C ) {
            $arg->{cC} = 1;
            exit( 1 );
        }
        elsif ( $key == KEY_ENTER ) {
            my @chosen;
            return if ! defined $arg->{wantarray};
            if ( $arg->{wantarray} ) {
                if ( $arg->{order} == 1 ) {
                    for my $col ( 0 .. $#{$arg->{rc2idx}[0]} ) {
                        for my $row ( 0 .. $#{$arg->{rc2idx}} ) {
                            if ( $arg->{marked}[$row][$col] || $row == $arg->{cursor}[ROW] && $col == $arg->{cursor}[COL] ) {
                                my $i = $arg->{rc2idx}[$row][$col];
                                push @chosen, $arg->{index} ? $i : $arg->{orig_list}[$i];
                            }
                        }
                    }
                }
                else {
                        for my $row ( 0 .. $#{$arg->{rc2idx}} ) {
                        for my $col ( 0 .. $#{$arg->{rc2idx}[$row]} ) {
                            if ( $arg->{marked}[$row][$col] || $row == $arg->{cursor}[ROW] && $col == $arg->{cursor}[COL] ) {
                                my $i = $arg->{rc2idx}[$row][$col];
                                push @chosen, $arg->{index} ? $i : $arg->{orig_list}[$i];
                            }
                        }
                    }
                }
                return @chosen;
            }
            else {
                my $i = $arg->{rc2idx}[$arg->{cursor}[ROW]][$arg->{cursor}[COL]];
                return $arg->{index} ? $i : $arg->{orig_list}[$i];
            }
        }
        elsif ( $key == KEY_SPACE ) {
            if ( defined $arg->{wantarray} && $arg->{wantarray} ) {
                if ( ! $arg->{marked}[$arg->{cursor}[ROW]][$arg->{cursor}[COL]] ) {
                    $arg->{marked}[$arg->{cursor}[ROW]][$arg->{cursor}[COL]] = 1;
                }
                else {
                    $arg->{marked}[$arg->{cursor}[ROW]][$arg->{cursor}[COL]] = 0;
                }
                _wr_cell( $arg, $arg->{cursor}[ROW], $arg->{cursor}[COL] );
            }
        }
        else {
            _beep( $arg );
        }
    }
}


sub _beep {
    my ( $arg ) = @_;
    print BEEP if $arg->{beep};
}


sub _goto {
    my ( $arg, $newrow, $newcol ) = @_;
    print CR, RIGHT x $newcol;
    if ( $newrow > $arg->{screen_row} ) {
        print LF x ( $newrow - $arg->{screen_row} );
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
    print CLEAR_TO_END_OF_SCREEN;
    if ( $arg->{page} && $arg->{pp} > 1 ) {
        _goto( $arg, $arg->{avail_height_idx} + $arg->{tail}, 0 );
        if ( $arg->{pp_printf_type} == 0 ) {
            printf $arg->{pp_printf_fmt}, $arg->{width_pp}, int( $arg->{row_on_top} / $arg->{avail_height} ) + 1, $arg->{pp};
        }
        elsif ( $arg->{pp_printf_type} == 1 ) {
            printf $arg->{pp_printf_fmt}, $arg->{width_pp}, $arg->{width_pp}, int( $arg->{row_on_top} / $arg->{avail_height} ) + 1;
        }
     }
    for my $row ( $arg->{p_begin} .. $arg->{p_end} ) {
        for my $col ( 0 .. $#{$arg->{rc2idx}[$row]} ) {
            _wr_cell( $arg, $row, $col );
        }
    }
    _wr_cell( $arg, $arg->{cursor}[ROW], $arg->{cursor}[COL] );
}


sub _wr_cell {
    my( $arg, $row, $col ) = @_;
    if ( $#{$arg->{rc2idx}} == 0 ) {
        my $lngth = 0;
        if ( $col > 0 ) {
            for my $cl ( 0 .. $col - 1 ) {
                my $gcs_element = Unicode::GCString->new( $arg->{list}[$arg->{rc2idx}[$row][$cl]] );
                $lngth += $gcs_element->columns();
                $lngth += $arg->{pad_one_row};
            }
        }
        _goto( $arg, $row - $arg->{row_on_top}, $lngth );
        print BOLD, UNDERLINE if $arg->{marked}[$row][$col];
        print REVERSE         if $row == $arg->{cursor}[ROW] && $col == $arg->{cursor}[COL];
        print $arg->{list}[$arg->{rc2idx}[$row][$col]];
    }
    else {
        _goto( $arg, $row - $arg->{row_on_top}, $col * $arg->{col_width} );
        print BOLD, UNDERLINE if $arg->{marked}[$row][$col];
        print REVERSE         if $row == $arg->{cursor}[ROW] && $col == $arg->{cursor}[COL];
        print _unicode_sprintf( $arg, $arg->{rc2idx}[$row][$col] );
    }
    print RESET if $arg->{marked}[$row][$col] || $row == $arg->{cursor}[ROW] && $col == $arg->{cursor}[COL];
}


sub _size_and_layout {
    my ( $arg ) = @_;
    my $layout = $arg->{layout};
    $arg->{rc2idx} = [];
    if ( $arg->{length_longest} > $arg->{avail_width} ) {
        $arg->{avail_col_width} = $arg->{avail_width};
        $layout = 3;
    }
    else {
        $arg->{avail_col_width} = $arg->{length_longest};
    }
    my $all_in_first_row;
    if ( $layout == 2 ) {
        $layout = 3 if scalar @{$arg->{list}} <= $arg->{avail_height};
    }
    elsif ( $layout < 2 ) {
        for my $idx ( 0 .. $#{$arg->{list}} ) {
            $all_in_first_row .= $arg->{list}[$idx];
            $all_in_first_row .= ' ' x $arg->{pad_one_row} if $idx < $#{$arg->{list}};
            my $gcs_first_row = Unicode::GCString->new( $all_in_first_row );
            if ( $gcs_first_row->columns() > $arg->{avail_width} ) {
                $all_in_first_row = '';
                last;
            }
        }
    }
    if ( $all_in_first_row ) {
        $arg->{rc2idx}[0] = [ 0 .. $#{$arg->{list}} ];
    }
    elsif ( $layout == 3 ) {
        if ( $arg->{length_longest} <= $arg->{avail_width} ) {
            for my $idx ( 0 .. $#{$arg->{list}} ) {
                $arg->{rc2idx}[$idx][0] = $idx;
            }
        }
        else {
            for my $idx ( 0 .. $#{$arg->{list}} ) {
                my $gcs_element = Unicode::GCString->new( $arg->{list}[$idx] );
                if ( $gcs_element->columns > $arg->{avail_width} ) {
                    $arg->{list}[$idx] = _unicode_trim( $gcs_element, $arg->{avail_width} - 3 ) . '...';
                }
                $arg->{rc2idx}[$idx][0] = $idx;
            }
        }
    }
    else {
        # auto_format
        my $tmp_avail_width = $arg->{avail_width};
        if ( $arg->{layout} == 1 || $arg->{layout} == 2 ) {
            my $tmc = int( @{$arg->{list}} / $arg->{avail_height} );
            $tmc++ if @{$arg->{list}} % $arg->{avail_height};
            $tmc *= $arg->{col_width};
            if ( $tmc < $tmp_avail_width ) {
                $tmc = int( $tmc + ( ( $tmp_avail_width - $tmc ) / 1.5 ) ) if $arg->{layout} == 1;
                $tmc = int( $tmc + ( ( $tmp_avail_width - $tmc ) / 6 ) )   if $arg->{layout} == 2;
                $tmp_avail_width = $tmc;
            }
        }
        # order
        my $cols_per_row = int( $tmp_avail_width / $arg->{col_width} );
        $cols_per_row = 1 if $cols_per_row < 1;
        my $rows = int( ( $#{$arg->{list}} + $cols_per_row ) / $cols_per_row );
        $arg->{rest} = @{$arg->{list}} % $cols_per_row;
        if ( $arg->{order} == 1 ) {
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
                push @{$arg->{rc2idx}}, \@temp_idx;
            }
        }
        else {
            my $begin = 0;
            my $end = $cols_per_row - 1;
            $end = $#{$arg->{list}} if $end > $#{$arg->{list}};
            push @{$arg->{rc2idx}}, [ $begin .. $end ];
            while ( $end < $#{$arg->{list}} ) {
                $begin += $cols_per_row;
                $end   += $cols_per_row;
                $end    = $#{$arg->{list}} if $end > $#{$arg->{list}};
                push @{$arg->{rc2idx}}, [ $begin .. $end ];
            }
        }
    }
}


sub _unicode_trim {
    my ( $gcs, $len ) = @_;
    return '' if $len <= 0; #
    my $pos = $gcs->pos;
    $gcs->pos( 0 );
    my $cols = 0;
    my $gc;
    while ( defined( $gc = $gcs->next ) ) {
        if ( $len < ( $cols += $gc->columns ) ) {
            my $ret = $gcs->substr( 0, $gcs->pos - 1 );
            $gcs->pos( $pos );
            return $ret->as_string;
        }
    }
    $gcs->pos( $pos );
    return $gcs->as_string;
}


sub _unicode_sprintf {
    my ( $arg, $idx ) = @_;
    my $unicode;
    my $str_length = $arg->{length}[$idx] // $arg->{length_longest};
    if ( $str_length > $arg->{avail_col_width} ) {
        my $gcs = Unicode::GCString->new( $arg->{list}[$idx] );
        $unicode = _unicode_trim( $gcs, $arg->{avail_col_width} );
    }
    elsif ( $str_length < $arg->{avail_col_width} ) {
        if ( $arg->{justify} == 0 ) {
            $unicode = $arg->{list}[$idx] . " " x ( $arg->{avail_col_width} - $str_length );
        }
        elsif ( $arg->{justify} == 1 ) {
            $unicode = " " x ( $arg->{avail_col_width} - $str_length ) . $arg->{list}[$idx];
        }
        elsif ( $arg->{justify} == 2 ) {
            my $all = $arg->{avail_col_width} - $str_length;
            my $half = int( $all / 2 );
            $unicode = " " x $half . $arg->{list}[$idx] . " " x ( $all - $half );
        }
    }
    else {
        $unicode = $arg->{list}[$idx];
    }
    return $unicode;
}


sub _handle_mouse {
    my ( $arg, $event_type, $abs_mouse_x, $abs_mouse_y ) = @_;
    my $button_drag = ( $event_type & 0x20 ) >> 5;
    return NEXT_read_key if $button_drag;
    my $button_number;
    my $low_2_bits = $event_type & 0x03;
    if ( $low_2_bits == 3 ) {
        $button_number = 0;
    }
    else {
        if ( $event_type & 0x40 ) {
            $button_number = $low_2_bits + 4; # 4,5
        }
        else {
            $button_number = $low_2_bits + 1; # 1,2,3
        }
    }
    if ( $button_number == 4 ) {
        return KEY_PAGE_UP;
    }
    elsif ( $button_number == 5 ) {
        return KEY_PAGE_DOWN;
    }
    my $pos_top_row = $arg->{abs_cursor_y} - $arg->{cursor_row};
    return NEXT_read_key if $abs_mouse_y < $pos_top_row;
    my $mouse_row = $abs_mouse_y - $pos_top_row;
    my $mouse_col = $abs_mouse_x;
    my( $found_row, $found_col );
    my $found = 0;
    if ( $#{$arg->{rc2idx}} == 0 ) {
        my $row = 0;
        if ( $row == $mouse_row ) {
            my $end_last_col = 0;
            COL: for my $col ( 0 .. $#{$arg->{rc2idx}[$row]} ) {
                my $gcs_element = Unicode::GCString->new( $arg->{list}[$arg->{rc2idx}[$row][$col]] );
                my $end_this_col = $end_last_col + $gcs_element->columns() + $arg->{pad_one_row};
                if ( $col == 0 ) {
                    $end_this_col -= int( $arg->{pad_one_row} / 2 );
                }
                if ( $col == $#{$arg->{rc2idx}[$row]} ) {
                    $end_this_col = $arg->{avail_width} if $end_this_col > $arg->{avail_width};
                }
                if ( $end_last_col < $mouse_col && $end_this_col >= $mouse_col ) {
                    $found = 1;
                    $found_row = $row + $arg->{row_on_top};
                    $found_col = $col;
                    last;
                }
                $end_last_col = $end_this_col;
            }
        }
    }
    else {
        ROW: for my $row ( 0 .. $#{$arg->{rc2idx}} ) {
            if ( $row == $mouse_row ) {
                my $end_last_col = 0;
                COL: for my $col ( 0 .. $#{$arg->{rc2idx}[$row]} ) {
                    my $end_this_col = $end_last_col + $arg->{col_width};
                    if ( $col == 0 ) {
                        $end_this_col -= int( $arg->{pad} / 2 );
                    }
                    if ( $col == $#{$arg->{rc2idx}[$row]} ) {
                        $end_this_col = $arg->{avail_width} if $end_this_col > $arg->{avail_width};
                    }
                    if ( $end_last_col < $mouse_col && $end_this_col >= $mouse_col ) {
                        $found = 1;
                        $found_row = $row + $arg->{row_on_top};
                        $found_col = $col;
                        last ROW;
                    }
                    $end_last_col = $end_this_col;
                }
            }
        }
    }
    return NEXT_read_key if ! $found;
    my $return_char = '';
    if ( $button_number == 1 ) {
        $return_char = KEY_ENTER;
    }
    elsif ( $button_number == 3  ) {
        $return_char = KEY_SPACE;
    }
    else {
        return NEXT_read_key;
    }
    if ( $found_row != $arg->{cursor}[ROW] || $found_col != $arg->{cursor}[COL] ) {
        my $tmp = $arg->{cursor};
        $arg->{cursor} = [ $found_row, $found_col ];
        _wr_cell( $arg, $tmp->[0], $tmp->[1] );
        _wr_cell( $arg, $arg->{cursor}[ROW], $arg->{cursor}[COL] );
    }
    return $return_char;
}


1;

__END__



=pod

=encoding UTF-8

=head1 NAME

Term::Choose - Choose items from a list.

=head1 VERSION

Version 1.061

=cut

=head1 SYNOPSIS

    use 5.10.0;
    use Term::Choose qw(choose);

    my $array_ref = [ qw( one two three four five ) ];

    my $choice = choose( $array_ref );                            # single choice
    say $choice;

    my @choices = choose( [ 1 .. 100 ], { justify => 1 } );       # multiple choice
    say "@choices";

    choose( [ 'Press ENTER to continue' ], { prompt => '' } );    # no choice


=head1 DESCRIPTION

Choose from a list of items.

Based on the I<choose> function from the L<Term::Clui> module - for more details see L</MOTIVATION>.

For OS 'MSWin32' see L<Term::Choose::Win32>.

=head1 EXPORT

Nothing by default.

    use Term::Choose qw(choose);

=head1 SUBROUTINES

=head2 choose

    $scalar = choose( $array_ref [, \%options] );

    @array =  choose( $array_ref [, \%options] );

              choose( $array_ref [, \%options] );

I<choose> expects as a first argument an array reference. The array the reference refers to holds the list items available for selection (in void context no selection can be made).

The array the reference - passed with the first argument - refers to is called in the documentation simply array or list resp. elements (of the array).

Options can be passed with a hash reference as a second (optional) argument.

=head3 Usage and return values

=over

=item

If I<choose> is called in a I<scalar context>, the user can choose an item by using the "move-around-keys" and confirming with "Return".

I<choose> then returns the chosen item.

=item

If I<choose> is called in an I<list context>, the user can also mark an item with the "SpaceBar".

I<choose> then returns - when "Return" is pressed - the list of marked items including the highlighted item.

In I<list context> "Ctrl-SpaceBar" inverts the choices: marked items are unmarked and unmarked items are marked.

=item

If I<choose> is called in an I<void context>, the user can move around but mark nothing; the output shown by I<choose> can be closed with "Return".

Called in void context I<choose> returns nothing.

=back

If the items of the list don't fit on the screen, the user can scroll to the next (previous) page(s).

If the window size is changed, then as soon as the user enters a keystroke I<choose> rewrites the screen. In list context marked items are reset.

The "q" key (or Ctrl-D) returns I<undef> or an empty list in list context.

With a I<mouse> mode enabled (and if supported by the terminal) the item can be chosen with the left mouse key, in list context the right mouse key can be used instead the "SpaceBar" key.

=head3 Keys to move around:

=over

=item *

Arrow keys (or hjkl),

=item *

Tab key (or Ctrl-I) to move forward, BackSpace key (or Ctrl-H or Shift-Tab) to move backward,

=item *

PageUp key (or Ctrl-B) to go back one page, PageDown key (or Ctrl-F) to go forward one page,

=item *

Home key (or Ctrl-A) to jump to the beginning of the list, End key (or Ctrl-E) to jump to the end of the list.

=back


=head3 Modifications for the output

For the output on the screen the array elements are modified:

=over

=item *

if an element is not defined the value from the option I<undef> is assigned to the element.

=item *

if an element holds an empty string the value from the option I<empty> is assigned to the element.

=item *

white-spaces in elements are replaced with simple spaces.

    $element =~ s/\p{Space}/ /g;

=item *

non printable characters are replaced with the I<replacement character> (U+FFFD).

    $element =~ s/\P{Print}/\x{fffd}/g;

=item *

if the length of an element is greater than the width of the screen the element is cut.

    $element = substr( $element, 0, $allowed_length - 3 ) . '...';*

* L<Term::Choose> uses its own function to cut strings which uses print columns for the arithmetic.

=back

All these modifications are made on a copy of the original array so I<choose> returns the chosen elements as they were passed to the function without modifications.

=head3 Options

Options which expect a number as their value expect integers.

There is a general upper limit of one billion for options which expect a number as their value and where no upper limit is mentioned.

=head4 prompt

If I<prompt> is undefined a default prompt-string will be shown.

If the I<prompt> value is  an empty string ("") no prompt-line will be shown.

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

=head4 max_height

If defined sets the maximal number of rows used for printing list items.

If the available height is less than I<max_height> I<max_height> is set to the available height.

Height in this context means print rows.

I<max_height> overwrites I<keep> if I<max_height> is set and less than I<keep>.

Allowed values: 1 or greater

(default: undef)

=head4 screen_width DEPRECATED

Use I<max_width> instead - I<screen_width> is now called I<max_width>.

The deprecated name I<screen_width> will be removed in a future release.

=head4 max_width

If defined, sets the output width to I<max_width> if the terminal width is greater than I<max_width>.

Width refers here to the number of print columns.

Allowed values: 1 or greater

(default: undef)

=head4 order

If the output has more than one row and more than one column:

0 - elements are ordered horizontally

1 - elements are ordered vertically (default)

Default may change in a future release.

=head4 justify

0 - elements ordered in columns are left justified (default)

1 - elements ordered in columns are right justified

2 - elements ordered in columns are centered

=head4 pad

Sets the number of whitespaces between columns. (default: 2)

Allowed values:  0 or greater

=head4 pad_one_row

Sets the number of whitespaces between elements if we have only one row. (default: value of the option I<pad>)

Allowed values:  0 or greater

=head4 clear_screen

0 - off (default)

1 - clears the screen before printing the choices

=head4 default

With the option I<default> it can be selected an element, which will be highlighted as the default instead of the first element.

I<default> expects a zero indexed value, so e.g. to highlight the third element the value would be I<2>.

If the passed value is greater than the index of the last array element the first element is highlighted.

Allowed values:  0 or greater

(default: undef)

=head4 index

0 - off (default)

1 - return the index of the chosen element instead of the chosen element resp. the indices of the chosen elements instead of the chosen elements.

=head4 page

0 - off

1 - print the page number on the bottom of the screen if there is more then one page. (default)

=head4 mouse

0 - no mouse mode (default)

1 - mouse mode 1003 enabled

2 - mouse mode 1003 enabled; the output width is limited to 223 print-columns and the height to 223 rows (mouse mode 1003 doesn't work above 223)

3 - extended mouse mode (1005) - uses utf8

4 - extended SGR mouse mode (1006)

If a mouse mode is enabled layers for STDIN are changed. Then before leaving I<choose> as a cleanup STDIN is marked as UTF-8 with ":encoding(UTF-8)".

=head4 keep

I<keep> prevents that all the terminal rows are used by the prompt lines.

Setting I<keep> ensures that at least I<keep> terminal rows are available for printing list rows.

If the terminal height is less than I<keep> I<keep> is set to the terminal height.

Allowed values: 1 or greater

(default: 5)

=head4 beep

0 - off (default)

1 - on

=head4 hide_cursor

0 - keep the terminals highlighting of the cursor position

1 - hide the terminals highlighting of the cursor position (default)

=head4 limit

Sets the maximal allowed length of the array. (default: 100_000)

Allowed values:  1 or greater

=head4 undef

Sets the string displayed on the screen instead an undefined element.

default: '<undef>'

=head4 empty

Sets the string displayed on the screen instead an empty string.

default: '<empty>'

=head4 lf

If I<prompt> lines are folded the option I<lf> allows to insert spaces at beginning of the folded lines.

The option I<lf> expects a reference to an array with two elements;

- first element (INITIAL_TAB): the number of spaces inserted at beginning of paragraphs

- second element (SUBSEQUENT_TAB): the number of spaces inserted at the beginning of all broken lines apart from the beginning of paragraphs

Allowed values for the two elements are: 0 or greater.

See INITIAL_TAB and SUBSEQUENT_TAB in L<Text::LineFold>.

(default: undef)

=head4 ll

If all elements have the same length and this length is known before calling I<choose> it can be passed with this option.

If I<ll> is set, then I<choose> doesn't calculate the length of the longest element itself but uses the value passed with this option.

I<length> refers here to the number of print columns the element will use on the terminal.

A way to determine the number of print columns is the use of I<columns> from L<Unicode::GCString>.

The length of undefined elements and elements with an empty string depends on the value of the option I<undef> resp. on the value of the option I<empty>.

If the option I<ll> is set the elements must be upgraded with utf8::upgrade or with an equivalent tool and not contain any non-printing character.

The upgrade with utf8::upgrade is needed because a limitation of L<Unicode::GCString> (L<Bug #84661|https://rt.cpan.org/Public/Bug/Display.html?id=84661>).

If I<ll> is set to a value less than the length of the elements the output will break.

If the value of I<ll> is greater than the screen width the elements will be trimmed to fit into the screen.

Allowed values: 1 or greater

(default: undef)

=head3 Error handling

=over

=item * With no arguments I<choose> dies.

=item * With more than two arguments I<choose> dies.

=item * If the first argument is not a array reference I<choose> dies.

=item * If the array referred by the first argument is empty I<choose> returns  I<undef> resp. an empty list and issues a warning.

=item * If the array referred by the first argument has more than I<limit> elements (default 100_000) I<choose> warns and uses the first I<limit> array elements.

=item * If the (optional) second argument is defined and not a hash reference I<choose> dies.

=item * If an option does not exist I<choose> warns.

=item * If an option value is not valid  I<choose> warns an falls back to the default value.

=item * If after pressing a key L<Term::ReadKey>::ReadKey returns I<undef> I<choose> warns with "EOT: $!" and returns I<undef> resp. an empty list.

=back

=head1 REQUIREMENTS

=head2 Perl Version

Requires Perl Version 5.10.0 or greater.

=head2 Modules

Used modules not provided as core modules:

=over

=item

L<Term::ReadKey>

=item

L<Text::LineFold>

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

If a "mouse" mode is enabled:

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

Multiline question/prompt: I<choose> from L<Term::Clui> puts the first line on the top, the subsequent lines are displayed below the list. I<choose> from L<Term::Choose> puts all lines of a multiline prompt on top of the list.

L<Term::Clui>'s I<choose> prints and returns the chosen items while I<choose> from L<Term::Choose> only returns the chosen items.

L<Term::Clui> disables the mouse mode if the environment variable I<CLUI_MOUSE> is set to I<off>. In L<Term::Choose> the mouse mode is set with the option I<mouse>.

=item Only in L<Term::Clui>

L<Term::Clui> provides a speaking interface, offers a bundle of command-line related functions and has a fallback to work when only Perl core modules are available.

The I<choose> function from L<Term::Clui> can remember choices made in scalar context.

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

Copyright 2012-2013 Matthäus Kiem.

This library is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

=cut
