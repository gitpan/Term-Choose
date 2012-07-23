use warnings;
use strict;
use 5.10.1;
use utf8;
package Term::Choose::GC;

our $VERSION = '0.7.6';
use Exporter 'import';
our @EXPORT_OK = qw(choose);

use Term::Choose qw(choose);
use Term::ReadKey;
use Unicode::GCString;

use constant {
    RESET                              => "\e[0m",
    UNDERLINE                          => "\e[4m",
    REVERSE                            => "\e[7m",
    BOLD                               => "\e[1m",
};

no warnings 'redefine';

sub Term::Choose::_length_longest {
    my ( $list ) = @_;
    my $longest;
    eval {
        my $gcs = Unicode::GCString->new( $list->[0] );
        $longest = $gcs->columns();
    };
    if ( $@ ) {
        $longest = length $list->[0];
    }
    for my $str ( @{$list} ) {
        eval {
            my $gcs = Unicode::GCString->new( $str ); # my
            $longest = $gcs->columns() if $gcs->columns() > $longest;     
        };
        if ( $@ ) {
            $longest = length $str if length $str > $longest;
        }
    }
    return $longest;
}

sub Term::Choose::_print_firstline {
    my ( $arg ) = @_;
    $arg->{prompt} =~ s/\p{Space}/ /g; ##############
    $arg->{prompt} =~ s/\p{Cntrl}//g;      
    $arg->{firstline} = $arg->{prompt};
    if ( defined $arg->{wantarray} and $arg->{wantarray} ) {
        if ( $arg->{prompt} ) {
            $arg->{firstline} = $arg->{prompt} . '  (multiple choice with spacebar)';
            my $length_first_line;
            eval {
                my $gcs = Unicode::GCString->new( $arg->{firstline} );
                $length_first_line = $gcs->columns();
            };
            if ( $@ ) {
                $length_first_line = length $arg->{firstline};
            }
            $arg->{firstline} = $arg->{prompt} . ' (multiple choice)' if $length_first_line > $arg->{maxcols};
        }
        else {
            $arg->{firstline} = '';
        }
    }
#    eval {
#        my $gcs = Unicode::GCString->new( $arg->{firstline} );
#        if ( $gcs->columns() > $arg->{maxcols} ) {
#            my $gcs = Unicode::GCString->new( $arg->{prompt} );
#            $arg->{firstline} = substr( $gcs->as_string(), 0, $arg->{maxcols} );
#        }
#    };
    eval {
        my $gcs = Unicode::GCString->new( $arg->{firstline} );
        if ( $gcs->columns() > $arg->{maxcols} ) {
            $arg->{firstline} = _unicode_cut( $arg->{prompt}, $arg->{maxcols}, $arg->{maxcols} x 2 );
        }
    };
    if ( $@ ) {
        if ( length $arg->{firstline} > $arg->{maxcols} ) {
            $arg->{firstline} = substr( $arg->{prompt}, 0, $arg->{maxcols} );
        }
    }     
    print $arg->{firstline};
    $arg->{head} = 1;       
}
    
sub Term::Choose::_wr_cell {
    my( $arg, $row, $col ) = @_;
    if ( $#{$arg->{new_list}} == 0 ) {
        my $lngth = 0;
        if ( $col > 0 ) {
            for my $cl ( 0 .. $col - 1 ) {
                eval {
                    my $gcs = Unicode::GCString->new( $arg->{new_list}[$row][$cl] );
                    $lngth += $gcs->columns();
                };
                if ( $@ ) {
                    $lngth += length $arg->{new_list}[$row][$cl];
                }
                $lngth += $arg->{pad_one_row} // 0;
            }
        }
        Term::Choose::_goto( $arg, $row + $arg->{head} - $arg->{page}, $lngth );
    } 
    else {
        Term::Choose::_goto( $arg, $row + $arg->{head} - $arg->{page}, $col * $arg->{col_width} );
    }
    print BOLD, UNDERLINE if $arg->{marked}[$row][$col];
    print REVERSE if [ $row, $col ] ~~ $arg->{this_cell};
    print $arg->{new_list}[$row][$col];
    print RESET if $arg->{marked}[$row][$col] or [ $row, $col ] ~~ $arg->{this_cell};
}

sub Term::Choose::_size_and_layout {
    my ( $arg ) = @_;
    my $layout = $arg->{layout};
    $arg->{new_list} = [];
    $arg->{rowcol_to_list_index} = [];
    $arg->{all_in_first_row} = 0;
    if ( $arg->{length_longest} > $arg->{maxcols} ) {
        $arg->{length_longest} = $arg->{maxcols};
        $layout = 2;
    }
    # layout
    $arg->{this_cell} = [ 0, 0 ];
    my $all_in_first_row;
    if ( $layout == 3 ) {
        $layout = 2 if scalar @{$arg->{list}} <= $arg->{maxrows};
    }
    elsif ( $layout < 2 ) {
        for my $element ( 0 .. $#{$arg->{list}} ) {
            $all_in_first_row .= $arg->{list}[$element];
            my $one_row_length;
            eval {
                my $gcs = Unicode::GCString->new( $all_in_first_row );
                $one_row_length = $gcs->columns();
            };
            if ( $@ ) {
                $one_row_length = length $all_in_first_row;
            }
            if ( $one_row_length > $arg->{maxcols} ) {
                $all_in_first_row = '';
                last;
            }
            $all_in_first_row .= ' ' x $arg->{pad_one_row} if $element < $#{$arg->{list}};
        }
    }
    if ( $all_in_first_row ) {
        $arg->{all_in_first_row} = 1;
        $arg->{new_list}[0] = [ @{$arg->{list}} ];
        $arg->{rowcol_to_list_index}[0] = [ 0 .. $#{$arg->{list}} ];    
    }
    elsif ( $layout == 2 ) {
        for my $idx ( 0 .. $#{$arg->{list}} ) {
#           eval {
#                my $gcs = Unicode::GCString->new( $arg->{list}[$idx] );            
#                if ( $gcs->columns() > $arg->{length_longest} ) {
#                    $arg->{list}[$idx] = substr( $gcs->as_string(), 0, $arg->{length_longest} - 3 ) . '...';
#                }
#            };
            eval {
                my $gcs = Unicode::GCString->new( $arg->{list}[$idx] );            
                if ( $gcs->columns() > $arg->{length_longest} ) {
                    $arg->{list}[$idx] = _unicode_cut( $arg->{list}[$idx], $arg->{length_longest} - 3, $arg->{maxcols} x 2 ) . '...';
                }
            };
            if ( $@ ) {
                if ( length $arg->{list}[$idx] > $arg->{length_longest} ) {
                    $arg->{list}[$idx] = substr( $arg->{list}[$idx], 0, $arg->{length_longest} - 3 ) . '...';
                }
            }
            $arg->{new_list}[$idx][0] = _unicode_sprintf( $arg->{length_longest}, $arg->{list}[$idx], $arg->{right_justify}, $arg->{maxcols} );
            $arg->{rowcol_to_list_index}[$idx][0] = $idx;
        }
    }
    else {
        # auto_format
        my $maxcls = $arg->{maxcols};
        if ( ( $arg->{layout} == 1 or $arg->{layout} == 3 ) and $arg->{maxrows} > 0 ) {
            my $tmc = int( @{$arg->{list}} / $arg->{maxrows} );
            $tmc++ if @{$arg->{list}} % $arg->{maxrows};
            $tmc *= $arg->{col_width};
            if ( $tmc < $maxcls ) {
                $tmc = int( $tmc + ( ( $maxcls - $tmc ) / 2 ) ) if $arg->{layout} == 1;
                $tmc = int( $tmc + ( ( $maxcls - $tmc ) / 6 ) ) if $arg->{layout} == 3;
                $maxcls = $tmc;
            }
        }
        # end auto_format
    # end layout
    # row_first
        my $cols_per_row = int( $maxcls / $arg->{col_width} );
        $cols_per_row = 1 if $cols_per_row < 1;
        my $rows = int( ( $#{$arg->{list}} + $cols_per_row ) / $cols_per_row );
        $arg->{rest} = @{$arg->{list}} % $cols_per_row;
        if ( $arg->{vertical_order} ) {
            my @rearranged_list;
            my @rearranged_idx;
            my $i = 0;
            my $idxs = [ 0 .. $#{$arg->{list}} ];
            for my $c ( 0 .. $cols_per_row - 1 ) {
                $i = 1 if $arg->{rest} and $c >= $arg->{rest};
                $rearranged_list[$c] = [ splice( @{$arg->{list}}, 0, $rows - $i ) ];
                $rearranged_idx[$c]  = [ splice( @{$idxs},        0, $rows - $i ) ];
            }
            for my $r ( 0 .. $rows - 1 ) {
                my @temp_new_list;
                my @temp_idx;
                for my $c ( 0 .. $cols_per_row - 1 ) {
                    next if $arg->{rest} and $r == $rows - 1 and $c >= $arg->{rest};
                    push @temp_new_list, _unicode_sprintf( $arg->{length_longest}, $rearranged_list[$c][$r], $arg->{right_justify}, $arg->{maxcols} );
                    push @temp_idx, $rearranged_idx[$c][$r];
                }
                push @{$arg->{new_list}}, \@temp_new_list;
                push @{$arg->{rowcol_to_list_index}}, \@temp_idx;
            }
        }
        else {
            my $begin = 0;
            my $end = $cols_per_row - 1;
            while ( my @rearranged_list = @{$arg->{list}}[$begin..$end] ) {
                my @temp_new_list;
                for my $rearranged_list_item ( @rearranged_list ) {
                    push @temp_new_list, _unicode_sprintf( $arg->{length_longest}, $rearranged_list_item, $arg->{right_justify}, $arg->{maxcols} );
                }
                push @{$arg->{new_list}}, \@temp_new_list;
                push @{$arg->{rowcol_to_list_index}}, [ $begin .. $end ];
                $begin = $end + 1;
                $end = $begin + $cols_per_row - 1;
                $end = $#{$arg->{list}} if $end > $#{$arg->{list}};
            }
        }
    }
}



sub _unicode_cut {    # if string has 0 length chars ?
    my ( $unicode, $length, $max_length ) = @_;
    my $gcs = Unicode::GCString->new( $unicode );
    my $colwidth = $gcs->columns();
    if ( $colwidth != length $unicode ) {
        if ( defined $max_length and $colwidth > $max_length ) {
            $unicode = substr( $gcs->as_string, 0, $max_length );
            $gcs = Unicode::GCString->new( $unicode );
            $colwidth = $gcs->columns();
        }
        while ( $colwidth > $length ) {
            $unicode =~ s/\X\z//;
            $gcs = Unicode::GCString->new( $unicode );
            $colwidth = $gcs->columns();
        }
        $unicode .= ' ' if $colwidth < $length;
    }
    else {
        $unicode = substr( $gcs->as_string, 0, $length );
    }
    return $unicode;
}



sub _unicode_sprintf {
    my ( $length, $word, $right_justify, $max_length ) = @_;
    my $unicode = $word;
    eval {
        my $gcs = Unicode::GCString->new( $unicode );
        my $colwidth = $gcs->columns();
        if ( $colwidth > $length ) {
            if ( $colwidth != length $unicode ) {
                if ( defined $max_length and $colwidth > $max_length ) {
                    $unicode = substr( $gcs->as_string, 0, $max_length );
                    $gcs = Unicode::GCString->new( $unicode );
                    $colwidth = $gcs->columns();
                }
                while ( $colwidth > $length ) {
                    $unicode =~ s/\X\z//;
                    $gcs = Unicode::GCString->new( $unicode );
                    $colwidth = $gcs->columns();
                }
                $unicode .= ' ' if $colwidth < $length;
            }
            else {
                $unicode = substr( $gcs->as_string, 0, $length );
            }
        } 
        else {
            if ( $right_justify ) {
                $unicode = " " x ( $length - $colwidth ) . $unicode;
            }
            else {
                $unicode = $unicode . " " x ( $length - $colwidth );
            }
            
        }
    };
    if ( $@ ) {
        my $colwidth = length $word;
        if ( $colwidth > $length ) {
            $word = substr( $word, 0, $length );
        } 
        else {
            if ( $right_justify ) {
                $word = " " x ( $length - $colwidth ) . $word;
            }
            else {
                $word = $word . " " x ( $length - $colwidth );
            }
            
        }
        return $word;
    }
    else {
        return $unicode;
    }
}


=pod

=head1 NAME

Term::Choose::GC - Works as L<Term::Choose>.

=head1 VERSION

Version 0.7.6

=cut

=head1 SYNOPSIS

    use 5.10.1;
    use Term::Choose:GC qw(choose);

    my $list = [ qw( one two three four five ) ];

    my $choice = choose( $list );                                 # single choice
    say $choice;

    my @choices = choose( [ 1 .. 100 ], { right_justify => 1 } ); # multiple choice
    say "@choices";
    
    choose( [ 'Press ENTER to continue' ], { prompt => 0 } );     # no choice


=head1 DESCRIPTION

Choose from a list of elements.

Requires Perl Version 5.10.1 or greater.

See L<Term::Choose> for details.

=head1 EXPORT

Nothing by default.

    use Term::Choose::GC qw(choose);

=head1 REQUIREMENTS

Additionally to the L<Term::Choose> requirements L<Term::Choose::GC> needs the module L<Unicode::GCString>.

=head1 UNICODE

While L<Term::Choose> uses the Perl builtin functions I<length> to determine the length of strings and I<sprintf> widths to justify strings L<Term::Choose::GC> uses L<Unicode::GCString::columns> to determine the length of strings. To justify strings it uses its own function based on L<Unicode::GCString>. The code using L<Unicode::GCString::columns> runs in I<eval> blocks: if eval fails builtin I<length> resp. I<substr> are used instead.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Term::Choose::GC

=head1 AUTHOR

Kuerbis cuer2s@gmail.com

=head1 CREDITS

Based on and inspired by the I<choose> function from L<Term::Clui> module.

Thanks to the L<http://www.perl-community.de> and the people form L<http://stackoverflow.com> for the help.

=head1 LICENSE AND COPYRIGHT

Copyright 2012 Kuerbis.

This library is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

=cut

1; # End of Term::Choose


















1;