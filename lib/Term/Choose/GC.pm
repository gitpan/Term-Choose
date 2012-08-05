use warnings;
use strict;
use 5.10.1;
use utf8;
package Term::Choose::GC;

our $VERSION = '0.7.13';
use Exporter 'import';
our @EXPORT_OK = qw(choose);

use Term::Choose qw(choose);
use Term::ReadKey;
use Unicode::GCString;

#use warnings FATAL => qw(all);
#use Log::Log4perl qw(get_logger);
#my $log = get_logger("Term::Choose::GC");


use constant {
    RESET       => "\e[0m",
    UNDERLINE   => "\e[4m",
    REVERSE     => "\e[7m",
    BOLD        => "\e[1m",
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
            my $gcs = Unicode::GCString->new( $str );
            my $length = $gcs->columns();
            $longest = $length if $length > $longest;
        };
        if ( $@ ) {
            $longest = length $str if length $str > $longest;
        }
    }
    return $longest;
}


sub Term::Choose::_print_firstline {
    my ( $arg ) = @_;
    $arg->{prompt} =~ s/\p{Space}/ /g;
    $arg->{prompt} =~ s/\p{Cntrl}//g;
    $arg->{firstline} = $arg->{prompt};
    if ( defined $arg->{wantarray} && $arg->{wantarray} ) {
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
    $arg->{step} = 5; # _unicode_cut, _unicode_sprintf
    eval {
        my $gcs = Unicode::GCString->new( $arg->{firstline} );
        if ( $gcs->columns() > $arg->{maxcols} ) {
            $arg->{firstline} = _unicode_cut( $arg, $arg->{prompt}, $arg->{maxcols} - 3 ) . '...';
        }
    };
    if ( $@ ) {
        if ( length $arg->{firstline} > $arg->{maxcols} ) {
            $arg->{firstline} = substr( $arg->{prompt}, 0, $arg->{maxcols} - 3 ) . '...';
        }
    }
    print $arg->{firstline};
    $arg->{head} = 1;
}


sub Term::Choose::_wr_cell {
    my( $arg, $row, $col ) = @_;
    if ( $#{$arg->{rowcol2list}} == 0 ) {
        my $lngth = 0;
        if ( $col > 0 ) {
            for my $cl ( 0 .. $col - 1 ) {
                eval {
                    my $gcs = Unicode::GCString->new( $arg->{list}[$arg->{rowcol2list}[$row][$cl]] );
                    $lngth += $gcs->columns();
                };
                if ( $@ ) {
                    $lngth += length $arg->{list}[$arg->{rowcol2list}[$row][$cl]];
                }
                $lngth += $arg->{pad_one_row} // 0;
            }
        }
        Term::Choose::_goto( $arg, $row + $arg->{head} - $arg->{page}, $lngth );
        print BOLD, UNDERLINE if $arg->{marked}[$row][$col];
        print REVERSE if [ $row, $col ] ~~ $arg->{this_cell};
        print $arg->{list}[$arg->{rowcol2list}[$row][$col]];
    }
    else {
        Term::Choose::_goto( $arg, $row + $arg->{head} - $arg->{page}, $col * $arg->{col_width} );
        print BOLD, UNDERLINE if $arg->{marked}[$row][$col];
        print REVERSE if [ $row, $col ] ~~ $arg->{this_cell};
        print _unicode_sprintf( $arg, $arg->{list}[$arg->{rowcol2list}[$row][$col]] );
    }
    print RESET if $arg->{marked}[$row][$col] || [ $row, $col ] ~~ $arg->{this_cell};
}


sub Term::Choose::_size_and_layout {
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
            $all_in_first_row .= ' ' x $arg->{pad_one_row} if $idx < $#{$arg->{list}};
        }
    }
    if ( $all_in_first_row ) {
        $arg->{all_in_first_row} = 1;
        $arg->{rowcol2list}[0] = [ 0 .. $#{$arg->{list}} ];
    }
    elsif ( $layout == 3 ) {
        for my $idx ( 0 .. $#{$arg->{list}} ) {
        	eval {
                my $gcs = Unicode::GCString->new( $arg->{list}[$idx] );
                if ( $gcs->columns() > $arg->{length_longest} ) {
                    $arg->{list}[$idx] = _unicode_cut( $arg, $arg->{list}[$idx], $arg->{length_longest} - 3 ) . '...';
                }
            };
            if ( $@ ) {
                if ( length $arg->{list}[$idx] > $arg->{length_longest} ) {
                    $arg->{list}[$idx] = substr( $arg->{list}[$idx], 0, $arg->{length_longest} - 3 ) . '...';
                }
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


sub _unicode_cut {
    my ( $arg, $unicode, $length ) = @_;
    my $gcs = Unicode::GCString->new( $unicode );
    my $colwidth = $gcs->columns();
    my $max_length = ( $arg->{maxcols} / 2 ) + 1;
    while ( 1 ) {
		my $tmp = substr( $unicode, 0, $max_length );
		my $gcs = Unicode::GCString->new( $tmp );
		$colwidth = $gcs->columns();
		if ( $colwidth > $arg->{maxcols} ) {
			$unicode = $tmp;
			last;
		}
		$max_length += $arg->{step};
	}
	while ( $colwidth > $length ) {
		$unicode =~ s/\X\z//;
		my $gcs = Unicode::GCString->new( $unicode );
		$colwidth = $gcs->columns();
	}
	$unicode .= ' ' if $colwidth < $length;
    return $unicode;
}


sub _unicode_sprintf {
    my ( $arg, $word ) = @_;
    my $unicode = $word;
    eval {
        my $gcs = Unicode::GCString->new( $unicode );
        my $colwidth = $gcs->columns();
        if ( $colwidth > $arg->{length_longest} ) {
			my $max_length = ( $arg->{length_longest} / 2 ) + 1;
			while ( 1 ) {
				my $tmp = substr( $unicode, 0, $max_length );
				my $gcs = Unicode::GCString->new( $tmp );
				$colwidth = $gcs->columns();
				if ( $colwidth > $arg->{length_longest} ) {
					$unicode = $tmp;
					last;
				}
				$max_length += $arg->{step};
			}
			while ( $colwidth > $arg->{length_longest} ) {
				$unicode =~ s/\X\z//;
				my $gcs = Unicode::GCString->new( $unicode );
				$colwidth = $gcs->columns();
			}
			$unicode .= ' ' if $colwidth < $arg->{length_longest};
        }
        elsif ( $colwidth < $arg->{length_longest} ) {
            if ( $arg->{right_justify} ) {
                $unicode = " " x ( $arg->{length_longest} - $colwidth ) . $unicode;
            }
            else {
                $unicode = $unicode . " " x ( $arg->{length_longest} - $colwidth );
            }
        }
    };
    if ( $@ ) {
        my $colwidth = length $word;
        if ( $colwidth > $arg->{length_longest} ) {
            $word = substr( $word, 0, $arg->{length_longest} );
        }
        elsif ( $colwidth < $arg->{length_longest} ) {
            if ( $arg->{right_justify} ) {
                $word = " " x ( $arg->{length_longest} - $colwidth ) . $word;
            }
            else {
                $word = $word . " " x ( $arg->{length_longest} - $colwidth );
            }
        }
        return $word;
    }
    else {
        return $unicode;
    }
}

# without the option "length_longest" this _unicode_sprintf would suffice
#
#sub _unicode_sprintf {
#    my ( $arg, $unicode ) = @_;
#    my $colwidth;
#    eval {
#        my $gcs = Unicode::GCString->new( $unicode );
#        $colwidth = $gcs->columns();
#    };
#    if ( $@ ) {
#        $colwidth = length $unicode;
#    }
#	if ( $colwidth < $arg->{length_longest} ) {
#		if ( $arg->{right_justify} ) {
#			$unicode = " " x ( $arg->{length_longest} - $colwidth ) . $unicode;
#		}
#		else {
#			$unicode = $unicode . " " x ( $arg->{length_longest} - $colwidth );
#		}
#	}
#	return $unicode;
#}



1;

__END__

=pod

=encoding utf8

=head1 NAME

Term::Choose::GC - Works as L<Term::Choose>.

=head1 VERSION

Version 0.7.13

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

=head1 DIFFERENCES

=head2 UNICODE

While L<Term::Choose> uses the Perl builtin functions I<length> to determine the length of strings and I<sprintf> widths to justify strings L<Term::Choose::GC> uses L<Unicode::GCString::columns|http://search.cpan.org/perldoc?Unicode::GCString#Sizes> to determine the length of strings. To justify strings it uses its own function based on L<Unicode::GCString>. The codeparts using L<Unicode::GCString::columns|http://search.cpan.org/perldoc?Unicode::GCString#Sizes> run in I<eval> blocks: if the code in the eval block fails builtin I<length> resp. I<substr> are used instead. The reason for this procedure with I<eval> is to make L<Term::Choose::GC>'s choose work also with non-unicode characters.

=head2 REQUIREMENTS

Additionally to the L<Term::Choose|http://search.cpan.org/perldoc?Term::Choose#REQUIREMENTS> requirements L<Term::Choose::GC> needs the module L<Unicode::GCString>.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Term::Choose::GC

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


















