#!/usr/bin/env perl
use warnings;
use 5.10.1;
use utf8;
binmode STDOUT, ':utf8';

use Cwd qw(realpath);
use File::Find qw(find); 
use File::Spec::Functions qw(catfile tmpdir);
use Getopt::Long qw(GetOptions :config bundling);
use List::Util qw(sum max);
use Scalar::Util qw(looks_like_number);
use Term::ANSIColor;

use CHI;
use DBI qw(:sql_types);
use File::HomeDir qw(my_home); 
use File::LibMagic;
use Term::Choose::GC qw(choose);
use Term::ReadKey qw(GetTerminalSize);
use Unicode::GCString;

use constant { GO_TO_TOP_LEFT => "\e[1;1H", CLEAR_EOS => "\e[0J" };

my $limit      = 5_000;    # The maximum number of rows read from tables.
my $root_dir   = tmpdir(); # Cache root directory
my $expires_in = '7d';     # Days until data expires. The cache holds the names of the found databases.
my $no_cache   = 0;        # Reset cache.
my $max_depth;             # Levels to descend at most when searching in directories for databases.
my $delete     = 1;        # Enable option "Delete Table".
my $min_width  = 20;       # The width the columns should have at least when printed (if possible).
my $tab        = 2;        # The number of spaces between columns.
my $undef      = '';       # The string, that will be shown instead of undefined table values.

# colors on a terminal with green fond and black background:
my @colors = ( 'cyan', '', 'magenta', 'white', 'green', 'blue', 'yellow', 'red' );
my $table_head_color = 'white reverse';

my $help;
GetOptions (
    'h|help'        => \$help,
    's|no-cache'    => \$no_cache,
    'l|limit:i'     => \$limit,
    'm|max-depth:i' => \$max_depth,
);

sub help {
    print << 'HELP';

Usage:
    table_info.pl [options] [directories to be searched]

Options:
    -h|--help       : Show this Info.
    -s|--no-cache   : Reset cache. The cache holds the names of the found databases.
    -m|--max-depth  : Levels to descend at most when searching in directories for databases.
    -l|--limit      : Sets the maximum number of rows read from tables.
HELP
}


my @dirs = @ARGV;
if ( not @dirs ) {
    my $home = File::HomeDir->my_home;
    @dirs = ( $home );
}

my $key = join ' ', @dirs, '|', $max_depth // '';
my $cached = ' (cached)';

my $cache = CHI->new ( 
    namespace => realpath( $0 ), 
    driver => 'File', 
    root_dir => $root_dir,  
    expires_in => $expires_in, 
    expires_variance => 0.25 
);
$cache->remove( $key ) if $no_cache;
my @databases = $cache->compute( $key, $expires_in, sub { return search_databases->( @dirs ) } );
    
sub search_databases {
    my @dirs = @_;
    my @databases;
    $cached = '';
    say "searching...";
    my $flm = File::LibMagic->new();
    for my $dir ( @dirs ) {
        find( {
            preprocess  =>  sub {
                if ( defined $max_depth ) {
                    my $depth = $File::Find::dir =~ tr[/][];
                    return @_ if $depth < $max_depth;
                    return grep { not -d } @_ if $depth == $max_depth;                     
                    return;
                } else {
                    return @_;
                }
            },
            wanted      =>  sub {
                my $file = $File::Find::name;
                return if not -f $file;
                push @databases, $file if $flm->describe_filename( $file ) =~ /^SQLite/; 
            },
            no_chdir    =>  1, 
        }, 
        $dir );
    }
    say "ended searching";
    return @databases;
}

say "no sqlite-databases found" and exit if not @databases;

my $quit = 'QUIT';
my $back = 'BACK';
my %lyt = ( layout => 2, clear_screen => 1 );

my %auswahl = ( 
    color_with_col_names => '° table', 
    color_cut_col_names  => '° table cut col_names', 
    color_choose_columns => '° choose columns',
    
    row_with_col_names => '* table', 
    row_cut_col_names  => '* table cut col_names', 
    row_choose_columns => '* choose columns',
    
    count_rows     => '  count rows',
    delete_table   => '  delete_table',
);

my @aw_keys = ( qw( 
    color_with_col_names
    color_cut_col_names
    color_choose_columns
    count_rows    
    row_with_col_names
    row_cut_col_names
    row_choose_columns
) );
push @aw_keys, 'delete_table' if $delete; 


DATABASES: while ( 1 ) {
    my $db = choose( [ undef, @databases ], { prompt => 'Choose Database' . $cached, %lyt, undef => $quit } );
    last DATABASES if not defined $db;
    chomp $db;
    my $dbh;
    eval {
        $dbh = DBI->connect( "DBI:SQLite:dbname=$db", '', '', { 
            RaiseError => 1, 
            PrintError => 0, 
            AutoCommit => 1, 
            sqlite_unicode => 1 
        } ) or die DBI->errstr;
        $dbh->do("PRAGMA cache_size = 400000");
        $dbh->do("PRAGMA synchronous = OFF");
    };
    if ( $@ ) {
        print $@, '  ';
        choose( [ 'Press ENTER to continue' ], { prompt => 0 } ); 
        next DATABASES;
    }

    TABLES: while ( 1 ) {
        my %tables;
        my( $master, $temp_master );
        for my $table ( $dbh->tables() ) {
           if ( $table eq '"main"."sqlite_master"' ) {
                $master = '  sqlite_master';
                next;
           }
           if ( $table eq '"temp"."sqlite_temp_master"' ) {
                $temp_master = '  sqlite_temp_master';
                next;
           }           
           if ( $table =~ /\A.*\."([^"]+)"\z/ ) {
                $table = "- $1";
            } 
            ++$tables{$table};
        }
        my @tables = sort keys %tables;
        push @tables, $master if $master;
        push @tables, $temp_master if $temp_master;
        my $table = choose( [ undef, @tables ], { prompt => 'Choose Table', %lyt, undef => "  $back" } );
        $table =~ s/\A..// if defined $table;
        last TABLES if not defined $table;

        CHOOSE: while ( 1 ) {
            my $choice = choose( [ undef, @auswahl{@aw_keys} ], { %lyt, undef => "  $back" } );
            given ( $choice ) {
                when ( not defined ) {
                    last CHOOSE;
                }
                when ( $auswahl{color_with_col_names} ) {
                    my $str = '*';
                    my $type = 'color';
                    my $cut = 0;
                    print_table( $dbh, $table, $str, $type, $cut );
                }
                when ( $auswahl{color_cut_col_names} ) {
                    my $str = '*';
                    my $type = 'color';
                    my $cut = 1;
                    print_table( $dbh, $table, $str, $type, $cut );
                }
                when ( $auswahl{color_choose_columns} ) {
                    my $sth = $dbh->prepare( "SELECT * FROM $table" );
                    my $col_names = $sth->{NAME};
                    my @choice_col = choose( $col_names, { prompt => 'Choose Columns', %lyt } );
                    my $str = join ', ', @choice_col;
                    my $type = 'color';
                    my $cut = 0;
                    print_table( $dbh, $table, $str, $type, $cut );                    
                }
                when ( $auswahl{row_with_col_names} ) {
                    my $str = '*';
                    my $type = 'row';
                    my $cut = 0;
                    print_table( $dbh, $table, $str, $type, $cut );
                }
                when ( $auswahl{row_cut_col_names} ) {
                    my $str = '*';
                    my $type = 'row';
                    my $cut = 1;
                    print_table( $dbh, $table, $str, $type, $cut );
                }
                when ( $auswahl{row_choose_columns} ) {
                    my $sth = $dbh->prepare( "SELECT * FROM $table" );
                    my $col_names = $sth->{NAME};
                    my @choice_col = choose( $col_names, { prompt => 'Choose Columns', %lyt } );
                    my $str = join ', ', @choice_col;
                    my $type = 'row';
                    my $cut = 0;
                    print_table( $dbh, $table, $str, $type, $cut );                    
                }
                when ( $auswahl{count_rows} ) {
                    my $rows = $dbh->selectrow_array( "SELECT COUNT(*) FROM $table" );
                    $rows =~ s/(\d)(?=(?:\d{3})+\b)/$1_/g;
                    say "\n  Table <", colored( $table, 'cyan' ), ">:";                            
                    say "\n  ", colored( $rows, 'cyan' ), " ", colored( 'Rows', '' ), "\n"; 
                    choose( [ 'Press ENTER to continue' ], { prompt => 0 } ); 
                }
                when ( $auswahl{delete_table} ) {
                    my $c = choose( [ ' No ', ' Yes ' ], { prompt => "realy delete table \"$table\"?", pad_one_row => 1 } );
                    if ( $c eq ' Yes ' ) {
                        $dbh->do( "DROP TABLE $table" );
                        last TABLES;
                    }
                }
                default {
                    die "Something wrong!";
                }
            }
        } 
    }
}
    
    
############################################################################################################


sub read_db_table {
    my ( $dbh, $table, $str ) = @_;
    my $ref;
    eval {
        my $sth = $dbh->prepare( "SELECT $str FROM $table LIMIT ?" );
        $sth->execute( $limit );
        my $col_names = $sth->{NAME};
        $ref = $sth->fetchall_arrayref();
        unshift @$ref, $col_names;
    };
    if ( $@ ) {
        print $@, '  ';
        choose( [ 'Press ENTER to continue' ], { prompt => 0 } ); 
        return;
    }
    return $ref;
}


sub cal_tab {
    my ( $ref, $cut ) = @_; 
    my ( $max, $not_a_number );
    my $count = 0;
    for my $row ( @$ref ) {
        $count++;
        for my $i ( 0 .. $#$row ) {
            $max->[$i] //= 0;
            next if not defined $row->[$i];
            $row->[$i] =~ s/\p{Space}/ /g; #
            $row->[$i] =~ s/\p{Cntrl}//g;  #
            next if $cut and $count == 1;
            eval {
                my $gcstring = Unicode::GCString->new( $row->[$i] );
                $max->[$i] = $gcstring->columns() if $gcstring->columns() > $max->[$i];
            }; 
            if ( $@ ) { 
                $max->[$i] = length $row->[$i] if length $row->[$i] > $max->[$i];
            }
            next if not $cut and $count == 1;
            $not_a_number->[$i]++ if not looks_like_number $row->[$i];
        }
    }
    return $max, $not_a_number;    
}



sub minus_x_percent {
    my ( $value, $percent ) = @_;
    return int $value - ( $value * 1/100 * $percent );
}

sub recalc_widths {
    my ( $maxcols, $ref, $cut ) = @_;
    my ( $max, $not_a_number );
    eval {
        ( $max, $not_a_number ) = cal_tab( $ref, $cut );
    };
    if ( $@ ) {
        print $@, '  ';
        choose( [ 'Press ENTER to continue' ], { prompt => 0 } ); 
        return;    
    }
    if ( $max and @$max ) {
        my $sum = sum( @$max ) + $tab * @$max; 
        $sum -= $tab;
        my @max_tmp = @$max;
        my $percent = 0;
        while ( $sum > $maxcols ) {
            $percent += 0.5;
            if ( $percent > 99 ) {
                say "Terminal window is not wide enough to print this table.";
                choose( [ 'Press ENTER to show the column names' ], { prompt => 0 } );
                choose( $ref->[0], { prompt => "Column names (close with ENTER):", layout => 0 } );
                return;
            }
            my $count = 0;
            for my $i ( 0 .. $#max_tmp ) {
                next if $min_width >= $max_tmp[$i];
                if ( $min_width >= minus_x_percent( $max_tmp[$i], $percent ) ) {
                    $max_tmp[$i] = $min_width;
                }
                else {
                    $max_tmp[$i] = minus_x_percent( $max_tmp[$i], $percent );
                }
                $count++;
                last if $sum <= $maxcols;   
            }
            $min_width-- if $count == 0 and $min_width > 1;
            $sum = sum( @max_tmp ) + $tab * @max_tmp; 
            $sum -= $tab;
        }
        my $rest = $maxcols - $sum;
        while ( $rest > 0 ) {
            my $count = 0;
            for my $i ( 0 .. $#max_tmp ) {
                if ( $max_tmp[$i] < $max->[$i] ) {
                    $max_tmp[$i]++;
                    $rest--;
                    $count++;
                    last if $rest < 1;
                }
            } 
            last if $count == 0;
            last if $rest < 1;
        }
        $max = [ @max_tmp ] if @max_tmp;
    }   
    return $max, $not_a_number;
}


sub print_table {
    my ( $dbh, $table, $str, $type, $cut ) = @_;
    my ( $ref ) = read_db_table ( $dbh, $table, $str );
    my ( $maxcols, $maxrows ) = GetTerminalSize( *STDOUT );
    return if not defined $ref;
    my ( $max, $not_a_number ) = recalc_widths( $maxcols, $ref );
    return if not defined $max;
    if ( $type eq 'row' ) {
        my @list;
        for my $row ( @$ref ) {
            my $string;
            for my $i ( 0 .. $#$max ) {
                my $word = $row->[$i] // $undef;
                my $right_justify = $not_a_number->[$i] ? 0 : 1;
                $string .= Term::Choose::GC::_unicode_sprintf( 
                            $max->[$i], 
                            $word, 
                            $right_justify, 
                            $maxcols + int( $maxcols / 10 )
                );
                $string .= ' ' x $tab if not $i == $#$max;
            }
            push @list, $string;
        }     
        choose( \@list, { prompt => 0, layout => 2 } );
        return;
    }
    my $first_row = '';
    my $f_row = shift @$ref;
    for my $i ( 0 .. $#$max ) {
        my $word = $f_row->[$i] // $undef;
        my $right_justify = $not_a_number->[$i] ? 0 : 1;
        $word = Term::Choose::GC::_unicode_sprintf( 
                    $max->[$i], 
                    $word, 
                    $right_justify, 
                    $maxcols + int( $maxcols / 10 )
        );
        $word .= ' ' x $tab if not $i == $#$max;
        $first_row .= colored( $word, $table_head_color );
    }
    $first_row .= "\n";
    my $text_rows = $maxrows;
    $text_rows -= 3; # space for aw + index + ?
    my ( $weiter, $zurück, $quit ) = ( 'Down', ' Up ', 'Close' );
    my $options = [ $weiter, $zurück, $quit ]; 
    my $prompt = 0;
    if ( $text_rows > $#$ref ) {
        $text_rows = $#$ref;
        $options = [ $quit ];
        $prompt = '';
    }
    my $begin = 0;
    my $end = $text_rows;
    SCROLL: while ( 1 ) { 
        print GO_TO_TOP_LEFT;
        my $string = $first_row;
        for my $row ( @$ref[$begin..$end] ) {
            my $ci = 0;
            for my $i ( 0 .. $#$max ) {
                my $word = $row->[$i] // $undef;
                my $right_justify = $not_a_number->[$i] ? 0 : 1;
                $word  = Term::Choose::GC::_unicode_sprintf( 
                            $max->[$i], 
                            $word, 
                            $right_justify, 
                            $maxcols + int( $maxcols / 10 ) 
                );
                $string .=  colored( $word, $colors[$ci] );
                $string .= ' ' x $tab if not $i == $#$max;
                $ci++;
                $ci = 0 if $ci >= @colors;
            }
            $string .= "\n";
        }        
        print $string;
        # # # # # # # # # # # # # # # # # # # # # # # # # # # #
        my $choice = choose( $options, { prompt => $prompt } );
        if ( $choice eq $weiter ) {
            $begin += $text_rows + 1 if $begin + $text_rows + 1 < $#$ref; #
            $end = $begin + $text_rows;
            $end = $#$ref if $end > $#$ref;
            $options = [ $weiter, $zurück, $quit ];
        }
        elsif ( $choice eq $zurück ) {
            $begin -= $text_rows + 1;
            $begin = 0 if $begin < 0;
            $end = $begin + $text_rows;
            $options = [ $zurück, $weiter, $quit ];
        }  
        else {
            print GO_TO_TOP_LEFT;
            print CLEAR_EOS;
            last SCROLL;
        }
    }
    return;
}




__DATA__
