#!/usr/bin/env perl
use warnings;
use 5.10.1;
use utf8;
binmode STDOUT, ':encoding(utf-8)';
binmode STDIN,  ':encoding(utf-8)';

# use warnings FATAL => qw(all);
# use Data::Dumper;
# Version 0.15

use File::Find qw(find);
use File::Spec::Functions qw(catfile rel2abs tmpdir);
use Getopt::Long qw(GetOptions :config bundling);
use List::Util qw(sum);
use Scalar::Util qw(looks_like_number);
use Term::ANSIColor;

use CHI;
use Config::Tiny;
use DBI qw(:sql_types);
use File::HomeDir qw(my_home); 
use File::LibMagic;
use List::MoreUtils qw(first_index);
use Term::Choose::GC qw(choose);
use Term::ProgressBar;
use Term::ReadKey qw(GetTerminalSize);
use Unicode::GCString;

use constant { 
    GO_TO_TOP_LEFT  => "\e[1;1H", 
    CLEAR_EOS       => "\e[0J", 
    UP              => "\e[A" 
};

my $home = File::HomeDir->my_home;

my $arg = {
    back            => 'BACK',
    quit            => 'QUIT',
    home            => $home,
    cached          => '',
    ini_section     => 'table_watch',
    cache_rootdir   => tmpdir(),
    config_file     => catfile( $home, '.table_info.conf' ),
};

sub help {
    print << 'HELP';

Usage:
    table_watch_SQLite.pl [help or options] [directories to be searched]

    If no directories are passed the home directory is searched for SQLite databases.
    Options with the parenthesis at the end can be used on the commandline too.
    Customized Table - REGEXP: for case sensitivity prefix pattern with (?-i).

Options:
    Help          : Show this Info.  (-h|--help)
    Settings      : Show settings.
    Cache expire  : Days until data expires. The cache holds the names of the found databases.
    Reset cache   : Reset the cache.  (-s|--no-cache)
    Maxdepth      : Levels to descend at most when searching in directories for databases.  (-m|--max-depth) 
    Limit         : Set the maximum number of table rows read in one time.
    Delete        : Enable the option "delete table" and "delete database".  (-d|--delete)
    No BLOB       : Print only "BLOB" if the column-type is BLOB.
    Tab           : Set the number of spaces between columns.
    Min-Width     : Set the width the columns should have at least when printed.
    Undef         : Set the string that will be shown on the screen if a table value is undefined.
    Cut col names : Set when column name should be cut.
    Thousands sep : Choose the thousands separator.

    
HELP
}

#------------------------------------------------------------------------#
#------------------------------   options   -----------------------------#
#------------------------------------------------------------------------#

my $opt = {
    cache_expire  => '7d',
    reset_cache   => 0,
    max_depth     => undef,
    limit         => 10_000,
    no_blob       => 1,
    tab           => 2,
    min_width     => 30,
    delete        => 0,
    undef         => '',
    cut_col_names => -1,
    kilo_sep      => ',',
};

my $help;
GetOptions (
    'h|help'        => \$help,
    's|no-cache'    => \$opt->{reset_cache},
    'm|max-depth:i' => \$opt->{max_depth},
    'd|delete'      => \$opt->{delete},
);


if ( not -f $arg->{config_file} ) {
    open my $fh, '>', $arg->{config_file} or warn $!;
    close $fh or warn $!;
}

if ( -f $arg->{config_file} and -s $arg->{config_file} ) {
    my $ini = Config::Tiny->new;
    $ini = Config::Tiny->read( $arg->{config_file} );
    my $section = $arg->{ini_section};
    for my $key ( keys %{$ini->{$section}} ) {
        if ( $ini->{$section}{$key} eq '' ) {
            $ini->{$section}{$key} = undef;
        }
        elsif ( $ini->{$section}{$key} eq "''" ) {
            $ini->{$section}{$key} = '';
        }
        else {
            $opt->{$key} = $ini->{$section}{$key};
        }
    }
}

$opt = options( $arg, $opt ) if $help;


#------------------------------------------------------------------------#
#------------------   database specific subroutines   -------------------#
#------------------------------------------------------------------------#

sub get_database_handle {
    my ( $db ) = @_;
    my $dbh;
    eval {
        die "\"$db\": $!. Maybe the cached data is not up to date." if not -f $db;
        $dbh = DBI->connect( "DBI:SQLite:dbname=$db", '', '', { 
            RaiseError     => 1, 
            PrintError     => 0, 
            AutoCommit     => 1, 
            sqlite_unicode => 1,
        } ) or die DBI->errstr;
        $dbh->sqlite_busy_timeout( 3000 );
        $dbh->do( 'PRAGMA cache_size = 400000' );
        $dbh->do( 'PRAGMA synchronous = OFF' );
    };
    if ( $@ ) {
        print $@, '  ';
        choose( [ 'Press ENTER to continue' ], { prompt => 0 } ); 
        return;
    }
    return $dbh;
}


sub available_databases {
    my ( $arg, $opt ) = @_;
    my @dirs = @ARGV ? @ARGV : ( $arg->{home} );
    $arg->{cache_key} = join ' ', @dirs, '|', $opt->{max_depth} // '';
    $arg->{cached} = ' (cached)';
    $arg->{cache} = CHI->new ( 
        namespace => 'table_watch_SQLite', 
        driver => 'File', 
        root_dir => $arg->{cache_rootdir},  
        expires_in => $opt->{cache_expire}, 
        expires_variance => 0.25, 
    );
    $arg->{cache}->remove( $arg->{cache_key} ) if $opt->{reset_cache};
    
    my @databases = $arg->{cache}->compute( 
        $arg->{cache_key}, 
        $opt->{cache_expire}, 
        sub { 
            my @databases;
            $arg->{cached} = '';
            say 'searching...';
            my $flm = File::LibMagic->new();
            for my $dir ( @dirs ) {
                my $max_depth;
                if ( defined $opt->{max_depth} ) {
                    $max_depth = $opt->{max_depth};
                    $dir = rel2abs $dir;
                    $max_depth += $dir =~ tr[/][];
                    $max_depth--;
                }
                find( {
                    preprocess => sub {
                        if ( defined $max_depth ) {
                            my $depth = $File::Find::dir =~ tr[/][];
                            return @_ if $depth < $max_depth;
                            return grep { not -d } @_ if $depth == $max_depth;
                            return;
                        } 
                        else {
                            return @_;
                        }
                    },
                    wanted     => sub {
                        my $file = $File::Find::name;
                        return if not -f $file;
                        push @databases, $file if $flm->describe_filename( $file ) =~ /\ASQLite/;
                    },
                    no_chdir   => 1, 
                }, 
                $dir );
            }
            say 'ended searching';
            return @databases;
        }
    );
    return @databases;
}


sub remove_database {
    my ( $db ) = @_;
    eval { unlink $db or die $! };
    if ( $@ ) {
        say "Could not remove database \"$db\"";
        print $@;
        return;
    }
    return 1;
}


sub get_table_names {
    my ( $dbh ) = @_;
    my $tables = $dbh->selectcol_arrayref( "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name" );
    return $tables;
}

#------------------------------------------------------------------------#
#-------------------------------   main   -------------------------------#
#------------------------------------------------------------------------#

my @databases = available_databases( $arg, $opt );
say 'no sqlite-databases found' and exit if not @databases;


DATABASES: while ( 1 ) {

    my %lyt = ( layout => 3, clear_screen => 1 );

    my $db = choose( [ undef, @databases ], { prompt => 'Choose Database' . $arg->{cached}, %lyt, undef => $arg->{quit} } );

    last DATABASES if not defined $db;
    my $dbh = get_database_handle( $db );
    next DATABASES if not defined $dbh;
    $arg->{db_type} = lc $dbh->{Driver}{Name};

    TABLES: while ( 1 ) {

        my $tables = get_table_names( $dbh );
        my @tables = map { "- $_" } @$tables;
        push @tables, '  sqlite_master', '  sqlite_temp_master' if $arg->{db_type} eq 'sqlite';
        push @tables, '  delete database' if $opt->{delete};

        my $table = choose( [ undef, @tables ], { prompt => 'Choose Table', %lyt, undef => '  ' . $arg->{back} } );
        
        last TABLES if not defined $table;
        $table =~ s/\A..//;
        if ( $table eq 'delete database' ) {
            say "\nRealy delete database ", colored( "\"$db\"", 'red' ), "?\n";
            my $c = choose( [ ' No ', ' Yes ' ], { prompt => 0, pad_one_row => 1 } );
            if ( $c eq ' Yes ' ) {
                my $ok = remove_database( $db );
                if ( $ok and $arg->{db_type} eq 'sqlite' ) {
                    $arg->{cache}->remove( $arg->{cache_key} );
                    @databases = grep { $_ ne $db } @databases;
                }
                last TABLES;
            }
        }
        my $table_q = $dbh->quote_identifier( $table );

        CHOOSE: while ( 1 ) {
                
            my %auswahl = ( 
                table_auto      => '= table auto', 
                table_customize => '= table customized',
                count_rows      => '  count rows',
                delete_table    => '  delete table',
            );
            my @aw_keys = ( qw( table_auto table_customize count_rows ) );
            push @aw_keys, 'delete_table' if $opt->{delete};
        
            my $choice = choose( [ undef, @auswahl{@aw_keys} ], { %lyt, undef => '  ' . $arg->{back} } );
            
            given ( $choice ) {
                when ( not defined ) {
                    last CHOOSE;
                }
                when ( $auswahl{table_auto} ) {
                    my $rows = $dbh->selectrow_array( "SELECT COUNT(*) FROM $table_q" );
                    my $select = "SELECT * FROM $table_q";
                    my $arguments;
                    PRINT: while ( 1 ) {
                        my ( $offset, $last ) = choose_table_range( $arg, $opt, $rows );
                        last PRINT if $last == 1;	
                        my ( $ref, $col_types ) = read_db_table( $opt, $dbh, $offset, $select, $arguments );
                        last PRINT if not defined $ref;
                        print_table( $opt, $ref, $col_types );
                        last PRINT if $last == 2;
                    }
                }
                when ( $auswahl{table_customize} ) {
                    my ( $rows, $select, $arguments ) = prepare_read_table( $arg, $opt, $dbh, $table );
                    next CHOOSE if not defined $rows;
                    PRINT: while ( 1 ) {
                        my ( $offset, $last ) = choose_table_range( $arg, $opt, $rows );	
                        last PRINT if $last == 1;
                        my ( $ref, $col_types ) = read_db_table( $opt, $dbh, $offset, $select, $arguments );
                        last PRINT if not defined $ref;
                        print_table( $opt, $ref, $col_types );
                        last PRINT if $last == 2;
                    }
                }
                when ( $auswahl{count_rows} ) {
                    my $rows = $dbh->selectrow_array( "SELECT COUNT(*) FROM $table_q" );
                    $rows =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{kilo_sep}/g;
                    choose( [ "  Table $table_q:  $rows Rows  ", '  Press ENTER to continue  ' ], { prompt => "\"$db\"", layout => 3 } ); 
                }
                when ( $auswahl{delete_table} ) {
                    say "\n$db";
                    say "";
                    say 'Realy delete table ', colored( "$table_q", 'red' ), '?';
                    say "";
                    my $c = choose( [ ' No ', ' Yes ' ], { prompt => 0, pad_one_row => 1 } );
                    if ( $c eq ' Yes ' ) {
                        eval { $dbh->do( "DROP TABLE $table_q" ) };
                        if ( $@ ) {
                            say "Could not drop table $table_q";
                            print $@;
                        }
                        else {
                            @tables = grep { $_ ne $table } @tables;
                        }
                        last CHOOSE;
                    }
                }
                default {
                    die 'Something wrong!';
                }
            }
        } 
    }
}
    
##########################################################################
############################   subroutines   #############################
##########################################################################


sub choose_table_range {
    my ( $arg, $opt, $rows ) = @_;
    my $offset = 0;
    my $last = 0;
    my $begin = 0;
    my $end = $opt->{limit} - 1;    
    if ( $rows > $opt->{limit} ) {
        my @choices;
        my $lr = length $rows;
        push @choices, sprintf "%${lr}d - %${lr}d", $begin, $end;
        $rows -= $opt->{limit};
        while ( $rows > 0 ) {
            $begin += $opt->{limit};
            $end   += ( $rows > $opt->{limit} ) ? $opt->{limit} : $rows;
            push @choices, sprintf "%${lr}d - %${lr}d", $begin, $end;
            $rows -= $opt->{limit};
        }
        my $choice = choose( [ undef, @choices ], { layout => 3, undef => $arg->{back} } );
        if ( defined $choice ) {
            $offset = ( split /\s*-\s*/, $choice )[0];
            $offset =~ s/\A\s+//;
        }
        else { 
            $last = 1; 
        }
    }
    else { 
        $last = 2;
    }
    return $offset, $last;
}


sub read_db_table {
    my ( $opt, $dbh, $offset, $select, $arguments ) = @_;
    my ( $ref, $col_types );
    eval {
        my $sth = $dbh->prepare( $select . " LIMIT ?, ?" );
        $sth->execute( defined $arguments ? @$arguments : (), $offset, $opt->{limit} );
        my $col_names = $sth->{NAME};
        $col_types = $sth->{TYPE};
        $ref = $sth->fetchall_arrayref();
        unshift @$ref, $col_names;
    };
    if ( $@ ) {
        print $@, '  ';
        choose( [ 'Press ENTER to continue' ], { prompt => 0 } ); 
        return;
    }
    return $ref, $col_types;
}


sub print_select {
    my ( $arg, $opt, $table, $columns, $chosen_columns, $order_columns, $order_direction, $search_columns, $search_pattern ) = @_;
    print GO_TO_TOP_LEFT;
    print CLEAR_EOS;
    $chosen_columns = @$chosen_columns ? $chosen_columns : $columns;
    say "SELECT ", ( @$chosen_columns ~~ @$columns ) ? '*' : join( ', ', @$chosen_columns );
    say " FROM $table";
    say " WHERE ", join " $arg->{AND_OR} ", map { "$_ REGEXP $search_pattern->{$_}" } @$search_columns  if @$search_columns;
    say " ORDER BY ", join ', ', map { "$_ $order_direction->{$_}" } @$order_columns                    if @$order_columns;
    say "";
}


sub prepare_read_table {
    my ( $arg, $opt, $dbh, $table ) = @_;
    $dbh->func( 'regexp', 2, sub { my ( $regex, $string ) = @_; $string //= ''; return $string =~ m/$regex/ism }, 'create_function' ) if $arg->{db_type} eq 'sqlite'; 
    my $continue = ' OK ';
    my $table_q = $dbh->quote_identifier( $table );
    my @keys = ( qw( print_table columns order_by regexp ) );
    my %customize = ( print_table => 'Print TABLE', columns => '- Columns', order_by => '- Order by', regexp => '- Regexp' ); 
    my $sth = $dbh->prepare( "SELECT * FROM $table_q" );
    $sth->execute();
    my $columns	= $sth->{NAME};   
    my $chosen_columns  = [];
    my $order_columns   = [];
    my $order_direction = {};
    my $search_columns  = [];	
    my $search_pattern  = {};
    $arg->{AND_OR} = '';

    CUSTOMIZE: while ( 1 ) {
        print_select( $arg, $opt, $table, $columns, $chosen_columns, $order_columns, $order_direction, $search_columns, $search_pattern );
        my $custom = choose( [ undef, @customize{@keys} ], { prompt => 'Customize:', layout => 3, undef => $arg->{back} } );
        for ( $custom ) {
            when ( not defined ) {
                last CUSTOMIZE;	
            }
            when( $customize{columns} ) {
                my @cols = @$columns;
                $chosen_columns = [];
                while ( @cols ) {
                    print_select( $arg, $opt, $table, $columns, $chosen_columns, $order_columns, $order_direction, $search_columns, $search_pattern );
                    my $col = choose( [ $continue, @cols ], { prompt => 'Choose: ', pad_one_row => 2 } );
                    if ( not defined $col ) {
                        $chosen_columns = [];
                        last;
                    }
                    last if $col eq $continue;
                    push @$chosen_columns, $col;
                    my $idx = first_index { $_ eq $col } @cols;
                    splice @cols, $idx, 1;
                }
                if ( not @$chosen_columns ) {
                    @$chosen_columns = @$columns;
                }[$table]
            }
            when( $customize{order_by} ) {
                my @cols = @$columns;
                $order_columns   = []; 
                $order_direction = {}; 
                while ( @cols ) {
                    print_select( $arg, $opt, $table, $columns, $chosen_columns, $order_columns, $order_direction, $search_columns, $search_pattern );
                    my $col = choose( [ $continue, @cols ], { prompt => 'Choose: ', pad_one_row => 2 } );
                    if ( not defined $col ) {
                        $order_columns   = [];
                        $order_direction = {}; 
                        last;
                    }
                    last if $col eq $continue;
                    push @$order_columns, $col;
                    my $idx = first_index { $_ eq $col } @cols;
                    splice @cols, $idx, 1;
                    my $direction = choose( [ " ASC ", " DESC " ], { prompt => 'Sort order:', layout => 1, pad_one_row => 1 } );
                    $direction =~ s/\A\s+|\s+\z//g;
                    $order_direction->{$col} = $direction;
                }
            }
            when ( $customize{regexp} ) {
                my @cols = @$columns;
                $search_columns = [];
                $search_pattern = {};
                while ( @cols ) {
                    print_select( $arg, $opt, $table, $columns, $chosen_columns, $order_columns, $order_direction, $search_columns, $search_pattern );
                    my $col = choose( [ $continue, @cols ], { prompt => 'Choose: ', pad_one_row => 2 } );
                    if ( not defined $col ) {
                        $search_columns = [];
                        $search_pattern = {};
                        last;
                    }
                    last if $col eq $continue;
                    if ( keys %$search_pattern == 1 ) {
                        $arg->{AND_OR} = choose( [ "     AND     ", "     OR     " ], { prompt => 'Join all REGEXP\'s with:', layout => 3, pad_one_row => 1 } );
                        $arg->{AND_OR} =~ s/\A\s+|\s+\z//g;
                    }
                    print "$col: ";
                    my $pattern = <>;
                    chomp $pattern;
                    $pattern= qr/$pattern/i if $arg->{db_type} eq 'sqlite';
                    $search_pattern->{$col} = $pattern;
                    push @$search_columns, $col;
                    my $idx = first_index { $_ eq $col } @cols;
                    splice @cols, $idx, 1;
                }
            }
            when( $customize{print_table} ) {
                $chosen_columns = @$chosen_columns ? $chosen_columns : $columns;
                
                # ORDER BY
                my $order_by_str = '';
                $order_by_str  = " ORDER BY " . join( ', ', map { $dbh->quote_identifier( $_ ) . " $order_direction->{$_}" } @$order_columns ) if @$order_columns;
                
                # REGEXP
                my $search_str = '';
                $search_str    = " WHERE " . join(  " $arg->{AND_OR} ", map { $dbh->quote_identifier( $_ ) . " REGEXP ?" } @$search_columns ) if @$search_columns;
                
                # COLUMNS
                my $cols_str = join( ', ', map { $dbh->quote_identifier( $_ ) } @$chosen_columns ? @$chosen_columns : @$columns );
                my $select = "SELECT $cols_str FROM $table_q" . $search_str . $order_by_str;
                my @arguments = ( @$search_pattern{@$search_columns} );
                my $rows = $dbh->selectrow_array( "SELECT COUNT(*) FROM $table_q" . $search_str, {}, @arguments );
                return $rows, $select, \@arguments;
            }
            default {
                say 'Something is wrong!';
            }
        }
    }
    return;
}


sub calc_widths {
    my ( $opt, $ref, $col_types, $cut, $maxcols ) = @_; 
    my ( $max_head, $max, $not_a_number );
    my $count = 0;
    for my $row ( @$ref ) {
        $count++;
        for my $i ( 0 .. $#$row ) {
            $row->[$i] = 'Blob' if $col_types->[$i] eq 'BLOB' and $count > 1 and $opt->{no_blob};
            $max->[$i] //= 0;
            next if not defined $row->[$i];
            $row->[$i] =~ s/\p{Space}/ /g;
            $row->[$i] =~ s/\p{Cntrl}//g;
            if ( $count == 1 and $#$ref > 0 ) {
                if ( $cut == 1 ) {
                    next ;
                }
                elsif ( $cut == -1 ) {
                    eval {
                        my $gcstring = Unicode::GCString->new( $row->[$i] );
                        $max_head->[$i] = $gcstring->columns();
                    }; 
                    if ( $@ ) { 
                        $max_head->[$i] = length $row->[$i];
                    }
                    next;
                }
            }
            eval {
                my $gcstring = Unicode::GCString->new( $row->[$i] );
                $max->[$i] = $gcstring->columns() if $gcstring->columns() > $max->[$i];
            }; 
            if ( $@ ) { 
                $max->[$i] = length $row->[$i] if length $row->[$i] > $max->[$i];
            }
            next if $count == 1;
            ++$not_a_number->[$i] if not looks_like_number $row->[$i];
        }
    }
    if ( $#$ref > 0 and $cut == -1 and sum( @$max ) < $maxcols and sum( @$max_head ) < $maxcols ) {
        for my $i ( 0 .. $#$max_head ) {
            $max->[$i] = ( $max->[$i] >= $max_head->[$i] ) ? $max->[$i] : $max_head->[$i];
        }
    }
    return $max, $not_a_number;    
}


sub minus_x_percent {
    my ( $value, $percent ) = @_;
    return int $value - ( $value * 1/100 * $percent );
}

sub recalc_widths {
    my ( $opt, $maxcols, $ref, $col_types ) = @_;
    my $cut = $opt->{cut_col_names};
    my ( $max, $not_a_number );
    eval {
        ( $max, $not_a_number ) = calc_widths( $opt, $ref, $col_types, $cut, $maxcols );
    };
    if ( $@ ) {
        print $@, '  ';
        choose( [ 'Press ENTER to continue' ], { prompt => 0 } ); 
        return;    
    }
    return if not defined $max;
    return if not @$max;
    my $sum = sum( @$max ) + $opt->{tab} * @$max; 
    $sum -= $opt->{tab};
    my @max_tmp = @$max;
    my $percent = 0;
    my $min_width_tmp = $opt->{min_width};
    while ( $sum > $maxcols ) {
        $percent += 0.5;
        if ( $percent > 99 ) {
            say 'Terminal window is not wide enough to print this table.';
            choose( [ 'Press ENTER to show the column names' ], { prompt => 0 } );
            choose( $ref->[0], { prompt => 'Column names (close with ENTER):', layout => 0 } );
            return;
        }
        my $count = 0;
        for my $i ( 0 .. $#max_tmp ) {
            next if $min_width_tmp >= $max_tmp[$i];
            if ( $min_width_tmp >= minus_x_percent( $max_tmp[$i], $percent ) ) {
                $max_tmp[$i] = $min_width_tmp;
            }
            else {
                $max_tmp[$i] = minus_x_percent( $max_tmp[$i], $percent );
            }
            $count++;
            last if $sum <= $maxcols;   
        }
        $min_width_tmp-- if $count == 0 and $min_width_tmp > 1;
        $sum = sum( @max_tmp ) + $opt->{tab} * @max_tmp; 
        $sum -= $opt->{tab};
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
    }
    $max = [ @max_tmp ] if @max_tmp;
    return $max, $not_a_number;
}

            
sub print_table {
    my ( $opt, $ref, $col_types ) = @_;
    my ( $maxcols, $maxrows ) = GetTerminalSize( *STDOUT );
    return if not defined $ref;
    my ( $max, $not_a_number ) = recalc_widths( $opt, $maxcols, $ref, $col_types );
    return if not defined $max;
    print GO_TO_TOP_LEFT;
    print CLEAR_EOS; 

    my $items = @$ref * @{$ref->[0]};         #
    my $start = 8_000;                        #
    my $total = $#{$ref};                     #
    my $next_update = 0;                      #
    my $c = 0;                                #
    my $progress;                             #
    if ( $items > $start ) {                  #
        $progress = Term::ProgressBar->new( { #
            name => 'Computing',              #
            count => $total,                  #
            remove => 1 } );                  #
        $progress->minor( 0 );                #
    }                                         #
    my @list;
    for my $row ( @$ref ) {
        my $string;
        for my $i ( 0 .. $#$max ) {
            my $word = $row->[$i] // $opt->{undef};
            my $right_justify = $not_a_number->[$i] ? 0 : 1;
            $string .= unicode_sprintf( 
                        $max->[$i], 
                        $word, 
                        $right_justify, 
                        $maxcols + int( $maxcols / 10 )
            );
            $string .= ' ' x $opt->{tab} if not $i == $#$max;
        }
        push @list, $string;
        if ( $items > $start ) {                                          #
            my $is_power = 0;                                             #
            for ( my $i = 0; 2 ** $i <= $c; $i++) {                       #
                $is_power = 1 if 2 ** $i == $c;                           #
            }                                                             #
            $next_update = $progress->update( $c ) if $c >= $next_update; #
            ++$c;                                                         #
        }                                                                 #
    }   
    $progress->update( $total ) if $total >= $next_update and $items > $start; #
    
    choose( \@list, { prompt => 0, layout => 3, length_longest => sum( @$max, $opt->{tab} * $#{$max} ), limit => $opt->{limit} + 1 } );
    
    return;
}


sub unicode_sprintf {
    my ( $length, $word, $right_justify, $max_length ) = @_;
    my $unicode = $word;
    eval {
        my $gcs = Unicode::GCString->new( $unicode );
        my $colwidth = $gcs->columns();
        if ( $colwidth > $length ) {
            if ( defined $max_length && $colwidth > $max_length ) {
                $unicode = substr( $gcs->as_string, 0, $max_length );
                my $gcs = Unicode::GCString->new( $unicode );
                $colwidth = $gcs->columns();
            }
            while ( $colwidth > $length ) {
                $unicode =~ s/\X\z//;
                my $gcs = Unicode::GCString->new( $unicode );
                $colwidth = $gcs->columns();
            }
            $unicode .= ' ' if $colwidth < $length;
        } 
        elsif ( $colwidth < $length ) {
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
        elsif ( $colwidth < $length ) {        
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


#-----------------------------------------------------------------------------#
#-------------------------    subroutine options     -------------------------#
#-----------------------------------------------------------------------------#

sub options {
    my ( $arg, $opt ) = @_;
    my $oh = {
        cache_expire    => '- Cache expire', 
        reset_cache     => '- Reset cache', 
        max_depth       => '- Maxdepth', 
        limit           => '- Limit', 
        delete          => '- Delete', 
        tab             => '- Tab', 
        min_width       => '- Min-Width', 
        no_blob         => '- No BLOB',
        undef           => '- Undef', 
        cut_col_names   => '- Cut col names',
        kilo_sep        => '- Thousands sep',
    };
    my @keys = ( qw( cache_expire reset_cache max_depth limit delete no_blob min_width tab kilo_sep undef cut_col_names ) );
    my $change;
    
    OPTIONS: while ( 1 ) {
        my ( $exit, $help, $show_settings, $continue ) = ( '  EXIT', '  HELP', '  SHOW SETTINGS', '  CONTINUE' );
        my $option = choose( [ $exit, $help, @{$oh}{@keys}, $show_settings, undef ], { undef => $continue, layout => 3, clear_screen => 1 } );
        my $back = '<<';
        my %number_lyt = ( layout => 1, vertical => 0, right_justify => 1, undef => $back );
        my %bol = ( undef => " $back ", pad_one_row => 1 );
        
        given ( $option ) {
            when ( not defined ) { last OPTIONS; }
            when ( $exit ) { exit() }
            when ( $help ) { help(); choose( [ '  Close with ENTER  ' ], { prompt => 0 } ) }
            when ( $show_settings ) {
                my @choices;
                for my $key ( @keys ) {
                    my $value;
                    if ( not defined $opt->{$key} ) {
                        $value = 'undef';
                    } 
                    elsif ( $key eq 'undef' && $opt->{$key} eq '' ) {
                        $value = "''";
                    }
                    elsif ( $key eq 'limit' ) {
                        $value = $opt->{$key};
                        $value =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{kilo_sep}/g;
                    }
                    elsif ( $key eq 'kilo_sep' ) {
                        $value = 'space " "'      if $opt->{$key} eq ' ';
                        $value = 'none'           if $opt->{$key} eq '';
                        $value = 'underscore "_"' if $opt->{$key} eq '_';
                        $value = 'full stop "."'  if $opt->{$key} eq '.';
                        $value = 'comma ","'      if $opt->{$key} eq ',';   
                    }
                    elsif ( $key eq 'reset_cache' or $key eq 'delete' or $key eq 'no_blob' ) {
                        $value = $opt->{$key} ? 'true' : 'false';
                    }
                    elsif ( $key eq 'cut_col_names' ) {
                        $value = 'auto'    if $opt->{$key} == -1;
                        $value = 'no cut'  if $opt->{$key} ==  0;
                        $value = 'cut col' if $opt->{$key} ==  1; 
                    }                    
                    else {
                        $value = $opt->{$key};
                    }
                    ( my $name = $oh->{$key} ) =~ s/\A..//;
                    push @choices, sprintf "%-16s : %s\n", "  $name", $value;
                }
                choose( [ @choices ], { prompt => 'Close with ENTER', layout => 3 } );
            }            
            when ( $oh->{cache_expire} ) { 
                my $number = choose( [ undef, 0 .. 99 ], { prompt => 'Days until data expires (' . $opt->{cache_expire} . '):', %number_lyt } ); 
                break if not defined $number;
                $opt->{cache_expire} = $number.'d';
                $change++;
            }
            when ( $oh->{reset_cache} ) {
                my ( $true, $false ) = ( ' YES ', ' NO ' );
                my $choice = choose( [ undef, $true, $false ], { prompt => 'Reset cache (' . $opt->{reset_cache} . '):', %bol } );
                break if not defined $choice;
                $opt->{reset_cache} = ( $choice eq $true ) ? 1 : 0;
                $change++;                
            }  
            when ( $oh->{max_depth} ) { 
                my $number = choose( [ undef, '--', 0 .. 99 ], { prompt => 'Levels to descend at most (' . ( $opt->{max_depth} // 'undef' ) . '):', %number_lyt } ); 
                break if not defined $number;
                if ( $number eq '--' ) {
                    $opt->{max_depth} = undef;
                }
                else {
                    $opt->{max_depth} = $number;
                }
                $change++;
            }
            when ( $oh->{tab} ) { 
                my $number = choose( [ undef, 0 .. 99 ], { prompt => 'Tab width (' . $opt->{tab} . '):',  %number_lyt } );
                break if not defined $number;
                $opt->{tab} = $number;
                $change++;
            }
            when ( $oh->{kilo_sep} ) {
                my %sep_h;
                my ( $comma, $full_stop, $underscore, $space, $none ) = ( ' comma ', ' full stop ', ' underscore ', ' space ', ' none ' );
                @sep_h{ $comma, $full_stop, $space, $none } = ( ',', '.', '_', ' ', '' );
                my $sep = choose( [ undef, $comma, $full_stop, $underscore, $space, $none ], { prompt => 'Thousands separator (' . $opt->{kilo_sep} . '):',  %bol } );
                break if not defined $sep;
                $opt->{kilo_sep} = $sep_h{$sep};
                $change++;
            }
            when ( $oh->{min_width} ) { 
                my $number = choose( [ undef, 0 .. 99 ], { prompt => 'Minimum Column width (' . $opt->{min_width} . '):',  %number_lyt } );
                break if not defined $number;
                $opt->{min_width} = $number;
                $change++;
            }
            when ( $oh->{limit} ) { 
                my %hash;
                my $limit;
                while ( 1 ) {
                    my $digits = 7; #
                    my $longest = $digits + int( ( $digits - 1 ) / 3 );
                    my @list;
                    for my $di ( 0 .. $digits - 1 ) {
                        my $begin = 1 . '0' x $di;
                        $begin =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{kilo_sep}/g;
                        ( my $end = $begin ) =~ s/\A1/9/;
                        unshift @list, sprintf " %*s  -  %*s", $longest, $begin, $longest, $end;
                    }
                    my $confirm;
                    if ( $limit ) {
                        $confirm = "confirm result: $limit";
                        push @list, $confirm;
                    }
                    ( my $limit_now = $opt->{limit} ) =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{kilo_sep}/g;
                    my $choice = choose( [ undef, @list ], { prompt => 'Compose new "limit" (' . $limit_now . '):', layout => 3, right_justify => 1, undef => $arg->{back} . ' ' x ( $longest * 2 + 1 ) } );
                    break if not defined $choice;

                    last if $confirm and $choice eq $confirm;
                    $choice = ( split /\s*-\s*/, $choice )[0];
                    $choice =~ s/\A\s*\d//;
                    my $reset = 'reset';
                    
                    my $c = choose( [ undef, map( $_ . $choice, 1 .. 9 ), $reset ], { pad_one_row => 2, undef => '<<' } );
                    next if not defined $c;
                    
                    if ( $c eq $reset ) {
                        delete $hash{length $c};
                    }
                    else {
                        $c =~ s/\Q$opt->{kilo_sep}\E//g;
                        $hash{length $c} = $c;
                    }
                    $limit = sum( @hash{keys %hash} );
                    $limit =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{kilo_sep}/g;
                } 
                $limit =~ s/\Q$opt->{kilo_sep}\E//g;
                $opt->{limit} = $limit;
                $change++;
            }
            when ( $oh->{delete} ) { 
                my ( $true, $false ) = ( ' Enable delete options ', ' Disable delete options ' );
                my $choice = choose( [ undef, $true, $false ], { prompt => 'Delete" options (' . ( $opt->{delete} ? 'Enabled' : 'Disabled' ) . '):', %bol } );
                break if not defined $choice;
                $opt->{delete} = ( $choice eq $true ) ? 1 : 0;
                $change++;
            }
            when ( $oh->{no_blob} ) { 
                my ( $true, $false ) = ( ' Enable NO BLOB ', ' Dissable NO BLOB ' );
                my $choice = choose( [ undef, $true, $false ], { prompt => 'NO BLOB (' . ( $opt->{no_blob} ? 'Enabled' : 'Disables' ) . '):', %bol } );
                break if not defined $choice;
                $opt->{no_blob} = ( $choice eq $true ) ? 1 : 0;
                $change++;
            }
            when ( $oh->{undef} ) { 
                print 'Choose a replacement-string for undefined table vales ("' , $opt->{undef} . '"): ';
                my $undef = <>;
                chomp $undef;
                break if not $undef;
                $opt->{undef} = $undef;
                $change++;
            }
            when ( $oh->{cut_col_names} ) {
                my $default = 'Auto: cut column names if the sum of length of column names is greater than screen width.';
                my $cut_col = 'Cut the column names to the length of the longest column value.';
                my $no_cut  = 'Do not cut column names deliberately, instead treat column names as normal column valules.';
                my @sd = ( 'No cut', 'Cut', 'Auto' );
                my $cut = choose( [ undef, $default, $cut_col, $no_cut ], { prompt => 'Column names (' . $sd[$opt->{cut_col_names}] . '):', undef => $back } );
                break if not defined $cut;
                if    ( $cut eq $cut_col ) { $opt->{cut_col_names} =  1 }
                elsif ( $cut eq $no_cut )  { $opt->{cut_col_names} =  0 }
                else                       { $opt->{cut_col_names} = -1 }
                $change++;
            }
            default { die 'Something is wrong $!'; }
        }
    }
    if ( $change ) {
        my ( $true, $false ) = ( ' Make changes permanent ', ' Use changes only this time ' );
        my $permanent = choose( [ $false, $true ], { prompt => 'Modifications:', layout => 3, pad_one_row => 1 } );
        exit if not defined $permanent;
        if ( $permanent eq $true and -f $arg->{config_file} ) {
            my $ini = Config::Tiny->new;
            my $section = $arg->{ini_section};
            for my $key ( keys %$opt ) {
                if ( not defined $opt->{$key} ) {
                    $ini->{$section}->{$key} = '';
                }
                elsif ( $opt->{$key} eq '' ) {
                    $ini->{$section}->{$key} = "''";
                }
                else {
                    $ini->{$section}->{$key} = $opt->{$key};
                }
            }
            $ini->write( $arg->{config_file} );
        }
        if ( $permanent eq $true ) {
            my $continue = choose( [ ' CONTINUE ', undef ], { prompt => 0, layout => 1, undef => ' QUIT ', pad_one_row => 1 } );
            exit() if not defined $continue;
        }
    }
    return $opt;
}


__DATA__

