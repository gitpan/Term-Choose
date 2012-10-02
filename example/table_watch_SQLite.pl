#!/usr/bin/env perl
use warnings;
use 5.10.1;
use utf8;
binmode STDOUT, ':encoding(utf-8)';
binmode STDIN,  ':encoding(utf-8)';

use warnings FATAL => qw(all);
use Data::Dumper;
# Version 0.16

use File::Spec::Functions qw(catfile rel2abs tmpdir);
use Getopt::Long qw(GetOptions :config bundling);
use List::Util qw(sum);
use Scalar::Util qw(looks_like_number);
use Term::ANSIColor;

use CHI;
use Config::Tiny;
use DBI qw(:sql_types);
use File::Find::Rule;
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
    back                => 'BACK',
    quit                => 'QUIT',
    home                => $home,
    cached              => '',
    ini_section         => 'table_watch',
    cache_rootdir       => tmpdir(),
    config_file         => catfile( $home, '.table_info.conf' ),
    filter_types        => [ "REGEXP", "<", ">", "=", "<>" ], # LIKE, >=, <=
    aggregate_functions => [ 'avg(X)', 'count(X)', 'count(*)', 'max(X)', 'min(X)', 'sum(X)' ], # group_concat(X), group_concat(X,Y), total(X)
};

sub help {
    print << 'HELP';

Usage:
    table_watch_SQLite.pl [help or options] [directories to be searched]
    If no directories are passed the home directory is searched for SQLite databases.
    Options with the parenthesis at the end can be used on the command line too.

Customized Table:
    To reset a sub-statement press the "q" key or leave the sub-menu with '- OK -' 
    then reenter in the same sub-menu and choose as the next step '- OK -' again. 
    Sub-menu REGEXP: for case sensitivity prefix pattern with (?-i).

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

my $help;
GetOptions (
    'h|help'        => \$help,
    's|no-cache'    => \$opt->{reset_cache},
    'm|max-depth:i' => \$opt->{max_depth},
    'd|delete'      => \$opt->{delete},
);

$opt = options( $arg, $opt ) if $help;


#------------------------------------------------------------------------#
#------------------   database specific subroutines   -------------------#
#------------------------------------------------------------------------#

sub get_database_handle {
    my ( $db ) = @_;
    die "\"$db\": $!. Maybe the cached data is not up to date." if not -f $db;
    my $dbh = DBI->connect( "DBI:SQLite:dbname=$db", '', '', { 
        RaiseError     => 1, 
        PrintError     => 0, 
        AutoCommit     => 1, 
        sqlite_unicode => 1,
    } ) or die DBI->errstr;
    $dbh->sqlite_busy_timeout( 3000 );
    $dbh->do( 'PRAGMA cache_size = 400000' );
    $dbh->do( 'PRAGMA synchronous = OFF' );
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
            $arg->{cached} = '';
            say 'searching...';
            my $flm = File::LibMagic->new();
            my $rule = File::Find::Rule->new();
            $rule->file();
            $rule->maxdepth( $opt->{max_depth} ) if defined $opt->{max_depth};
            $rule->exec( sub{ return $flm->describe_filename( $_ ) =~ /\ASQLite/ } );
            my @databases = $rule->in( @dirs );
            say 'ended searching';
            return @databases;                
        }
    );
    return @databases;
}


sub remove_database {
    my ( $db ) = @_;
    unlink $db or die $!;
}


sub get_table_names {
    my ( $dbh ) = @_;
    my $tables = $dbh->selectcol_arrayref( "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name" );
    return $tables;
}

#------------------------------------------------------------------------#
#-------------------------------   main   -------------------------------#
#------------------------------------------------------------------------#

my @databases;
if ( not eval {     ##  TRY  ##
    @databases = available_databases( $arg, $opt );
    1 }
) {                 ## CATCH ##
    say 'Available databases:';
    print $@;
    choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
}

say 'no sqlite-databases found' and exit if not @databases;

my %lyt = ( layout => 3, clear_screen => 1 );

DATABASES: while ( 1 ) {
    # CHOOSE
    my $db = choose( [ undef, @databases ], { prompt => 'Choose Database' . $arg->{cached}, %lyt, undef => $arg->{quit} } );
    last DATABASES if not defined $db;
    my ( $dbh, $tables );
    if ( not eval {     ##  TRY  ## 
        $dbh = get_database_handle( $db );
        $arg->{db_type} = lc $dbh->{Driver}{Name};
        $tables = get_table_names( $dbh );
        1 }
    ) {                 ## CATCH #
        say 'Get database handle and table names:';
        print $@;
        choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
        # remove database from @databases
        next DATABASES;
    }

    my @tables = map { "- $_" } @$tables;
    push @tables, '  sqlite_master', '  sqlite_temp_master' if $arg->{db_type} eq 'sqlite';
    push @tables, '  delete database' if $opt->{delete};

    TABLES: while ( 1 ) {
        # CHOOSE
        my $table = choose( [ undef, @tables ], { prompt => 'Choose Table', %lyt, undef => '  ' . $arg->{back} } );
        last TABLES if not defined $table;
        $table =~ s/\A..//;
        if ( $table eq 'delete database' ) {
            if ( not eval {     ##  TRY  ##
                say "\nRealy delete database ", colored( "\"$db\"", 'red' ), "?\n";
                # CHOOSE
                my $c = choose( [ ' No ', ' Yes ' ], { prompt => 0, pad_one_row => 1 } );
                if ( $c eq ' Yes ' ) {
                    remove_database( $db );
                }
                1 }
            ) {                 ## CATCH #
                say 'Delete database:';
                say "Could not remove database \"$db\"";
                print $@;
                choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
            }
            else {
                $arg->{cache}->remove( $arg->{cache_key} ) if $arg->{db_type} eq 'sqlite';
                @databases = grep { $_ ne $db } @databases;
                last TABLES;
            }
            # last TABLES;
        }

        my $table_q = $dbh->quote_identifier( $table );
        
        my %mode = ( 
            table_auto      => '= table auto', 
            table_customize => '= table customized',
            count_rows      => '  count rows',
            delete_table    => '  delete table',
        );
        my @mode_keys = ( qw( table_auto table_customize count_rows ) );
        push @mode_keys, 'delete_table' if $opt->{delete};

        CHOOSE: while ( 1 ) {
            # CHOOSE   
            my $choice = choose( [ undef, @mode{@mode_keys} ], { %lyt, undef => '  ' . $arg->{back} } );
            
            given ( $choice ) {
                when ( not defined ) {
                    last CHOOSE;
                }
                when ( $mode{table_auto} ) {
                    if ( not eval {     ##  TRY  ##
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
                        1 }
                    ) {                 ## CATCH ##
                        say 'Table auto:';
                        print $@;
                        choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
                    }
                }
                when ( $mode{table_customize} ) {
                    if ( not eval {     ##  TRY  ##
                        my ( $rows, $select, $arguments ) = prepare_read_table( $arg, $opt, $dbh, $table );
                        #next CHOOSE if not defined $rows;
                        break if not defined $rows;
                        PRINT: while ( 1 ) {
                            my ( $offset, $last ) = choose_table_range( $arg, $opt, $rows );	
                            last PRINT if $last == 1;
                            my ( $ref, $col_types ) = read_db_table( $opt, $dbh, $offset, $select, $arguments );
                            last PRINT if not defined $ref;
                            print_table( $opt, $ref, $col_types );
                            last PRINT if $last == 2;
                        }
                        1 }
                    ) {                 ## CATCH ##
                        say 'Table customized:';
                        print $@;
                        choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
                    }
                }
                when ( $mode{count_rows} ) {
                    if ( not eval {     ##  TRY  ##
                        my $rows = $dbh->selectrow_array( "SELECT COUNT(*) FROM $table_q" );
                        $rows =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{kilo_sep}/g;
                        choose( [ "  Table $table_q:  $rows Rows  ", '  Press ENTER to continue  ' ], { prompt => "\"$db\"", layout => 3 } );
                        1 }
                    ) {                 ## CATCH ##
                        say 'Count rows:';
                        print $@;
                        choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
                    }
                }
                when ( $mode{delete_table} ) {
                    if ( not eval {     ##  TRY  ##
                        say "\n$db";
                        say "";
                        say 'Realy delete table ', colored( "$table_q", 'red' ), '?';
                        say "";
                        # CHOOSE
                        my $c = choose( [ ' No ', ' Yes ' ], { prompt => 0, pad_one_row => 1 } );
                        if ( $c eq ' Yes ' ) {
                            $dbh->do( "DROP TABLE $table_q" );
                        }
                        1 }
                    ) {                 ## CATCH ##
                        say 'Delete table:';
                        say "Could not drop table $table_q";
                        print $@;
                        choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
                    }
                    else {              ## ELSE ##
                        @tables = grep { $_ ne $table } @tables;
                        last CHOOSE;                        
                    }
                    #last CHOOSE;
                }
                default {
                    die "$choice: no such value in the hash \%mode";
                }
            }
        } 
    }
}


#------------------------------------------------------------------------#
#---------------------------   subroutines   ----------------------------#
#------------------------------------------------------------------------#


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
        # CHOOSE
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
    my $sth = $dbh->prepare( $select . " LIMIT ?, ?" );
    $sth->execute( defined $arguments ? @$arguments : (), $offset, $opt->{limit} );
    my $col_names = $sth->{NAME};
    my $col_types = $sth->{TYPE};
    my $ref = $sth->fetchall_arrayref();
    unshift @$ref, $col_names;
    return $ref, $col_types;
}


sub print_select {
    my ( $arg, $opt, $table, $columns, $chosen_columns, $print ) = @_;
    print GO_TO_TOP_LEFT;
    print CLEAR_EOS;
    my $cols_str = '';
    $cols_str = ' '. join( ', ', @$chosen_columns ) if @$chosen_columns;  
    $cols_str = $print->{group_by_cols}        if not @$chosen_columns and $print->{group_by_cols};
    if ( $print->{aggregate_stmt} ) {
        $cols_str .= ',' if $cols_str;
        $cols_str .= $print->{aggregate_stmt};
    }
    $cols_str = ' *' if not $cols_str;
    print "SELECT";
    print $print->{distinct_stmt} if $print->{distinct_stmt};
    say $cols_str;
    say " FROM $table";
    say $print->{where_stmt}    if $print->{where_stmt};
    say $print->{group_by_stmt} if $print->{group_by_stmt};
    say $print->{having_stmt}   if $print->{having_stmt};    
    say $print->{order_by_stmt} if $print->{order_by_stmt};
    say "";
}


sub prepare_read_table {
    my ( $arg, $opt, $dbh, $table ) = @_;
    $dbh->func( 'regexp', 2, sub { my ( $regex, $string ) = @_; $string //= ''; return $string =~ m/$regex/ism }, 'create_function' ) if $arg->{db_type} eq 'sqlite'; 
    my $continue = '- OK -';
    my $back = '- reset -';
    my %bol  = ( layout => 1, pad_one_row => 1, undef => $back );
    my %list = ( layout => 1, pad_one_row => 2, undef => $back );
    my $table_q = $dbh->quote_identifier( $table );
    my @keys = ( qw( print_table columns aggregate distinct where group_by having order_by ) );
    my %customize = ( 
        print_table     => 'Print TABLE', 
        columns         => '- COLUMNS',
        aggregate       => '- AGGREGATE',
        distinct        => '- DISTINCT',
        where           => '- WHERE', 
        group_by        => '- GROUP BY',
        having          => '- HAVING',        
        order_by        => '- ORDER BY', 
    ); 
    my $sth = $dbh->prepare( "SELECT * FROM $table_q" );
    $sth->execute();
    
    my $columns = $sth->{NAME};   
    my $chosen_columns = [];
    my @aliases        = ();
    my @stmt_keys = ( qw( distinct_stmt group_by_cols aggregate_stmt where_stmt group_by_stmt having_stmt order_by_stmt ) );
    my $print = {};
    my $quote = {};
    @$print{@stmt_keys} = ( '' ) x @stmt_keys;  
    @$quote{@stmt_keys} = ( '' ) x @stmt_keys;
    $quote->{where_args}  = [];    
    $quote->{having_args} = [];    

    CUSTOMIZE: while ( 1 ) {
        print_select( $arg, $opt, $table, $columns, $chosen_columns, $print );
        # CHOOSE
        my $custom = choose( [ undef, @customize{@keys} ], { prompt => 'Customize:', layout => 3, undef => $arg->{back} } );
        for ( $custom ) {
            when ( not defined ) {
                last CUSTOMIZE; 
            }
            when( $customize{columns} ) {
                my @cols = @$columns;
                $chosen_columns = [];
                COLUMNS: while ( @cols ) {
                    print_select( $arg, $opt, $table, $columns, $chosen_columns, $print );
                    # CHOOSE
                    my $col = choose( [ $continue, @cols ], { prompt => 'Choose: ', %list } ); # undef
                    if ( not defined $col ) {
                        $chosen_columns = [];
                        last COLUMNS;
                    }
                    if ( $col eq $continue ) {
                        last COLUMNS;
                    }   
                    push @$chosen_columns, $col;
                }
            }
            when( $customize{distinct} ) {
                my ( $distinct, $all ) = ( " DISTINCT ", " ALL " );
                $quote->{distinct_stmt} = '';
                $print->{distinct_stmt} = '';
                DISTINCT: while ( 1 ) {
                    print_select( $arg, $opt, $table, $columns, $chosen_columns, $print );
                    # CHOOSE
                    my $select_distinct = choose( [ $continue, $distinct, $all ], { prompt => 'Choose: ', %bol } ); # undef
                    if ( not defined $select_distinct ) {
                        $quote->{distinct_stmt} = '';
                        $print->{distinct_stmt} = '';
                        last DISTINCT;
                    }
                    if ( $select_distinct eq $continue ) {
                        last DISTINCT;
                    }
                    $select_distinct =~ s/\A\s+|\s+\z//g;
                    $quote->{distinct_stmt} = ' ' . $select_distinct;
                    $print->{distinct_stmt} = ' ' . $select_distinct;
                }
            }
            when( $customize{order_by} ) {
                my @cols = ( @$columns, @aliases );
                $arg->{col_sep} = ' ';
                $quote->{order_by_stmt} = " ORDER BY";
                $print->{order_by_stmt} = " ORDER BY";
                ORDER_BY: while ( 1 ) {
                    print_select( $arg, $opt, $table, $columns, $chosen_columns, $print );
                    # CHOOSE
                    my $col = choose( [ $continue, @cols ], { prompt => 'Choose: ', %list } ); # undef
                    if ( not defined $col ) {
                        $quote->{order_by_stmt} = '';
                        $print->{order_by_stmt} = '';
                        last ORDER_BY;
                    }
                    if ( $col eq $continue ) {
                        last ORDER_BY;
                    }
                    $quote->{order_by_stmt} .= $arg->{col_sep} . $dbh->quote_identifier( $col );
                    $print->{order_by_stmt} .= $arg->{col_sep} .                         $col  ;
                    print_select( $arg, $opt, $table, $columns, $chosen_columns, $print );
                    # CHOOSE
                    my $direction = choose( [ " ASC ", " DESC " ], { prompt => 'Sort order:', %bol } ); # undef
                    if ( not defined $direction ){
                        $quote->{order_by_stmt} = '';
                        $print->{order_by_stmt} = '';
                        last ORDER_BY;
                    }
                    $direction =~ s/\A\s+|\s+\z//g;
                    $quote->{order_by_stmt} .= ' ' . $direction;
                    $print->{order_by_stmt} .= ' ' . $direction;
                    $arg->{col_sep} = ', ';
                }
            }
            when ( $customize{where} ) {
                my @cols = @$columns;
                my $AND_OR = '';
                $quote->{where_args} = [];
                $quote->{where_stmt} = " WHERE";
                $print->{where_stmt} = " WHERE";
                WHERE: while ( 1 ) { 
                    print_select( $arg, $opt, $table, $columns, $chosen_columns, $print );
                    # CHOOSE
                    my $col = choose( [ $continue, @cols ], { prompt => 'Choose: ', %list } ); # undef
                    if ( not defined $col ) {
                        $quote->{where_args} = [];
                        $quote->{where_stmt} = '';
                        $print->{where_stmt} = '';
                        last WHERE;
                    }
                    if ( $col eq $continue ) {
                        last WHERE;
                    }
                    if ( @{$quote->{where_args}} >= 1 ) {
                        print_select( $arg, $opt, $table, $columns, $chosen_columns, $print );
                        # CHOOSE
                        $AND_OR = choose( [ " AND ", " OR " ], { prompt => 'Append with:', %bol } ); # undef
                        if ( not defined $AND_OR ) {
                            last WHERE;
                        }
                        $AND_OR =~ s/\A\s+|\s+\z//g;
                        $AND_OR = ' ' . $AND_OR;
                    }
                    $quote->{where_stmt} .= $AND_OR . ' ' . $dbh->quote_identifier( $col );
                    $print->{where_stmt} .= $AND_OR . ' ' .                         $col  ;
                    print_select( $arg, $opt, $table, $columns, $chosen_columns, $print );
                    # CHOOSE
                    my $filter_type = choose( [ @{$arg->{filter_types}} ], { prompt => 'Filter type:', %list } ); # undef
                    if ( not defined $filter_type ) {
                        $quote->{where_args} = [];
                        $quote->{where_stmt} = '';
                        $print->{where_stmt} = '';
                        last WHERE;
                    }
                    if ( $filter_type eq $continue ) {
                        last WHERE;
                    }
                    $filter_type =~ s/\A\s+|\s+\z//g;
                    $quote->{where_stmt} .= ' ' . $filter_type;
                    $print->{where_stmt} .= ' ' . $filter_type;
                    print_select( $arg, $opt, $table, $columns, $chosen_columns, $print );                    
                    print "argument: ";
                    my $pattern = <STDIN>;
                    chomp $pattern;
                    $pattern= qr/$pattern/i if $arg->{db_type} eq 'sqlite' and $filter_type eq "REGEXP";
                    $quote->{where_stmt} .= " ?";
                    $print->{where_stmt} .= " $pattern";
                    push @{$quote->{where_args}}, $pattern;
                }
            }
            when( $customize{aggregate} ) {
                my @cols = @$columns;
                $arg->{col_sep} = ' ';
                $quote->{aggregate_stmt} = '';
                $print->{aggregate_stmt} = '';
                AGGREGATE: while ( 1 ) {
                    print_select( $arg, $opt, $table, $columns, $chosen_columns, $print );
                    # CHOOSE
                    my $func = choose( [ $continue, @{$arg->{aggregate_functions}} ], { prompt => 'SELECT aggregate - choose function', %list } ); # undef
                    if ( not defined $func ) {
                        $quote->{aggregate_stmt} = '';
                        $print->{aggregate_stmt} = '';
                        last AGGREGATE;
                    }
                    if ( $func eq $continue ) {
                        last AGGREGATE;
                    }
                    my $col;
                    if ( $func =~ /\Acount\s*\(\s*\*\s*\)\z/) {
                        $col = '*';
                        $func = 'count';
                        $quote->{aggregate_stmt} .= $arg->{col_sep} . $func . '(';
                        $print->{aggregate_stmt} .= $arg->{col_sep} . $func . '(';  
                    }
                    else {
                        $func =~ s/\s*\(\s*\S\s*\)\z//;
                        $quote->{aggregate_stmt} .= $arg->{col_sep} . $func . '(';
                        $print->{aggregate_stmt} .= $arg->{col_sep} . $func . '(';  
                        print_select( $arg, $opt, $table, $columns, $chosen_columns, $print );
                        # CHOOSE
                        $col = choose( [ @cols ], { prompt => 'SELECT aggregate - choose column', %list } ); # undef
                        if ( not defined $col ) {
                            $quote->{aggregate_stmt} = '';
                            $print->{aggregate_stmt} = '';
                            last AGGREGATE;
                        }
                    }                
                    my $alias = $func . '_' . $col;
                    $quote->{aggregate_stmt} .= $dbh->quote_identifier( $col ) . ') AS ' . $dbh->quote( $alias );
                    $print->{aggregate_stmt} .=                         $col   . ') AS ' .              $alias  ; 
                    push @aliases, $alias;
                    $arg->{col_sep} = ', ';
                }
            }
            when( $customize{group_by} ) {
                my @cols = @$columns;
                $arg->{col_sep} = ' ';
                $quote->{group_by_stmt} = " GROUP BY";
                $print->{group_by_stmt} = " GROUP BY";
                GROUP_BY: while ( 1 ) {
                    print_select( $arg, $opt, $table, $columns, $chosen_columns, $print );
                    # CHOOSE
                    my $col = choose( [ $continue, @cols ], { prompt => 'Choose: ', %list } ); # undef
                    if ( not defined $col ) {
                        $quote->{group_by_stmt} = '';
                        $print->{group_by_stmt} = '';
                        $quote->{group_by_cols} = '';
                        $print->{group_by_cols} = '';                       
                        last GROUP_BY;
                    }
                    if ( $col eq $continue ) {
                        last GROUP_BY;
                    }
                    if ( not @$chosen_columns ) {
                        $quote->{group_by_cols} .= $arg->{col_sep} . $dbh->quote_identifier( $col );
                        $print->{group_by_cols} .= $arg->{col_sep} .                         $col  ;
                    }
                    $quote->{group_by_stmt} .= $arg->{col_sep} . $dbh->quote_identifier( $col );
                    $print->{group_by_stmt} .= $arg->{col_sep} .                         $col  ;
                    $arg->{col_sep} = ', ';
                }
            }
            when( $customize{having} ) {
                my @cols = @$columns;
                my $AND_OR = '';
                $quote->{having_args} = [];
                $quote->{having_stmt} = " HAVING";
                $print->{having_stmt} = " HAVING";
                HAVING: while ( 1 ) { 
                    print_select( $arg, $opt, $table, $columns, $chosen_columns, $print );
                    # CHOOSE
                    my $func = choose( [ $continue, @{$arg->{aggregate_functions}} ], { prompt => 'Choose: ', %list } ); # undef
                    if ( not defined $func ) {
                        $quote->{having_args} = [];
                        $quote->{having_stmt} = '';
                        $print->{having_stmt} = '';
                        last HAVING;
                    }
                    if ( $func eq $continue ) {
                        last HAVING;
                    }
                    if ( @{$quote->{having_args}} >= 1 ) {
                        print_select( $arg, $opt, $table, $columns, $chosen_columns, $print );
                        # CHOOSE
                        $AND_OR = choose( [ " AND ", " OR " ], { prompt => 'Append with:', %bol } ); # undef
                        if ( not defined $AND_OR ) {
                            last HAVING;
                        }
                        $AND_OR =~ s/\A\s+|\s+\z//g;
                        $AND_OR = ' ' . $AND_OR;
                    }
                    my $col;
                    if ( $func =~ /\Acount\s*\(\s*\*\s*\)\z/) {
                        $col = '*';
                        $func = 'count';
                        $quote->{having_stmt} .= $AND_OR . ' ' . $func . '(';
                        $print->{having_stmt} .= $AND_OR . ' ' . $func . '(';  
                    }
                    else {
                        $func =~ s/\s*\(\s*\S\s*\)\z//;
                        $quote->{having_stmt} .= $AND_OR . ' ' . $func . '(';
                        $print->{having_stmt} .= $AND_OR . ' ' . $func . '(';  
                        print_select( $arg, $opt, $table, $columns, $chosen_columns, $print );
                        # CHOOSE
                        $col = choose( [ @cols, undef ], { prompt => 'HAVING aggregate - choose column', %list } ); # undef                       
                        if ( not defined $col ) {
                            $quote->{having_args} = [];
                            $quote->{having_stmt} = '';
                            $print->{having_stmt} = '';
                            last HAVING;
                        }
                    }
                    $quote->{having_stmt} .= $dbh->quote_identifier( $col ) . ')';
                    $print->{having_stmt} .=                         $col   . ')'; 
                    print_select( $arg, $opt, $table, $columns, $chosen_columns, $print );
                    # CHOOSE
                    my $filter_type = choose( [ @{$arg->{filter_types}} ], { prompt => 'Filter type:', %list } ); # undef
                    if ( not defined $filter_type ) {
                        $quote->{having_args} = [];
                        $quote->{having_stmt} = '';
                        $print->{having_stmt} = '';
                        last HAVING;
                    }
                    if ( $filter_type eq $continue ) {
                        last HAVING;
                    }
                    $filter_type =~ s/\A\s+|\s+\z//g;
                    $quote->{having_stmt} .= ' ' . $filter_type;
                    $print->{having_stmt} .= ' ' . $filter_type;
                    print_select( $arg, $opt, $table, $columns, $chosen_columns, $print );                    
                    print "argument: ";
                    my $pattern = <STDIN>;
                    chomp $pattern;
                    $pattern= qr/$pattern/i if $arg->{db_type} eq 'sqlite' and $filter_type eq "REGEXP";
                    $quote->{having_stmt} .= " ?";
                    $print->{having_stmt} .= " $pattern";
                    push @{$quote->{having_args}}, $pattern;
                }
            }
            when( $customize{print_table} ) {
                my $cols_str = '';
                $cols_str = ' ' . join( ', ', map { $dbh->quote_identifier( $_ ) } @$chosen_columns ) if @$chosen_columns;
                $cols_str = $print->{group_by_cols} if not @$chosen_columns and $print->{group_by_cols};
                if ( $quote->{aggregate_stmt} ) {
                    $cols_str .= ',' if $cols_str;
                    $cols_str .= $quote->{aggregate_stmt};
                }
                $cols_str = ' *' if not $cols_str;
                my $select = "SELECT" . $quote->{distinct_stmt} . $cols_str . " FROM $table_q";
                $select .= $quote->{where_stmt};
                $select .= $quote->{group_by_stmt};
                $select .= $quote->{having_stmt};
                $select .= $quote->{order_by_stmt};
                my @arguments = ( @{$quote->{where_args}}, @{$quote->{having_args}} );
                my $rows = $dbh->selectrow_array( 
                    "SELECT COUNT(*) FROM $table_q" . $quote->{where_stmt},     # . $quote->{group_by_stmt} . $quote->{having_stmt}, 
                    {}, 
                    @{$quote->{where_args}},                                    # @arguments, 
                );
                return $rows, $select, \@arguments;
            }
            default {
                die "$custom: no such value in the hash \%customize";
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
                    if ( not eval {     ##  TRY  ##
                        my $gcstring = Unicode::GCString->new( $row->[$i] );
                        $max_head->[$i] = $gcstring->columns();
                        1 }
                    ) {                 ## CATCH ##
                        $max_head->[$i] = length $row->[$i];
                    }
                    next;
                }
            }
            if ( not eval {     ##  TRY  ##
                my $gcstring = Unicode::GCString->new( $row->[$i] );
                $max->[$i] = $gcstring->columns() if $gcstring->columns() > $max->[$i];
                1 } 
            ) {                 ## CATCH ##
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
    my ( $max, $not_a_number ) = calc_widths( $opt, $ref, $col_types, $cut, $maxcols );
    return if not defined $max or not @$max;
    my $sum = sum( @$max ) + $opt->{tab} * @$max; 
    $sum -= $opt->{tab};
    my @max_tmp = @$max;
    my $percent = 0;
    my $min_width_tmp = $opt->{min_width};
    while ( $sum > $maxcols ) {
        $percent += 0.5;
        if ( $percent > 99 ) {
            print GO_TO_TOP_LEFT;
            print CLEAR_EOS;
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
    # CHOOSE
    choose( \@list, { prompt => 0, layout => 3, length_longest => sum( @$max, $opt->{tab} * $#{$max} ), limit => $opt->{limit} + 1 } );
    return;
}


sub unicode_sprintf {
    my ( $length, $word, $right_justify, $max_length ) = @_;
    my $unicode = $word;
    if ( not eval {     ##  TRY  ##
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
        1 }
    ) {                 ## CATCH ##
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
    return $unicode;
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
        customized      => '- Customized',
    };
    my @keys = ( qw( cache_expire reset_cache max_depth limit delete no_blob min_width tab kilo_sep undef cut_col_names customized ) );
    my $change;
    
    OPTIONS: while ( 1 ) {
        my ( $exit, $help, $show_settings, $continue ) = ( '  EXIT', '  HELP', '  SHOW SETTINGS', '  CONTINUE' );
        # CHOOSE
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
                # CHOOSE
                my $number = choose( [ undef, 0 .. 99 ], { prompt => 'Days until data expires (' . $opt->{cache_expire} . '):', %number_lyt } ); 
                break if not defined $number;
                $opt->{cache_expire} = $number.'d';
                $change++;
            }
            when ( $oh->{reset_cache} ) {
                my ( $true, $false ) = ( ' YES ', ' NO ' );
                # CHOOSE
                my $choice = choose( [ undef, $true, $false ], { prompt => 'Reset cache (' . $opt->{reset_cache} . '):', %bol } );
                break if not defined $choice;
                $opt->{reset_cache} = ( $choice eq $true ) ? 1 : 0;
                $change++;                
            }  
            when ( $oh->{max_depth} ) { 
                # CHOOSE
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
                # CHOOSE
                my $number = choose( [ undef, 0 .. 99 ], { prompt => 'Tab width (' . $opt->{tab} . '):',  %number_lyt } );
                break if not defined $number;
                $opt->{tab} = $number;
                $change++;
            }
            when ( $oh->{kilo_sep} ) {
                my %sep_h;
                my ( $comma, $full_stop, $underscore, $space, $none ) = ( ' comma ', ' full stop ', ' underscore ', ' space ', ' none ' );
                @sep_h{ $comma, $full_stop, $space, $none } = ( ',', '.', '_', ' ', '' );
                # CHOOSE
                my $sep = choose( [ undef, $comma, $full_stop, $underscore, $space, $none ], { prompt => 'Thousands separator (' . $opt->{kilo_sep} . '):',  %bol } );
                break if not defined $sep;
                $opt->{kilo_sep} = $sep_h{$sep};
                $change++;
            }
            when ( $oh->{min_width} ) {
                # CHOOSE
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
                    # CHOOSE
                    my $choice = choose( [ undef, @list ], { prompt => 'Compose new "limit" (' . $limit_now . '):', layout => 3, right_justify => 1, undef => $arg->{back} . ' ' x ( $longest * 2 + 1 ) } );
                    break if not defined $choice;

                    last if $confirm and $choice eq $confirm;
                    $choice = ( split /\s*-\s*/, $choice )[0];
                    $choice =~ s/\A\s*\d//;
                    my $reset = 'reset';
                    # CHOOSE
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
                # CHOOSE
                my $choice = choose( [ undef, $true, $false ], { prompt => 'Delete" options (' . ( $opt->{delete} ? 'Enabled' : 'Disabled' ) . '):', %bol } );
                break if not defined $choice;
                $opt->{delete} = ( $choice eq $true ) ? 1 : 0;
                $change++;
            }
            when ( $oh->{no_blob} ) { 
                my ( $true, $false ) = ( ' Enable NO BLOB ', ' Dissable NO BLOB ' );
                # CHOOSE
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
                # CHOOSE
                my $cut = choose( [ undef, $default, $cut_col, $no_cut ], { prompt => 'Column names (' . $sd[$opt->{cut_col_names}] . '):', undef => $back } );
                break if not defined $cut;
                if    ( $cut eq $cut_col ) { $opt->{cut_col_names} =  1 }
                elsif ( $cut eq $no_cut )  { $opt->{cut_col_names} =  0 }
                else                       { $opt->{cut_col_names} = -1 }
                $change++;
            }
            default { die "$option: no such value in the hash \%oh";; }
        }
    }
    if ( $change ) {
        my ( $true, $false ) = ( ' Make changes permanent ', ' Use changes only this time ' );
        # CHOOSE
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
            # CHOOSE
            my $continue = choose( [ ' CONTINUE ', undef ], { prompt => 0, layout => 1, undef => ' QUIT ', pad_one_row => 1 } );
            exit() if not defined $continue;
        }
    }
    return $opt;
}

__DATA__
