#!/usr/bin/env perl
use warnings;
use 5.10.1;
use utf8;
binmode STDOUT, ':encoding(utf-8)';
binmode STDIN,  ':encoding(utf-8)';

use warnings FATAL => qw(all);
use Data::Dumper;
# Version 0.20

use File::Basename;
use File::Spec::Functions qw(catfile tmpdir);
use Getopt::Long qw(GetOptions :config bundling);
use List::Util qw(sum);
use Scalar::Util qw(looks_like_number);

use CHI;
use Config::Tiny;
use DBI qw(:sql_types);
use File::Find::Rule;
use File::HomeDir qw(my_home);
use File::LibMagic;
use List::MoreUtils qw(first_index);
use Term::Choose qw(choose);
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
    cache_rootdir       => tmpdir(),
    config_file         => catfile( $home, '.table_info.conf' ),
    filter_types        => [ " REGEXP ", " NOT REGEXP ", " = ", " != ", " < ", " > ", " IS NULL ", " IS NOT NULL ", " IN " ], #, " >= ", " <= ", "LIKE", "NOT LIKE"
    aggregate_functions => [ 'AVG(X)', 'COUNT(X)', 'COUNT(*)', 'MAX(X)', 'MIN(X)', 'SUM(X)' ], # group_concat(X), group_concat(X,Y), total(X)
    binary_regex        => qr/(?:blob|binary|image)\z/i,
    binary_string       => 'BNRY',
};

utf8::upgrade( $arg->{binary_string} );
my $gcs = Unicode::GCString->new( $arg->{binary_string} );
my $colwidth = $gcs->columns();
$arg->{binary_length} = $colwidth;

sub help {
    print << 'HELP';

Usage:
    table_watch_SQLite.pl [help or options] [directories to be searched]
    If no directories are passed the home directory is searched for SQLite databases.
    Options with the parenthesis at the end can be used on the command line too.
    This script works with SQLite databases,

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
    Binary filter : Print "BNRY" instead of binary data (printing binary data could break the output).
    Tab           : Set the number of spaces between columns.
    Min-Width     : Set the width the columns should have at least when printed.
    Undef         : Set the string that will be shown on the screen if a table value is undefined.
    Cut col names : Set when column name should be cut.
                    - cut : don't consider length of column names when calculating the column width.
                    - auto: don't cut column names if enough space.
    Thousands sep : Choose the thousands separator.


HELP
}

#------------------------------------------------------------------------#
#------------------------------   options   -----------------------------#
#------------------------------------------------------------------------#

my $opt = {
    chi => {    cache_expire => '7d',
                reset_cache  => 0,
    },
    find => {   max_depth => undef,
    },
    sqlite => { sqlite_unicode             => 1,
                sqlite_see_if_its_a_number => 1,
                sqlite_busy_timeout        => 3_000,
                sqlite_cache_size          => 400_000,
    },
    all => {    limit         => 25_000,
                binary_filter => 1,
                tab           => 2,
                min_width     => 30,
                undef         => '',
                cut_col_names => 'auto',
                kilo_sep      => ',',
    },
};

if ( not eval {
    if ( not -f $arg->{config_file} ) {
        open my $fh, '>', $arg->{config_file} or die $!;
        close $fh or die $!;
    }
    read_config_file( $arg, $opt );

    my $help;
    GetOptions (
        'h|help'        => \$help,
        's|no-cache'    => \$opt->{chi}{reset_cache},
        'm|max-depth:i' => \$opt->{find}{max_depth},
    );

    $opt = options( $arg, $opt ) if $help;
    1 }
) {
    say 'Configfile/Options:';
    print $@;
    choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
}


#------------------------------------------------------------------------#
#------------------   database specific subroutines   -------------------#
#------------------------------------------------------------------------#


sub get_database_handle {
    my ( $opt, $db ) = @_;
    die "\"$db\": $!. Maybe the cached data is not up to date." if not -f $db;
    my $dbh = DBI->connect( "DBI:SQLite:dbname=$db", '', '', {
        RaiseError                  => 1,
        PrintError                  => 0,
        AutoCommit                  => 1,
        sqlite_unicode              => $opt->{$db}{sqlite_unicode}              // $opt->{sqlite}{sqlite_unicode},
        sqlite_see_if_its_a_number  => $opt->{$db}{sqlite_see_if_its_a_number}  // $opt->{sqlite}{sqlite_see_if_its_a_number},
    } ) or die DBI->errstr;
    $dbh->sqlite_busy_timeout( $opt->{$db}{sqlite_busy_timeout} // $opt->{sqlite}{sqlite_busy_timeout} );
    $dbh->do( 'PRAGMA cache_size = ' . ( $opt->{$db}{sqlite_cache_size} // $opt->{sqlite}{sqlite_cache_size} ) );
    return $dbh;
}

sub available_databases {
    my ( $arg, $opt ) = @_;
    my @dirs = @ARGV ? @ARGV : ( $arg->{home} );
    $arg->{cache_key} = join ' ', @dirs, '|', $opt->{find}{max_depth} // '';
    $arg->{cached} = ' (cached)';
    $arg->{cache} = CHI->new (
        namespace => 'table_watch_SQLite',
        driver => 'File',
        root_dir => $arg->{cache_rootdir},
        expires_in => $opt->{chi}{cache_expire},
        expires_variance => 0.25,
    );
    $arg->{cache}->remove( $arg->{cache_key} ) if $opt->{chi}{reset_cache};

    my @databases = $arg->{cache}->compute(
        $arg->{cache_key},
        $opt->{chi}{cache_expire},
        sub {
            $arg->{cached} = '';
            say 'searching...';
            my $flm = File::LibMagic->new();
            my $rule = File::Find::Rule->new();
            $rule->file();
            $rule->maxdepth( $opt->{find}{max_depth} ) if defined $opt->{find}{max_depth};
            $rule->exec( sub{ return $flm->describe_filename( $_ ) =~ /\ASQLite/ } );
            my @databases = $rule->in( @dirs );
            say 'ended searching';
            return @databases;
        }
    );
    return \@databases;
}

sub get_table_names {
    my ( $dbh ) = @_;
    my $tables = $dbh->selectcol_arrayref( "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name" );
    return $tables;
}


#------------------------------------------------------------------------#
#-------------------------------   main   -------------------------------#
#------------------------------------------------------------------------#

my $databases;
if ( not eval {
    $databases = available_databases( $arg, $opt );
    1 }
) {
    say 'Available databases:';
    print $@;
    choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
}

say 'no sqlite-databases found' and exit if not @$databases;

my %lyt = ( layout => 3, clear_screen => 1 );
my $new_db_setting = 0;
my $db;

DATABASES: while ( 1 ) {
    # CHOOSE
    if ( not $new_db_setting ) {
        $db = choose( [ undef, @$databases ], { prompt => 'Choose Database' . $arg->{cached}, %lyt, undef => $arg->{quit} } );
        last DATABASES if not defined $db;
    }
    else {
        $new_db_setting = 0;
    }
    my ( $dbh, $tables );
    if ( not eval {
        $dbh = get_database_handle( $opt, $db );
        $arg->{db_type} = lc $dbh->{Driver}{Name};
        $tables = get_table_names( $dbh );
        1 }
    ) {
        say 'Get database handle and table names:';
        print $@;
        choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
        # remove database from @databases
        next DATABASES;
    }
    my $db_setting = 'database setting';
    my @tables = map { "- $_" } @$tables;
    push @tables, '  sqlite_master', '  sqlite_temp_master' if $arg->{db_type} eq 'sqlite';
    push @tables, "  $db_setting";

    TABLES: while ( 1 ) {
        # CHOOSE
        my $table = choose( [ undef, @tables ], { prompt => 'db: "'. basename( $db ) . '"', %lyt, undef => '  ' . $arg->{back} } );
        last TABLES if not defined $table;
        $table =~ s/\A..//;
        if ( $table eq $db_setting ) {
            $new_db_setting = database_setting( $arg, $opt, $db );
            next DATABASES if $new_db_setting;
            next TABLES;
        }
        my $table_q = $dbh->quote_identifier( $table );
        if ( not eval {

            CUSTOMIZE: while ( 1 ) {
                my ( $total_ref, $col_names, $col_types ) = read_table( $arg, $opt, $dbh, $table );
                last CUSTOMIZE if not defined $total_ref;
                print_loop( $arg, $opt, $dbh, $total_ref, $col_names, $col_types );
            }
            1 }
        ) {
            say 'Print table:';
            print $@;
            choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
        }
    }
}

#------------------------------------------------------------------------#
#---------------------------   subroutines   ----------------------------#
#------------------------------------------------------------------------#


sub print_select {
    my ( $arg, $opt, $table, $chosen_columns, $print ) = @_;
    print GO_TO_TOP_LEFT;
    print CLEAR_EOS;
    my $cols_str = '';
    $cols_str = ' '. join( ', ', @$chosen_columns ) if @$chosen_columns;
    $cols_str = $print->{group_by_cols}             if not @$chosen_columns and $print->{group_by_cols};
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


sub read_table {
    my ( $arg, $opt, $dbh, $table ) = @_;
    $dbh->func( 'regexp', 2, sub { my ( $regex, $string ) = @_; $string //= ''; return $string =~ m/$regex/ism }, 'create_function' ) if $arg->{db_type} eq 'sqlite';
    my $continue = '- OK -';
    my $back = '- reset -';
    my %bol    = ( layout => 1, pad_one_row => 1, undef => $back );
    my %list   = ( layout => 1, pad_one_row => 2, undef => $back );
    my %filter = ( layout => 1, pad_one_row => 1, undef => $back );
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
    my $before_col  = ' ';
    my $between_col = ', ';
    my ( $DISTINCT, $ALL, $ASC, $DESC, $AND, $OR ) = ( " DISTINCT ", " ALL ", " ASC ", " DESC ", " AND ", " OR " );

    CUSTOMIZE: while ( 1 ) {
        print_select( $arg, $opt, $table, $chosen_columns, $print );
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
                    print_select( $arg, $opt, $table, $chosen_columns, $print );
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
                $quote->{distinct_stmt} = '';
                $print->{distinct_stmt} = '';
                DISTINCT: while ( 1 ) {
                    print_select( $arg, $opt, $table, $chosen_columns, $print );
                    # CHOOSE
                    my $select_distinct = choose( [ $continue, $DISTINCT, $ALL ], { prompt => 'Choose: ', %bol } ); # undef
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
            when ( $customize{where} ) {
                my @cols = @$columns;
                my $AND_OR = '';
                $quote->{where_args} = [];
                $quote->{where_stmt} = " WHERE";
                $print->{where_stmt} = " WHERE";
                my $count = 0;
                WHERE: while ( 1 ) {
                    print_select( $arg, $opt, $table, $chosen_columns, $print );
                    # CHOOSE
                    my $col = choose( [ $continue, @cols ], { prompt => 'Choose: ', %list } ); # undef
                    if ( not defined $col ) {
                        $quote->{where_args} = [];
                        $quote->{where_stmt} = '';
                        $print->{where_stmt} = '';
                        last WHERE;
                    }
                    if ( $col eq $continue ) {
                        if ( $count == 0 ) {
                            $quote->{where_args} = [];
                            $quote->{where_stmt} = '';
                            $print->{where_stmt} = '';
                        }
                        last WHERE;
                    }
                    if ( $count >= 1 ) {
                        print_select( $arg, $opt, $table, $chosen_columns, $print );
                        # CHOOSE
                        $AND_OR = choose( [ $AND, $OR ], { prompt => 'Choose:', %bol } ); # undef
                        if ( not defined $AND_OR ) {
                            $quote->{where_args} = [];
                            $quote->{where_stmt} = '';
                            $print->{where_stmt} = '';
                            last WHERE;
                        }
                        $AND_OR =~ s/\A\s+|\s+\z//g;
                        $AND_OR = ' ' . $AND_OR;
                    }
                    $quote->{where_stmt} .= $AND_OR . ' ' . $dbh->quote_identifier( $col );
                    $print->{where_stmt} .= $AND_OR . ' ' .                         $col  ;
                    print_select( $arg, $opt, $table, $chosen_columns, $print );
                    # CHOOSE
                    my $filter_type = choose( [ @{$arg->{filter_types}} ], { prompt => 'Choose:', %filter } ); # undef
                    if ( not defined $filter_type ) {
                        $quote->{where_args} = [];
                        $quote->{where_stmt} = '';
                        $print->{where_stmt} = '';
                        last WHERE;
                    }
                    $filter_type =~ s/\A\s+|\s+\z//g;
                    $quote->{where_stmt} .= ' ' . $filter_type;
                    $print->{where_stmt} .= ' ' . $filter_type;
                    if ( $filter_type =~ /NULL\z/ ) {
                        # do nothing
                    }
                    elsif ( $filter_type eq 'IN' ) {
                        $arg->{col_sep} = $before_col;
                        $quote->{where_stmt} .= '(';
                        $print->{where_stmt} .= '(';

                        IN: while ( 1 ) {
                            print_select( $arg, $opt, $table, $chosen_columns, $print );
                            # CHOOSE
                            my $col = choose( [ $continue, @cols ], { prompt => 'Choose: ', %list } ); # undef
                            if ( not defined $col ) {
                                $quote->{where_args} = [];
                                $quote->{where_stmt} = '';
                                $print->{where_stmt} = '';
                                last WHERE;
                            }
                            if ( $col eq $continue ) {
                                $quote->{where_stmt} .= ' )';
                                $print->{where_stmt} .= ' )';
                                if ( $arg->{col_sep} eq $before_col ) {
                                    $quote->{where_args} = [];
                                    $quote->{where_stmt} = '';
                                    $print->{where_stmt} = '';
                                }
                                last WHERE;
                            }
                            $quote->{where_stmt} .= $arg->{col_sep} . $dbh->quote_identifier( $col );
                            $print->{where_stmt} .= $arg->{col_sep} .                         $col  ;
                            $arg->{col_sep} = $between_col;
                        }
                    }
                    else {
                        print_select( $arg, $opt, $table, $chosen_columns, $print );
                        print "arg: ";
                        my $pattern = <STDIN>;
                        chomp $pattern;
                        $pattern= qr/$pattern/i if $arg->{db_type} eq 'sqlite' and $filter_type eq "REGEXP";
                        $quote->{where_stmt} .= " ?";
                        $print->{where_stmt} .= " $pattern";
                        push @{$quote->{where_args}}, $pattern;
                    }
                    $count++;
                }
            }
            when( $customize{aggregate} ) {
                my @cols = @$columns;
                $arg->{col_sep} = $before_col;
                @aliases                 = ();
                $quote->{aggregate_stmt} = '';
                $print->{aggregate_stmt} = '';
                AGGREGATE: while ( 1 ) {
                    print_select( $arg, $opt, $table, $chosen_columns, $print );
                    # CHOOSE
                    my $func = choose( [ $continue, @{$arg->{aggregate_functions}} ], { prompt => 'Choose:', %list } ); # undef
                    if ( not defined $func ) {
                        @aliases                 = ();
                        $quote->{aggregate_stmt} = '';
                        $print->{aggregate_stmt} = '';
                        last AGGREGATE;
                    }
                    if ( $func eq $continue ) {
                        last AGGREGATE;
                    }
                    my $col;
                    if ( $func =~ /\Acount\s*\(\s*\*\s*\)\z/i ) {
                        $col = '*';
                    }
                    $func =~ s/\s*\(\s*\S\s*\)\z//;
                    $quote->{aggregate_stmt} .= $arg->{col_sep} . $func . '(';
                    $print->{aggregate_stmt} .= $arg->{col_sep} . $func . '(';
                    if ( not defined $col ) {
                        print_select( $arg, $opt, $table, $chosen_columns, $print );
                        # CHOOSE
                        $col = choose( [ @cols ], { prompt => 'Choose:', %list } ); # undef
                        if ( not defined $col ) {
                            @aliases                 = ();
                            $quote->{aggregate_stmt} = '';
                            $print->{aggregate_stmt} = '';
                            last AGGREGATE;
                        }
                    }
                    my $alias = $func . '_' . $col;
                    $quote->{aggregate_stmt} .=                         $col   . ') AS ' . $dbh->quote( $alias ) if $col eq '*';
                    $quote->{aggregate_stmt} .= $dbh->quote_identifier( $col ) . ') AS ' . $dbh->quote( $alias ) if $col ne '*';
                    $print->{aggregate_stmt} .=                         $col   . ') AS ' .              $alias  ;
                    push @aliases, $alias;
                    $arg->{col_sep} = $between_col;
                }
            }
            when( $customize{group_by} ) {
                my @cols = @$columns;
                $arg->{col_sep} = $before_col;
                $quote->{group_by_stmt} = " GROUP BY";
                $print->{group_by_stmt} = " GROUP BY";
                GROUP_BY: while ( 1 ) {
                    print_select( $arg, $opt, $table, $chosen_columns, $print );
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
                        if ( $arg->{col_sep} eq $before_col ) {
                            $quote->{group_by_stmt} = '';
                            $print->{group_by_stmt} = '';
                            $quote->{group_by_cols} = '';
                            $print->{group_by_cols} = '';
                        }
                        last GROUP_BY;
                    }
                    if ( not @$chosen_columns ) {
                        $quote->{group_by_cols} .= $arg->{col_sep} . $dbh->quote_identifier( $col );
                        $print->{group_by_cols} .= $arg->{col_sep} .                         $col  ;
                    }
                    $quote->{group_by_stmt} .= $arg->{col_sep} . $dbh->quote_identifier( $col );
                    $print->{group_by_stmt} .= $arg->{col_sep} .                         $col  ;
                    $arg->{col_sep} = $between_col;
                }
            }
            when( $customize{having} ) {
                my @cols = @$columns;
                my $AND_OR = '';
                $quote->{having_args} = [];
                $quote->{having_stmt} = " HAVING";
                $print->{having_stmt} = " HAVING";
                my $count = 0;
                HAVING: while ( 1 ) {
                    print_select( $arg, $opt, $table, $chosen_columns, $print );
                    # CHOOSE
                    my $func = choose( [ $continue, @{$arg->{aggregate_functions}} ], { prompt => 'Choose:', %list } ); # undef
                    if ( not defined $func ) {
                        $quote->{having_args} = [];
                        $quote->{having_stmt} = '';
                        $print->{having_stmt} = '';
                        last HAVING;
                    }
                    if ( $func eq $continue ) {
                        if ( $count == 0 ) {
                            $quote->{having_args} = [];
                            $quote->{having_stmt} = '';
                            $print->{having_stmt} = '';
                        }
                        last HAVING;
                    }
                    if ( $count >= 1 ) {
                        print_select( $arg, $opt, $table, $chosen_columns, $print );
                        # CHOOSE
                        $AND_OR = choose( [ $AND, $OR ], { prompt => 'Choose:', %bol } ); # undef
                        if ( not defined $AND_OR ) {
                            last HAVING;
                        }
                        $AND_OR =~ s/\A\s+|\s+\z//g;
                        $AND_OR = ' ' . $AND_OR;
                    }
                    my $col;
                    if ( $func =~ /\Acount\s*\(\s*\*\s*\)\z/i ) {
                        $col = '*';
                    }
                    $func =~ s/\s*\(\s*\S\s*\)\z//;
                    $quote->{having_stmt} .= $AND_OR . ' ' . $func . '(';
                    $print->{having_stmt} .= $AND_OR . ' ' . $func . '(';
                    if ( not defined $col ) {
                        print_select( $arg, $opt, $table, $chosen_columns, $print );
                        # CHOOSE
                        $col = choose( [ @cols ], { prompt => 'Choose:', %list } ); # undef
                        if ( not defined $col ) {
                            $quote->{having_args} = [];
                            $quote->{having_stmt} = '';
                            $print->{having_stmt} = '';
                            last HAVING;
                        }
                    }
                    $quote->{having_stmt} .=                         $col   . ')' if $col eq '*';
                    $quote->{having_stmt} .= $dbh->quote_identifier( $col ) . ')' if $col ne '*';
                    $print->{having_stmt} .=                         $col   . ')';
                    print_select( $arg, $opt, $table, $chosen_columns, $print );
                    # CHOOSE
                    my $filter_type = choose( [ @{$arg->{filter_types}} ], { prompt => 'Choose:', %filter } ); # undef
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
                    if ( $filter_type =~ /NULL\z/ ) {
                        # do nothing
                    }
                    elsif ( $filter_type eq 'IN' ) {
                        $arg->{col_sep} = $before_col;
                        $quote->{having_stmt} .= '(';
                        $print->{having_stmt} .= '(';

                        IN: while ( 1 ) {
                            print_select( $arg, $opt, $table, $chosen_columns, $print );
                            # CHOOSE
                            my $col = choose( [ $continue, @cols ], { prompt => 'Choose: ', %list } ); # undef
                            if ( not defined $col ) {
                                $quote->{having_args} = [];
                                $quote->{having_stmt} = '';
                                $print->{having_stmt} = '';
                                last WHERE;
                            }
                            if ( $col eq $continue ) {
                                $quote->{having_stmt} .= ' )';
                                $print->{having_stmt} .= ' )';
                                if ( $arg->{col_sep} eq $before_col ) {
                                    $quote->{having_args} = [];
                                    $quote->{having_stmt} = '';
                                    $print->{having_stmt} = '';
                                }
                                last WHERE;
                            }
                            $quote->{having_stmt} .= $arg->{col_sep} . $dbh->quote_identifier( $col );
                            $print->{having_stmt} .= $arg->{col_sep} .                         $col  ;
                            $arg->{col_sep} = $between_col;
                        }
                    }
                    else {
                        print_select( $arg, $opt, $table, $chosen_columns, $print );
                        print "arg: ";
                        my $pattern = <STDIN>;
                        chomp $pattern;
                        $pattern= qr/$pattern/i if $arg->{db_type} eq 'sqlite' and $filter_type eq "REGEXP";
                        $quote->{having_stmt} .= " ?";
                        $print->{having_stmt} .= " $pattern";
                        push @{$quote->{having_args}}, $pattern;
                    }
                    $count++;
                }
            }
            when( $customize{order_by} ) {
                my @cols = ( @$columns, @aliases );
                $arg->{col_sep} = $before_col;
                $quote->{order_by_stmt} = " ORDER BY";
                $print->{order_by_stmt} = " ORDER BY";
                ORDER_BY: while ( 1 ) {
                    print_select( $arg, $opt, $table, $chosen_columns, $print );
                    # CHOOSE
                    my $col = choose( [ $continue, @cols ], { prompt => 'Choose: ', %list } ); # undef
                    if ( not defined $col ) {
                        $quote->{order_by_stmt} = '';
                        $print->{order_by_stmt} = '';
                        last ORDER_BY;
                    }
                    if ( $col eq $continue ) {
                        if ( $arg->{col_sep} eq $before_col ) {
                            $quote->{order_by_stmt} = '';
                            $print->{order_by_stmt} = '';
                        }
                        last ORDER_BY;
                    }
                    $quote->{order_by_stmt} .= $arg->{col_sep} . $dbh->quote_identifier( $col );
                    $print->{order_by_stmt} .= $arg->{col_sep} .                         $col  ;
                    print_select( $arg, $opt, $table, $chosen_columns, $print );
                    # CHOOSE
                    my $direction = choose( [ $ASC, $DESC ], { prompt => 'Choose:', %bol } ); # undef
                    if ( not defined $direction ){
                        $quote->{order_by_stmt} = '';
                        $print->{order_by_stmt} = '';
                        last ORDER_BY;
                    }
                    $direction =~ s/\A\s+|\s+\z//g;
                    $quote->{order_by_stmt} .= ' ' . $direction;
                    $print->{order_by_stmt} .= ' ' . $direction;
                    $arg->{col_sep} = $between_col;
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

                my $sth = $dbh->prepare( $select );
                $sth->execute( @arguments );
                my $col_names = $sth->{NAME};
                my $col_types = $sth->{TYPE};
                my $total_ref = $sth->fetchall_arrayref;
                print GO_TO_TOP_LEFT;
                print CLEAR_EOS;
                return $total_ref, $col_names, $col_types;
            }
            default {
                die "$custom: no such value in the hash \%customize";
            }
        }
    }
    return;
}



sub print_loop {
    my ( $arg, $opt, $dbh, $total_ref, $col_names, $col_types ) = @_;
    my $offset = 0;
    my $begin = 0;
    my $end = $opt->{all}{limit} - 1;
    my @choices;
    my $rows = @$total_ref;
    if ( $rows > $opt->{all}{limit} ) {
        my $lr = length $rows;
        push @choices, sprintf "%${lr}d - %${lr}d", $begin, $end;
        $rows -= $opt->{all}{limit};
        while ( $rows > 0 ) {
            $begin += $opt->{all}{limit};
            $end   += ( $rows > $opt->{all}{limit} ) ? $opt->{all}{limit} : $rows;
            push @choices, sprintf "%${lr}d - %${lr}d", $begin, $end;
            $rows -= $opt->{all}{limit};
        }
    }

    PRINT: while ( 1 ) {
        if ( @choices ) {
            # CHOOSE
            my $choice = choose( [ undef, @choices ], { layout => 3, undef => $arg->{back} } );
            last PRINT if not defined $choice;
            $offset = ( split /\s*-\s*/, $choice )[0];
            $offset =~ s/\A\s+//;
            print_table( $opt, [ @{$total_ref}[ $offset .. $offset + $opt->{all}{limit} - 1 ] ], $col_names, $col_types );
        }
        else {
            print_table( $opt, $total_ref, $col_names, $col_types );
            last PRINT;
        }
    }
}


sub is_binary_data {
    my $data = shift;
    return $data =~ /\x00/;
}


sub calc_widths {
    my ( $opt, $ref, $col_types, $maxcols ) = @_;
    my ( $max_head, $max, $not_a_number );
    my $count = 0;
    say 'Computing: ...';
    for my $row ( @$ref ) {
        $count++;
        for my $i ( 0 .. $#$row ) {
            $max->[$i] ||= 1;
            next if not defined $row->[$i];
            if ( $count == 1 ) { # column name
                $row->[$i] =~ s/\p{Space}/ /g;
                $row->[$i] =~ s/\p{Cntrl}//g;
                utf8::upgrade( $row->[$i] );
                my $gcstring = Unicode::GCString->new( $row->[$i] );
                $max_head->[$i] = $gcstring->columns();
            }
            else { # normal row
                if ( $opt->{all}{binary_filter} and ( $col_types->[$i] =~ $arg->{binary_regex} or is_binary_data( substr $row->[$i], 0, 200 ) ) ) {
                    $row->[$i] = $arg->{binary_string};
                    $max->[$i] = $arg->{binary_length} if $arg->{binary_length} > $max->[$i];
                }
                else {
                    $row->[$i] =~ s/\p{Space}/ /g;
                    $row->[$i] =~ s/\p{Cntrl}//g;
                    utf8::upgrade( $row->[$i] );
                    my $gcstring = Unicode::GCString->new( $row->[$i] );
                    $max->[$i] = $gcstring->columns() if $gcstring->columns() > $max->[$i];
                }
                ++$not_a_number->[$i] if not looks_like_number $row->[$i];
            }
        }
    }
    if ( $opt->{all}{cut_col_names} eq 'auto' ) {
        if ( sum( @$max ) + $opt->{all}{tab} * ( @$max - 1 ) < $maxcols ) {
            MAX: while ( 1 ) {
                my $count = 0;
                my $sum = sum( @$max ) + $opt->{all}{tab} * ( @$max - 1 );
                for my $i ( 0 .. $#$max_head ) {
                    if ( $max_head->[$i] > $max->[$i] ) {
                        $max->[$i]++;
                        $count++;
                        last MAX if ( $sum + $count ) == $maxcols;
                    }
                }
                last MAX if $count == 0;
            }
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
    my ( $max, $not_a_number ) = calc_widths( $opt, $ref, $col_types, $maxcols );
    return if not defined $max or not @$max;
    my $sum = sum( @$max ) + $opt->{all}{tab} * @$max;
    $sum -= $opt->{all}{tab};
    my @max_tmp = @$max;
    my $percent = 0;
    my $minimum_with = $opt->{all}{min_width};
    while ( $sum > $maxcols ) {
        $percent += 0.5;
        if ( $percent >= 100 ) {
            say 'Terminal window is not wide enough to print this table.';
            choose( [ 'Press ENTER to show the column names' ], { prompt => 0 } );
            choose( $ref->[0], { prompt => 'Column names (close with ENTER):', layout => 0 } );
            return;
        }
        my $count = 0;
        for my $i ( 0 .. $#max_tmp ) {
            next if $minimum_with >= $max_tmp[$i];
            if ( $minimum_with >= minus_x_percent( $max_tmp[$i], $percent ) ) {
                $max_tmp[$i] = $minimum_with;
            }
            else {
                $max_tmp[$i] = minus_x_percent( $max_tmp[$i], $percent );
            }
            $count++;
            last if $sum <= $maxcols;
        }
        $minimum_with-- if $count == 0 and $minimum_with > 1;
        $sum = sum( @max_tmp ) + $opt->{all}{tab} * ( @max_tmp - 1 );
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
    my ( $opt, $ref, $col_names, $col_types ) = @_;
    my ( $maxcols, $maxrows ) = GetTerminalSize( *STDOUT );
    return if not defined $ref;
    unshift @$ref, $col_names if defined $col_names;
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
            my $word = $row->[$i] // $opt->{all}{undef};
            my $right_justify = $not_a_number->[$i] ? 0 : 1;
            $string .= unicode_sprintf(
                        $max->[$i],
                        $word,
                        $right_justify,
                        $maxcols + int( $maxcols / 10 )
            );
            $string .= ' ' x $opt->{all}{tab} if not $i == $#$max;
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
    choose( \@list, { prompt => 0, layout => 3, length_longest => sum( @$max, $opt->{all}{tab} * $#{$max} ), limit => $opt->{all}{limit} + 1 } );
    return;
}


sub unicode_sprintf {
    my ( $length, $unicode, $right_justify, $max_length ) = @_;
    utf8::upgrade( $unicode );
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
        tab             => '- Tab',
        min_width       => '- Min-Width',
        binary_filter   => '- Binary filter',
        undef           => '- Undef',
        cut_col_names   => '- Cut col names',
        kilo_sep        => '- Thousands sep',
    };
    my $keys;
    $keys->{all} = [ qw( limit binary_filter min_width tab kilo_sep undef cut_col_names ) ];
    $keys->{chi} = [ qw( cache_expire reset_cache ) ];
    $keys->{find} = [ qw( max_depth ) ];
    my $change;
    my @keys_choose = ( qw( cache_expire reset_cache max_depth limit binary_filter min_width tab kilo_sep undef cut_col_names ) );
    OPTIONS: while ( 1 ) {
        my ( $exit, $help, $show_settings, $continue ) = ( '  EXIT', '  HELP', '  SHOW SETTINGS', '  CONTINUE' );
        # CHOOSE
        my $option = choose( [ $exit, $help, @{$oh}{@keys_choose}, $show_settings, undef ], { undef => $continue, layout => 3, clear_screen => 1 } );
        my $back = '<<';
        my %number_lyt = ( layout => 1, vertical => 0, right_justify => 1, undef => $back );
        my %bol = ( undef => " $back ", pad_one_row => 1 );

        given ( $option ) {
            when ( not defined ) { last OPTIONS; }
            when ( $exit ) { exit() }
            when ( $help ) { help(); choose( [ '  Close with ENTER  ' ], { prompt => 0 } ) }
            when ( $show_settings ) {
                my @choices;
                for my $section ( 'find', 'chi', 'all' ) {
                    for my $key ( sort @{$keys->{$section}} ) {
                        my $value;
                        if ( not defined $opt->{$section}{$key} ) {
                            $value = 'undef';
                        }
                        elsif ( $section eq 'all' and $key eq 'undef' and $opt->{$section}{$key} eq '' ) {
                            $value = "''";
                        }
                        elsif ( $section eq 'all' and $key eq 'limit' ) {
                            $value = $opt->{$section}{$key};
                            $value =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{all}{kilo_sep}/g;
                        }
                        elsif ( $section eq 'all' and $key eq 'kilo_sep' ) {
                            $value = 'space " "'      if $opt->{$section}{$key} eq ' ';
                            $value = 'none'           if $opt->{$section}{$key} eq '';
                            $value = 'underscore "_"' if $opt->{$section}{$key} eq '_';
                            $value = 'full stop "."'  if $opt->{$section}{$key} eq '.';
                            $value = 'comma ","'      if $opt->{$section}{$key} eq ',';
                        }
                        elsif ( $section eq 'chi' and $key eq 'reset_cache' or  $section eq 'all' and $key eq 'binary_filter' ) {
                            $value = $opt->{$section}{$key} ? 'true' : 'false';
                        }
                        else {
                            $value = $opt->{$section}{$key};
                        }
                        my $name = $oh->{$key};
                        $name =~ s/\A..//;
                        push @choices, sprintf "%-16s : %s\n", "  $name", $value;
                    }
                }
                choose( [ @choices ], { prompt => 'Close with ENTER', layout => 3 } );
            }
            when ( $oh->{cache_expire} ) {
                # CHOOSE
                my $number = choose( [ undef, 0 .. 99 ], { prompt => 'Days until data expires (' . $opt->{chi}{cache_expire} . '):', %number_lyt } );
                break if not defined $number;
                $opt->{chi}{cache_expire} = $number.'d';
                $change++;
            }
            when ( $oh->{reset_cache} ) {
                my ( $true, $false ) = ( ' YES ', ' NO ' );
                # CHOOSE
                my $choice = choose( [ undef, $true, $false ], { prompt => 'Reset cache (' . $opt->{chi}{reset_cache} . '):', %bol } );
                break if not defined $choice;
                $opt->{chi}{reset_cache} = ( $choice eq $true ) ? 1 : 0;
                $change++;
            }
            when ( $oh->{max_depth} ) {
                # CHOOSE
                my $number = choose( [ undef, '--', 0 .. 99 ], { prompt => 'Levels to descend at most (' . ( $opt->{find}{max_depth} // 'undef' ) . '):', %number_lyt } );
                break if not defined $number;
                if ( $number eq '--' ) {
                    $opt->{find}{max_depth} = undef;
                }
                else {
                    $opt->{find}{max_depth} = $number;
                }
                $change++;
            }
            when ( $oh->{tab} ) {
                # CHOOSE
                my $number = choose( [ undef, 0 .. 99 ], { prompt => 'Tab width (' . $opt->{all}{tab} . '):',  %number_lyt } );
                break if not defined $number;
                $opt->{all}{tab} = $number;
                $change++;
            }
            when ( $oh->{kilo_sep} ) {
                my %sep_h;
                my ( $comma, $full_stop, $underscore, $space, $none ) = ( ' comma ', ' full stop ', ' underscore ', ' space ', ' none ' );
                @sep_h{ $comma, $full_stop, $space, $none } = ( ',', '.', '_', ' ', '' );
                # CHOOSE
                my $sep = choose( [ undef, $comma, $full_stop, $underscore, $space, $none ], { prompt => 'Thousands separator (' . $opt->{all}{kilo_sep} . '):',  %bol } );
                break if not defined $sep;
                $opt->{all}{kilo_sep} = $sep_h{$sep};
                $change++;
            }
            when ( $oh->{min_width} ) {
                # CHOOSE
                my $number = choose( [ undef, 0 .. 99 ], { prompt => 'Minimum Column width (' . $opt->{all}{min_width} . '):',  %number_lyt } );
                break if not defined $number;
                $opt->{all}{min_width} = $number;
                $change++;
            }
            when ( $oh->{limit} ) {
                # CHOOSE
                my $limit = choose_a_number( $arg, $opt, 7, 'limit', 'all', 'limit' );
                break if not defined $limit;
                $opt->{all}{limit} = $limit;
                $change++;
            }
            when ( $oh->{binary_filter} ) {
                my ( $true, $false ) = ( ' Enable Binary Filter ', ' Dissable Binary Filter ' );
                # CHOOSE
                my $choice = choose( [ undef, $true, $false ], { prompt => 'Binary Filter (' . ( $opt->{all}{binary_filter} ? 'Enabled' : 'Disables' ) . '):', %bol } );
                break if not defined $choice;
                $opt->{all}{binary_filter} = ( $choice eq $true ) ? 1 : 0;
                $change++;
            }
            when ( $oh->{undef} ) {
                print 'Choose a replacement-string for undefined table vales ("' , $opt->{all}{undef} . '"): ';
                my $undef = <STDIN>;
                chomp $undef;
                break if not $undef;
                $opt->{all}{undef} = $undef;
                $change++;
            }
            when ( $oh->{cut_col_names} ) {
                my $auto = 'auto';
                my $cut  = 'cut';
                # CHOOSE
                my $choice = choose( [ undef, $auto, $cut ], { prompt => 'Column names (' . $opt->{all}{cut_col_names} . '):', undef => $back } );
                break if not defined $cut;
                $opt->{all}{cut_col_names} = $choice;
                $change++;
            }
            default { die "$option: no such value in the hash \%oh"; }
        }
    }
    if ( $change ) {
        my ( $true, $false ) = ( ' Make changes permanent ', ' Use changes only this time ' );
        # CHOOSE
        my $permanent = choose( [ $false, $true ], { prompt => 'Modifications:', layout => 3, pad_one_row => 1 } );
        exit if not defined $permanent;
        if ( $permanent eq $true ) {
            write_config_file( $arg, $opt );
            # CHOOSE
            my $continue = choose( [ ' CONTINUE ', undef ], { prompt => 0, layout => 1, undef => ' QUIT ', pad_one_row => 1 } );
            exit() if not defined $continue;
        }
    }
    return $opt;
}


sub database_setting {
    my ( $arg, $opt, $db ) = @_;
    $arg->{sqlite} = [ qw( sqlite_unicode sqlite_see_if_its_a_number sqlite_busy_timeout sqlite_cache_size ) ];
    my $oh = {
        sqlite_unicode                  => '- Unicode',
        sqlite_see_if_its_a_number      => '- See if its a number',
        sqlite_busy_timeout             => '- Busy timeout (ms)',
        sqlite_cache_size               => '- Cache size (kb)',
    };
    my $section = $db;
    my $change;
    my $option;
    my $confirm = 'CONFIRM';
    my ( $true, $false ) = ( ' YES ', ' NO ' );

    DB_OPTIONS: while ( 1 ) {
        # CHOOSE
        $option = choose( [ undef, @{$oh}{@{$arg->{$arg->{db_type}}}}, $confirm ], { undef => $arg->{back}, layout => 3, clear_screen => 1 } );
        last DB_OPTIONS if not defined $option;
        last DB_OPTIONS if $option eq $confirm;
        my $back = '<<';
        my %number_lyt = ( layout => 1, vertical => 0, right_justify => 1, undef => $back );
        my %bol = ( undef => " $back ", pad_one_row => 1 );
        given ( $option ) {
            when ( $oh->{sqlite_unicode} ) {
                # CHOOSE
                my $unicode = $opt->{$section}{sqlite_unicode} // $opt->{$arg->{db_type}}{sqlite_unicode};
                my $choice = choose( [ undef, $true, $false ], { prompt => 'Unicode (' . ( $unicode ? 'YES' : 'NO' ) . '):', %bol } );
                break if not defined $choice;
                $opt->{$section}{sqlite_unicode} = ( $choice eq $true ) ? 1 : 0;
                $change++;
            }
            when ( $oh->{sqlite_see_if_its_a_number} ) {
                # CHOOSE
                my $see_if_its_a_number = $opt->{$section}{sqlite_see_if_its_a_number} // $opt->{$arg->{db_type}}{sqlite_see_if_its_a_number};
                my $choice = choose( [ undef, $true, $false ], { prompt => 'See if its a number (' . ( $see_if_its_a_number ? 'YES' : 'NO' ) . '):', %bol } );
                break if not defined $choice;
                $opt->{$section}{sqlite_see_if_its_a_number} = ( $choice eq $true ) ? 1 : 0;
                $change++;
            }
            when ( $oh->{sqlite_busy_timeout} ) {
                # CHOOSE
                my $timeout = choose_a_number( $arg, $opt, 6, 'Busy timeout (ms)', $section, 'sqlite_busy_timeout' );
                break if not defined $timeout;
                $opt->{$section}{sqlite_busy_timeout} = $timeout;
                $change++;
            }
            when ( $oh->{sqlite_cache_size} ) {
                # CHOOSE
                my $cache_size = choose_a_number( $arg, $opt, 8, 'Cache size (kb)', $section, 'sqlite_cache_size' );
                break if not defined $cache_size;
                $opt->{$section}{sqlite_cache_size} = $cache_size;
                $change++;
            }
            default { die "$option: no such value in the hash \%oh"; }
        }
    }
    if ( defined $option and $option eq $confirm and $change ) {
        write_config_file( $arg, $opt );
        return 1;
    }
    return 0;
}


sub write_config_file {
    my ( $arg, $opt ) = @_;
    my $ini = Config::Tiny->new;
    for my $section ( keys %$opt ) {
        for my $key ( keys %{$opt->{$section}} ) {
            if ( not defined $opt->{$section}{$key} ) {
                $ini->{$section}{$key} = '';
            }
            elsif ( $opt->{$section}{$key} eq '' ) {
                $ini->{$section}{$key} = "''";
            }
            else {
                $ini->{$section}{$key} = $opt->{$section}{$key};
            }
        }
    }
    $ini->write( $arg->{config_file} ) or die Config::Tiny->errstr;
}


sub read_config_file {
    my ( $arg, $opt ) = @_;
    my $ini = Config::Tiny->new;
    $ini = Config::Tiny->read( $arg->{config_file} ) or die Config::Tiny->errstr;;
    for my $section ( keys %$ini ) {
        for my $key ( keys %{$ini->{$section}} ) {
            if ( $ini->{$section}{$key} eq '' ) {
                $ini->{$section}{$key} = undef;
            }
            elsif ( $ini->{$section}{$key} eq "''" ) {
                $ini->{$section}{$key} = '';
            }
            else {
                $opt->{$section}{$key} = $ini->{$section}{$key};
            }
        }
    }
}


sub choose_a_number {
    my ( $arg, $opt, $digits, $name, $opt_section, $opt_key ) = @_;
    my %hash;
    my $number;
    while ( 1 ) {
        my $longest = $digits + int( ( $digits - 1 ) / 3 );
        my @list;
        for my $di ( 0 .. $digits - 1 ) {
            my $begin = 1 . '0' x $di;
            $begin =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{all}{kilo_sep}/g;
            ( my $end = $begin ) =~ s/\A1/9/;
            unshift @list, sprintf " %*s  -  %*s", $longest, $begin, $longest, $end;
        }
        my $confirm;
        if ( $number ) {
            $confirm = "confirm result: $number";
            push @list, $confirm;
        }
        my $number_now = $opt->{$opt_section}{$opt_key} // $opt->{$arg->{db_type}}{$opt_key};
        $number_now =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{all}{kilo_sep}/g;
        # CHOOSE
        my $choice = choose( [ undef, @list ], { prompt => 'Compose new "' . $name . '" (' . $number_now . '):', layout => 3, right_justify => 1, undef => $arg->{back} . ' ' x ( $longest * 2 + 1 ) } );
        return if not defined $choice;

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
            $c =~ s/\Q$opt->{all}{kilo_sep}\E//g;
            $hash{length $c} = $c;
        }
        $number = sum( @hash{keys %hash} );
        $number =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{all}{kilo_sep}/g;
    }
    $number =~ s/\Q$opt->{all}{kilo_sep}\E//g;
    return $number;
}



__DATA__
