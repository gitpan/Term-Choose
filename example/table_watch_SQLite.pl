#!/usr/bin/env perl
use warnings;
use 5.10.1;
use utf8;
binmode STDOUT, ':encoding(utf-8)';
binmode STDIN,  ':encoding(utf-8)';

#use warnings FATAL => qw(all);
#use Data::Dumper;
# Version 1.020

use File::Basename;
use File::Spec::Functions qw(catfile catdir tmpdir);
use Getopt::Long qw(GetOptions :config bundling);
use IO::File;
use List::Util qw(sum);
use Scalar::Util qw(looks_like_number);
use Term::ReadLine;

use DBI;
use JSON;
use File::Find::Rule;
use File::HomeDir qw(my_home);
use List::MoreUtils qw(any none first_index pairwise);
use Term::Choose qw(choose);
use Term::ProgressBar;
use Term::ReadKey qw(GetTerminalSize ReadLine ReadMode);
use Term::ReadPassword;
use Text::LineFold;
use Unicode::GCString;

my $term = Term::ReadLine->new( 'table_watch', *STDIN, *STDOUT );
$term->ornaments( 0 );

use constant {
    GO_TO_TOP_LEFT  => "\e[1;1H",
    CLEAR_EOS       => "\e[0J",
};

my $home = File::HomeDir->my_home();
my $config_dir = '.table_watch_conf';
if ( $home ) {
    $config_dir = catdir( $home, $config_dir );
}
else {
    say "Could not find home directory!";
    exit;
}

mkdir $config_dir or die $! if not -d $config_dir;

my $arg = {
    back                => 'BACK',
    ok                  => '- OK -',
    _back               => '  BACK',
    _confirm            => '  CONFIRM',
    _continue           => '  CONTINUE',
    _info               => '  INFO',
    _reset              => '  RESET',
    home                => $home,
    config_file         => catfile( $config_dir, '.tw_config.json' ),
    db_cache_file       => catfile( $config_dir, '.tw_cache_db_search.json' ),
    cached              => '',
    binary_regex        => qr/(?:blob|binary|image)\z/i,
    aggregate_functions => [ "AVG(X)", "COUNT(X)", "COUNT(*)", "MAX(X)", "MIN(X)", "SUM(X)" ],
    filter_types        => [ "REGEXP", "NOT REGEXP", " = ", " != ", " < ", " > ", "IS NULL", "IS NOT NULL", "IN" ], # "NOT IN" "LIKE" "NOT LIKE"
    binary_string       => 'BNRY',
    available_db_types  => [ 'sqlite', 'mysql' ],
    db_user             => undef,
    db_passwd           => undef,
};

utf8::upgrade( $arg->{binary_string} );
my $gcs = Unicode::GCString->new( $arg->{binary_string} );
my $colwidth = $gcs->columns();
$arg->{binary_length} = $colwidth;


sub help {
    print << 'HELP';

    Search and read SQLite/MySQL databases.

Usage:
    table_watch_SQLite.pl [-h|--help or -s|--search ] [directories to be searched (SQLite)]
    If no directories are passed the home directory is searched for SQLite databases.
    "q" key goes back.

Options:
    Help            : Show this Info.
    Settings        : Show settings.
    New search      : Search SQLite databases instead of using cached data. (-s|--search)
    Limit           : Set the maximum number of table rows printed in one time.
    Binary filter   : Print "BNRY" instead of binary data (printing binary data could break the output).
    Tab             : Set the number of spaces between columns.
    Min-Width       : Set the width the columns should have at least when printed.
    Undef           : Set the string that will be shown on the screen if a table value is undefined.
    Thousands sep   : Choose the thousands separator.
    Keep statement  : Set the deault value: Lk0 or Lk1.

    Lk0: Reset the SQL-statement after each "PrintTable".
    Lk1: Reset the SQL-statement only when a table is selected.
    To reset a "sub-statement": enter in the "sub-statement" (e.g WHERE) and choose '- OK -'.
    REGEXP: for case sensitivity prefix pattern with (?-i) (SQLite only).


HELP
}


#----------------------------------------------------------------------------------------------------#
#-- 111 -------------------------------------   options   -------------------------------------------#
#----------------------------------------------------------------------------------------------------#


use constant {
    v    => 0,
    chs  => 1,
};

my $opt = {
    db_search => {
        reset_cache      => [ 0, '- New search' ],
    },
    all => {
        kilo_sep      => [ ',', '- Thousands sep' ],
        binary_filter => [ 1, '- Binary filter' ],
        limit         => [ 50_000, '- Limit' ],
        min_width     => [ 30, '- Min-Width' ],
        tab           => [ 2, '- Tab' ],
        undef         => [ '', '- Undef' ],
        lock_stmt     => [ 0, '- Keep statement' ],
    },
    sqlite => {
        unicode             => [ 1, '- Unicode' ],
        see_if_its_a_number => [ 1, '- See if its a number' ],
        busy_timeout        => [ 3_000, '- Busy timeout (ms)' ],
        cache_size          => [ 500_000, '- Cache size (kb)' ],
    },
    mysql => {
        enable_utf8         => [ 1, '- Enable utf8' ],
        connect_timeout     => [ 4, '- Connect timeout' ],
        bind_type_guessing  => [ 1, '- Bind type guessing' ],
    },
};

$arg->{option_sections} = [ qw( db_search all ) ];
$arg->{all}{keys}       = [ qw( kilo_sep binary_filter limit min_width tab undef lock_stmt ) ];
$arg->{db_search}{keys} = [ qw( reset_cache ) ];

for my $section ( @{$arg->{option_sections}} ) {
    if ( join( ' ', sort keys %{$opt->{$section}} ) ne join( ' ', sort @{$arg->{$section}{keys}} ) ) {
        say join( ' ', sort keys %{$opt->{$section}} );
        say join( ' ', sort      @{$arg->{$section}{keys}} );
        die;
    }
}


if ( not eval {
    $opt = read_config_file( $arg->{config_file}, $opt );
    my $help;
    GetOptions (
        'h|help'        => \$help,
        's|search'      => \$opt->{db_search}{reset_cache}[v],
    );
    $opt = options( $arg, $opt ) if $help;
    1 }
) {
    say 'Configfile/Options:';
    print $@;
    choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
}


#----------------------------------------------------------------------------------------------------#
#-- 222 -------------------------   database specific subroutines   ---------------------------------#
#----------------------------------------------------------------------------------------------------#


sub get_database_handle {
    my ( $arg, $opt, $database ) = @_;
    my $dbh;
    if ( $arg->{db_type} eq 'sqlite' ) {
        die "\"$database\": $!. Maybe the cached data is not up to date." if not -f $database;
        $dbh = DBI->connect( "DBI:SQLite:dbname=$database", '', '', {
            RaiseError => 1,
            PrintError => 0,
            AutoCommit => 1,
            sqlite_unicode               =>  $opt->{$database}{unicode}[v]             // $opt->{sqlite}{unicode}[v],
            sqlite_see_if_its_a_number   =>  $opt->{$database}{see_if_its_a_number}[v] // $opt->{sqlite}{see_if_its_a_number}[v],
        } ) or die DBI->errstr;
        $dbh->sqlite_busy_timeout(           $opt->{$database}{busy_timeout}[v]        // $opt->{sqlite}{busy_timeout}[v] );
        $dbh->do( 'PRAGMA cache_size = ' . ( $opt->{$database}{cache_size}[v]          // $opt->{sqlite}{cache_size}[v] ) );
    }
    else {
        if ( not defined $arg->{db_user} ) {
            $arg->{db_user} = $term->readline( 'Enter username: ' );
        }
        if ( not defined $arg->{db_passwd} ) {
            $arg->{db_passwd} = read_password( 'Enter password: ', 0, 1 );
        }
        $dbh = DBI->connect( "DBI:mysql:dbname=$database", $arg->{db_user}, $arg->{db_passwd}, {
            PrintError => 0,
            RaiseError => 1,
            AutoCommit => 1,
            mysql_enable_utf8        => $opt->{$database}{enable_utf8}[v]        // $opt->{mysql}{enable_utf8}[v],
            mysql_connect_timeout    => $opt->{$database}{connect_timeout}[v]    // $opt->{mysql}{connect_timeout}[v],
            mysql_bind_type_guessing => $opt->{$database}{bind_type_guessing}[v] // $opt->{mysql}{bind_type_guessing}[v],
        } ) or die DBI->errstr;
    }
    return $dbh;
}

sub available_databases {
    my ( $arg, $opt ) = @_;
    my $databases = [];
    if ( $arg->{db_type} eq 'sqlite' ) {
        my @dirs = @ARGV ? @ARGV : ( $arg->{home} );
        my $c_dirs  = join ' ', @dirs;
        $arg->{cache} = read_json( $arg->{db_cache_file} );
        if ( $opt->{db_search}{reset_cache}[v] ) {
            delete $arg->{cache}{$c_dirs};
            $arg->{cached} = '';
        }
        else {
            if ( $arg->{cache}{$c_dirs} ) {
                $databases = $arg->{cache}{$c_dirs};
                $arg->{cached} = ' (cached)';
            }
            else {
                $arg->{cached} = '';
            }
        }
        if ( not $arg->{cached} ) {
            say 'searching...';
            my $rule = File::Find::Rule->new();
            $rule->file();
            $rule->nonempty;
            $rule->exec( sub{
                #say $_[2];
                my $fh = IO::File->new( $_, 'r' );
                if ( defined $fh ) {
                    my $firstline = readline( $fh );
                    undef $fh;
                    return $firstline =~ /\ASQLite\sformat/;
                }
            } );
            $databases = [ $rule->in( @dirs ) ];
            $arg->{cache}{$c_dirs} = $databases;
            write_json( $arg->{db_cache_file}, $arg->{cache} );
            say 'ended searching';
        }
    }
    elsif( $arg->{db_type} eq 'mysql' ) {
        my $dbh = get_database_handle( $arg, $opt, 'information_schema' );
        my $stmt = "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name";
        $databases = $dbh->selectcol_arrayref( $stmt, {} );
    }
    return $databases;
}

sub get_table_names {
    my ( $dbh, $database ) = @_;
    my $tables = [];
    if ( $arg->{db_type} eq 'sqlite' ) {
        $tables = $dbh->selectcol_arrayref( "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name" );
    }
    else {
        my $dbh = get_database_handle( $arg, $opt, 'information_schema' );
        my $stmt = "SELECT table_name FROM information_schema.tables WHERE table_schema = ? ORDER BY table_name";
        $tables = $dbh->selectcol_arrayref( $stmt, {}, ( $database ) );
    }
    return $tables;
}


#----------------------------------------------------------------------------------------------------#
#-- 333 --------------------------------------   main   ---------------------------------------------#
#----------------------------------------------------------------------------------------------------#

if ( @{$arg->{available_db_types}} == 1 ) {
    $arg->{db_type} = $arg->{available_db_types}[0];
}
else {
    $arg->{db_type} = choose( [ @{$arg->{available_db_types}} ], { prompt => 'Database Type: ', layout => 1, pad_one_row => 2 } );
    exit if not defined $arg->{db_type};
    $arg->{db_type} =~ s/\A\s|\s\z//g;
}

my $databases = [];
if ( not eval {
    $databases = available_databases( $arg, $opt );
    1 }
) {
    say 'Available databases:';
    print $@;
    choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
}
say 'no ' . $arg->{db_type} . '-databases found' and exit if not @$databases;

my %lyt = ( layout => 3, clear_screen => 1 );
my $new_db_settings = 0;
my $database;

DATABASES: while ( 1 ) {
    # Choose
    if ( not $new_db_settings ) {
        $database = choose( [ undef, @$databases ], { prompt => 'Choose Database' . $arg->{cached}, %lyt, undef => 'QUIT' } );
        last DATABASES if not defined $database;
    }
    else {
        $new_db_settings = 0;
    }
    my ( $dbh, $tables );
    if ( not eval {
        $dbh = get_database_handle( $arg, $opt, $database );
        $tables = get_table_names( $dbh, $database );
        1 }
    ) {
        say 'Get database handle and table names:';
        print $@;
        choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
        # remove database from @databases
        next DATABASES;
    }
    $arg->{info_db_tables} = undef;

    my $join_tables  = '  Join';
    my $union_tables = '  Union';
    my $db_setting   = '  Database settings';
    my @choices = ();
    push @choices, map { "- $_" } @$tables;
    push @choices, '  sqlite_master' if $arg->{db_type} eq 'sqlite';
    push @choices, $join_tables, $union_tables, $db_setting;

    TABLES: while ( 1 ) {
        # Choose
        my $table = choose( [ undef, @choices ], { prompt => 'DB: "'. basename( $database ) . '"', %lyt, undef => $arg->{_back} } );
        last TABLES if not defined $table;
        my $select_from_stmt = '';
        my $print_quote_cols = [];
        if ( $table eq $db_setting ) {
            if ( not eval {
                $new_db_settings = database_setting( $arg, $opt, $database );
                1 }
            ) {
                say 'Database settings:';
                print $@;
                choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
            }
            next DATABASES if $new_db_settings;
            next TABLES;
        }
        elsif ( $table eq $join_tables ) {
            if ( not eval {
                ( $select_from_stmt, $print_quote_cols ) = join_tables( $arg, $dbh, $tables );
                $table = 'joined_tables';
                1 }
            ) {
                say 'Join tables:';
                print $@;
                choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
            }
            next TABLES if not defined $select_from_stmt;
        }
        elsif ( $table eq $union_tables ) {
            if ( not eval {
                ( $select_from_stmt, $print_quote_cols ) = union_tables( $arg, $dbh, $tables );
                $table = 'union_tables';
                1 }
            ) {
                say 'Union tables:';
                print $@;
                choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
            }
            next TABLES if not defined $select_from_stmt;
        }
        else {
            $table =~ s/\A..//;
        }
        if ( not eval {

            $arg->{stmt_keys} = [ qw( distinct_stmt group_by_cols aggregate_stmt where_stmt group_by_stmt having_stmt order_by_stmt limit_stmt ) ];
            $arg->{list_keys} = [ qw( chosen_columns aliases where_args having_args limit_args ) ];

            my @stmt_keys = @{$arg->{stmt_keys}};
            my @list_keys = @{$arg->{list_keys}};
            @{$arg->{print}}{@stmt_keys} = ( '' ) x @stmt_keys;
            @{$arg->{quote}}{@stmt_keys} = ( '' ) x @stmt_keys;
            @{$arg->{print}}{@list_keys} = ( [] ) x @list_keys;
            @{$arg->{quote}}{@list_keys} = ( [] ) x @list_keys;

            $arg->{lock} = $opt->{all}{lock_stmt}[v];

            my $from_stmt       = '';
            my $col_default_str = '';
            my $quote_cols      = {};
            my $col_names       = [];
            if ( $select_from_stmt ) {
                if ( $select_from_stmt =~ /\ASELECT\s(.*?)(\sFROM\s.*)\z/ ) {
                    $col_default_str = $1;
                    $from_stmt       = $2;
                    for my $ref ( @$print_quote_cols ) {
                        $quote_cols->{$ref->[0]} = $ref->[1];
                        push @$col_names, $ref->[0];
                    }
                } else { die $select_from_stmt }
            }
            else {
                $col_default_str = ' *';
                my $table_q = $dbh->quote_identifier( $table );
                $from_stmt = " FROM $table_q";
                my $sth = $dbh->prepare( "SELECT *" . $from_stmt );
                $sth->execute();
                for my $col ( @{$sth->{NAME}} ) {
                    $quote_cols->{$col} = $dbh->quote_identifier( $col );
                    push @$col_names, $col;
                }
            }

            MAIN_LOOP: while ( 1 ) {
                my ( $total_ref, $col_names, $col_types ) = read_table( $arg, $opt, $dbh, $table, $from_stmt, $col_default_str, $quote_cols, $col_names );
                last MAIN_LOOP if not defined $total_ref;
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


#----------------------------------------------------------------------------------------------------#
#-- 444 ---------------------------------   union routines   ----------------------------------------#
#----------------------------------------------------------------------------------------------------#


sub union_tables {
    my ( $arg, $dbh, $tables ) = @_;
    my ( $table_column_names, $table_column_types ) = table_columns( $arg, $dbh, $tables );
    my $enough_tables  = '  Enough TABLES';
    my @tables_unused = map { "- $_" } @$tables;
    my $used_tables = [];
    my $cols        = {};

    UNION_TABLE: while ( 1 ) {
        print_union_stmt( $used_tables, $cols );
        # Choose
        my $union_table = choose( [ undef, map( "° $_", @$used_tables ), @tables_unused, $arg->{_info}, $enough_tables ], { prompt => 'Choose UNION table:', layout => 3, undef => $arg->{_back} } );
        return if not defined $union_table;
        if ( $union_table eq $arg->{_info} ) {
            $arg->{info_db_tables} = info( $arg, $dbh, $tables, $table_column_names, $table_column_types ) if not $arg->{info_db_tables};
            choose( $arg->{info_db_tables}, { prompt => 0, layout => 3, clear_screen => 1 } );
            next UNION_TABLE;
        }
        if ( $union_table eq $enough_tables ) {
            last UNION_TABLE;
        }
        my $idx = first_index { $_ eq $union_table } @tables_unused;
        $union_table =~ s/\A..//;
        if ( $idx == -1 ) {
            delete $cols->{$union_table};
        }
        else {
            splice( @tables_unused, $idx, 1 );
            push @{$used_tables}, $union_table;
        }

        UNION_COLUMNS: while ( 1 ) {
            my $all_cols = q[' * '];
            print_union_stmt( $used_tables, $cols );
            # Choose
            my $col = choose( [ $arg->{ok}, $all_cols, @{$table_column_names->{$union_table}} ], { prompt => 'Choose Column: ', layout => 1, pad_one_row => 2 } );
            if ( not defined $col ) {
                delete $cols->{$union_table};
                my $idx = first_index { $_ eq $union_table } @$used_tables;
                my $tbl = splice( @$used_tables, $idx, 1 );
                push @tables_unused, "- $tbl";
                last UNION_COLUMNS;
            }
            if ( $col eq $arg->{ok} ) {
                if ( not exists $cols->{$union_table} ) {
                    my $idx = first_index { $_ eq $union_table } @$used_tables;
                    my $tbl = splice( @$used_tables, $idx, 1 );
                    push @tables_unused, "- $tbl";
                }
                last UNION_COLUMNS;
            }
            if ( $col eq $all_cols ) {
                push @{$cols->{$union_table}}, '*';
                last UNION_COLUMNS;
            }
            else {
                push @{$cols->{$union_table}}, $col;
            }
        }
    }
    my $print_quote_cols = [];
    my $union_statement = "SELECT * FROM (";
    my $count = 0;
    for my $table ( @$used_tables ) {
        $count++;
        if ( $count == 1 ) { # column names in the result-set of a UNION are taken from the first query.
            if ( ${$cols->{$table}}[0] eq '*' ) {
                for my $col ( @{$table_column_names->{$table}} ) {
                    push @$print_quote_cols, [ $col, $dbh->quote_identifier( $col ) ];
                }
            }
            else {
                for my $col ( @{$cols->{$table}} ) {
                    push @$print_quote_cols, [ $col, $dbh->quote_identifier( $col ) ];
                }
            }
        }
        $union_statement .= " SELECT";
        if ( $cols->{$table}[0] eq '*' ) {
            $union_statement .= " *";
        }
        else {
            $union_statement .= " " . join( ', ', map { $dbh->quote_identifier( $_ ) } @{$cols->{$table}} );
        }
        $union_statement .= " FROM " . $dbh->quote_identifier( $table );
        $union_statement .= ( $count < @$used_tables ? " UNION ALL " : " )" );
    }
    my $derived_table_name = join '_', @$used_tables;
    $union_statement .= " AS $derived_table_name";
    return $union_statement, $print_quote_cols;
}


sub print_union_stmt {
    my ( $used_tables, $cols ) = @_;
    my $print_string = "SELECT * FROM (\n";
    my $count;
    for my $table ( @$used_tables ) {
        $count++;
        $print_string .= "  SELECT ";
        $print_string .= ( defined $cols->{$table} ? join( ', ', @{$cols->{$table}} ) : '?' );
        $print_string .= " FROM $table";
        $print_string .= ( $count < @$used_tables ? " UNION ALL" : "" );
        $print_string .= "\n";
    }
    my $derived_table_name = join '_', @$used_tables;
    $print_string .= ") AS $derived_table_name\n" if @$used_tables;
    $print_string .= "\n";
    print GO_TO_TOP_LEFT;
    print CLEAR_EOS;
    print $print_string;
}


#----------------------------------------------------------------------------------------------------#
#-- 555 ---------------------------------   join routines   -----------------------------------------#
#----------------------------------------------------------------------------------------------------#


sub table_columns {
    my ( $arg, $dbh, $tables ) = @_;
    my $table_column_names = {};
    my $table_column_types = {};
    my $type;
    $type = 'TYPE'            if $arg->{db_type} eq 'sqlite';
    $type = 'mysql_type_name' if $arg->{db_type} eq 'mysql';
    for my $table ( @$tables ) {
        my $sth = $dbh->prepare( "SELECT * FROM " . $dbh->quote_identifier( $table ) );
        $sth->execute();
        $table_column_names->{$table} = $sth->{NAME};
        $table_column_types->{$table} = $sth->{$type};
    }
    return $table_column_names, $table_column_types;
}


sub info {
    my ( $arg, $dbh, $tables, $table_column_names, $table_column_types ) = @_;
    my %print_hash;
    for my $table ( @$tables ) {
        push @{$print_hash{$table}}, [ 'TABLE: ', '== ' . $table . ' ==' ];
        push @{$print_hash{$table}}, [ 'COLUMNS: ', join( ' | ', pairwise { no warnings q(once); "$a $b" } @{$table_column_types->{$table}}, @{$table_column_names->{$table}} ) ];
        my @primary_key_columns = $dbh->primary_key( undef, undef, $table );
        push @{$print_hash{$table}}, [ 'PK: ', 'primary key (' . join( ',', @primary_key_columns ) . ')' ] if @primary_key_columns;
        if ( $arg->{db_type} eq 'sqlite' ) {
            my $stmt = "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?";
            my $ref = $dbh->selectcol_arrayref( $stmt, {}, ( $table ) );
            my @references = grep { /\AFOREIGN\sKEY/ } split /\s*\n\s*/, $ref->[0];
            for my $reference ( @references ) {
                $reference =~ s/FOREIGN KEY\(([^)]+)\)\sREFERENCES\s([^(]+)\(([^)]+)\),?/foreign key ($1) references $2($3)/;
                push @{$print_hash{$table}}, [ 'FK: ', $reference ];
            }
        }
        elsif ( $arg->{db_type} eq 'mysql' ) {
            my $foreign_keys = {};
            $sth = $dbh->foreign_key_info( undef, undef, undef, undef, undef, $table );
            while ( my $row = $sth->fetchrow_hashref ) {
                if ( defined $row->{PKTABLE_SCHEM} and defined $row->{PKTABLE_NAME} and defined $row->{PKCOLUMN_NAME} ) {
                    $foreign_keys->{$row->{FK_NAME}}{$row->{KEY_SEQ}} = {
                        foreign_key_col   => $row->{FKCOLUMN_NAME},
                        reference_key_col => $row->{PKCOLUMN_NAME},
                        reference_table   => $row->{PKTABLE_NAME},
                    };
                }
            }
            for my $fk_name ( sort keys %$foreign_keys ) {
                my $reference_table = $foreign_keys->{$fk_name}{1}{reference_table};
                my @foreign_key_columns;
                my @reference_key_columns;
                for my $pos ( sort keys %{$foreign_keys->{$fk_name}} ) {
                    push @foreign_key_columns,   $foreign_keys->{$fk_name}{$pos}{foreign_key_col};
                    push @reference_key_columns, $foreign_keys->{$fk_name}{$pos}{reference_key_col};
                }
                push @{$print_hash{$table}}, [ 'FK: ', 'foreign key (' . join( ',', @foreign_key_columns ) . ') references ' . $reference_table . '(' . join( ',', @reference_key_columns ) .')' ];
            }
        }
    }
    my $longest = 10;
    my ( $maxcols ) = GetTerminalSize( *STDOUT );
    my $col_max = $maxcols - $longest;
    my $line_fold = Text::LineFold->new(
        Charset       => 'utf-8',
        ColMax        => $col_max > 140 ? 140 : $col_max,
        OutputCharset => '_UNICODE_',
        Urgent        => 'FORCE',
    );
    my $print_info_array_ref = [];
    push @$print_info_array_ref, 'Close with ENTER';
    for my $table ( @$tables ) {
        push @$print_info_array_ref, " ";
        for my $line ( @{$print_hash{$table}} ) {
            my $key = $line->[0];
            my $text = $line_fold->fold( '' , '', $line->[1] );
            for my $row ( split /\R+/, $text ) {
                push @$print_info_array_ref, sprintf "%${longest}s%s", $key, $row;
                $key = '';
            }
        }
    }
    return $table_column_names, $print_info_array_ref;
}


sub join_tables {
    my ( $arg, $dbh, $tables ) = @_;
    my ( $table_column_names, $table_column_types ) = table_columns( $arg, $dbh, $tables );
    my $join_statement_quote = "SELECT * FROM";
    my $join_statement_print = "SELECT * FROM";
    my @tables = map { "- $_" } @$tables;
    my $mastertable;
    MASTER: while ( 1 ) {
        join_tables_print_info( $join_statement_print );
        # Choose
        $mastertable = choose( [ undef, @tables, $arg->{_info} ], { prompt => 'Choose MASTER table:', layout => 3, undef => $arg->{_back} } );
        return if not defined $mastertable;
        if ( $mastertable eq $arg->{_info} ) {
            $arg->{info_db_tables} = info( $arg, $dbh, $tables, $table_column_names, $table_column_types ) if not $arg->{info_db_tables};
            choose( $arg->{info_db_tables}, { prompt => 0, layout => 3, clear_screen => 1 } );
            next MASTER;
        }
        last MASTER;
    }
    my $idx = first_index { $_ eq $mastertable } @tables;
    splice( @tables, $idx, 1 );
    $mastertable =~ s/\A..//;
    my @used_tables = ( $mastertable );
    my @available_tables = @tables;
    my $mastertable_q = $dbh->quote_identifier( $mastertable );
    $join_statement_quote = "SELECT * FROM " . $mastertable_q;
    $join_statement_print = "SELECT * FROM " . $mastertable;
    my ( @primary_keys, @foreign_keys, @old_primary_keys, @old_foreign_keys, @old_used_tables, @old_avail_tables );
    my $old_stmt_quote = '';
    my $old_stmt_print = '';
    SLAVE_TABLES: while ( 1 ) {
        my $enough_slaves = '  Enough SLAVES';
        my $slave_table;
        @old_primary_keys = @primary_keys;
        @old_foreign_keys = @foreign_keys;
        $old_stmt_quote   = $join_statement_quote;
        $old_stmt_print   = $join_statement_print;
        @old_used_tables  = @used_tables;
        @old_avail_tables = @available_tables;
        SLAVE: while ( 1 ) {
            join_tables_print_info( $join_statement_print );
            # Choose
            $slave_table = choose( [ undef, @available_tables, $arg->{_info}, $enough_slaves ], { prompt => 'Add a SLAVE table:', layout => 3, undef => $arg->{_reset} } );
            if ( not defined $slave_table ) {
                return if @used_tables == 1;
                @used_tables = ( $mastertable );
                @available_tables = @tables;
                $join_statement_quote = "SELECT * FROM " . $mastertable_q;
                $join_statement_print = "SELECT * FROM " . $mastertable;
                @primary_keys = ();
                @foreign_keys = ();
                next SLAVE_TABLES;
            }
            last SLAVE_TABLES if $slave_table eq $enough_slaves;
            if ( $slave_table eq $arg->{_info} ) {
                $arg->{info_db_tables} = info( $arg, $dbh, $tables, $table_column_names, $table_column_types ) if not $arg->{info_db_tables};
                choose( $arg->{info_db_tables}, { prompt => 0, layout => 3, clear_screen => 1 } );
                next SLAVE;
            }
            last SLAVE;
        }
        my $idx = first_index { $_ eq $slave_table } @available_tables;
        splice( @available_tables, $idx, 1 );
        $slave_table =~ s/\A..//;
        my $slave_table_q = $dbh->quote_identifier( $slave_table );
        $join_statement_quote .= " LEFT OUTER JOIN " . $slave_table_q . " ON";
        $join_statement_print .= " LEFT OUTER JOIN " . $slave_table   . " ON";
        my %av_primary_key_columns = ();
        for my $used_table ( @used_tables ) {
            for my $col ( @{$table_column_names->{$used_table}} ) {
                $av_primary_key_columns{"$used_table.$col"} = $dbh->quote_identifier( undef, $used_table, $col );
            }
        }
        my %av_foreign_key_columns = ();
        for my $col ( @{$table_column_names->{$slave_table}} ) {
            $av_foreign_key_columns{"$slave_table.$col"} = $dbh->quote_identifier( undef, $slave_table, $col );
        }
        my $AND = '';

        ON: while ( 1 ) {
            join_tables_print_info( $join_statement_print );
            # Choose
            my $pkc_choise = choose( [ undef, map( "- $_", sort keys %av_primary_key_columns ), $arg->{_continue} ], { prompt => 'Choose PRIMARY KEY column:', layout => 3, undef => $arg->{_reset} } );
            if ( not defined $pkc_choise ) {
                @primary_keys         = @old_primary_keys;
                @foreign_keys         = @old_foreign_keys;
                $join_statement_quote = $old_stmt_quote;
                $join_statement_print = $old_stmt_print;
                @used_tables          = @old_used_tables;
                @available_tables     = @old_avail_tables;
                next SLAVE_TABLES;
            }
            if ( $pkc_choise eq $arg->{_continue} ) {
                if ( @primary_keys == @old_primary_keys ) {
                    $join_statement_quote = $old_stmt_quote;
                    $join_statement_print = $old_stmt_print;
                    @used_tables          = @old_used_tables;
                    @available_tables     = @old_avail_tables;
                    next SLAVE_TABLES;
                }
                last ON;
            }
            $pkc_choise =~ s/\A..//;
            push @primary_keys, $av_primary_key_columns{$pkc_choise};
            $join_statement_quote .= $AND;
            $join_statement_print .= $AND;
            $join_statement_quote .= ' ' . $av_primary_key_columns{$pkc_choise} . " =";
            $join_statement_print .= ' ' . $pkc_choise                          . " =";
            join_tables_print_info( $join_statement_print );
            # Choose
            my $fkc_choice = choose( [ undef, map{ "- $_" } sort keys %av_foreign_key_columns ], { prompt => 'Choose FOREIGN KEY column:', layout => 3, undef => $arg->{_reset} } );
            if ( not defined $fkc_choice ) {
                @primary_keys         = @old_primary_keys;
                @foreign_keys         = @old_foreign_keys;
                $join_statement_quote = $old_stmt_quote;
                $join_statement_print = $old_stmt_print;
                @used_tables          = @old_used_tables;
                @available_tables     = @old_avail_tables;
                next SLAVE_TABLES;
            }
            $fkc_choice =~ s/\A..//;
            push @foreign_keys, $av_foreign_key_columns{$fkc_choice};
            $join_statement_quote .= ' ' . $av_foreign_key_columns{$fkc_choice};
            $join_statement_print .= ' ' . $fkc_choice;
            $AND = " AND";
        }
        push @used_tables, $slave_table;
    }
    my $length_uniq = 2;
    ABBR: while ( 1 ) {
        $length_uniq++;
        my %abbr;
        for my $table ( @used_tables, ) {
            next ABBR if $abbr{ substr $table, 0, $length_uniq }++;
        }
        last ABBR;
    }
    my @dup;
    my %seen;
    for my $table ( keys %{$table_column_names} ) {
        next if none { $_ eq $table } @used_tables;
        for my $col ( @{$table_column_names->{$table}} ) {
            $seen{$col}++;
            push @dup, $col if $seen{$col} == 2;
        }
    }
    my @col_strings;
    my $print_quote_cols;
    for my $table ( @$tables ) {
        next if none { $_ eq $table } @used_tables;
        for my $col ( @{$table_column_names->{$table}} ) {
            my $q_table_col = $dbh->quote_identifier( undef, $table, $col );
            next if any { $_ eq $q_table_col } @foreign_keys;
            my ( $alias, $col_string );
            if ( any { $_ eq $col } @dup ) {
                #if ( any { $_ eq $q_table_col } @primary_keys ) {
                #    $alias = $col . '_PK';
                #}
                #else {
                    $alias = $col . '_' . substr $table, 0, $length_uniq;
                #}
                $col_string = $q_table_col . " AS " . $alias;
            }
            else {
                $alias = $col;
                $col_string = $q_table_col;
            }
            push @col_strings, $col_string;
            push @$print_quote_cols , [ $alias, $col_string ];
        }
    }
    my $col_statement = join ', ', @col_strings;
    $join_statement_quote =~ s/\s\*\s/ $col_statement /;
    return $join_statement_quote, $print_quote_cols;
}


sub join_tables_print_info {
    my ( $join_statement_print ) = @_;
    my $print_string = '';
    if ( $join_statement_print ) {
        my @array = split /(?=\sLEFT\sOUTER\sJOIN)/, $join_statement_print;
        $print_string .= shift( @array ) . "\n";
        for my $join ( @array ) {
            $print_string .=  "  $join\n";
        }
        $print_string .= "\n";
    }
    print GO_TO_TOP_LEFT;
    print CLEAR_EOS;
    print $print_string;
}


#----------------------------------------------------------------------------------------------------#
#-- 666 -------------------------------   statement routine   ---------------------------------------#
#----------------------------------------------------------------------------------------------------#


sub print_select {
    my ( $arg, $table, $print ) = @_;
    my $cols_str = '';
    $cols_str = ' '. join( ', ', @{$arg->{print}{chosen_columns}} ) if @{$arg->{print}{chosen_columns}};
    $cols_str = $arg->{print}{group_by_cols} if not @{$arg->{print}{chosen_columns}} and $arg->{print}{group_by_cols};
    if ( $arg->{print}{aggregate_stmt} ) {
        $cols_str .= ',' if $cols_str;
        $cols_str .= $arg->{print}{aggregate_stmt};
    }
    $cols_str = ' *' if not $cols_str;
    print GO_TO_TOP_LEFT;
    print CLEAR_EOS;
    print "SELECT";
    print $arg->{print}{distinct_stmt} if $arg->{print}{distinct_stmt};
    say $cols_str;
    say " FROM $table";
    say $arg->{print}{where_stmt}    if $arg->{print}{where_stmt};
    say $arg->{print}{group_by_stmt} if $arg->{print}{group_by_stmt};
    say $arg->{print}{having_stmt}   if $arg->{print}{having_stmt};
    say $arg->{print}{order_by_stmt} if $arg->{print}{order_by_stmt};
    say $arg->{print}{limit_stmt}    if $arg->{print}{limit_stmt};
    say "";
}


sub read_table {
    my ( $arg, $opt, $dbh, $table, $from_stmt, $col_default_str, $quote_cols, $col_names ) = @_;
    $dbh->func( 'regexp', 2, sub { my ( $regex, $string ) = @_; $string //= ''; return $string =~ m/$regex/ism }, 'create_function' ) if $arg->{db_type} eq 'sqlite';
    my %read_table_style = ( layout => 1, order => 0, justify => 2, pad_one_row => 2 );
    my @keys = ( qw( print_table columns aggregate distinct where group_by having order_by limit lock ) );
    my $lock = [ '  Lk0', '  Lk1' ];
    my %customize = (
        print_table     => 'Print TABLE',
        columns         => '- COLUMNS',
        aggregate       => '- AGGREGATE',
        distinct        => '- DISTINCT',
        where           => '- WHERE',
        group_by        => '- GROUP BY',
        having          => '- HAVING',
        order_by        => '- ORDER BY',
        limit           => '- LIMIT',
        lock            => $lock->[$arg->{lock}],
    );
    my $before_col  = ' ';
    my $between_col = ', ';
    my ( $DISTINCT, $ALL, $ASC, $DESC, $AND, $OR ) = ( "DISTINCT", "ALL", "ASC", "DESC", "AND", "OR" );
    if ( $arg->{lock} == 0 ) {
        my @stmt_keys = @{$arg->{stmt_keys}};
        my @list_keys = @{$arg->{list_keys}};
        @{$arg->{print}}{@stmt_keys} = ( '' ) x @stmt_keys;
        @{$arg->{quote}}{@stmt_keys} = ( '' ) x @stmt_keys;
        @{$arg->{print}}{@list_keys} = ( [] ) x @list_keys;
        @{$arg->{quote}}{@list_keys} = ( [] ) x @list_keys;
    }

    CUSTOMIZE: while ( 1 ) {
        print_select( $arg, $table, $print );
        # Choose
        my $custom = choose( [ undef, @customize{@keys} ], { prompt => 'Customize:', layout => 3, undef => $arg->{back} } );
        for ( $custom ) {
            when ( not defined ) {
                last CUSTOMIZE;
            }
            when ( $customize{'lock'} ) {
                if ( $arg->{lock} == 1 ) {
                    $arg->{lock} = 0;
                    $customize{lock} = $lock->[0];
                    my @stmt_keys = @{$arg->{stmt_keys}};
                    my @list_keys = @{$arg->{list_keys}};
                    @{$arg->{print}}{@stmt_keys} = ( '' ) x @stmt_keys;
                    @{$arg->{quote}}{@stmt_keys} = ( '' ) x @stmt_keys;
                    @{$arg->{print}}{@list_keys} = ( [] ) x @list_keys;
                    @{$arg->{quote}}{@list_keys} = ( [] ) x @list_keys;
                }
                elsif ( $arg->{lock} == 0 )   {
                    $arg->{lock} = 1;
                    $customize{lock} = $lock->[1];
                }
            }
            when( $customize{'columns'} ) {
                my @cols = @$col_names;
                $arg->{quote}{chosen_columns} = [];
                $arg->{print}{chosen_columns} = [];

                COLUMNS: while ( 1 ) {
                    print_select( $arg, $table, $print );
                    # Choose
                    my $print_col = choose( [ $arg->{ok}, @cols ], { prompt => 'Choose: ', %read_table_style } );
                    if ( not defined $print_col ) {
                        $arg->{quote}{chosen_columns} = [];
                        $arg->{print}{chosen_columns} = [];
                        last COLUMNS;
                    }
                    if ( $print_col eq $arg->{ok} ) {
                        last COLUMNS;
                    }
                    push @{$arg->{quote}{chosen_columns}}, $quote_cols->{$print_col};
                    push @{$arg->{print}{chosen_columns}}, $print_col;
                }
            }
            when( $customize{'distinct'} ) {
                $arg->{quote}{distinct_stmt} = '';
                $arg->{print}{distinct_stmt} = '';

                DISTINCT: while ( 1 ) {
                    print_select( $arg, $table, $print );
                    # Choose
                    my $select_distinct = choose( [ $arg->{ok}, $DISTINCT, $ALL ], { prompt => 'Choose: ', %read_table_style } );
                    if ( not defined $select_distinct ) {
                        $arg->{quote}{distinct_stmt} = '';
                        $arg->{print}{distinct_stmt} = '';
                        last DISTINCT;
                    }
                    if ( $select_distinct eq $arg->{ok} ) {
                        last DISTINCT;
                    }
                    $select_distinct =~ s/\A\s+|\s+\z//g;
                    $arg->{quote}{distinct_stmt} = ' ' . $select_distinct;
                    $arg->{print}{distinct_stmt} = ' ' . $select_distinct;
                }
            }
            when ( $customize{'where'} ) {
                my @cols = @$col_names;
                my $AND_OR = '';
                $arg->{quote}{where_args} = [];
                $arg->{quote}{where_stmt} = " WHERE";
                $arg->{print}{where_stmt} = " WHERE";
                my $count = 0;

                WHERE: while ( 1 ) {
                    print_select( $arg, $table, $print );
                    # Choose
                    my $print_col = choose( [ $arg->{ok}, @cols ], { prompt => 'Choose: ', %read_table_style } );
                    if ( not defined $print_col ) {
                        $arg->{quote}{where_args} = [];
                        $arg->{quote}{where_stmt} = '';
                        $arg->{print}{where_stmt} = '';
                        last WHERE;
                    }
                    if ( $print_col eq $arg->{ok} ) {
                        if ( $count == 0 ) {
                            $arg->{quote}{where_stmt} = '';
                            $arg->{print}{where_stmt} = '';
                        }
                        last WHERE;
                    }
                    if ( $count >= 1 ) {
                        print_select( $arg, $table, $print );
                        # Choose
                        $AND_OR = choose( [ $AND, $OR ], { prompt => 'Choose:', %read_table_style } );
                        if ( not defined $AND_OR ) {
                            $arg->{quote}{where_args} = [];
                            $arg->{quote}{where_stmt} = '';
                            $arg->{print}{where_stmt} = '';
                            last WHERE;
                        }
                        $AND_OR =~ s/\A\s+|\s+\z//g;
                        $AND_OR = ' ' . $AND_OR;
                    }
                    ( my $quote_col = $quote_cols->{$print_col} ) =~ s/\sAS\s\S+\z//;
                    $arg->{quote}{where_stmt} .= $AND_OR . ' ' . $quote_col;
                    $arg->{print}{where_stmt} .= $AND_OR . ' ' . $print_col;
                    print_select( $arg, $table, $print );
                    # Choose
                    my $filter_type = choose( [ @{$arg->{filter_types}} ], { prompt => 'Choose:', %read_table_style } );
                    if ( not defined $filter_type ) {
                        $arg->{quote}{where_args} = [];
                        $arg->{quote}{where_stmt} = '';
                        $arg->{print}{where_stmt} = '';
                        last WHERE;
                    }
                    $filter_type =~ s/\A\s+|\s+\z//g;
                    $arg->{quote}{where_stmt} .= ' ' . $filter_type;
                    $arg->{print}{where_stmt} .= ' ' . $filter_type;
                    if ( $filter_type =~ /NULL\z/ ) {
                        # do nothing
                    }
                    elsif ( $filter_type =~ /\A(?:NOT\s)?IN\z/ ) {
                        $arg->{col_sep} = $before_col;
                        $arg->{quote}{where_stmt} .= '(';
                        $arg->{print}{where_stmt} .= '(';

                        IN: while ( 1 ) {
                            print_select( $arg, $table, $print );
                            # Choose
                            my $print_col = choose( [ $arg->{ok}, @cols ], { prompt => "$filter_type: ", %read_table_style } );
                            if ( not defined $print_col ) {
                                $arg->{quote}{where_args} = [];
                                $arg->{quote}{where_stmt} = '';
                                $arg->{print}{where_stmt} = '';
                                last WHERE;
                            }
                            if ( $print_col eq $arg->{ok} ) {
                                if ( $arg->{col_sep} eq $before_col ) {
                                    $arg->{quote}{where_args} = [];
                                    $arg->{quote}{where_stmt} = '';
                                    $arg->{print}{where_stmt} = '';
                                    last WHERE;
                                }
                                $arg->{quote}{where_stmt} .= ' )';
                                $arg->{print}{where_stmt} .= ' )';
                                last IN;
                            }
                            ( my $quote_col = $quote_cols->{$print_col} ) =~ s/\sAS\s\S+\z//;
                            $arg->{quote}{where_stmt} .= $arg->{col_sep} . $quote_col;
                            $arg->{print}{where_stmt} .= $arg->{col_sep} . $print_col;
                            $arg->{col_sep} = $between_col;
                        }
                    }
                    else {
                        print_select( $arg, $table, $print );
                        # Read
                        my $pattern = $term->readline( $filter_type =~ /REGEXP\z/ ? 'Regexp: ' : 'Arg: ' );
                        if ( not defined $pattern ) {
                            $arg->{quote}{where_args} = [];
                            $arg->{quote}{where_stmt} = '';
                            $arg->{print}{where_stmt} = '';
                            last WHERE;
                        }
                        $pattern= '^$' if not length $pattern and $filter_type =~ /REGEXP\z/;
                        $pattern= qr/$pattern/i if $arg->{db_type} eq 'sqlite' and $filter_type =~ /REGEXP\z/;
                        $arg->{quote}{where_stmt} .= ' ' . '?';
                        $arg->{print}{where_stmt} .= ' ' . $dbh->quote( $pattern );
                        push @{$arg->{quote}{where_args}}, $pattern;
                    }
                    $count++;
                }
            }
            when( $customize{'aggregate'} ) {
                my @cols = @$col_names;
                $arg->{col_sep} = $before_col;
                $arg->{quote}{aliases}        = [];
                $arg->{quote}{aggregate_stmt} = '';
                $arg->{print}{aggregate_stmt} = '';

                AGGREGATE: while ( 1 ) {
                    print_select( $arg, $table, $print );
                    # Choose
                    my $func = choose( [ $arg->{ok}, @{$arg->{aggregate_functions}} ], { prompt => 'Choose:', %read_table_style } );
                    if ( not defined $func ) {
                        $arg->{quote}{aliases}        = [];
                        $arg->{quote}{aggregate_stmt} = '';
                        $arg->{print}{aggregate_stmt} = '';
                        last AGGREGATE;
                    }
                    if ( $func eq $arg->{ok} ) {
                        last AGGREGATE;
                    }
                    my ( $print_col, $quote_col );
                    if ( $func =~ /\Acount\s*\(\s*\*\s*\)\z/i ) {
                        $print_col = '*';
                        $quote_col = '*';
                    }
                    $func =~ s/\s*\(\s*\S\s*\)\z//;
                    $arg->{quote}{aggregate_stmt} .= $arg->{col_sep} . $func . '(';
                    $arg->{print}{aggregate_stmt} .= $arg->{col_sep} . $func . '(';
                    if ( not defined $print_col ) {
                        print_select( $arg, $table, $print );
                        # Choose
                        $print_col = choose( [ @cols ], { prompt => 'Choose:', %read_table_style } );
                        if ( not defined $print_col ) {
                            $arg->{quote}{aliases}        = [];
                            $arg->{quote}{aggregate_stmt} = '';
                            $arg->{print}{aggregate_stmt} = '';
                            last AGGREGATE;
                        }
                        ( $quote_col = $quote_cols->{$print_col} ) =~ s/\sAS\s\S+\z//;
                    }
                    my $alias = '@' . $func . '_' . $print_col; # ( $print_col eq '*' ? 'ROWS' : $print_col );
                    $arg->{quote}{aggregate_stmt} .= $quote_col . ') AS ' . $dbh->quote_identifier( $alias );
                    $arg->{print}{aggregate_stmt} .= $print_col . ') AS ' .                         $alias  ;
                    push @{$arg->{quote}{aliases}}, $alias;
                    $quote_cols->{$alias} = $func . '(' . $quote_col . ')';
                    $arg->{col_sep} = $between_col;
                }
            }
            when( $customize{'group_by'} ) {
                my @cols = @$col_names;
                $arg->{col_sep} = $before_col;
                $arg->{quote}{group_by_stmt} = " GROUP BY";
                $arg->{print}{group_by_stmt} = " GROUP BY";
                $arg->{quote}{group_by_cols} = '';
                $arg->{print}{group_by_cols} = '';

                GROUP_BY: while ( 1 ) {
                    print_select( $arg, $table, $print );
                    # Choose
                    my $print_col = choose( [ $arg->{ok}, @cols ], { prompt => 'Choose: ', %read_table_style } );
                    if ( not defined $print_col ) {
                        $arg->{quote}{group_by_stmt} = '';
                        $arg->{print}{group_by_stmt} = '';
                        $arg->{quote}{group_by_cols} = '';
                        $arg->{print}{group_by_cols} = '';
                        last GROUP_BY;
                    }
                    if ( $print_col eq $arg->{ok} ) {
                        if ( $arg->{col_sep} eq $before_col ) {
                            $arg->{quote}{group_by_stmt} = '';
                            $arg->{print}{group_by_stmt} = '';
                        }
                        last GROUP_BY;
                    }
                    ( my $quote_col = $quote_cols->{$print_col} ) =~ s/\sAS\s\S+\z//;
                    if ( not @{$arg->{quote}{chosen_columns}} ) {
                        $arg->{quote}{group_by_cols} .= $arg->{col_sep} . $quote_col;
                        $arg->{print}{group_by_cols} .= $arg->{col_sep} . $print_col;
                    }
                    $arg->{quote}{group_by_stmt} .= $arg->{col_sep} . $quote_col;
                    $arg->{print}{group_by_stmt} .= $arg->{col_sep} . $print_col;
                    $arg->{col_sep} = $between_col;
                }
            }
            when( $customize{'having'} ) {
                my @cols = @$col_names;
                my $AND_OR = '';
                $arg->{quote}{having_args} = [];
                $arg->{quote}{having_stmt} = " HAVING";
                $arg->{print}{having_stmt} = " HAVING";
                my $count = 0;

                HAVING: while ( 1 ) {
                    print_select( $arg, $table, $print );
                    # Choose
                    my $func = choose( [ $arg->{ok}, @{$arg->{aggregate_functions}}, @{$arg->{quote}{aliases}} ], { prompt => 'Choose:', %read_table_style } );
                    if ( not defined $func ) {
                        $arg->{quote}{having_args} = [];
                        $arg->{quote}{having_stmt} = '';
                        $arg->{print}{having_stmt} = '';
                        last HAVING;
                    }
                    if ( $func eq $arg->{ok} ) {
                        if ( $count == 0 ) {
                            $arg->{quote}{having_stmt} = '';
                            $arg->{print}{having_stmt} = '';
                        }
                        last HAVING;
                    }
                    if ( $count >= 1 ) {
                        print_select( $arg, $table, $print );
                        # Choose
                        $AND_OR = choose( [ $AND, $OR ], { prompt => 'Choose:', %read_table_style } );
                        last HAVING if not defined $AND_OR;
                        $AND_OR =~ s/\A\s+|\s+\z//g;
                        $AND_OR = ' ' . $AND_OR;
                    }
                    if ( any { $_ eq $func } @{$arg->{quote}{aliases}} ) {
                        $arg->{quote}{having_stmt} .= $AND_OR . ' ' . $quote_cols->{$func};  #####
                        $arg->{print}{having_stmt} .= $AND_OR . ' ' . $func;
                    }
                    else {
                        my ( $print_col, $quote_col );
                        if ( $func =~ /\Acount\s*\(\s*\*\s*\)\z/i ) {
                            $print_col      = '*';
                            $quote_col = '*';
                        }
                        $func =~ s/\s*\(\s*\S\s*\)\z//;
                        $arg->{quote}{having_stmt} .= $AND_OR . ' ' . $func . '(';
                        $arg->{print}{having_stmt} .= $AND_OR . ' ' . $func . '(';
                        if ( not defined $print_col ) {
                            print_select( $arg, $table, $print );
                            # Choose
                            $print_col = choose( [ @cols ], { prompt => 'Choose:', %read_table_style } );
                            if ( not defined $print_col ) {
                                $arg->{quote}{having_args} = [];
                                $arg->{quote}{having_stmt} = '';
                                $arg->{print}{having_stmt} = '';
                                last HAVING;
                            }
                            ( $quote_col = $quote_cols->{$print_col} ) =~ s/\sAS\s\S+\z//;
                        }
                        $arg->{quote}{having_stmt} .= $quote_col . ')';
                        $arg->{print}{having_stmt} .= $print_col . ')';
                    }
                    print_select( $arg, $table, $print );
                    # Choose
                    my $filter_type = choose( [ @{$arg->{filter_types}} ], { prompt => 'Choose:', %read_table_style } );
                    if ( not defined $filter_type ) {
                        $arg->{quote}{having_args} = [];
                        $arg->{quote}{having_stmt} = '';
                        $arg->{print}{having_stmt} = '';
                        last HAVING;
                    }
                    $filter_type =~ s/\A\s+|\s+\z//g;
                    $arg->{quote}{having_stmt} .= ' ' . $filter_type;
                    $arg->{print}{having_stmt} .= ' ' . $filter_type;
                    if ( $filter_type =~ /NULL\z/ ) {
                        # do nothing
                    }
                    elsif ( $filter_type =~ /\A(?:NOT\s)?IN\z/ ) {
                        $arg->{col_sep} = $before_col;
                        $arg->{quote}{having_stmt} .= '(';
                        $arg->{print}{having_stmt} .= '(';

                        IN: while ( 1 ) {
                            print_select( $arg, $table, $print );
                            # Choose
                            my $print_col = choose( [ $arg->{ok}, @cols ], { prompt => "$filter_type: ", %read_table_style } );
                            if ( not defined $print_col ) {
                                $arg->{quote}{having_args} = [];
                                $arg->{quote}{having_stmt} = '';
                                $arg->{print}{having_stmt} = '';
                                last HAVING;
                            }
                            if ( $print_col eq $arg->{ok} ) {
                                if ( $arg->{col_sep} eq $before_col ) {
                                    $arg->{quote}{having_args} = [];
                                    $arg->{quote}{having_stmt} = '';
                                    $arg->{print}{having_stmt} = '';
                                    last HAVING;
                                }
                                $arg->{quote}{having_stmt} .= ' )';
                                $arg->{print}{having_stmt} .= ' )';
                                last IN;
                            }
                            ( my $quote_col = $quote_cols->{$print_col} ) =~ s/\sAS\s\S+\z//;
                            $arg->{quote}{having_stmt} .= $arg->{col_sep} . $quote_col;
                            $arg->{print}{having_stmt} .= $arg->{col_sep} . $print_col;
                            $arg->{col_sep} = $between_col;
                        }
                    }
                    else {
                        print_select( $arg, $table, $print );
                        # Read
                        my $pattern = $term->readline( $filter_type =~ /REGEXP\z/ ? 'Regexp: ' : 'Arg: ' );
                        if ( not defined $pattern ) {
                            $arg->{quote}{having_args} = [];
                            $arg->{quote}{having_stmt} = '';
                            $arg->{print}{having_stmt} = '';
                            last HAVING;
                        }
                        $pattern= '^$' if not length $pattern and $filter_type =~ /REGEXP\z/;
                        $pattern= qr/$pattern/i if $arg->{db_type} eq 'sqlite' and $filter_type =~ /REGEXP\z/;
                        $arg->{quote}{having_stmt} .= ' ' . '?';
                        $arg->{print}{having_stmt} .= ' ' . $dbh->quote( $pattern );
                        push @{$arg->{quote}{having_args}}, $pattern;
                    }
                    $count++;
                }
            }
            when( $customize{'order_by'} ) {
                my @cols = ( @$col_names, @{$arg->{quote}{aliases}} );
                $arg->{col_sep} = $before_col;
                $arg->{quote}{order_by_stmt} = " ORDER BY";
                $arg->{print}{order_by_stmt} = " ORDER BY";

                ORDER_BY: while ( 1 ) {
                    print_select( $arg, $table, $print );
                    # Choose
                    my $print_col = choose( [ $arg->{ok}, @cols ], { prompt => 'Choose: ', %read_table_style } );
                    if ( not defined $print_col ) {
                        $arg->{quote}{order_by_stmt} = '';
                        $arg->{print}{order_by_stmt} = '';
                        last ORDER_BY;
                    }
                    if ( $print_col eq $arg->{ok} ) {
                        if ( $arg->{col_sep} eq $before_col ) {
                            $arg->{quote}{order_by_stmt} = '';
                            $arg->{print}{order_by_stmt} = '';
                        }
                        last ORDER_BY;
                    }
                    ( my $quote_col = $quote_cols->{$print_col} ) =~ s/\sAS\s\S+\z//;   #####
                    $arg->{quote}{order_by_stmt} .= $arg->{col_sep} . $quote_col;
                    $arg->{print}{order_by_stmt} .= $arg->{col_sep} . $print_col;
                    print_select( $arg, $table, $print );
                    # Choose
                    my $direction = choose( [ $ASC, $DESC ], { prompt => 'Choose:', %read_table_style } );
                    if ( not defined $direction ){
                        $arg->{quote}{order_by_stmt} = '';
                        $arg->{print}{order_by_stmt} = '';
                        last ORDER_BY;
                    }
                    $direction =~ s/\A\s+|\s+\z//g;
                    $arg->{quote}{order_by_stmt} .= ' ' . $direction;
                    $arg->{print}{order_by_stmt} .= ' ' . $direction;
                    $arg->{col_sep} = $between_col;
                }
            }
            when( $customize{'limit'} ) {
                $arg->{quote}{limit_args} = [];
                $arg->{quote}{limit_stmt} = " LIMIT";
                $arg->{print}{limit_stmt} = " LIMIT";
                my ( $rows ) = $dbh->selectrow_array( "SELECT COUNT(*)" . $from_stmt, {} );
                my $digits = length $rows;
                $digits = 4 if $digits < 4;
                my ( $only_limit, $offset_and_limit ) = ( 'LIMIT', 'OFFSET-LIMIT' );
                LIMIT: while ( 1 ) {
                    print_select( $arg, $table, $print );
                    # Choose
                    my $choice = choose( [ $arg->{ok}, $only_limit, $offset_and_limit ], { prompt => 'Choose: ', %read_table_style } );
                    if ( not defined $choice ) {
                        $arg->{quote}{limit_args} = [];
                        $arg->{quote}{limit_stmt} = '';
                        $arg->{print}{limit_stmt} = '';
                        last LIMIT;
                    }
                    if ( $choice eq $arg->{ok} ) {
                        if ( not @{$arg->{quote}{limit_args}} ) {
                            $arg->{quote}{limit_stmt} = '';
                            $arg->{print}{limit_stmt} = '';
                        }
                        last LIMIT;
                    }
                    $arg->{quote}{limit_args} = [];
                    $arg->{quote}{limit_stmt} = " LIMIT";
                    $arg->{print}{limit_stmt} = " LIMIT";
                    if ( $choice eq $offset_and_limit ) {
                        print_select( $arg, $table, $print );
                        # Choose
                        my $offset = choose_a_number( $arg, $opt, $digits, 'Compose OFFSET:' );
                        if ( not defined $offset ) {
                            $arg->{quote}{limit_stmt} = '';
                            $arg->{print}{limit_stmt} = '';
                            next LIMIT;
                        }
                        push @{$arg->{quote}{limit_args}}, $offset;
                        $arg->{quote}{limit_stmt} .= ' ' . '?'     . ',';
                        $arg->{print}{limit_stmt} .= ' ' . $offset . ',';
                    }
                    print_select( $arg, $table, $print );
                    # Choose
                    my $limit = choose_a_number( $arg, $opt, $digits, 'Compose LIMIT:' );
                    if ( not defined $limit ) {
                        $arg->{quote}{limit_args} = [];
                        $arg->{quote}{limit_stmt} = '';
                        $arg->{print}{limit_stmt} = '';
                        next LIMIT;
                    }
                    push @{$arg->{quote}{limit_args}}, $limit;
                    $arg->{quote}{limit_stmt} .= ' ' . '?';
                    $arg->{print}{limit_stmt} .= ' ' . $limit;
                }
            }
            when( $customize{'print_table'} ) {
                my $cols_str = '';
                $cols_str = ' ' . join( ', ', @{$arg->{quote}{chosen_columns}} ) if @{$arg->{quote}{chosen_columns}};
                $cols_str = $arg->{print}{group_by_cols} if not @{$arg->{quote}{chosen_columns}} and $arg->{print}{group_by_cols};
                if ( $arg->{quote}{aggregate_stmt} ) {
                    $cols_str .= ',' if $cols_str;
                    $cols_str .= $arg->{quote}{aggregate_stmt};
                }
                $cols_str = $col_default_str if not $cols_str;
                my $select .= "SELECT" . $arg->{quote}{distinct_stmt} . $cols_str . $from_stmt;
                $select .= $arg->{quote}{where_stmt};
                $select .= $arg->{quote}{group_by_stmt};
                $select .= $arg->{quote}{having_stmt};
                $select .= $arg->{quote}{order_by_stmt};
                $select .= $arg->{quote}{limit_stmt};
                my @arguments = ( @{$arg->{quote}{where_args}}, @{$arg->{quote}{having_args}}, @{$arg->{quote}{limit_args}} );
                my $sth = $dbh->prepare( $select );
                $sth->execute( @arguments );
                my $col_names = $sth->{NAME};
                my $col_types = $sth->{TYPE};
                if ( $table eq 'union_tables' and $arg->{db_type} eq 'sqlite' and @{$arg->{quote}{chosen_columns}} ) {
                    $col_names = [ map { s/\A"([^"]+)"\z/$1/; $_ } @$col_names ];
                }
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


#----------------------------------------------------------------------------------------------------#
#-- 777 -----------------------------   prepare print routines   ------------------------------------#
#----------------------------------------------------------------------------------------------------#


sub print_loop {
    my ( $arg, $opt, $dbh, $total_ref, $col_names, $col_types ) = @_;
    my $begin = 0;
    my $end = $opt->{all}{limit}[v] - 1;
    my @choices;
    my $rows = @$total_ref;
    if ( $rows > $opt->{all}{limit}[v] ) {
        my $lr = length $rows;
        push @choices, sprintf "  %${lr}d - %${lr}d  ", $begin, $end;
        $rows -= $opt->{all}{limit}[v];
        while ( $rows > 0 ) {
            $begin += $opt->{all}{limit}[v];
            $end   += ( $rows > $opt->{all}{limit}[v] ) ? $opt->{all}{limit}[v] : $rows;
            push @choices, sprintf "  %${lr}d - %${lr}d  ", $begin, $end;
            $rows -= $opt->{all}{limit}[v];
        }
    }
    my $start;
    my $stop;
    PRINT: while ( 1 ) {
        if ( @choices ) {
            # Choose
            my $choice = choose( [ undef, @choices ], { layout => 3, undef => $arg->{_back} } );
            last PRINT if not defined $choice;
            $start = ( split /\s*-\s*/, $choice )[0];
            $start =~ s/\A\s+//;
            $stop = $start + $opt->{all}{limit}[v] - 1;
            $stop = $#$total_ref if $stop > $#$total_ref;
            print_table( $arg, $opt, [ @{$total_ref}[ $start .. $stop ] ], $col_names, $col_types );
        }
        else {
            print_table( $arg, $opt, $total_ref, $col_names, $col_types );
            last PRINT;
        }
    }
}


sub is_binary_data {
    my ( $db_type ) = @_;
    if ( $db_type eq 'sqlite' ) {
        return sub { return substr( $_[0], 0, 100 ) =~ /\x00/; };
    }
    return sub { return; };
}


sub calc_widths {
    my ( $arg, $opt, $ref, $col_types, $maxcols ) = @_;
    my ( $max_head, $max, $not_a_number );
    my $count = 0;
    say 'Computing: ...';
    my $is_binary_data = is_binary_data( $arg->{db_type} );
    for my $row ( @$ref ) {
        $count++;
        for my $i ( 0 .. $#$row ) {
            $max->[$i] ||= 1;
            $row->[$i] = $opt->{all}{undef}[v] if not defined $row->[$i];
            if ( $count == 1 ) { # column name
                $row->[$i] =~ s/\p{Space}+/ /g;
                $row->[$i] =~ s/\p{Cntrl}//g;
                utf8::upgrade( $row->[$i] );
                my $gcstring = Unicode::GCString->new( $row->[$i] );
                $max_head->[$i] = $gcstring->columns();
            }
            else { # normal row
                if ( $opt->{all}{binary_filter}[v] and ( $col_types->[$i] =~ $arg->{binary_regex} or $is_binary_data->( $row->[$i] ) ) ) {
                    $row->[$i] = $arg->{binary_string};
                    $max->[$i] = $arg->{binary_length} if $arg->{binary_length} > $max->[$i];
                }
                else {
                    $row->[$i] =~ s/\p{Space}+/ /g;
                    $row->[$i] =~ s/\p{Cntrl}//g;
                    utf8::upgrade( $row->[$i] );
                    my $gcstring = Unicode::GCString->new( $row->[$i] );
                    $max->[$i] = $gcstring->columns() if $gcstring->columns() > $max->[$i];
                }
                ++$not_a_number->[$i] if not looks_like_number $row->[$i];
            }
        }
    }
    if ( sum( @$max ) + $opt->{all}{tab}[v] * ( @$max - 1 ) < $maxcols ) { # auto cut
        MAX: while ( 1 ) {
            my $count = 0;
            my $sum = sum( @$max ) + $opt->{all}{tab}[v] * ( @$max - 1 );
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
    return $max, $not_a_number;
}


sub minus_x_percent {
    my ( $value, $percent ) = @_;
    return int $value - ( $value * 1/100 * $percent );
}

sub recalc_widths {
    my ( $arg, $opt, $maxcols, $ref, $col_types ) = @_;
    my ( $max, $not_a_number ) = calc_widths( $arg, $opt, $ref, $col_types, $maxcols );
    return if not defined $max or not @$max;
    my $sum = sum( @$max ) + $opt->{all}{tab}[v] * @$max;
    $sum -= $opt->{all}{tab}[v];
    my @max_tmp = @$max;
    my $percent = 0;
    my $minimum_with = $opt->{all}{min_width}[v];
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
        $sum = sum( @max_tmp ) + $opt->{all}{tab}[v] * ( @max_tmp - 1 );
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


#----------------------------------------------------------------------------------------------------#
#-- 888 ----------------------------------   print routine   ----------------------------------------#
#----------------------------------------------------------------------------------------------------#


sub print_table {
    my ( $arg, $opt, $ref, $col_names, $col_types ) = @_;
    my ( $maxcols, $maxrows ) = GetTerminalSize( *STDOUT );
    return if not defined $ref;
    unshift @$ref, $col_names if defined $col_names;
    my ( $max, $not_a_number ) = recalc_widths( $arg, $opt, $maxcols, $ref, $col_types );
    return if not defined $max;
    my $items = @$ref * @{$ref->[0]};         #
    my $start = 10_000;                       #
    my $total = $#{$ref};                     #
    my $next_update = 0;                      #
    my $c = 0;                                #
    my $progress;                             #
    if ( $items > $start ) {                  #
        print GO_TO_TOP_LEFT;                 #
        print CLEAR_EOS;                      #
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
            my $word = $row->[$i];
            my $right_justify = $not_a_number->[$i] ? 0 : 1;
            $string .= unicode_sprintf(
                        $max->[$i],
                        $word,
                        $right_justify,
            );
            $string .= ' ' x $opt->{all}{tab}[v] if not $i == $#$max;
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
    say 'Computing: ...' if $items > $start * 2;                               #
    # Choose
    choose( \@list, { prompt => 0, layout => 3, clear_screen => 1, length_longest => sum( @$max, $opt->{all}{tab}[v] * $#{$max} ), limit => $opt->{all}{limit}[v] + 1 } );
    return;
}


#----------------------------------------------------------------------------------------------------#
#-- 999 --------------------------------    option routines     -------------------------------------#
#----------------------------------------------------------------------------------------------------#


sub options {
    my ( $arg, $opt ) = @_;
    my @choices = ();
    for my $section ( @{$arg->{option_sections}} ) {
        for my $key ( @{$arg->{$section}{keys}} ) {
            if ( not defined $opt->{$section}{$key}[chs] ) {
                delete $opt->{$section}{$key};
            }
            else {
                push @choices, $opt->{$section}{$key}[chs];
            }
        }
    }
    my $change = 0;
    OPTIONS: while ( 1 ) {
        my ( $exit, $help, $show_settings ) = ( '  EXIT', '  HELP', '  SHOW SETTINGS' );
        # Choose
        my $option = choose( [ $exit, $help, @choices, $show_settings, undef ], { undef => $arg->{_continue}, layout => 3, clear_screen => 1 } );
        my $back = '<<';
        my %number_lyt = ( layout => 1, order => 0, justify => 1, undef => $back );
        my %bol = ( undef => $back, pad_one_row => 2 );
        my ( $true, $false ) = ( 'YES', 'NO' );
        SWITCH: for ( $option ) {
            when ( not defined ) { last OPTIONS; }
            when ( $exit ) { exit() }
            when ( $help ) { help(); choose( [ ' Close with ENTER ' ], { prompt => 0 } ) }
            when ( $show_settings ) {
                my @choices;
                SECTION: for my $section ( @{$arg->{option_sections}} ) {
                    KEY: for my $key ( @{$arg->{$section}{keys}} ) {
                        my $value;
                        if ( not defined $opt->{$section}{$key}[v] ) {
                            $value = 'undef';
                        }
                        elsif ( $section eq 'all' and $key eq 'undef' ) {
                            $value = "\"$opt->{$section}{$key}[v]\"";
                        }
                        elsif ( $section eq 'all' and $key eq 'limit' ) {
                            $value = $opt->{$section}{$key}[v];
                            $value =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{all}{kilo_sep}[v]/g;
                        }
                        elsif ( $section eq 'all' and $key eq 'kilo_sep' ) {
                            $value = 'space " "'      if $opt->{$section}{$key}[v] eq ' ';
                            $value = 'none'           if $opt->{$section}{$key}[v] eq '';
                            $value = 'underscore "_"' if $opt->{$section}{$key}[v] eq '_';
                            $value = 'full stop "."'  if $opt->{$section}{$key}[v] eq '.';
                            $value = 'comma ","'      if $opt->{$section}{$key}[v] eq ',';
                        }
                        elsif ( $section eq 'db_search' and $key eq 'reset_cache'
                            or  $section eq 'all'       and ( $key eq 'binary_filter' or $key eq 'lock_stmt' ) ) {
                            $value = $opt->{$section}{$key}[v] ? 'Yes' : 'No';
                        }
                        else {
                            $value = $opt->{$section}{$key}[v];
                        }
                        my $name = $opt->{$section}{$key}[chs];
                        $name =~ s/\A..//;
                        push @choices, sprintf "%-16s : %s\n", "  $name", $value;
                    }
                }
                choose( [ @choices ], { prompt => 'Close with ENTER', layout => 3 } );
            }
            when ( $opt->{'db_search'}{'reset_cache'}[chs] ) {
                # Choose
                my $choice = choose( [ undef, $true, $false ], { prompt => 'New SQLite DB search [' . ( $opt->{db_search}{reset_cache}[v] ? 'YES' : 'NO' ) . ']:', %bol } );
                next SWITCH if not defined $choice;
                $opt->{db_search}{reset_cache}[v] = ( $choice eq $true ) ? 1 : 0;
                $change++;
            }
            when ( $opt->{'all'}{'tab'}[chs] ) {
                # Choose
                my $number = choose( [ undef, 0 .. 99 ], { prompt => 'Tab width [' . $opt->{all}{tab}[v] . ']:',  %number_lyt } );
                next SWITCH if not defined $number;
                $opt->{all}{tab}[v] = $number;
                $change++;
            }
            when ( $opt->{'all'}{'kilo_sep'}[chs] ) {
                my ( $comma, $full_stop, $underscore, $space, $none ) = ( 'comma', 'full stop', 'underscore', 'space', 'none' );
                my %sep_h = (
                    $comma      => ',',
                    $full_stop  => '.',
                    $underscore => '_',
                    $space      => ' ',
                    $none       => '',
                );
                # Choose
                my $sep = choose( [ undef, $comma, $full_stop, $underscore, $space, $none ], { prompt => 'Thousands separator [' . $opt->{all}{kilo_sep}[v] . ']:',  %bol } );
                next SWITCH if not defined $sep;
                $opt->{all}{kilo_sep}[v] = $sep_h{$sep};
                $change++;
            }
            when ( $opt->{'all'}{'min_width'}[chs] ) {
                # Choose
                my $number = choose( [ undef, 0 .. 99 ], { prompt => 'Minimum Column width [' . $opt->{all}{min_width}[v] . ']:',  %number_lyt } );
                next SWITCH if not defined $number;
                $opt->{all}{min_width}[v] = $number;
                $change++;
            }
            when ( $opt->{'all'}{'limit'}[chs] ) {
                my $number_now = $opt->{all}{limit}[v];
                $number_now =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{all}{kilo_sep}[v]/g;
                my $prompt = 'Compose new "limit" [' . $number_now . ']:';
                # Choose
                my $limit = choose_a_number( $arg, $opt, 7, $prompt );
                next SWITCH if not defined $limit;
                $opt->{all}{limit}[v] = $limit;
                $change++;
            }
            when ( $opt->{'all'}{'binary_filter'}[chs] ) {
                # Choose
                my $choice = choose( [ undef, $true, $false ], { prompt => 'Enable Binary Filter [' . ( $opt->{all}{binary_filter}[v] ? 'YES' : 'NO' ) . ']:', %bol } );
                next SWITCH if not defined $choice;
                $opt->{all}{binary_filter}[v] = ( $choice eq $true ) ? 1 : 0;
                $change++;
            }
            when ( $opt->{'all'}{'undef'}[chs] ) {
                # Read
                my $undef = $term->readline( 'Choose a replacement-string for undefined table vales ["' . $opt->{all}{undef}[v] . '"]: ' );
                next SWITCH if not defined $undef;
                $opt->{all}{undef}[v] = $undef;
                $change++;
            }
            when ( $opt->{'all'}{'lock_stmt'}[chs] ) {
                # Choose
                my ( $lk0, $lk1 ) = ( 'Lk0', 'Lk1' );
                my $choice = choose( [ undef, $lk0, $lk1 ], { prompt => 'Keep statement: set the default value " [' . ( $opt->{all}{lock_stmt}[v] ? $lk1 : $lk0 ) . ']:', %bol } );
                next SWITCH if not defined $choice;
                $opt->{all}{lock_stmt}[v] = ( $choice eq $lk1 ) ? 1 : 0;
                $change++;
            }
            default { die "$option: no such value in the hash \%opt"; }
        }
    }
    if ( $change ) {
        my ( $true, $false ) = ( 'Make changes permanent', 'Use changes only this time' );
        # Choose
        my $permanent = choose( [ $false, $true ], { prompt => 'Modifications:', layout => 3, pad_one_row => 1 } );
        exit if not defined $permanent;
        if ( $permanent eq $true ) {
            write_config_file( $arg->{config_file}, $opt );
        }
    }
    return $opt;
}


sub database_setting {
    my ( $arg, $opt, $database ) = @_;
    my @choices = ();
    for my $section ( $arg->{db_type} ) {
        for my $key ( sort keys %{$opt->{$section}} ) {
            if ( not defined $opt->{$section}{$key}[chs] ) {
                delete $opt->{$section}{$key};
            }
            else {
                push @choices, $opt->{$section}{$key}[chs];
            }
        }
    }
    my $change;
    my ( $true, $false ) = ( 'YES', 'NO' );
    OPTIONS_DB: while ( 1 ) {
        # Choose
        my $option = choose( [ undef, @choices ], { undef => $arg->{_back}, layout => 3, clear_screen => 1 } );
        last OPTIONS_DB if not defined $option;
        my $back = '<<';
        my %number_lyt = ( layout => 1, order => 0, justify => 2, undef => $back );
        my %bol = ( undef => $back, pad_one_row => 2 );

        SWITCH_DB: for ( $option ) {
            when ( $opt->{'sqlite'}{unicode}[chs] ) {
                # Choose
                my $unicode = $opt->{$database}{unicode}[v] // $opt->{$arg->{db_type}}{unicode}[v];
                my $choice = choose( [ undef, $true, $false ], { prompt => 'Unicode [' . ( $unicode ? 'YES' : 'NO' ) . ']:', %bol } );
                next SWITCH_DB if not defined $choice;
                $opt->{$database}{unicode}[v] = ( $choice eq $true ) ? 1 : 0;
                write_config_file( $arg->{config_file}, $opt );
                $change++;
            }
            when ( $opt->{'sqlite'}{see_if_its_a_number}[chs] ) {
                # Choose
                my $see_if_its_a_number = $opt->{$database}{see_if_its_a_number}[v] // $opt->{$arg->{db_type}}{see_if_its_a_number}[v];
                my $choice = choose( [ undef, $true, $false ], { prompt => 'See if its a number [' . ( $see_if_its_a_number ? 'YES' : 'NO' ) . ']:', %bol } );
                next SWITCH_DB if not defined $choice;
                $opt->{$database}{see_if_its_a_number}[v] = ( $choice eq $true ) ? 1 : 0;
                write_config_file( $arg->{config_file}, $opt );
                $change++;
            }
            when ( $opt->{'sqlite'}{busy_timeout}[chs] ) {
                my $busy_timeout = $opt->{$database}{busy_timeout}[v] // $opt->{$arg->{db_type}}{busy_timeout}[v];
                $busy_timeout =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{all}{kilo_sep}[v]/g;
                my $prompt = 'Compose new "Busy timeout (ms)" [' . $busy_timeout . ']:';
                # Choose
                my $new_timeout = choose_a_number( $arg, $opt, 6, $prompt );
                next SWITCH_DB if not defined $new_timeout;
                $opt->{$database}{busy_timeout}[v] = $new_timeout;
                write_config_file( $arg->{config_file}, $opt );
                $change++;
            }
            when ( $opt->{'sqlite'}{cache_size}[chs] ) {
                my $number_now = $opt->{$database}{cache_size}[v] // $opt->{$arg->{db_type}}{cache_size}[v];
                $number_now =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{all}{kilo_sep}[v]/g;
                my $prompt = 'Compose new "Cache size (kb)" [' . $number_now . ']:';
                # Choose
                my $cache_size = choose_a_number( $arg, $opt, 8, $prompt );
                next SWITCH_DB if not defined $cache_size;
                $opt->{$database}{cache_size}[v] = $cache_size;
                write_config_file( $arg->{config_file}, $opt );
                $change++;
            }
            when ( $opt->{'mysql'}{enable_utf8}[chs] ) {
                # Choose
                my $utf8 = $opt->{$database}{enable_utf8}[v] // $opt->{$arg->{db_type}}{enable_utf8}[v];
                my $choice = choose( [ undef, $true, $false ], { prompt => 'Enable utf8 [' . ( $utf8 ? 'YES' : 'NO' ) . ']:', %bol } );
                next SWITCH_DB if not defined $choice;
                $opt->{$database}{enable_utf8}[v] = ( $choice eq $true ) ? 1 : 0;
                write_config_file( $arg->{config_file}, $opt );
                $change++;
            }
            when ( $opt->{'mysql'}{bind_type_guessing}[chs] ) {
                # Choose
                my $bind_type_guessing = $opt->{$database}{bind_type_guessing}[v] // $opt->{$arg->{db_type}}{bind_type_guessing}[v];
                my $choice = choose( [ undef, $true, $false ], { prompt => 'Bind type guessing [' . ( $bind_type_guessing ? 'YES' : 'NO' ) . ']:', %bol } );
                next SWITCH_DB if not defined $choice;
                $opt->{$database}{bind_type_guessing}[v] = ( $choice eq $true ) ? 1 : 0;
                write_config_file( $arg->{config_file}, $opt );
                $change++;
            }
            when ( $opt->{'mysql'}{connect_timeout}[chs] ) {
                my $connect_timeout = $opt->{$database}{connect_timeout}[v] // $opt->{$arg->{db_type}}{connect_timeout}[v];
                $connect_timeout =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{all}{kilo_sep}[v]/g;
                my $prompt = 'Compose new "Busy timeout (s)" [' . $connect_timeout . ']:';
                # Choose
                my $new_timeout = choose_a_number( $arg, $opt, 4, $prompt );
                next SWITCH_DB if not defined $new_timeout;
                $opt->{$database}{connect_timeout}[v] = $new_timeout;
                write_config_file( $arg->{config_file}, $opt );
                $change++;
            }
            default { die "$option: no such value in the hash \%opt"; }
        }
    }
    return $change ? 1 : 0;
}


#----------------------------------------------------------------------------------------------------#
#-- aaa --------------------------------    helper routines     -------------------------------------#
#----------------------------------------------------------------------------------------------------#


sub write_config_file {
    my ( $file, $opt ) = @_;
    my $tmp = {};
    for my $section ( keys %$opt ) {
        for my $key ( keys %{$opt->{$section}} ) {
            $tmp->{$section}{$key} = $opt->{$section}{$key}[v];
        }
    }
    write_json( $file, $tmp );
}


sub read_config_file {
    my ( $file, $opt ) = @_;
    my $tmp = read_json( $file );
    for my $section ( keys %$tmp ) {
        for my $key ( keys %{$tmp->{$section}} ) {
            $opt->{$section}{$key}[v] = $tmp->{$section}{$key};
        }
    }
    return $opt;
}


sub write_json {
    my ( $file, $ref ) = @_;
    my $json = JSON->new->utf8->pretty->encode( $ref );
    open my $fh, '>', $file or die $!;
    print $fh $json;
    close $fh or die $!;
}


sub read_json {
    my ( $file ) = @_;
    return {} if not -f $file;
    my $json;
    {
        local $/ = undef;
        open my $fh, '<', $file or die $!;
        $json = readline $fh;
        close $fh or die $!;
    }
    my $ref = JSON->new->utf8->pretty->decode( $json ) if $json;
    return $ref;
}


sub choose_a_number {
    my ( $arg, $opt, $digits, $prompt ) = @_;
    my %hash;
    my $number;
    my $reset = 'reset';
    NUMBER: while ( 1 ) {
        my $longest = $digits;
        $longest += int( ( $digits - 1 ) / 3 ) if $opt->{all}{kilo_sep}[v] ne '';
        my @list = ();
        for my $di ( 0 .. $digits - 1 ) {
            my $begin = 1 . '0' x $di;
            $begin =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{all}{kilo_sep}[v]/g;
            ( my $end = $begin ) =~ s/\A1/9/;
            unshift @list, sprintf " %*s  -  %*s", $longest, $begin, $longest, $end;
        }
        my $confirm;
        if ( $number ) {
            $confirm = "Confirm: $number";
            push @list, $confirm;
        }
        # Choose
        my $range = choose( [ undef, @list ], { prompt => $prompt, layout => 3, justify => 1, undef => $arg->{back} . ' ' x ( $longest * 2 + 1 ) } );
        return if not defined $range;
        last if $confirm and $range eq $confirm;
        $range = ( split /\s*-\s*/, $range )[0];
        $range =~ s/\A\s*\d//;
        ( my $range_no_sep = $range ) =~ s/\Q$opt->{all}{kilo_sep}[v]\E//g if $opt->{all}{kilo_sep}[v] ne '';
        my $key = length $range_no_sep;
        # Choose
        my $choice = choose( [ undef, map( $_ . $range, 1 .. 9 ), $reset ], { pad_one_row => 2, undef => '<<' } );
        next if not defined $choice;
        if ( $choice eq $reset ) {
            $hash{$key} = 0;
        }
        else {
            $choice =~ s/\Q$opt->{all}{kilo_sep}[v]\E//g if $opt->{all}{kilo_sep}[v] ne '';
            $hash{$key} = $choice;
        }
        $number = sum( @hash{keys %hash} );
        $number =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{all}{kilo_sep}[v]/g;
    }
    $number =~ s/\Q$opt->{all}{kilo_sep}[v]\E//g if $opt->{all}{kilo_sep}[v] ne '';
    return $number;
}


sub unicode_sprintf {
    my ( $length, $unicode, $right_justify ) = @_;
    utf8::upgrade( $unicode );
    my $gcs = Unicode::GCString->new( $unicode );
    my $colwidth = $gcs->columns();
    if ( $colwidth > $length ) {
        my $max_length = int( $length / 2 ) + 1;
        while ( 1 ) {
            my $tmp = substr( $unicode, 0, $max_length );
            my $gcs = Unicode::GCString->new( $tmp );
            $colwidth = $gcs->columns();
            if ( $colwidth > $length ) {
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


__DATA__


