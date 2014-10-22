#!/usr/bin/env perl
use warnings;
use strict;
use 5.10.1;
use utf8;
use open qw(:utf8 :std);

#use warnings FATAL => qw(all);
#use Data::Dumper;
# Version 1.023

use Encode qw(encode_utf8 decode_utf8);
use File::Basename;
use File::Find;
use File::Spec::Functions qw(catfile catdir rel2abs);
use Getopt::Long qw(GetOptions :config bundling);
use List::Util qw(sum);
use Scalar::Util qw(looks_like_number);

use DBI;
use JSON::XS;
use File::HomeDir qw(my_home);
use List::MoreUtils qw(any none first_index pairwise);
use Term::Choose qw(choose);
use Term::ProgressBar;
use Term::ReadKey qw(GetTerminalSize ReadLine ReadMode);
use Text::LineFold;
use Unicode::GCString;

use constant {
    GO_TO_TOP_LEFT => "\e[1;1H",
    CLEAR_EOS      => "\e[0J",
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

# available operators = (
#   "REGEXP", "NOT REGEXP", "LIKE", "NOT LIKE", "IS NULL", "IS NOT NULL", "IN", "NOT IN",
#   "BETWEEN", "NOT BETWEEN", " = ", " != ", " <> ", " < ", " > ", " >= ", " <= "
#)
# available database types = ( 'sqlite', 'mysql', 'postgres' )

my $info = {
    home                => $home,
    config_file         => catfile( $config_dir, 'tw_config.json' ),
    db_cache_file       => catfile( $config_dir, 'tw_cache_db_search.json' ),
    back                => 'BACK',
    ok                  => '- OK -',
    _back               => '  BACK',
    _confirm            => '  CONFIRM',
    _continue           => '  CONTINUE',
    _info               => '  INFO',
    _reset              => '  RESET',
    cached              => '',
    binary_regex        => qr/(?:blob|binary|image|bytea)\z/i,
    aggregate_functions => [ "AVG(X)", "COUNT(X)", "COUNT(*)", "MAX(X)", "MIN(X)", "SUM(X)" ],
    operators           => [ "REGEXP", "NOT REGEXP", " = ", " != ", " < ", " > ", "IS NULL", "IS NOT NULL", "IN" ],
    binary_string       => 'BNRY',
    database_types      => [ 'sqlite', 'mysql', 'postgres' ],
    mysql_user          => undef,
    mysql_passwd        => undef,
    postgres_user       => undef,
    postgres_passwd     => undef,
};

$info->{regexp} = {
    sqlite   => { "REGEXP" => 'REGEXP', "NOT REGEXP" => 'NOT REGEXP' },
    mysql    => { "REGEXP" => 'REGEXP', "NOT REGEXP" => 'NOT REGEXP' },
    postgres => { "REGEXP" => '~*',     "NOT REGEXP" => '!~*' },
};

utf8::upgrade( $info->{binary_string} );
my $gcs = Unicode::GCString->new( $info->{binary_string} );
my $colwidth = $gcs->columns();
$info->{binary_length} = $colwidth;


sub help {
    print << 'HELP';

    Search and read SQLite/MySQL databases.

Usage:
    table_watch_SQLite.pl [-h|--help]
    table_watch_SQLite.pl
    table_watch_SQLite.pl [-s|--search] [-m|--max-depth] [directories to be searched] (SQLite)
    If no directories are passed the home directory is searched for SQLite databases.

Options:
    Help           : Show this Info.
    Settings       : Show settings.
    New search     : Search SQLite databases instead of using cached data. (-s|--search)
    Maxdepth       : Levels to descend at most when searching in directories for SQLite databases.  (-m|--max-depth)
    Limit          : Set the maximum number of table rows printed in one time.
    Binary filter  : Print "BNRY" instead of binary data (printing binary data could break the output).
    Tab            : Set the number of spaces between columns.
    Min-Width      : Set the width the columns should have at least when printed.
    Undef          : Set the string that will be shown on the screen if a table value is undefined.
    Thousands sep  : Choose the thousands separator.
    System info    : If enabled system tables/schemas/databases are appended to the respective list.
    Keep statement : Set the deault value: Lk0 or Lk1.

    Lk0: Reset the SQL-statement after each "PrintTable".
    Lk1: Reset the SQL-statement only when a table is selected.
    To reset a "sub-statement": enter in the "sub-statement" (e.g WHERE) and choose '- OK -'.
    REGEXP is case insensitiv.
    "q" key goes back.

HELP
}


#----------------------------------------------------------------------------------------------------#
#--------------------------------------------   options   -------------------------------------------#
#----------------------------------------------------------------------------------------------------#


use constant {
    v    => 0,
    chs  => 1,
};

my $opt = {
    db_search => {
        reset_cache => [ 0, '- New search' ],
        max_depth   => [ undef, '- Maxdepth' ],
    },
    all => {
        kilo_sep       => [ ',', '- Thousands sep' ],
        binary_filter  => [ 1, '- Binary filter' ],
        limit          => [ 50_000, '- Limit' ],
        min_width      => [ 30, '- Min-Width' ],
        tab            => [ 2, '- Tab' ],
        undef          => [ '', '- Undef' ],
        lock_stmt      => [ 0, '- Keep statement' ],
        system_info    => [ 0, '- System info' ],
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
    postgres => {
        pg_enable_utf8 => [ 1, '- Enable utf8' ],
    }
};

$info->{option_sections} = [ qw( db_search all ) ];
$info->{all}{keys}       = [ qw( kilo_sep binary_filter limit min_width tab undef lock_stmt system_info ) ];
$info->{db_search}{keys} = [ qw( reset_cache max_depth ) ];

for my $section ( @{$info->{option_sections}} ) {
    if ( join( ' ', sort keys %{$opt->{$section}} ) ne join( ' ', sort @{$info->{$section}{keys}} ) ) {
        say join( ' ', sort keys %{$opt->{$section}} );
        say join( ' ', sort      @{$info->{$section}{keys}} );
        die;
    }
}


if ( not eval {
    $opt = read_config_file( $opt, $info->{config_file} );
    my $help;
    GetOptions (
        'h|help'        => \$help,
        's|search'      => \$opt->{db_search}{reset_cache}[v],
        'm|max-depth:i' => \$opt->{db_search}{max_depth}[v],
    );
    $opt = options( $info, $opt ) if $help;
    1 }
) {
    say 'Configfile/Options:';
    print_error_message( $@ );
    choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
}


################################################################################################################
#--------------------------------------------------   MAIN   --------------------------------------------------#
################################################################################################################


if ( @{$info->{database_types}} == 1 ) {
    $info->{db_type} = $info->{database_types}[0];
}
else {
    $info->{db_type} = choose(
        [  @{$info->{database_types}} ],
        { prompt => 'Database Type: ', layout => 1, pad_one_row => 2 }
    );
    exit if not defined $info->{db_type};
    $info->{db_type} =~ s/\A\s|\s\z//g;
}

my $databases = [];
if ( not eval {
    $databases = available_databases( $info, $opt );
    1 }
) {
    say 'Available databases:';
    print_error_message( $@ );
    choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
}
say 'no ' . $info->{db_type} . '-databases found' and exit if not @$databases;

if ( $opt->{all}{system_info}[v] ) {
    my $system_tables = system_tables( $info );
    my @system;
    for my $system_table ( @$system_tables ) {
        my $i = first_index{ $_ eq $system_table } @$databases;
        push @system, splice( @$databases, $i, 1 );
    }
    @$databases = (              @$databases  ,              @system   ) if $info->{db_type} eq 'sqlite';
    @$databases = ( map( "- $_", @$databases ), map( "  $_", @system ) ) if $info->{db_type} ne 'sqlite';
}
else {
    @$databases = map{ "- $_" } @$databases if $info->{db_type} ne 'sqlite';
}

my %layout = ( layout => 3, clear_screen => 1 );
my $new_db_settings = 0;
my $db;
my $data = {};

DATABASES: while ( 1 ) {

    if ( not $new_db_settings ) {
        # Choose
        $db = choose(
            [ undef, @$databases ],
            { prompt => 'Choose Database' . $info->{cached}, %layout, undef => 'QUIT' }
        );
        last DATABASES if not defined $db;
        $db =~ s/\A[-\ ]\s// if $info->{db_type} ne 'sqlite';
        #$data = {};
    }
    else {
        $new_db_settings = 0;
    }
    my $dbh;
    if ( not eval {
        $dbh = get_db_handle( $info, $opt, $db );
        $data->{$db}{schema_names} = get_schema_names( $dbh, $db ) if not defined $data->{$db}{schema_names};
        1 }
    ) {
        say 'Get database handle and schema names:';
        print_error_message( $@ );
        choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
        # remove database from @databases
        next DATABASES;
    }

    SCHEMA: while ( 1 ) {

        my $schema = undef;
        if ( defined $data->{$db}{schema_names} ) {
            if ( @{$data->{$db}{schema_names}} == 1 ) {
                $schema = $data->{$db}{schema_names}[0];
            }
            elsif ( @{$data->{$db}{schema_names}} > 1 ) {
                if ( $opt->{all}{system_info}[v] and $info->{db_type} eq 'postgres' ) {
                    my @system;
                    my @normal;
                    for my $schema ( @{$data->{$db}{schema_names}} ) {
                        if ( $schema =~ /\Apg_/ or $schema eq 'information_schema' ) {
                            push @system, $schema;
                        }
                        else {
                            push @normal, $schema;
                        }
                    }
                    # Choose
                    $schema = choose(
                        [ undef, map( "- $_", @normal ), map( "  $_", @system ) ],
                        { prompt => 'DB "'. basename( $db ) . '" - choose Schema:', %layout, undef => $info->{_back} }
                    );
                }
                else {
                    # Choose
                    $schema = choose(
                        [ undef, map( "- $_", @{$data->{$db}{schema_names}} ) ],
                        { prompt => 'DB "'. basename( $db ) . '" - choose Schema:', %layout, undef => $info->{_back} }
                    );
                }
                next DATABASES if not defined $schema;
                $schema =~ s/\A[-\ ]\s//;
            }
        }
        if ( not eval {
            $data->{$db}{$schema}{tables} = get_table_names( $dbh, $db, $schema ) if not defined $data->{$db}{$schema}{tables};
            1 }
        ) {
            say 'Get table names:';
            print_error_message( $@ );
            choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
            next DATABASES;
        }

        my $join_tables  = '  Join';
        my $union_tables = '  Union';
        my $db_setting   = '  Database settings';
        my @tables = ();
        push @tables, map { "- $_" } @{$data->{$db}{$schema}{tables}};
        push @tables, '  sqlite_master' if $info->{db_type} eq 'sqlite' and $opt->{all}{system_info}[v];

        TABLES: while ( 1 ) {

            my $prompt = 'DB: "'. basename( $db );
            $prompt .= '.' . $schema if defined $data->{$db}{schema_names} and @{$data->{$db}{schema_names}} > 1;
            $prompt .= '"';
            # Choose
            my $table = choose(
                [ undef, @tables, $join_tables, $union_tables, $db_setting ],
                { prompt => $prompt, %layout, undef => $info->{_back} }
            );
            if ( not defined $table ) {
                next SCHEMA  if defined $data->{$db}{schema_names} and @{$data->{$db}{schema_names}} > 1;
                next DATABASES;
            }
            my $select_from_stmt = '';
            my $print_and_quote_cols = [];
            if ( $table eq $db_setting ) {
                if ( not eval {
                    $new_db_settings = database_setting( $info, $opt, $db );
                    1 }
                ) {
                    say 'Database settings:';
                    print_error_message( $@ );
                    choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
                }
                next DATABASES if $new_db_settings;
                next TABLES;
            }
            elsif ( $table eq $join_tables ) {
                if ( not eval {
                    ( $select_from_stmt, $print_and_quote_cols ) = join_tables( $info, $dbh, $db, $schema, $data );
                    $table = 'joined_tables';
                    1 }
                ) {
                    say 'Join tables:';
                    print_error_message( $@ );
                    choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
                }
                next TABLES if not defined $select_from_stmt;
            }
            elsif ( $table eq $union_tables ) {
                if ( not eval {
                    ( $select_from_stmt, $print_and_quote_cols ) = union_tables( $info, $dbh, $db, $schema, $data );
                    $table = 'union_tables';
                    1 }
                ) {
                    say 'Union tables:';
                    print_error_message( $@ );
                    choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
                }
                next TABLES if not defined $select_from_stmt;
            }
            else {
                $table =~ s/\A[-\ ]\s//;
            }
            if ( not eval {
                my $sql;
                $sql->{stmt_keys} = [ qw( distinct_stmt group_by_cols aggregate_stmt where_stmt group_by_stmt having_stmt order_by_stmt limit_stmt ) ];
                $sql->{list_keys} = [ qw( chosen_columns aliases where_args having_args limit_args ) ];
                @{$sql->{print}}{ @{$sql->{stmt_keys}} } = ( '' ) x @{$sql->{stmt_keys}};
                @{$sql->{quote}}{ @{$sql->{stmt_keys}} } = ( '' ) x @{$sql->{stmt_keys}};
                @{$sql->{print}}{ @{$sql->{list_keys}} } = ( [] ) x @{$sql->{list_keys}};
                @{$sql->{quote}}{ @{$sql->{list_keys}} } = ( [] ) x @{$sql->{list_keys}};

                $info->{lock} = $opt->{all}{lock_stmt}[v];

                my $from_stmt        = '';
                my $default_cols_sql = '';
                my $quote_cols       = {};
                my $print_cols       = [];
                if ( $select_from_stmt ) {
                    if ( $select_from_stmt =~ /\ASELECT\s(.*?)(\sFROM\s.*)\z/ ) {
                        $default_cols_sql = $1;
                        $from_stmt        = $2;
                        for my $ref ( @$print_and_quote_cols ) {
                            $quote_cols->{$ref->[0]} = $ref->[1];
                            push @$print_cols, $ref->[0];
                        }
                    } else { die $select_from_stmt }
                }
                else {
                    $default_cols_sql = ' *';
                    my $table_q = quote_table( $info, $dbh, $schema, $table );
                    $from_stmt = " FROM $table_q";
                    my $sth = $dbh->prepare( "SELECT *" . $from_stmt );
                    $sth->execute();
                    for my $col ( @{$sth->{NAME}} ) {
                        $quote_cols->{$col} = $dbh->quote_identifier( $col );
                        push @$print_cols, $col;
                    }
                }

                MAIN_LOOP: while ( 1 ) {
                    my ( $all_arrayref, $print_cols, $col_types ) = read_table( $info, $opt, $sql, $dbh, $table, $from_stmt, $default_cols_sql, $quote_cols, $print_cols );
                    last MAIN_LOOP if not defined $all_arrayref;
                    print_loop( $info, $opt, $dbh, $all_arrayref, $print_cols, $col_types );
                }

                1 }
            ) {
                say 'Print table:';
                print_error_message( $@ );
                choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
            }
        }
    }
}


#----------------------------------------------------------------------------------------------------#
#-- 111 ---------------------------------   union routines   ----------------------------------------#
#----------------------------------------------------------------------------------------------------#


sub union_tables {
    my ( $info, $dbh, $db, $schema, $data ) = @_;
    if ( not defined $data->{$db}{$schema}{col_names} or not defined $data->{$db}{$schema}{col_types} ) {
        $data = column_names_and_types( $info, $dbh, $db, $schema, $data );
    }
    my $enough_tables = '  Enough TABLES';
    my @tables_unused = map { "- $_" } @{$data->{$db}{$schema}{tables}};
    my $used_tables   = [];
    my $cols          = {};
    my $saved_columns = [];

    UNION_TABLE: while ( 1 ) {
        print_union_statement( $used_tables, $cols );
        # Choose
        my $union_table = choose(
            [ undef, map( "+ $_", @$used_tables ), @tables_unused, $info->{_info}, $enough_tables ],
            { prompt => 'Choose UNION table:', layout => 3, undef => $info->{_back} }
        );
        return if not defined $union_table;
        if ( $union_table eq $info->{_info} ) {
            $data->{$db}{$schema}{tables_info} = get_tables_info( $info, $dbh, $db, $schema, $data ) if not defined $data->{$db}{$schema}{tables_info};
            choose( $data->{$db}{$schema}{tables_info}, { prompt => 0, layout => 3, clear_screen => 1 } );
            next UNION_TABLE;
        }
        if ( $union_table eq $enough_tables ) {
            last UNION_TABLE;
        }
        my $idx = first_index { $_ eq $union_table } @tables_unused;
        $union_table =~ s/\A[-+\ ]\s//;
        if ( $idx == -1 ) {
            delete $cols->{$union_table};
        }
        else {
            splice( @tables_unused, $idx, 1 );
            push @{$used_tables}, $union_table;
        }

        UNION_COLUMNS: while ( 1 ) {
            my $all_cols      = q['*'];
            my $privious_cols = q['^'];
            my $void          = q[' '];
            my @short_cuts = ( ( @$saved_columns ? $privious_cols : $void ), $all_cols );
            print_union_statement( $used_tables, $cols );
            # Choose
            my $col = choose(
                [ $info->{ok}, @short_cuts, @{$data->{$db}{$schema}{col_names}{$union_table}} ],
                { prompt => 'Choose Column: ', layout => 1, pad_one_row => 2 }
            );
            if ( not defined $col ) {
                delete $cols->{$union_table};
                my $idx = first_index { $_ eq $union_table } @$used_tables;
                my $tbl = splice( @$used_tables, $idx, 1 );
                push @tables_unused, "- $tbl";
                last UNION_COLUMNS;
            }
            if ( $col eq $info->{ok} ) {
                if ( not exists $cols->{$union_table} ) {
                    my $idx = first_index { $_ eq $union_table } @$used_tables;
                    my $tbl = splice( @$used_tables, $idx, 1 );
                    push @tables_unused, "- $tbl";
                }
                last UNION_COLUMNS;
            }
            if ( $col eq $void ) {
                next UNION_COLUMNS;
            }
            if ( $col eq $privious_cols ) {
                $cols->{$union_table} = $saved_columns;
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
        $saved_columns = $cols->{$union_table};
    }
    my $print_and_quote_cols = [];
    my $union_statement = "SELECT * FROM (";
    my $count = 0;
    for my $table ( @$used_tables ) {
        $count++;
        if ( $count == 1 ) { 
            # column names in the result-set of a UNION are taken from the first query.
            if ( ${$cols->{$table}}[0] eq '*' ) {
                for my $col ( @{$data->{$db}{$schema}{col_names}{$table}} ) {
                    push @$print_and_quote_cols, [ $col, $dbh->quote_identifier( $col ) ];
                }
            }
            else {
                for my $col ( @{$cols->{$table}} ) {
                    push @$print_and_quote_cols, [ $col, $dbh->quote_identifier( $col ) ];
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
        $union_statement .= " FROM " . quote_table( $info, $dbh, $schema, $table );
        $union_statement .= ( $count < @$used_tables ? " UNION ALL " : " )" );
    }
    my $derived_table_name = join '_', @$used_tables;
    $union_statement .= " AS $derived_table_name";
    return $union_statement, $print_and_quote_cols;
}


sub print_union_statement {
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
#-- 222 ---------------------------------   join routines   -----------------------------------------#
#----------------------------------------------------------------------------------------------------#


sub get_tables_info {
    my ( $info, $dbh, $db, $schema, $data ) = @_;
    my %print_hash;
    my $sth; 
    my ( $pk, $fk ) = primary_and_foreign_keys( $info, $dbh, $db, $schema, $data );
    for my $table ( @{$data->{$db}{$schema}{tables}}  ) {
        push @{$print_hash{$table}}, [ 'Table: ', '== ' . $table . ' ==' ];
        push @{$print_hash{$table}}, [ 
            'Columns: ',
            join( ' | ', pairwise { no warnings q(once); lc( $a ) . ' ' . $b }
                @{$data->{$db}{$schema}{col_types}{$table}},
                @{$data->{$db}{$schema}{col_names}{$table}} 
            ) 
        ];
        push @{$print_hash{$table}}, [ 'PK: ', 'primary key (' . join( ',', @{$pk->{$table}} ) . ')' ] if @{$pk->{$table}};  
        for my $fk_name ( sort keys %{$fk->{$table}} ) {
            push @{$print_hash{$table}}, [ 
                'FK: ',
                'foreign key (' . join( ',', @{$fk->{$table}{$fk_name}{foreign_key_col}} ) . ') references ' . 
                $fk->{$table}{$fk_name}{reference_table} . '(' . join( ',', @{$fk->{$table}{$fk_name}{reference_key_col}} ) .')'
            ] if $fk->{$table}{$fk_name};
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
    my $tables_info = [];
    push @{$tables_info}, 'Close with ENTER';
    for my $table ( @{$data->{$db}{$schema}{tables}} ) {    
        push @{$tables_info}, " ";
        for my $line ( @{$print_hash{$table}} ) {
            my $key = $line->[0];
            my $text = $line_fold->fold( '' , '', $line->[1] );
            for my $row ( split /\R+/, $text ) {
                push @{$tables_info}, sprintf "%${longest}s%s", $key, $row;
                $key = '' if $key;
            }
        }
    }
    return $tables_info;
}


sub join_tables {
    my ( $info, $dbh, $db, $schema, $data ) = @_;
    if ( not defined $data->{$db}{$schema}{col_names} or not defined $data->{$db}{$schema}{col_types} ) {
        $data = column_names_and_types( $info, $dbh, $db, $schema, $data );
    }
    my $join_statement_quote = "SELECT * FROM";
    my $join_statement_print = "SELECT * FROM";
    my @tables = map { "- $_" } @{$data->{$db}{$schema}{tables}};
    my $mastertable;

    MASTER: while ( 1 ) {
        print_join_statement( $join_statement_print );
        # Choose
        $mastertable = choose(
            [ undef, @tables, $info->{_info} ],
            { prompt => 'Choose MASTER table:', layout => 3, undef => $info->{_back} }
        );
        return if not defined $mastertable;
        if ( $mastertable eq $info->{_info} ) {
            $data->{$db}{$schema}{tables_info} = get_tables_info( $info, $dbh, $db, $schema, $data ) if not defined $data->{$db}{$schema}{tables_info};
            choose( $data->{$db}{$schema}{tables_info}, { prompt => 0, layout => 3, clear_screen => 1 } );
            next MASTER;
        }
        last MASTER;
    }
    my $idx = first_index { $_ eq $mastertable } @tables;
    splice( @tables, $idx, 1 );
    $mastertable =~ s/\A[-\ ]\s//;
    my @used_tables = ( $mastertable );
    my @available_tables = @tables;
    my $mastertable_q = quote_table( $info, $dbh, $schema, $mastertable );
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
            print_join_statement( $join_statement_print );
            # Choose
            $slave_table = choose(
                [ undef, @available_tables, $info->{_info}, $enough_slaves ],
                { prompt => 'Add a SLAVE table:', layout => 3, undef => $info->{_reset} }
            );
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
            if ( $slave_table eq $info->{_info} ) {
                $data->{$db}{$schema}{tables_info} = get_tables_info( $info, $dbh, $db, $schema, $data ) if not defined $data->{$db}{$schema}{tables_info};
                choose( $data->{$db}{$schema}{tables_info}, { prompt => 0, layout => 3, clear_screen => 1 } );
                next SLAVE;
            }
            last SLAVE;
        }
        my $idx = first_index { $_ eq $slave_table } @available_tables;
        splice( @available_tables, $idx, 1 );
        $slave_table =~ s/\A[-\ ]\s//;
        my $slave_table_q = quote_table( $info, $dbh, $schema, $slave_table );
        $join_statement_quote .= " LEFT OUTER JOIN " . $slave_table_q . " ON";
        $join_statement_print .= " LEFT OUTER JOIN " . $slave_table   . " ON";
        my %avail_primary_key_cols = ();
        for my $used_table ( @used_tables ) {
            for my $col ( @{$data->{$db}{$schema}{col_names}{$used_table}} ) {
                $avail_primary_key_cols{"$used_table.$col"} = $dbh->quote_identifier( undef, $used_table, $col );
            }
        }
        my %avail_foreign_key_cols = ();
        for my $col ( @{$data->{$db}{$schema}{col_names}{$slave_table}} ) {
            $avail_foreign_key_cols{"$slave_table.$col"} = $dbh->quote_identifier( undef, $slave_table, $col );
        }
        my $AND = '';

        ON: while ( 1 ) {
            print_join_statement( $join_statement_print );
            # Choose
            my $pk_col = choose(
                [ undef, map( "- $_", sort keys %avail_primary_key_cols ), $info->{_continue} ],
                { prompt => 'Choose PRIMARY KEY column:', layout => 3, undef => $info->{_reset} }
            );
            if ( not defined $pk_col ) {
                @primary_keys         = @old_primary_keys;
                @foreign_keys         = @old_foreign_keys;
                $join_statement_quote = $old_stmt_quote;
                $join_statement_print = $old_stmt_print;
                @used_tables          = @old_used_tables;
                @available_tables     = @old_avail_tables;
                next SLAVE_TABLES;
            }
            if ( $pk_col eq $info->{_continue} ) {
                if ( @primary_keys == @old_primary_keys ) {
                    $join_statement_quote = $old_stmt_quote;
                    $join_statement_print = $old_stmt_print;
                    @used_tables          = @old_used_tables;
                    @available_tables     = @old_avail_tables;
                    next SLAVE_TABLES;
                }
                last ON;
            }
            $pk_col =~ s/\A[-\ ]\s//;
            push @primary_keys, $avail_primary_key_cols{$pk_col};
            $join_statement_quote .= $AND;
            $join_statement_print .= $AND;
            $join_statement_quote .= ' ' . $avail_primary_key_cols{$pk_col} . " =";
            $join_statement_print .= ' ' . $pk_col                          . " =";
            print_join_statement( $join_statement_print );
            # Choose
            my $fk_col = choose(
                [ undef, map{ "- $_" } sort keys %avail_foreign_key_cols ],
                { prompt => 'Choose FOREIGN KEY column:', layout => 3, undef => $info->{_reset} }
            );
            if ( not defined $fk_col ) {
                @primary_keys         = @old_primary_keys;
                @foreign_keys         = @old_foreign_keys;
                $join_statement_quote = $old_stmt_quote;
                $join_statement_print = $old_stmt_print;
                @used_tables          = @old_used_tables;
                @available_tables     = @old_avail_tables;
                next SLAVE_TABLES;
            }
            $fk_col =~ s/\A[-\ ]\s//;
            push @foreign_keys, $avail_foreign_key_cols{$fk_col};
            $join_statement_quote .= ' ' . $avail_foreign_key_cols{$fk_col};
            $join_statement_print .= ' ' . $fk_col;
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
    for my $table ( keys %{$data->{$db}{$schema}{col_names}} ) {
        next if none { $_ eq $table } @used_tables;
        for my $col ( @{$data->{$db}{$schema}{col_names}{$table}} ) {
            $seen{$col}++;
            push @dup, $col if $seen{$col} == 2;
        }
    }
    my @columns_sql;
    my $print_and_quote_cols;
    for my $table ( @{$data->{$db}{$schema}{tables}} ) {
        next if none { $_ eq $table } @used_tables;
        for my $col ( @{$data->{$db}{$schema}{col_names}{$table}} ) {
            my $tbl_col_q = $dbh->quote_identifier( undef, $table, $col );
            next if any { $_ eq $tbl_col_q } @foreign_keys;
            my ( $alias, $col_sql );
            if ( any { $_ eq $col } @dup ) {
                #if ( any { $_ eq $tbl_col_q } @primary_keys ) {
                #    $alias = $col . '_PK';
                #}
                #else {
                    $alias = $col . '_' . substr $table, 0, $length_uniq;
                #}
                $col_sql = $tbl_col_q . " AS " . $alias;
            }
            else {
                $alias = $col;
                $col_sql = $tbl_col_q;
            }
            push @columns_sql, $col_sql;
            push @$print_and_quote_cols , [ $alias, $col_sql ];
        }
    }
    my $col_statement = join ', ', @columns_sql;
    $join_statement_quote =~ s/\s\*\s/ $col_statement /;
    return $join_statement_quote, $print_and_quote_cols;
}


sub print_join_statement {
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
#-- 333 -------------------------------   statement routine   ---------------------------------------#
#----------------------------------------------------------------------------------------------------#


sub print_select_statement {
    my ( $sql, $table ) = @_;
    my $cols_sql = '';
    $cols_sql = ' '. join( ', ', @{$sql->{print}{chosen_columns}} ) if @{$sql->{print}{chosen_columns}};
    $cols_sql = $sql->{print}{group_by_cols} if not @{$sql->{print}{chosen_columns}} and $sql->{print}{group_by_cols};
    if ( $sql->{print}{aggregate_stmt} ) {
        $cols_sql .= ',' if $cols_sql;
        $cols_sql .= $sql->{print}{aggregate_stmt};
    }
    $cols_sql = ' *' if not $cols_sql;
    print GO_TO_TOP_LEFT;
    print CLEAR_EOS;
    print "SELECT";
    print $sql->{print}{distinct_stmt} if $sql->{print}{distinct_stmt};
    say $cols_sql;
    say " FROM $table";
    say $sql->{print}{where_stmt}    if $sql->{print}{where_stmt};
    say $sql->{print}{group_by_stmt} if $sql->{print}{group_by_stmt};
    say $sql->{print}{having_stmt}   if $sql->{print}{having_stmt};
    say $sql->{print}{order_by_stmt} if $sql->{print}{order_by_stmt};
    say $sql->{print}{limit_stmt}    if $sql->{print}{limit_stmt};
    say "";
}


sub read_table {
    my ( $info, $opt, $sql, $dbh, $table, $from_stmt, $default_cols_sql, $quote_cols, $print_cols  ) = @_;
    my %setup_stmt = ( layout => 1, order => 0, justify => 2, pad_one_row => 2 );
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
        lock            => $lock->[$info->{lock}],
    );
    my ( $DISTINCT, $ALL, $ASC, $DESC, $AND, $OR ) = ( "DISTINCT", "ALL", "ASC", "DESC", "AND", "OR" );
    if ( $info->{lock} == 0 ) {
        delete @{$quote_cols}{@{$sql->{quote}{aliases}}};
        @{$sql->{print}}{ @{$sql->{stmt_keys}} } = ( '' ) x @{$sql->{stmt_keys}};
        @{$sql->{quote}}{ @{$sql->{stmt_keys}} } = ( '' ) x @{$sql->{stmt_keys}};
        @{$sql->{print}}{ @{$sql->{list_keys}} } = ( [] ) x @{$sql->{list_keys}};
        @{$sql->{quote}}{ @{$sql->{list_keys}} } = ( [] ) x @{$sql->{list_keys}};
    }

    CUSTOMIZE: while ( 1 ) {
        print_select_statement( $sql, $table );
        # Choose
        my $custom = choose(
            [ undef, @customize{@keys} ],
            { prompt => 'Customize:', layout => 3, undef => $info->{back} }
        );
        for ( $custom ) {
            when ( not defined ) {
                last CUSTOMIZE;
            }
            when ( $customize{'lock'} ) {
                if ( $info->{lock} == 1 ) {
                    $info->{lock} = 0;
                    $customize{lock} = $lock->[0];
                    delete @{$quote_cols}{@{$sql->{quote}{aliases}}};
                    @{$sql->{print}}{ @{$sql->{stmt_keys}} } = ( '' ) x @{$sql->{stmt_keys}};
                    @{$sql->{quote}}{ @{$sql->{stmt_keys}} } = ( '' ) x @{$sql->{stmt_keys}};
                    @{$sql->{print}}{ @{$sql->{list_keys}} } = ( [] ) x @{$sql->{list_keys}};
                    @{$sql->{quote}}{ @{$sql->{list_keys}} } = ( [] ) x @{$sql->{list_keys}};
                }
                elsif ( $info->{lock} == 0 )   {
                    $info->{lock} = 1;
                    $customize{lock} = $lock->[1];
                }
            }
            when( $customize{'columns'} ) {
                my @cols = @$print_cols;
                $sql->{quote}{chosen_columns} = [];
                $sql->{print}{chosen_columns} = [];

                COLUMNS: while ( 1 ) {
                    print_select_statement( $sql, $table );
                    # Choose
                    my $print_col = choose(
                        [ $info->{ok}, @cols ],
                        { prompt => 'Choose: ', %setup_stmt }
                    );
                    if ( not defined $print_col ) {
                        $sql->{quote}{chosen_columns} = [];
                        $sql->{print}{chosen_columns} = [];
                        last COLUMNS;
                    }
                    if ( $print_col eq $info->{ok} ) {
                        last COLUMNS;
                    }
                    push @{$sql->{quote}{chosen_columns}}, $quote_cols->{$print_col};
                    push @{$sql->{print}{chosen_columns}}, $print_col;
                }
            }
            when( $customize{'distinct'} ) {
                $sql->{quote}{distinct_stmt} = '';
                $sql->{print}{distinct_stmt} = '';

                DISTINCT: while ( 1 ) {
                    print_select_statement( $sql, $table );
                    # Choose
                    my $select_distinct = choose(
                        [ $info->{ok}, $DISTINCT, $ALL ],
                        { prompt => 'Choose: ', %setup_stmt }
                    );
                    if ( not defined $select_distinct ) {
                        $sql->{quote}{distinct_stmt} = '';
                        $sql->{print}{distinct_stmt} = '';
                        last DISTINCT;
                    }
                    if ( $select_distinct eq $info->{ok} ) {
                        last DISTINCT;
                    }
                    $select_distinct =~ s/\A\s+|\s+\z//g;
                    $sql->{quote}{distinct_stmt} = ' ' . $select_distinct;
                    $sql->{print}{distinct_stmt} = ' ' . $select_distinct;
                }
            }
            when ( $customize{'where'} ) {
                my @cols = @$print_cols;
                my $AND_OR = '';
                $sql->{quote}{where_args} = [];
                $sql->{quote}{where_stmt} = " WHERE";
                $sql->{print}{where_stmt} = " WHERE";
                my $count = 0;

                WHERE: while ( 1 ) {
                    print_select_statement( $sql, $table );
                    # Choose
                    my $print_col = choose(
                        [ $info->{ok}, @cols ],
                        { prompt => 'Choose: ', %setup_stmt }
                    );
                    if ( not defined $print_col ) {
                        $sql->{quote}{where_args} = [];
                        $sql->{quote}{where_stmt} = '';
                        $sql->{print}{where_stmt} = '';
                        last WHERE;
                    }
                    if ( $print_col eq $info->{ok} ) {
                        if ( $count == 0 ) {
                            $sql->{quote}{where_stmt} = '';
                            $sql->{print}{where_stmt} = '';
                        }
                        last WHERE;
                    }
                    if ( $count > 0 ) {
                        print_select_statement( $sql, $table );
                        # Choose
                        $AND_OR = choose(
                            [ $AND, $OR ],
                            { prompt => 'Choose:', %setup_stmt }
                        );
                        if ( not defined $AND_OR ) {
                            $sql->{quote}{where_args} = [];
                            $sql->{quote}{where_stmt} = '';
                            $sql->{print}{where_stmt} = '';
                            last WHERE;
                        }
                        $AND_OR =~ s/\A\s+|\s+\z//g;
                        $AND_OR = ' ' . $AND_OR;
                    }
                    ( my $quote_col = $quote_cols->{$print_col} ) =~ s/\sAS\s\S+\z//;
                    $sql->{quote}{where_stmt} .= $AND_OR . ' ' . $quote_col;
                    $sql->{print}{where_stmt} .= $AND_OR . ' ' . $print_col;
                    $sql = operator_sql( $info, $sql, 'where', $table, \@cols, $quote_cols, \%setup_stmt );
                    $count++;
                }
            }
            when( $customize{'aggregate'} ) {
                my @cols = @$print_cols;
                $info->{col_sep} = ' ';
                delete @{$quote_cols}{@{$sql->{quote}{aliases}}};
                $sql->{quote}{aliases}        = [];
                $sql->{quote}{aggregate_stmt} = '';
                $sql->{print}{aggregate_stmt} = '';

                AGGREGATE: while ( 1 ) {
                    print_select_statement( $sql, $table );
                    # Choose
                    my $func = choose(
                        [ $info->{ok}, @{$info->{aggregate_functions}} ],
                        { prompt => 'Choose:', %setup_stmt }
                    );
                    if ( not defined $func ) {
                        delete @{$quote_cols}{@{$sql->{quote}{aliases}}};
                        $sql->{quote}{aliases}        = [];
                        $sql->{quote}{aggregate_stmt} = '';
                        $sql->{print}{aggregate_stmt} = '';
                        last AGGREGATE;
                    }
                    if ( $func eq $info->{ok} ) {
                        last AGGREGATE;
                    }
                    my ( $print_col, $quote_col );
                    if ( $func =~ /\Acount\s*\(\s*\*\s*\)\z/i ) {
                        $print_col = '*';
                        $quote_col = '*';
                    }
                    $func =~ s/\s*\(\s*\S\s*\)\z//;
                    $sql->{quote}{aggregate_stmt} .= $info->{col_sep} . $func . '(';
                    $sql->{print}{aggregate_stmt} .= $info->{col_sep} . $func . '(';
                    if ( not defined $print_col ) {
                        print_select_statement( $sql, $table );
                        # Choose
                        $print_col = choose(
                            [ @cols ],
                            { prompt => 'Choose:', %setup_stmt }
                        );
                        if ( not defined $print_col ) {
                            delete @{$quote_cols}{@{$sql->{quote}{aliases}}};
                            $sql->{quote}{aliases}        = [];
                            $sql->{quote}{aggregate_stmt} = '';
                            $sql->{print}{aggregate_stmt} = '';
                            last AGGREGATE;
                        }
                        ( $quote_col = $quote_cols->{$print_col} ) =~ s/\sAS\s\S+\z//;
                    }
                    my $alias = '@' . $func . '_' . $print_col; # ( $print_col eq '*' ? 'ROWS' : $print_col );
                    $sql->{quote}{aggregate_stmt} .= $quote_col . ') AS ' . $dbh->quote_identifier( $alias );
                    $sql->{print}{aggregate_stmt} .= $print_col . ') AS ' .                         $alias  ;
                    $quote_cols->{$alias} = $func . '(' . $quote_col . ')';
                    push @{$sql->{quote}{aliases}}, $alias;
                    $info->{col_sep} = ', ';
                }
            }
            when( $customize{'group_by'} ) {
                my @cols = @$print_cols;
                $info->{col_sep} = ' ';
                $sql->{quote}{group_by_stmt} = " GROUP BY";
                $sql->{print}{group_by_stmt} = " GROUP BY";
                $sql->{quote}{group_by_cols} = '';
                $sql->{print}{group_by_cols} = '';

                GROUP_BY: while ( 1 ) {
                    print_select_statement( $sql, $table );
                    # Choose
                    my $print_col = choose(
                        [ $info->{ok}, @cols ],
                        { prompt => 'Choose: ', %setup_stmt }
                    );
                    if ( not defined $print_col ) {
                        $sql->{quote}{group_by_stmt} = '';
                        $sql->{print}{group_by_stmt} = '';
                        $sql->{quote}{group_by_cols} = '';
                        $sql->{print}{group_by_cols} = '';
                        last GROUP_BY;
                    }
                    if ( $print_col eq $info->{ok} ) {
                        if ( $info->{col_sep} eq ' ' ) {
                            $sql->{quote}{group_by_stmt} = '';
                            $sql->{print}{group_by_stmt} = '';
                        }
                        last GROUP_BY;
                    }
                    ( my $quote_col = $quote_cols->{$print_col} ) =~ s/\sAS\s\S+\z//;
                    if ( not @{$sql->{quote}{chosen_columns}} ) {
                        $sql->{quote}{group_by_cols} .= $info->{col_sep} . $quote_col;
                        $sql->{print}{group_by_cols} .= $info->{col_sep} . $print_col;
                    }
                    $sql->{quote}{group_by_stmt} .= $info->{col_sep} . $quote_col;
                    $sql->{print}{group_by_stmt} .= $info->{col_sep} . $print_col;
                    $info->{col_sep} = ', ';
                }
            }
            when( $customize{'having'} ) {
                my @cols = @$print_cols;
                my $AND_OR = '';
                $sql->{quote}{having_args} = [];
                $sql->{quote}{having_stmt} = " HAVING";
                $sql->{print}{having_stmt} = " HAVING";
                my $count = 0;

                HAVING: while ( 1 ) {
                    print_select_statement( $sql, $table );
                    # Choose
                    my $func = choose(
                        [ $info->{ok}, @{$info->{aggregate_functions}}, @{$sql->{quote}{aliases}} ],
                        { prompt => 'Choose:', %setup_stmt }
                    );
                    if ( not defined $func ) {
                        $sql->{quote}{having_args} = [];
                        $sql->{quote}{having_stmt} = '';
                        $sql->{print}{having_stmt} = '';
                        last HAVING;
                    }
                    if ( $func eq $info->{ok} ) {
                        if ( $count == 0 ) {
                            $sql->{quote}{having_stmt} = '';
                            $sql->{print}{having_stmt} = '';
                        }
                        last HAVING;
                    }
                    if ( $count > 0 ) {
                        print_select_statement( $sql, $table );
                        # Choose
                        $AND_OR = choose(
                            [ $AND, $OR ],
                            { prompt => 'Choose:', %setup_stmt }
                        );
                        if ( not defined $AND_OR ) {
                            $sql->{quote}{having_args} = [];
                            $sql->{quote}{having_stmt} = '';
                            $sql->{print}{having_stmt} = '';
                            last HAVING;
                        }
                        $AND_OR =~ s/\A\s+|\s+\z//g;
                        $AND_OR = ' ' . $AND_OR;
                    }
                    if ( any { $_ eq $func } @{$sql->{quote}{aliases}} ) {
                        $sql->{quote}{having_stmt} .= $AND_OR . ' ' . $quote_cols->{$func};  #
                        $sql->{print}{having_stmt} .= $AND_OR . ' ' . $func;
                    }
                    else {
                        my ( $print_col, $quote_col );
                        if ( $func =~ /\Acount\s*\(\s*\*\s*\)\z/i ) {
                            $print_col = '*';
                            $quote_col = '*';
                        }
                        $func =~ s/\s*\(\s*\S\s*\)\z//;
                        $sql->{quote}{having_stmt} .= $AND_OR . ' ' . $func . '(';
                        $sql->{print}{having_stmt} .= $AND_OR . ' ' . $func . '(';
                        if ( not defined $print_col ) {
                            print_select_statement( $sql, $table );
                            # Choose
                            $print_col = choose(
                                [ @cols ],
                                { prompt => 'Choose:', %setup_stmt }
                            );
                            if ( not defined $print_col ) {
                                $sql->{quote}{having_args} = [];
                                $sql->{quote}{having_stmt} = '';
                                $sql->{print}{having_stmt} = '';
                                last HAVING;
                            }
                            ( $quote_col = $quote_cols->{$print_col} ) =~ s/\sAS\s\S+\z//;
                        }
                        $sql->{quote}{having_stmt} .= $quote_col . ')';
                        $sql->{print}{having_stmt} .= $print_col . ')';
                    }
                    $sql = operator_sql( $info, $sql, 'having', $table, \@cols, $quote_cols, \%setup_stmt );
                    $count++;
                }
            }
            when( $customize{'order_by'} ) {
                my @cols = ( @$print_cols, @{$sql->{quote}{aliases}} );
                $info->{col_sep} = ' ';
                $sql->{quote}{order_by_stmt} = " ORDER BY";
                $sql->{print}{order_by_stmt} = " ORDER BY";

                ORDER_BY: while ( 1 ) {
                    print_select_statement( $sql, $table );
                    # Choose
                    my $print_col = choose(
                        [ $info->{ok}, @cols ],
                        { prompt => 'Choose: ', %setup_stmt }
                    );
                    if ( not defined $print_col ) {
                        $sql->{quote}{order_by_stmt} = '';
                        $sql->{print}{order_by_stmt} = '';
                        last ORDER_BY;
                    }
                    if ( $print_col eq $info->{ok} ) {
                        if ( $info->{col_sep} eq ' ' ) {
                            $sql->{quote}{order_by_stmt} = '';
                            $sql->{print}{order_by_stmt} = '';
                        }
                        last ORDER_BY;
                    }
                    ( my $quote_col = $quote_cols->{$print_col} ) =~ s/\sAS\s\S+\z//;   #
                    $sql->{quote}{order_by_stmt} .= $info->{col_sep} . $quote_col;
                    $sql->{print}{order_by_stmt} .= $info->{col_sep} . $print_col;
                    print_select_statement( $sql, $table );
                    # Choose
                    my $direction = choose(
                        [ $ASC, $DESC ],
                        { prompt => 'Choose:', %setup_stmt }
                    );
                    if ( not defined $direction ){
                        $sql->{quote}{order_by_stmt} = '';
                        $sql->{print}{order_by_stmt} = '';
                        last ORDER_BY;
                    }
                    $direction =~ s/\A\s+|\s+\z//g;
                    $sql->{quote}{order_by_stmt} .= ' ' . $direction;
                    $sql->{print}{order_by_stmt} .= ' ' . $direction;
                    $info->{col_sep} = ', ';
                }
            }
            when( $customize{'limit'} ) {
                $sql->{quote}{limit_args} = [];
                $sql->{quote}{limit_stmt} = " LIMIT";
                $sql->{print}{limit_stmt} = " LIMIT";
                my ( $rows ) = $dbh->selectrow_array( "SELECT COUNT(*)" . $from_stmt, {} );
                my $digits = length $rows;
                $digits = 4 if $digits < 4;
                my ( $only_limit, $offset_and_limit ) = ( 'LIMIT', 'OFFSET-LIMIT' );

                LIMIT: while ( 1 ) {
                    print_select_statement( $sql, $table );
                    # Choose
                    my $choice = choose(
                        [ $info->{ok}, $only_limit, $offset_and_limit ],
                        { prompt => 'Choose: ', %setup_stmt }
                    );
                    if ( not defined $choice ) {
                        $sql->{quote}{limit_args} = [];
                        $sql->{quote}{limit_stmt} = '';
                        $sql->{print}{limit_stmt} = '';
                        last LIMIT;
                    }
                    if ( $choice eq $info->{ok} ) {
                        if ( not @{$sql->{quote}{limit_args}} ) {
                            $sql->{quote}{limit_stmt} = '';
                            $sql->{print}{limit_stmt} = '';
                        }
                        last LIMIT;
                    }
                    $sql->{quote}{limit_args} = [];
                    $sql->{quote}{limit_stmt} = " LIMIT";
                    $sql->{print}{limit_stmt} = " LIMIT";
                    if ( $choice eq $offset_and_limit ) {
                        print_select_statement( $sql, $table );
                        # Choose
                        my $offset = choose_a_number( $info, $opt, $digits, 'Compose OFFSET:' );
                        if ( not defined $offset ) {
                            $sql->{quote}{limit_stmt} = '';
                            $sql->{print}{limit_stmt} = '';
                            next LIMIT;
                        }
                        push @{$sql->{quote}{limit_args}}, $offset;
                        $sql->{quote}{limit_stmt} .= ' ' . '?'     . ',';
                        $sql->{print}{limit_stmt} .= ' ' . $offset . ',';
                    }
                    print_select_statement( $sql, $table );
                    # Choose
                    my $limit = choose_a_number( $info, $opt, $digits, 'Compose LIMIT:' );
                    if ( not defined $limit ) {
                        $sql->{quote}{limit_args} = [];
                        $sql->{quote}{limit_stmt} = '';
                        $sql->{print}{limit_stmt} = '';
                        next LIMIT;
                    }
                    push @{$sql->{quote}{limit_args}}, $limit;
                    $sql->{quote}{limit_stmt} .= ' ' . '?';
                    $sql->{print}{limit_stmt} .= ' ' . $limit;
                }
            }
            when( $customize{'print_table'} ) {
                my $cols_sql = '';
                $cols_sql = ' ' . join( ', ', @{$sql->{quote}{chosen_columns}} ) if @{$sql->{quote}{chosen_columns}};
                $cols_sql = $sql->{print}{group_by_cols} if not @{$sql->{quote}{chosen_columns}} and $sql->{print}{group_by_cols};
                if ( $sql->{quote}{aggregate_stmt} ) {
                    $cols_sql .= ',' if $cols_sql;
                    $cols_sql .= $sql->{quote}{aggregate_stmt};
                }
                $cols_sql = $default_cols_sql if not $cols_sql;
                my $select .= "SELECT" . $sql->{quote}{distinct_stmt} . $cols_sql . $from_stmt;
                $select .= $sql->{quote}{where_stmt};
                $select .= $sql->{quote}{group_by_stmt};
                $select .= $sql->{quote}{having_stmt};
                $select .= $sql->{quote}{order_by_stmt};
                $select .= $sql->{quote}{limit_stmt};
                my @arguments = ( @{$sql->{quote}{where_args}}, @{$sql->{quote}{having_args}}, @{$sql->{quote}{limit_args}} );
                # $dbh->{LongReadLen} = (GetTerminalSize)[0] * 4;
                # $dbh->{LongTruncOk} = 1;
                my $sth = $dbh->prepare( $select );
                $sth->execute( @arguments );
                my $col_names = $sth->{NAME};
                my $col_types = $sth->{db_TYPE_string( $info )};
                if ( $info->{db_type} eq 'sqlite' and $table eq 'union_tables' and @{$sql->{quote}{chosen_columns}} ) {
                    $col_names = [ map { s/\A"([^"]+)"\z/$1/; $_ } @$col_names ];
                }
                my $all_arrayref = $sth->fetchall_arrayref;
                print GO_TO_TOP_LEFT;
                print CLEAR_EOS;
                return $all_arrayref, $col_names, $col_types;
            }
            default {
                die "$custom: no such value in the hash \%customize";
            }
        }
    }
    return;
}


sub operator_sql {
    my ( $info, $sql, $clause, $table, $cols, $quote_cols, $setup_stmt ) = @_;
    my ( $stmt, $args );
    if ( $clause eq 'where' ) {
        $stmt = 'where_stmt';
        $args  = 'where_args';
    }
    elsif ( $clause eq 'having' ) {
        $stmt = 'having_stmt';
        $args  = 'having_args';
    }
    print_select_statement( $sql, $table );
    # Choose
    my $operator = choose(
        [ @{$info->{operators}} ],
        { prompt => 'Choose:', %$setup_stmt }
    );
    if ( not defined $operator ) {
        $sql->{quote}{$args} = [];
        $sql->{quote}{$stmt} = '';
        $sql->{print}{$stmt} = '';
        return $sql;
    }
    $operator =~ s/\A\s+|\s+\z//g;
    if ( $operator =~ /REGEXP\z/ ) {
        $sql->{quote}{$stmt} .= ' ' . $info->{regexp}{$info->{db_type}}{$operator};
        $sql->{print}{$stmt} .= ' ' . $info->{regexp}{$info->{db_type}}{$operator};
    }
    else {
        $sql->{quote}{$stmt} .= ' ' . $operator;
        $sql->{print}{$stmt} .= ' ' . $operator;
    }
    if ( $operator =~ /NULL\z/ ) {
        # do nothing
    }
    elsif ( $operator =~ /\A(?:NOT\s)?IN\z/ ) {
        $info->{col_sep} = ' ';
        $sql->{quote}{$stmt} .= '(';
        $sql->{print}{$stmt} .= '(';

        IN: while ( 1 ) {
            print_select_statement( $sql, $table );
            # Choose
            my $print_col = choose(
                [ $info->{ok}, @$cols ],
                { prompt => "$operator: ", %$setup_stmt }
            );
            if ( not defined $print_col ) {
                $sql->{quote}{$args} = [];
                $sql->{quote}{$stmt} = '';
                $sql->{print}{$stmt} = '';
                return $sql;
            }
            if ( $print_col eq $info->{ok} ) {
                if ( $info->{col_sep} eq ' ' ) {
                    $sql->{quote}{$args} = [];
                    $sql->{quote}{$stmt} = '';
                    $sql->{print}{$stmt} = '';
                    return $sql;
                }
                $sql->{quote}{$stmt} .= ' )';
                $sql->{print}{$stmt} .= ' )';
                last IN;
            }
            ( my $quote_col = $quote_cols->{$print_col} ) =~ s/\sAS\s\S+\z//;
            $sql->{quote}{$stmt} .= $info->{col_sep} . $quote_col;
            $sql->{print}{$stmt} .= $info->{col_sep} . $print_col;
            $info->{col_sep} = ', ';
        }
    }
    elsif ( $operator =~ /\A(?:NOT\s)?BETWEEN\z/ ) {
        print_select_statement( $sql, $table );
        # Readline
        my $value_1 = local_read_line( prompt => 'Value_1: ' );
    #    if ( not defined $value_1 ) {
    #        $sql->{quote}{$args} = [];
    #        $sql->{quote}{$stmt} = '';
    #        $sql->{print}{$stmt} = '';
    #        return $sql;
    #    }
        $sql->{quote}{$stmt} .= ' ' . '?' .      ' AND';
        $sql->{print}{$stmt} .= ' ' . $value_1 . ' AND';
        push @{$sql->{quote}{$args}}, $value_1;
        print_select_statement( $sql, $table );
        # Readline
        my $value_2 = local_read_line( prompt => 'Value_2: ' );
    #    if ( not defined $value_2 ) {
    #        $sql->{quote}{$args} = [];
    #        $sql->{quote}{$stmt} = '';
    #        $sql->{print}{$stmt} = '';
    #        return $sql;
    #    }
        $sql->{quote}{$stmt} .= ' ' . '?';
        $sql->{print}{$stmt} .= ' ' . $value_2;
        push @{$sql->{quote}{$args}}, $value_2;
    }
    else {
        print_select_statement( $sql, $table );
        # Readline
        my $value = local_read_line( prompt => $operator =~ /(?:REGEXP|LIKE)\z/ ? 'Pattern: ' : 'Value: ' );
        #if ( not defined $value ) {
        #    $sql->{quote}{$args} = [];
        #    $sql->{quote}{$stmt} = '';
        #    $sql->{print}{$stmt} = '';
        #    return $sql;
        #}
        $value= '^$' if not length $value and $operator =~ /REGEXP\z/;
        $value= qr/$value/i if $info->{db_type} eq 'sqlite' and $operator =~ /REGEXP\z/;
        $sql->{quote}{$stmt} .= ' ' . '?';
        $sql->{print}{$stmt} .= ' ' . "'$value'";
        push @{$sql->{quote}{$args}}, $value;
    }
    return $sql;
}


#----------------------------------------------------------------------------------------------------#
#-- 444 -----------------------------   prepare print routines   ------------------------------------#
#----------------------------------------------------------------------------------------------------#


sub print_loop {
    my ( $info, $opt, $dbh, $all_arrayref, $col_names, $col_types ) = @_;
    my $begin = 0;
    my $end = $opt->{all}{limit}[v] - 1;
    my @choices;
    my $rows = @$all_arrayref;
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
            my $choice = choose(
                [ undef, @choices ],
                { layout => 3, undef => $info->{_back} }
            );
            last PRINT if not defined $choice;
            $start = ( split /\s*-\s*/, $choice )[0];
            $start =~ s/\A\s+//;
            $stop = $start + $opt->{all}{limit}[v] - 1;
            $stop = $#$all_arrayref if $stop > $#$all_arrayref;
            print_table( $info, $opt, [ @{$all_arrayref}[ $start .. $stop ] ], $col_names, $col_types );
        }
        else {
            print_table( $info, $opt, $all_arrayref, $col_names, $col_types );
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
    my ( $info, $opt, $ref, $col_types, $maxcols ) = @_;
    my ( $head_width_cols, $max_width_cols, $not_a_number );
    my $count = 0;
    say 'Computing: ...';
    my $is_binary_data = is_binary_data( $info->{db_type} );
    for my $row ( @$ref ) {
        $count++;
        for my $i ( 0 .. $#$row ) {
            $max_width_cols->[$i] ||= 1;
            $row->[$i] = $opt->{all}{undef}[v] if not defined $row->[$i];
            if ( $count == 1 ) {
                # column name
                $row->[$i] =~ s/\p{Space}+/ /g;
                $row->[$i] =~ s/\p{Cntrl}//g;
                utf8::upgrade( $row->[$i] );
                my $gcstring = Unicode::GCString->new( $row->[$i] );
                $head_width_cols->[$i] = $gcstring->columns();
            }
            else { 
                # normal row
                if ( $opt->{all}{binary_filter}[v] and ( $col_types->[$i] =~ $info->{binary_regex} or $is_binary_data->( $row->[$i] ) ) ) {
                    $row->[$i] = $info->{binary_string};
                    $max_width_cols->[$i] = $info->{binary_length} if $info->{binary_length} > $max_width_cols->[$i];
                }
                else {
                    $row->[$i] =~ s/\p{Space}+/ /g;
                    $row->[$i] =~ s/\p{Cntrl}//g;
                    utf8::upgrade( $row->[$i] );
                    my $gcstring = Unicode::GCString->new( $row->[$i] );
                    $max_width_cols->[$i] = $gcstring->columns() if $gcstring->columns() > $max_width_cols->[$i];
                }
                ++$not_a_number->[$i] if not looks_like_number $row->[$i];
            }
        }
    }
    if ( sum( @$max_width_cols ) + $opt->{all}{tab}[v] * ( @$max_width_cols - 1 ) < $maxcols ) { 
        # auto cut
        MAX: while ( 1 ) {
            my $count = 0;
            my $sum = sum( @$max_width_cols ) + $opt->{all}{tab}[v] * ( @$max_width_cols - 1 );
            for my $i ( 0 .. $#$head_width_cols ) {
                if ( $head_width_cols->[$i] > $max_width_cols->[$i] ) {
                    $max_width_cols->[$i]++;
                    $count++;
                    last MAX if ( $sum + $count ) == $maxcols;
                }
            }
            last MAX if $count == 0;
        }
    }
    return $max_width_cols, $not_a_number;
}


sub minus_x_percent {
    my ( $value, $percent ) = @_;
    return int $value - ( $value * 1/100 * $percent );
}

sub recalc_widths {
    my ( $info, $opt, $maxcols, $ref, $col_types ) = @_;
    my ( $max_width_cols, $not_a_number ) = calc_widths( $info, $opt, $ref, $col_types, $maxcols );
    return if not defined $max_width_cols or not @$max_width_cols;
    my $sum = sum( @$max_width_cols ) + $opt->{all}{tab}[v] * ( @$max_width_cols - 1 );
    my @max_tmp = @$max_width_cols;
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
            if ( $max_tmp[$i] < $max_width_cols->[$i] ) {
                $max_tmp[$i]++;
                $rest--;
                $count++;
                last if $rest < 1;
            }
        }
        last if $count == 0;
    }
    $max_width_cols = [ @max_tmp ] if @max_tmp;
    return $max_width_cols, $not_a_number;
}


#----------------------------------------------------------------------------------------------------#
#-- 555 ----------------------------------   print routine   ----------------------------------------#
#----------------------------------------------------------------------------------------------------#


sub print_table {
    my ( $info, $opt, $ref, $col_names, $col_types ) = @_;
    my ( $maxcols, $maxrows ) = GetTerminalSize( *STDOUT );
    return if not defined $ref;
    unshift @$ref, $col_names if defined $col_names;
    my ( $max_width_cols, $not_a_number ) = recalc_widths( $info, $opt, $maxcols, $ref, $col_types );
    return if not defined $max_width_cols;
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
        for my $i ( 0 .. $#$max_width_cols ) {
            my $word = $row->[$i];
            my $right_justify = $not_a_number->[$i] ? 0 : 1;
            $string .= unicode_sprintf(
                        $max_width_cols->[$i],
                        $word,
                        $right_justify,
            );
            $string .= ' ' x $opt->{all}{tab}[v] if not $i == $#$max_width_cols;
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
    choose( \@list, {
        prompt => 0, layout => 3, clear_screen => 1, limit => $opt->{all}{limit}[v] + 1,
        length_longest => sum( @$max_width_cols, $opt->{all}{tab}[v] * $#{$max_width_cols} ) }
    );
    return;
}


#----------------------------------------------------------------------------------------------------#
#-- 666 --------------------------------    option routines     -------------------------------------#
#----------------------------------------------------------------------------------------------------#


sub options {
    my ( $info, $opt ) = @_;
    my @choices = ();
    for my $section ( @{$info->{option_sections}} ) {
        for my $key ( @{$info->{$section}{keys}} ) {
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
        my ( $exit, $help ) = ( '  EXIT', '  HELP' );
        # Choose
        my $option = choose(
            [ $exit, $help, @choices, undef ],
            { undef => $info->{_continue}, layout => 3, clear_screen => 1 }
        );
        my $back = '<<';
        my %number_lyt = ( layout => 1, order => 0, justify => 1, undef => $back );
        my %bol = ( undef => $back, pad_one_row => 2 );
        my ( $true, $false ) = ( 'YES', 'NO' );
        SWITCH: for ( $option ) {
            when ( not defined ) { last OPTIONS; }
            when ( $exit ) { exit() }
            when ( $help ) { help(); choose( [ ' Close with ENTER ' ], { prompt => 0 } ) }
            when ( $opt->{'db_search'}{'reset_cache'}[chs] ) {
                # Choose
                my $choice = choose(
                    [ undef, $true, $false ],
                    { prompt => 'New SQLite DB search [' . ( $opt->{db_search}{reset_cache}[v] ? 'YES' : 'NO' ) . ']:', %bol }
                );
                next SWITCH if not defined $choice;
                $opt->{db_search}{reset_cache}[v] = ( $choice eq $true ) ? 1 : 0;
                $change++;
            }
            when ( $opt->{'db_search'}{'max_depth'}[chs] ) {
                # Choose
                my $number = choose(
                    [ undef, '--', 0 .. 99 ],
                    { prompt => 'SQLite DB search - max-depth [' . ( $opt->{db_search}{max_depth}[v] // 'undef' ) . ']:', %number_lyt }
                );
                next SWITCH if not defined $number;
                if ( $number eq '--' ) {
                    $opt->{db_search}{max_depth}[v] = undef;
                }
                else {
                    $opt->{db_search}{max_depth}[v] = $number;
                }
                $change++;
            }
            when ( $opt->{'all'}{'tab'}[chs] ) {
                # Choose
                my $number = choose(
                    [ undef, 0 .. 99 ],
                    { prompt => 'Tab width [' . $opt->{all}{tab}[v] . ']:',  %number_lyt }
                );
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
                my $sep = choose(
                    [ undef, $comma, $full_stop, $underscore, $space, $none ],
                    { prompt => 'Thousands separator [' . $opt->{all}{kilo_sep}[v] . ']:',  %bol }
                );
                next SWITCH if not defined $sep;
                $opt->{all}{kilo_sep}[v] = $sep_h{$sep};
                $change++;
            }
            when ( $opt->{'all'}{'min_width'}[chs] ) {
                # Choose
                my $number = choose(
                    [ undef, 0 .. 99 ],
                    { prompt => 'Minimum Column width [' . $opt->{all}{min_width}[v] . ']:',  %number_lyt }
                );
                next SWITCH if not defined $number;
                $opt->{all}{min_width}[v] = $number;
                $change++;
            }
            when ( $opt->{'all'}{'limit'}[chs] ) {
                my $number_now = $opt->{all}{limit}[v];
                $number_now =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{all}{kilo_sep}[v]/g;
                my $prompt = 'Compose new "limit" [' . $number_now . ']:';
                # Choose
                my $limit = choose_a_number( $info, $opt, 7, $prompt );
                next SWITCH if not defined $limit;
                $opt->{all}{limit}[v] = $limit;
                $change++;
            }
            when ( $opt->{'all'}{'binary_filter'}[chs] ) {
                # Choose
                my $choice = choose(
                    [ undef, $true, $false ],
                    { prompt => 'Enable Binary Filter [' . ( $opt->{all}{binary_filter}[v] ? 'YES' : 'NO' ) . ']:', %bol }
                );
                next SWITCH if not defined $choice;
                $opt->{all}{binary_filter}[v] = ( $choice eq $true ) ? 1 : 0;
                $change++;
            }
            when ( $opt->{'all'}{'undef'}[chs] ) {
                # Readline
                my $undef = local_read_line( prompt => 'Print replacement for undefined table vales ["' . $opt->{all}{undef}[v] . '"]: ' );
                next SWITCH if not defined $undef;
                $opt->{all}{undef}[v] = $undef;
                $change++;
            }
            when ( $opt->{'all'}{'lock_stmt'}[chs] ) {
                # Choose
                my ( $lk0, $lk1 ) = ( 'Lk0', 'Lk1' );
                my $choice = choose(
                    [ undef, $lk0, $lk1 ],
                    { prompt => 'Keep statement: set the default value " [' . ( $opt->{all}{lock_stmt}[v] ? $lk1 : $lk0 ) . ']:', %bol }
                );
                next SWITCH if not defined $choice;
                $opt->{all}{lock_stmt}[v] = ( $choice eq $lk1 ) ? 1 : 0;
                $change++;
            }
            when ( $opt->{'all'}{'system_info'}[chs] ) {
                # Choose
                my $choice = choose(
                    [ undef, $true, $false ],
                    { prompt => 'Enable system dbs/schemas/tables " [' . ( $opt->{all}{system_info}[v] ? 'YES' : 'NO' ) . ']:', %bol }
                );
                next SWITCH if not defined $choice;
                $opt->{all}{system_info}[v] = ( $choice eq $true ) ? 1 : 0;
                $change++;
            }
            default { die "$option: no such value in the hash \%opt"; }
        }
    }
    if ( $change ) {
        my ( $true, $false ) = ( 'Make changes permanent', 'Use changes only this time' );
        # Choose
        my $permanent = choose(
            [ $false, $true ],
            { prompt => 'Modifications:', layout => 3, pad_one_row => 1 }
        );
        exit if not defined $permanent;
        if ( $permanent eq $true ) {
            write_config_file( $opt, $info->{config_file} );
        }
    }
    return $opt;
}


sub database_setting {
    my ( $info, $opt, $db ) = @_;
    my @choices = ();
    for my $section ( $info->{db_type} ) {
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
        my $option = choose(
            [ undef, @choices ],
            { undef => $info->{_back}, layout => 3, clear_screen => 1 }
        );
        last OPTIONS_DB if not defined $option;
        my $back = '<<';
        my %number_lyt = ( layout => 1, order => 0, justify => 2, undef => $back );
        my %bol = ( undef => $back, pad_one_row => 2 );
        if ( $info->{db_type} eq 'sqlite' ) {
            SQLITE: for ( $option ) {
                when ( $opt->{'sqlite'}{unicode}[chs] ) {
                    # Choose
                    my $unicode = $opt->{$db}{unicode}[v] // $opt->{$info->{db_type}}{unicode}[v];
                    my $choice = choose(
                        [ undef, $true, $false ],
                        { prompt => 'Unicode [' . ( $unicode ? 'YES' : 'NO' ) . ']:', %bol }
                    );
                    next SQLITE if not defined $choice;
                    $opt->{$db}{unicode}[v] = ( $choice eq $true ) ? 1 : 0;
                    write_config_file( $opt, $info->{config_file} );
                    $change++;
                }
                when ( $opt->{'sqlite'}{see_if_its_a_number}[chs] ) {
                    # Choose
                    my $see_if_its_a_number = $opt->{$db}{see_if_its_a_number}[v] // $opt->{$info->{db_type}}{see_if_its_a_number}[v];
                    my $choice = choose(
                        [ undef, $true, $false ],
                        { prompt => 'See if its a number [' . ( $see_if_its_a_number ? 'YES' : 'NO' ) . ']:', %bol }
                    );
                    next SQLITE if not defined $choice;
                    $opt->{$db}{see_if_its_a_number}[v] = ( $choice eq $true ) ? 1 : 0;
                    write_config_file( $opt, $info->{config_file} );
                    $change++;
                }
                when ( $opt->{'sqlite'}{busy_timeout}[chs] ) {
                    my $busy_timeout = $opt->{$db}{busy_timeout}[v] // $opt->{$info->{db_type}}{busy_timeout}[v];
                    $busy_timeout =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{all}{kilo_sep}[v]/g;
                    my $prompt = 'Compose new "Busy timeout (ms)" [' . $busy_timeout . ']:';
                    # Choose
                    my $new_timeout = choose_a_number( $info, $opt, 6, $prompt );
                    next SQLITE if not defined $new_timeout;
                    $opt->{$db}{busy_timeout}[v] = $new_timeout;
                    write_config_file( $opt, $info->{config_file} );
                    $change++;
                }
                when ( $opt->{'sqlite'}{cache_size}[chs] ) {
                    my $number_now = $opt->{$db}{cache_size}[v] // $opt->{$info->{db_type}}{cache_size}[v];
                    $number_now =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{all}{kilo_sep}[v]/g;
                    my $prompt = 'Compose new "Cache size (kb)" [' . $number_now . ']:';
                    # Choose
                    my $cache_size = choose_a_number( $info, $opt, 8, $prompt );
                    next SQLITE if not defined $cache_size;
                    $opt->{$db}{cache_size}[v] = $cache_size;
                    write_config_file( $opt, $info->{config_file} );
                    $change++;
                }
                default { die "$option: no such value in the hash \%opt"; }
            }
        }
        elsif ( $info->{db_type} eq 'mysql' ) {
            MYSQL: for ( $option ) {
                when ( $opt->{'mysql'}{enable_utf8}[chs] ) {
                    # Choose
                    my $utf8 = $opt->{$db}{enable_utf8}[v] // $opt->{$info->{db_type}}{enable_utf8}[v];
                    my $choice = choose(
                        [ undef, $true, $false ],
                        { prompt => 'Enable utf8 [' . ( $utf8 ? 'YES' : 'NO' ) . ']:', %bol }
                    );
                    next MYSQL if not defined $choice;
                    $opt->{$db}{enable_utf8}[v] = ( $choice eq $true ) ? 1 : 0;
                    write_config_file( $opt, $info->{config_file} );
                    $change++;
                }
                when ( $opt->{'mysql'}{bind_type_guessing}[chs] ) {
                    # Choose
                    my $bind_type_guessing = $opt->{$db}{bind_type_guessing}[v] // $opt->{$info->{db_type}}{bind_type_guessing}[v];
                    my $choice = choose(
                        [ undef, $true, $false ],
                        { prompt => 'Bind type guessing [' . ( $bind_type_guessing ? 'YES' : 'NO' ) . ']:', %bol }
                    );
                    next MYSQL if not defined $choice;
                    $opt->{$db}{bind_type_guessing}[v] = ( $choice eq $true ) ? 1 : 0;
                    write_config_file( $opt, $info->{config_file} );
                    $change++;
                }
                when ( $opt->{'mysql'}{connect_timeout}[chs] ) {
                    my $connect_timeout = $opt->{$db}{connect_timeout}[v] // $opt->{$info->{db_type}}{connect_timeout}[v];
                    $connect_timeout =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{all}{kilo_sep}[v]/g;
                    my $prompt = 'Compose new "Busy timeout (s)" [' . $connect_timeout . ']:';
                    # Choose
                    my $new_timeout = choose_a_number( $info, $opt, 4, $prompt );
                    next MYSQL if not defined $new_timeout;
                    $opt->{$db}{connect_timeout}[v] = $new_timeout;
                    write_config_file( $opt, $info->{config_file} );
                    $change++;
                }
                default { die "$option: no such value in the hash \%opt"; }
            }
        }
        elsif ( $info->{db_type} eq 'postgres' ) {
            POSTGRES: for ( $option ) {
                when ( $opt->{'postgres'}{pg_enable_utf8}[chs] ) {
                    # Choose
                    my $utf8 = $opt->{$db}{pg_enable_utf8}[v] // $opt->{$info->{db_type}}{pg_enable_utf8}[v];
                    my $choice = choose(
                        [ undef, $true, $false ],
                        { prompt => 'Enable utf8 [' . ( $utf8 ? 'YES' : 'NO' ) . ']:', %bol }
                    );
                    next POSTGRES if not defined $choice;
                    $opt->{$db}{pg_enable_utf8}[v] = ( $choice eq $true ) ? 1 : 0;
                    write_config_file( $opt, $info->{config_file} );
                    $change++;
                }
                default { die "$option: no such value in the hash \%opt"; }
            }
        }
    }
    return $change ? 1 : 0;
}


#----------------------------------------------------------------------------------------------------#
#-- 777 --------------------------------    helper routines     -------------------------------------#
#----------------------------------------------------------------------------------------------------#

sub print_error_message {
    my ( $message ) = @_;
    utf8::decode( $message );
    print $message;
}


sub local_read_line {
    my %args = @_;
    ReadMode 'noecho' if $args{no_echo};
    my $line;
    print $args{prompt};
    while ( not defined( $line ) ) {
        $line = ReadLine( -1 );
    }
    ReadMode 'restore' if $args{no_echo};
    chomp $line;
    return $line;
}


sub write_config_file {
    my ( $opt, $file ) = @_;
    my $tmp = {};
    for my $section ( keys %$opt ) {
        for my $key ( keys %{$opt->{$section}} ) {
            $tmp->{$section}{$key} = $opt->{$section}{$key}[v];
        }
    }
    write_json( $file, $tmp );
}


sub read_config_file {
    my ( $opt, $file ) = @_;
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
    my $json = JSON::XS->new->pretty->encode( $ref );
    open my $fh, '>', encode_utf8( $file ) or die $!;
    print $fh $json;
    close $fh or die $!;
}


sub read_json {
    my ( $file ) = @_;
    return {} if not -f encode_utf8( $file );
    my $json;
    {
        local $/ = undef;
        open my $fh, '<', encode_utf8( $file ) or die $!;
        $json = readline $fh;
        close $fh or die $!;
    }
    my $ref = JSON::XS->new->pretty->decode( $json ) if $json;
    return $ref;
}


sub choose_a_number {
    my ( $info, $opt, $digits, $prompt ) = @_;
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
        my $range = choose(
            [ undef, @list ],
            { prompt => $prompt, layout => 3, justify => 1, undef => $info->{back} . ' ' x ( $longest * 2 + 1 ) }
        );
        return if not defined $range;
        last if $confirm and $range eq $confirm;
        $range = ( split /\s*-\s*/, $range )[0];
        $range =~ s/\A\s*\d//;
        ( my $range_no_sep = $range ) =~ s/\Q$opt->{all}{kilo_sep}[v]\E//g if $opt->{all}{kilo_sep}[v] ne '';
        my $key = length $range_no_sep;
        # Choose
        my $choice = choose(
            [ undef, map( $_ . $range, 1 .. 9 ), $reset ],
            { pad_one_row => 2, undef => '<<' }
        );
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



##########################################################################################################################
###########################################   database specific subroutines   ############################################
##########################################################################################################################


sub get_db_handle {
    my ( $info, $opt, $db ) = @_;
    my $dbh;
    if ( $info->{db_type} eq 'sqlite' ) {
        die "\"$db\": $!. Maybe the cached data is not up to date." if not -f $db;
        $dbh = DBI->connect( "DBI:SQLite:dbname=$db", '', '', {
            PrintError => 0,
            RaiseError => 1,
            AutoCommit => 1,
            sqlite_unicode             => $opt->{$db}{unicode}[v]             // $opt->{sqlite}{unicode}[v],
            sqlite_see_if_its_a_number => $opt->{$db}{see_if_its_a_number}[v] // $opt->{sqlite}{see_if_its_a_number}[v],
        } ) or die DBI->errstr;
        $dbh->sqlite_busy_timeout(           $opt->{$db}{busy_timeout}[v] // $opt->{sqlite}{busy_timeout}[v] );
        $dbh->do( 'PRAGMA cache_size = ' . ( $opt->{$db}{cache_size}[v]   // $opt->{sqlite}{cache_size}[v] ) );
        $dbh->func( 'regexp', 2,
                sub { my ( $regex, $string ) = @_; $string //= ''; return $string =~ m/$regex/ism },
                'create_function'
        );
    }
    elsif ( $info->{db_type} eq 'mysql' )  {
        $info->{mysql_user}   = local_read_line( prompt => 'Enter username: ' )               if not defined $info->{mysql_user};
        $info->{mysql_passwd} = local_read_line( prompt => 'Enter password: ', no_echo => 1 ) if not defined $info->{mysql_passwd};
        $dbh = DBI->connect( "DBI:mysql:dbname=$db", $info->{mysql_user}, $info->{mysql_passwd}, {
            PrintError  => 0,
            RaiseError  => 1,
            AutoCommit  => 1,
            mysql_enable_utf8        => $opt->{$db}{enable_utf8}[v]        // $opt->{mysql}{enable_utf8}[v],
            mysql_connect_timeout    => $opt->{$db}{connect_timeout}[v]    // $opt->{mysql}{connect_timeout}[v],
            mysql_bind_type_guessing => $opt->{$db}{bind_type_guessing}[v] // $opt->{mysql}{bind_type_guessing}[v],
        } ) or die DBI->errstr;
    }
    elsif ( $info->{db_type} eq 'postgres' ) {
        $info->{postgres_user}   = local_read_line( prompt => 'Enter username: ' )               if not defined $info->{postgres_user};
        $info->{postgres_passwd} = local_read_line( prompt => 'Enter password: ', no_echo => 1 ) if not defined $info->{postgres_passwd};
        $dbh = DBI->connect( "DBI:Pg:dbname=$db", $info->{postgres_user}, $info->{postgres_passwd}, {
            PrintError => 0,
            RaiseError => 1,
            AutoCommit => 1,
            pg_enable_utf8 => $opt->{$db}{pg_enable_utf8}[v] // $opt->{postgres}{pg_enable_utf8}[v],
        } ) or die DBI->errstr;
    }
    return $dbh;
}


sub available_databases {
    my ( $info, $opt ) = @_;
    my $databases = [];
    if ( $info->{db_type} eq 'sqlite' ) {
        my @dirs = @ARGV ? map { decode_utf8( $_ ) } @ARGV :  map { decode_utf8( $_ ) } ( $info->{home} );
        my $c_dirs  = join ' ', @dirs;
        my $c_depth = $opt->{db_search}{max_depth}[v] // '';
        $info->{cache} = read_json( $info->{db_cache_file} );
        if ( $opt->{db_search}{reset_cache}[v] ) {
            delete $info->{cache}{$c_dirs}{$c_depth};
            $info->{cached} = '';
        }
        else {
            if ( $info->{cache}{$c_dirs}{$c_depth} ) {
                $databases = $info->{cache}{$c_dirs}{$c_depth};
                $info->{cached} = ' (cached)';
            }
            else {
                $info->{cached} = '';
            }
        }
        if ( not $info->{cached} ) {
            say 'Searching...';
            for my $dir ( @dirs ) {
                my $max_depth;
                if ( defined $opt->{db_search}{max_depth}[v] ) {
                    $max_depth = $opt->{db_search}{max_depth}[v];
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
                        return if not -s $file;
                        return if not -r $file;
                        #say $file;
                        if ( not eval {
                            open my $fh, '<:raw', $file or die "$file: $!";
                            defined( read $fh, my $string, 13 ) or die "$file: $!";
                            close $fh or die $!;
                            push @$databases, decode_utf8( $file ) if $string eq 'SQLite format';
                            1 }
                        ) {
                            print_error_message( $@ );
                        }
                    },
                    no_chdir   => 1,
                },
                encode_utf8( $dir ) );
            }
            $info->{cache}{$c_dirs}{$c_depth} = $databases;
            write_json( $info->{db_cache_file}, $info->{cache} );
            say 'Ended searching';
        }
    }
    elsif( $info->{db_type} eq 'mysql' ) {
        my $dbh = get_db_handle( $info, $opt, 'information_schema' );
        my $stmt;
        if ( not $opt->{all}{system_info}[v] ) {
            $stmt = "SELECT schema_name FROM information_schema.schemata
                     WHERE schema_name != 'mysql' AND schema_name != 'information_schema' AND schema_name != 'performance_schema'
                     ORDER BY schema_name";
        }
        elsif ( $opt->{all}{system_info}[v] ) {
            $stmt = "SELECT schema_name FROM information_schema.schemata
                     ORDER BY schema_name";
        }
        $databases = $dbh->selectcol_arrayref( $stmt, {} );
    }
    elsif( $info->{db_type} eq 'postgres' ) {
        my $dbh = get_db_handle( $info, $opt, 'postgres' );
        my $stmt;
        if ( not $opt->{all}{system_info}[v] ) {
            $stmt = "SELECT datname FROM pg_database
                     WHERE datistemplate = false AND datname != 'postgres'
                     ORDER BY datname";
        }
        elsif ( $opt->{all}{system_info}[v] ) {
            $stmt = "SELECT datname FROM pg_database
                     ORDER BY datname";
        }
        $databases = $dbh->selectcol_arrayref( $stmt, {} );
    }
    return $databases;
}


sub get_schema_names {
    my ( $dbh, $db ) = @_;
    return [ $db ] if $info->{db_type} eq 'sqlite';
    return [ $db ] if $info->{db_type} eq 'mysql';
    if ( $info->{db_type} eq 'postgres' ) {
        return [ 'public' ]  if not $opt->{all}{system_info}[v];
        # Schema names beginning with pg_ are reserved for system purposes and cannot be created by users.
        my $stmt;
        if ( not $opt->{all}{system_info}[v] ) {
            $stmt = "SELECT schema_name FROM information_schema.schemata
                     WHERE schema_name != 'information_schema' AND schema_name !~ '^pg_'
                     ORDER BY schema_name";
        }
        elsif ( $opt->{all}{system_info}[v] ) {
            $stmt = "SELECT schema_name FROM information_schema.schemata
                     ORDER BY schema_name";
        }
        my $schema_names = $dbh->selectcol_arrayref( $stmt, {} );
        return $schema_names;
    }
}


sub get_table_names {
    my ( $dbh, $db, $schema ) = @_;
    my $tables = [];
    if ( $info->{db_type} eq 'sqlite' ) {
        $tables = $dbh->selectcol_arrayref( "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name" );
    }
    else {
        my $stmt = "SELECT table_name FROM information_schema.tables WHERE table_schema = ? ORDER BY table_name";  # AND table_type = 'BASE TABLE'
        $tables = $dbh->selectcol_arrayref( $stmt, {}, ( $schema ) );
    }
    return $tables;
}

sub system_tables {
    my ( $info ) = @_;
    my $system_tables;
    if ( $info->{db_type} eq 'mysql' ) {
        $system_tables = [ qw( mysql information_schema performance_schema ) ];
    }
    elsif ( $info->{db_type} eq 'postgres' ) {
        $system_tables = [ qw( postgres template0 template1 ) ];
    }
    return $system_tables;
}


sub quote_table {
    my ( $info, $dbh, $schema, $table ) = @_;
    if ( $info->{db_type} eq 'sqlite' ) {
        return $dbh->quote_identifier( undef, undef, $table );
    }
    else {
        return $dbh->quote_identifier( undef, $schema, $table );
    }
}

sub db_TYPE_string {
    my ( $info ) = @_;
    my $type = 'TYPE';  # :sql_types
    $type = 'mysql_type_name' if $info->{db_type} eq 'mysql';
    $type = 'pg_type'         if $info->{db_type} eq 'postgres';
    return $type;
}

sub column_names_and_types {
    my ( $info, $dbh, $db, $schema, $data ) = @_;
    if ( $info->{db_type} eq 'sqlite' ) {
        for my $table ( @{$data->{$db}{$schema}{tables}} ) {
            my $sth = $dbh->prepare( "SELECT * FROM " . $dbh->quote_identifier( undef, undef, $table ) );
            #$sth->execute();
            $data->{$db}{$schema}{col_names}{$table} = $sth->{NAME};
            $data->{$db}{$schema}{col_types}{$table} = $sth->{TYPE};            
        }
    }
    else {
        my $stmt;
        $stmt = "SELECT table_name, column_name, column_type FROM information_schema.columns WHERE table_schema = ?" if $info->{db_type} eq 'mysql';
        $stmt = "SELECT table_name, column_name, data_type   FROM information_schema.columns WHERE table_schema = ?" if $info->{db_type} eq 'postgres';
        my $sth = $dbh->prepare( $stmt );
        $sth->execute( $schema );
        while ( my $row = $sth->fetchrow_arrayref() ) {
            push @{$data->{$db}{$schema}{col_names}{$row->[0]}}, $row->[1];
            push @{$data->{$db}{$schema}{col_types}{$row->[0]}}, $row->[2];   
        }
    }
    return $data;
}


sub primary_and_foreign_keys {
    my ( $info, $dbh, $db, $schema, $data ) = @_;
    my $foreign_keys        = {};
    my $primary_key_columns = {};
    my $sth;
    
    if ( $info->{db_type} eq 'mysql' ) {
        my $stmt = "SELECT constraint_name, table_name, column_name, referenced_table_name, referenced_column_name, position_in_unique_constraint
                    FROM information_schema.key_column_usage
                    WHERE table_schema = ? AND table_name = ? AND referenced_table_name IS NOT NULL";
        $sth = $dbh->prepare( $stmt );
    }
    elsif ( $info->{db_type} eq 'sqlite' ) {
        my $stmt = "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?";
        $sth = $dbh->prepare( $stmt );
    }
    
    for my $table ( @{$data->{$db}{$schema}{tables}} ) {
        if ( $info->{db_type} eq 'sqlite' ) {
            $sth->execute( $table );
            my $ref = $sth->fetchrow_arrayref();
            my @references = grep { /FOREIGN\sKEY/ } split /\s*\n\s*/, $ref->[0];
            for my $i ( 0 .. $#references ) {
                if ( $references[$i] =~ /FOREIGN\sKEY\s?\(([^)]+)\)\sREFERENCES\s([^(]+)\(([^)]+)\),?/ ) {
                    $foreign_keys->{$table}{$i}{foreign_key_col}   = [ split /\s*,\s*/, $1 ];
                    $foreign_keys->{$table}{$i}{reference_key_col} = [ split /\s*,\s*/, $3 ];                
                    $foreign_keys->{$table}{$i}{reference_table}   = $2;
                }
            }
            $primary_key_columns->{$table} = [ $dbh->primary_key( undef, undef  , $table ) ];
        }    
        elsif ( $info->{db_type} eq 'mysql' ) {
            $sth->execute( $schema, $table );
            while ( my $row = $sth->fetchrow_hashref ) {
                $foreign_keys->{$table}{$row->{constraint_name}}{foreign_key_col  }[$row->{position_in_unique_constraint}-1] = $row->{column_name};
                $foreign_keys->{$table}{$row->{constraint_name}}{reference_key_col}[$row->{position_in_unique_constraint}-1] = $row->{referenced_column_name};                    
                $foreign_keys->{$table}{$row->{constraint_name}}{reference_table} = $row->{referenced_table_name} if not $foreign_keys->{$table}{$row->{constraint_name}}{reference_table};
            }
            $primary_key_columns->{$table} = [ $dbh->primary_key( undef, $schema, $table ) ];
        }    
        elsif ( $info->{db_type} eq 'postgres' ) {
            my $sth = $dbh->foreign_key_info( undef, undef, undef, undef, $schema, $table );
            if ( defined $sth ) {
                while ( my $row = $sth->fetchrow_hashref ) {
                    push @{$foreign_keys->{$table}{$row->{FK_NAME}}{foreign_key_col  }}, $row->{FK_COLUMN_NAME};
                    push @{$foreign_keys->{$table}{$row->{FK_NAME}}{reference_key_col}}, $row->{UK_COLUMN_NAME};
                    $foreign_keys->{$table}{$row->{FK_NAME}}{reference_table} = $row->{UK_TABLE_NAME} if not $foreign_keys->{$table}{$row->{FK_NAME}}{reference_table};
                }
            }
            $primary_key_columns->{$table} = [ $dbh->primary_key( undef, $schema, $table ) ];
        }
    }
    
    return $primary_key_columns, $foreign_keys;
}















__DATA__
