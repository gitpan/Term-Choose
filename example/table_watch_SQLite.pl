#!/usr/bin/env perl
use warnings;
use strict;
use 5.10.1;
use open qw(:utf8 :std);

#use warnings FATAL => qw(all);
#use Data::Dumper;
# Version 1.027

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

    v       => 0,
    chs     => 1,
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

mkdir $config_dir or die $! if ! -d $config_dir;

my $info = {
    home                => $home,
    config_file         => catfile( $config_dir, 'tw_config.json' ),
    db_cache_file       => catfile( $config_dir, 'tw_cache_db_search.json' ),
    lyt_h               => { layout => 1, order => 0, justify => 2, undef => '<<' },
    lyt_nr              => { layout => 1, order => 0, justify => 1, undef => '<<' },
    back                => 'BACK',
    confirm             => 'CONFIRM',
    ok                  => '- OK -',
    _exit               => '  EXIT',
    _help               => '  HELP',
    _back               => '  BACK',
    _confirm            => '  CONFIRM',
    _continue           => '  CONTINUE',
    _info               => '  INFO',
    _reset              => '  RESET',
    cached              => '',
    binary_string       => 'BNRY',
    aggregate_functions => [ "AVG(X)", "COUNT(X)", "COUNT(*)", "MAX(X)", "MIN(X)", "SUM(X)" ],
    avilable_operators  => [ "REGEXP", "NOT REGEXP", "LIKE", "NOT LIKE", "IS NULL", "IS NOT NULL", "IN", "NOT IN",
                             "BETWEEN", "NOT BETWEEN", " = ", " != ", " <> ", " < ", " > ", " >= ", " <= " ],
    avilable_db_types   => [ 'sqlite', 'mysql', 'postgres' ],
    mysql => {
        user   => undef,
        passwd => undef,
    },
    postgres => {
        user   => undef,
        passwd => undef,
    },
};


utf8::upgrade( $info->{binary_string} );
my $gcs = Unicode::GCString->new( $info->{binary_string} );
my $colwidth = $gcs->columns();
$info->{binary_length} = $colwidth;


sub help {
    print << 'HELP';

    Search and read SQLite/MySQL/PostgreSQL databases.

Usage:
    table_watch_SQLite.pl [-h|--help]   (SQLite/MySQL/PostgreSQL)

    table_watch_SQLite.pl [-s|--search] [-m|--max-depth] [directories to be searched] (only SQLite)
      -s|--search   : new search of SQLite databases instead of using cached data.
      -m|--max-depth: levels to descend at most when searching in directories for SQLite databases.
    If no directories are passed the home directory is searched for SQLite databases.

Options:
    Help           : Show this Info.

    Tab width      : Set the number of spaces between columns.
    Min col width  : Set the width the columns should have at least when printed.
    Undef          : Set the string that will be shown on the screen if a table value is undefined.
    Binary filter  : Print "BNRY" instead of binary data (printing binary data could break the output).
    Max rows       : Set the maximum number of table rows available at once.

    Database types : Choose the needed database types.
    Operators      : Choose the needed operators.
    Thousands sep  : Choose the thousands separator displayed in menus.
    sssc mode      : With the sssc mode "compat" enabled back-arrows are offered in the sql "sub-statement" menu.
                     To reset a sql "sub-statement" in the "simple" mode re-enter in the "sub-statement" (e.g WHERE)
                     and choose '- OK -' or use the "q" key.

    Keep statement : Set the default value: Lk0 or Lk1.
    Metadata       : If enabled system tables/schemas/databases are appended to the respective list.
    Regexp case    : If enabled REGEXP will match case sensitiv.
                     With MySQL
                         if enabled the BINARY operator is used to achieve a case sensitiv match.
                         if disabled the default case sensitivity is used.

    DB defaults    : Set Database defaults.
    DB login       : If enabled username and password are asked for each new DB connection.
                     If not enabled username and password are asked once and used for all connections.

    Lk0: Reset the SQL-statement after each "PrintTable".
    Lk1: Reset the SQL-statement only when a table is selected.
    "q" key goes back.

HELP
}


#----------------------------------------------------------------------------------------------------#
#--------------------------------------------   options   -------------------------------------------#
#----------------------------------------------------------------------------------------------------#


my $default_db_types  = [ 'sqlite', 'mysql', 'postgres' ];
my $default_operators = [ "REGEXP", "NOT REGEXP", " = ", " != ", " < ", " > ", "IS NULL", "IS NOT NULL", "IN" ];

my $opt = {
    print => {
        tab_width      => [ 2,      '- Tab width' ],
        min_col_width  => [ 30,     '- Min col width' ],
        undef          => [ '',     '- Undef' ],
        binary_filter  => [ 1,      '- Binary filter' ],
        limit          => [ 50_000, '- Max rows' ],
    },
    sql => {
        lock_stmt      => [ 0, '- Keep statement' ],
        system_info    => [ 0, '- Metadata' ],
        regexp_case    => [ 0, '- Regexp case' ],
    },
    menu => {
        kilo_sep       => [ ',',                '- Thousands sep' ],
        database_types => [ $default_db_types,  '- Database types' ],
        operators      => [ $default_operators, '- Operators' ],
        sssc_mode      => [ 0, '- sssc mode' ],
    },
    dbs => {
        db_login       => [ 0,       '- DB login' ],
        db_defaults    => [ 'Dummy', '- DB defaults' ],
    },
    sqlite => {
        reset_cache         => [ 0,       '- New search' ],
        max_depth           => [ undef,   '- Max depth' ],
        unicode             => [ 1,       '- Unicode' ],
        see_if_its_a_number => [ 1,       '- See if its a number' ],
        busy_timeout        => [ 3_000,   '- Busy timeout (ms)' ],
        cache_size          => [ 500_000, '- Cache size (kb)' ],
    },
    mysql => {
        enable_utf8         => [ 1, '- Enable utf8' ],
        connect_timeout     => [ 4, '- Connect timeout' ],
        bind_type_guessing  => [ 1, '- Bind type guessing' ],
        ChopBlanks          => [ 1, '- Chop blanks off' ],
    },
    postgres => {
        pg_enable_utf8      => [ 1, '- Enable utf8' ],
    }
};

$info->{option_sections} = [ qw( print sql menu dbs ) ];
$info->{print}{keys}     = [ qw( tab_width min_col_width undef binary_filter limit ) ];
$info->{sql}{keys}       = [ qw( lock_stmt system_info regexp_case ) ];
$info->{dbs}{keys}       = [ qw( db_login db_defaults ) ];
$info->{menu}{keys}      = [ qw( kilo_sep database_types operators sssc_mode ) ];

$info->{db_sections}     = [ qw( sqlite mysql postgres ) ];
$info->{sqlite}{keys}    = [ qw( reset_cache max_depth unicode see_if_its_a_number busy_timeout cache_size ) ];
$info->{mysql}{keys}     = [ qw( enable_utf8 connect_timeout bind_type_guessing ChopBlanks ) ];
$info->{postgres}{keys}  = [ qw( pg_enable_utf8 ) ];

$info->{sqlite}{commandline_only} = [ qw( reset_cache max_depth ) ];

for my $section ( @{$info->{option_sections}}, @{$info->{db_sections}} ) {
    if ( join( ' ', sort keys %{$opt->{$section}} ) ne join( ' ', sort @{$info->{$section}{keys}} ) ) {
        say join( ' ', sort keys %{$opt->{$section}} );
        say join( ' ', sort      @{$info->{$section}{keys}} );
        die;
    }
}


if ( ! eval {
    $opt = read_config_file( $opt, $info->{config_file} );
    my $help;
    GetOptions (
        'h|help'        => \$help,
        's|search'      => \$opt->{sqlite}{reset_cache}[v],
        'm|max-depth:i' => \$opt->{sqlite}{max_depth}[v],
    );
    $opt = options( $info, $opt ) if $help;
    1 }
) {
    say 'Configfile/Options:';
    print_error_message( $@ );
    choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
}


if ( $opt->{menu}{sssc_mode}[v] ) {
    $info->{ok} = '<OK>';
}


################################################################################################################
#--------------------------------------------------   MAIN   --------------------------------------------------#
################################################################################################################


DB_TYPES: while ( 1 ) {

    $info->{db_type} = choose(
        [ undef, @{$opt->{menu}{database_types}[v]} ],
        { prompt => 'Database Type: ', %{$info->{lyt_h}}, undef => 'Quit' }
    );
    last DB_TYPES if ! defined $info->{db_type};
    $info->{db_type} =~ s/^\s|\s\z//g;

    my $databases = [];
    if ( ! eval {
        $databases = available_databases( $info, $opt );
        1 }
    ) {
        say 'Available databases:';
        print_error_message( $@ );
        choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
    }
    if ( ! @$databases ) {
        say 'no ' . $info->{db_type} . '-databases found';
        choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
        next DB_TYPES;
    }
    if ( $opt->{sql}{system_info}[v] ) {
        my $system_databases = system_databases( $info );
        my @system;
        for my $system_db ( @$system_databases ) {
            my $i = first_index{ $_ eq $system_db } @$databases;
            push @system, splice( @$databases, $i, 1 );
        }
        if ( $info->{db_type} eq 'sqlite' ) {
            @$databases = ( @$databases, @system );
        }
        else {
            @$databases = ( map( "- $_", @$databases ), map( "  $_", @system ) );
        }
    }
    else {
        if ( $info->{db_type} ne 'sqlite' ) {
            @$databases = map{ "- $_" } @$databases;
        }
    }

    my $new_db_settings = 0;
    my $db;
    my $data = {};
    my %lyt_v_cs = ( layout => 3, clear_screen => 1 );

    DATABASES: while ( 1 ) {

        if ( ! $new_db_settings ) {
            # Choose
            $db = choose(
                [ undef, @$databases ],
                { prompt => 'Choose Database' . $info->{cached}, %lyt_v_cs, undef => 'BACK' }
            );
            next DB_TYPES if ! defined $db;
            $db =~ s/^[-\ ]\s// if $info->{db_type} ne 'sqlite';
            # $data = {};
        }
        else {
            $new_db_settings = 0;
        }
        my $dbh;
        if ( ! eval {
            $dbh = get_db_handle( $info, $opt, $db );
            $data->{$db}{schemas} = get_schema_names( $info, $opt, $dbh, $db ) if ! defined $data->{$db}{schemas};
            1 }
        ) {
            say 'Get database handle and schema names:';
            print_error_message( $@ );
            choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
            # remove database from @databases
            next DATABASES;
        }

        SCHEMA: while ( 1 ) {

            my $schema;
            if ( @{$data->{$db}{schemas}} == 1 ) {
                $schema = $data->{$db}{schemas}[0];
            }
            elsif ( @{$data->{$db}{schemas}} > 1 ) {
                if ( $opt->{sql}{system_info}[v] && $info->{db_type} eq 'postgres' ) {
                    my @system;
                    my @normal;
                    for my $schema ( @{$data->{$db}{schemas}} ) {
                        if ( $schema =~ /^pg_/ || $schema eq 'information_schema' ) {
                            push @system, $schema;
                        }
                        else {
                            push @normal, $schema;
                        }
                    }
                    # Choose
                    $schema = choose(
                        [ undef, map( "- $_", @normal ), map( "  $_", @system ) ],
                        { prompt => 'DB "'. basename( $db ) . '" - choose Schema:', %lyt_v_cs, undef => $info->{_back} }
                    );
                }
                else {
                    # Choose
                    $schema = choose(
                        [ undef, map( "- $_", @{$data->{$db}{schemas}} ) ],
                        { prompt => 'DB "'. basename( $db ) . '" - choose Schema:', %lyt_v_cs, undef => $info->{_back} }
                    );
                }
                next DATABASES if ! defined $schema;
                $schema =~ s/^[-\ ]\s//;
            }

            if ( ! eval {
                $data->{$db}{$schema}{tables} = get_table_names( $dbh, $db, $schema ) if ! defined $data->{$db}{$schema}{tables};
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
            push @tables, '  sqlite_master' if $info->{db_type} eq 'sqlite' && $opt->{sql}{system_info}[v];

            TABLES: while ( 1 ) {

                my $prompt = 'DB: "'. basename( $db );
                $prompt .= '.' . $schema if defined $data->{$db}{schemas} && @{$data->{$db}{schemas}} > 1;
                $prompt .= '"';
                # Choose
                my $table = choose(
                    [ undef, @tables, $join_tables, $union_tables, $db_setting ],
                    { prompt => $prompt, %lyt_v_cs, undef => $info->{_back} }
                );
                if ( ! defined $table ) {
                    next SCHEMA if defined $data->{$db}{schemas} && @{$data->{$db}{schemas}} > 1;
                    next DATABASES;
                }
                my $select_from_stmt = '';
                my $print_and_quote_cols = [];
                if ( $table eq $db_setting ) {
                    if ( ! eval {
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
                    if ( ! eval {
                        ( $select_from_stmt, $print_and_quote_cols ) = join_tables( $info, $dbh, $db, $schema, $data );
                        $table = 'joined_tables';
                        1 }
                    ) {
                        say 'Join tables:';
                        print_error_message( $@ );
                        choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
                    }
                    next TABLES if ! defined $select_from_stmt;
                }
                elsif ( $table eq $union_tables ) {
                    if ( ! eval {
                        ( $select_from_stmt, $print_and_quote_cols ) = union_tables( $info, $dbh, $db, $schema, $data );
                        $table = 'union_tables';
                        1 }
                    ) {
                        say 'Union tables:';
                        print_error_message( $@ );
                        choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
                    }
                    next TABLES if ! defined $select_from_stmt;
                }
                else {
                    $table =~ s/^[-\ ]\s//;
                }
                if ( ! eval {
                    my $sql;
                    $sql->{stmt_keys} = [ qw( distinct_stmt group_by_cols aggregate_stmt where_stmt group_by_stmt having_stmt order_by_stmt limit_stmt ) ];
                    $sql->{list_keys} = [ qw( chosen_columns aliases where_args having_args limit_args ) ];
                    @{$sql->{print}}{ @{$sql->{stmt_keys}} } = ( '' ) x @{$sql->{stmt_keys}};
                    @{$sql->{quote}}{ @{$sql->{stmt_keys}} } = ( '' ) x @{$sql->{stmt_keys}};
                    @{$sql->{print}}{ @{$sql->{list_keys}} } = map{ [] } @{$sql->{list_keys}};
                    @{$sql->{quote}}{ @{$sql->{list_keys}} } = map{ [] } @{$sql->{list_keys}};

                    $info->{lock} = $opt->{sql}{lock_stmt}[v];

                    my $from_stmt        = '';
                    my $default_cols_sql = '';
                    my $quote_cols       = {};
                    my $print_cols       = [];
                    if ( $select_from_stmt ) {
                        if ( $select_from_stmt =~ /^SELECT\s(.*?)(\sFROM\s.*)\z/ ) {
                            $default_cols_sql = $1;
                            $from_stmt        = $2;
                            for my $a_ref ( @$print_and_quote_cols ) {
                                $quote_cols->{$a_ref->[0]} = $a_ref->[1];
                                push @$print_cols, $a_ref->[0];
                            }
                        } else { die $select_from_stmt }
                    }
                    else {
                        $default_cols_sql = ' *';
                        my $table_q = $dbh->quote_identifier( undef, $schema, $table );
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
                        last MAIN_LOOP if ! defined $all_arrayref;
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
}

#----------------------------------------------------------------------------------------------------#
#-- 111 ---------------------------------   union routines   ----------------------------------------#
#----------------------------------------------------------------------------------------------------#


sub union_tables {
    my ( $info, $dbh, $db, $schema, $data ) = @_;
    if ( ! defined $data->{$db}{$schema}{col_names} || ! defined $data->{$db}{$schema}{col_types} ) {
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
        return if ! defined $union_table;
        if ( $union_table eq $info->{_info} ) {
            if ( ! defined $data->{$db}{$schema}{tables_info} ) {
                $data->{$db}{$schema}{tables_info} = get_tables_info( $info, $dbh, $db, $schema, $data );
            }
            choose( $data->{$db}{$schema}{tables_info}, { prompt => 0, layout => 3, clear_screen => 1 } );
            next UNION_TABLE;
        }
        if ( $union_table eq $enough_tables ) {
            return if ! @$used_tables;
            last UNION_TABLE;
        }
        my $idx = first_index { $_ eq $union_table } @tables_unused;
        $union_table =~ s/^[-+\ ]\s//;
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
            my $choices = [ $info->{ok}, @short_cuts, @{$data->{$db}{$schema}{col_names}{$union_table}} ];
            unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
            my $col = choose(
                $choices,
                { prompt => 'Choose Column: ', %{$info->{lyt_h}} }
            );
            if ( ! defined $col ) {
                if ( defined $cols->{$union_table} ) {
                    delete $cols->{$union_table};
                    next UNION_COLUMNS;
                }
                else {
                    my $idx = first_index { $_ eq $union_table } @$used_tables;
                    my $tbl = splice( @$used_tables, $idx, 1 );
                    push @tables_unused, "- $tbl";
                    last UNION_COLUMNS;
                }
            }
            if ( $col eq $info->{ok} ) {
                if ( ! defined $cols->{$union_table} ) {
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
                next UNION_COLUMNS if $opt->{menu}{sssc_mode}[v];
                last UNION_COLUMNS;
            }
            if ( $col eq $all_cols ) {
                push @{$cols->{$union_table}}, '*';
                next UNION_COLUMNS if $opt->{menu}{sssc_mode}[v];
                last UNION_COLUMNS;
            }
            else {
                push @{$cols->{$union_table}}, $col;
            }
        }
        $saved_columns = $cols->{$union_table} if defined $cols->{$union_table};
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
        $union_statement .= " FROM " . $dbh->quote_identifier( undef, $schema, $table );
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
            join( ' | ',
                pairwise { no warnings q(once); lc( $a ) . ' ' . $b }
                    @{$data->{$db}{$schema}{col_types}{$table}},
                    @{$data->{$db}{$schema}{col_names}{$table}}
            )
        ];
        if ( @{$pk->{$table}} ) {
            push @{$print_hash{$table}}, [ 'PK: ', 'primary key (' . join( ',', @{$pk->{$table}} ) . ')' ];
        }
        for my $fk_name ( sort keys %{$fk->{$table}} ) {
            if ( $fk->{$table}{$fk_name} ) {
                push @{$print_hash{$table}}, [
                    'FK: ',
                    'foreign key (' . join( ',', @{$fk->{$table}{$fk_name}{foreign_key_col}} ) .
                    ') references ' . $fk->{$table}{$fk_name}{reference_table} .
                    '(' . join( ',', @{$fk->{$table}{$fk_name}{reference_key_col}} ) .')'
                ];
            }
        }
    }
    my $longest = 10;
    my ( $terminal_width ) = GetTerminalSize( *STDOUT );
    my $col_max = $terminal_width - $longest;
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
    if ( ! defined $data->{$db}{$schema}{col_names} || ! defined $data->{$db}{$schema}{col_types} ) {
        $data = column_names_and_types( $info, $dbh, $db, $schema, $data );
    }
    my $join_statement_quote = "SELECT * FROM";
    my $join_statement_print = "SELECT * FROM";
    my @tables = map { "- $_" } @{$data->{$db}{$schema}{tables}};
    my ( $mastertable, @used_tables, @primary_keys, @foreign_keys );

    MASTER: while ( 1 ) {
        print_join_statement( $join_statement_print );
        # Choose
        $mastertable = choose(
            [ undef, @tables, $info->{_info} ],
            { prompt => 'Choose MASTER table:', layout => 3, undef => $info->{_back} }
        );
        return if ! defined $mastertable;
        if ( $mastertable eq $info->{_info} ) {
            if ( ! defined $data->{$db}{$schema}{tables_info} ) {
                $data->{$db}{$schema}{tables_info} = get_tables_info( $info, $dbh, $db, $schema, $data );
            }
            choose( $data->{$db}{$schema}{tables_info}, { prompt => 0, layout => 3, clear_screen => 1 } );
            next MASTER;
        }
        my $idx = first_index { $_ eq $mastertable } @tables;
        splice( @tables, $idx, 1 );
        $mastertable =~ s/^[-\ ]\s//;
        @used_tables = ( $mastertable );
        my @available_tables = @tables;
        my $mastertable_q = $dbh->quote_identifier( undef, $schema, $mastertable );
        $join_statement_quote = "SELECT * FROM " . $mastertable_q;
        $join_statement_print = "SELECT * FROM " . $mastertable;
        my ( @old_primary_keys, @old_foreign_keys, @old_used_tables, @old_avail_tables );
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
                if ( ! defined $slave_table ) {
                    if ( @used_tables == 1 ) {
                        $join_statement_quote = "SELECT * FROM";
                        $join_statement_print = "SELECT * FROM";
                        @tables = map { "- $_" } @{$data->{$db}{$schema}{tables}};
                        next MASTER;
                    }
                    else {
                        @used_tables = ( $mastertable );
                        @available_tables = @tables;
                        $join_statement_quote = "SELECT * FROM " . $mastertable_q;
                        $join_statement_print = "SELECT * FROM " . $mastertable;
                        @primary_keys = ();
                        @foreign_keys = ();
                        next SLAVE_TABLES;
                    }
                }
                last SLAVE_TABLES if $slave_table eq $enough_slaves;
                if ( $slave_table eq $info->{_info} ) {
                    if ( ! defined $data->{$db}{$schema}{tables_info} ) {
                        $data->{$db}{$schema}{tables_info} = get_tables_info( $info, $dbh, $db, $schema, $data );
                    }
                    choose( $data->{$db}{$schema}{tables_info}, { prompt => 0, layout => 3, clear_screen => 1 } );
                    next SLAVE;
                }
                last SLAVE;
            }
            my $idx = first_index { $_ eq $slave_table } @available_tables;
            splice( @available_tables, $idx, 1 );
            $slave_table =~ s/^[-\ ]\s//;
            my $slave_table_q = $dbh->quote_identifier( undef, $schema, $slave_table );
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
                if ( ! defined $pk_col ) {
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
                $pk_col =~ s/^[-\ ]\s//;
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
                if ( ! defined $fk_col ) {
                    @primary_keys         = @old_primary_keys;
                    @foreign_keys         = @old_foreign_keys;
                    $join_statement_quote = $old_stmt_quote;
                    $join_statement_print = $old_stmt_print;
                    @used_tables          = @old_used_tables;
                    @available_tables     = @old_avail_tables;
                    next SLAVE_TABLES;
                }
                $fk_col =~ s/^[-\ ]\s//;
                push @foreign_keys, $avail_foreign_key_cols{$fk_col};
                $join_statement_quote .= ' ' . $avail_foreign_key_cols{$fk_col};
                $join_statement_print .= ' ' . $fk_col;
                $AND = " AND";
            }
            push @used_tables, $slave_table;
        }
        last MASTER;
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
                $alias = $col . '_' . substr $table, 0, $length_uniq;
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
    $cols_sql = $sql->{print}{group_by_cols} if ! @{$sql->{print}{chosen_columns}} && $sql->{print}{group_by_cols};
    if ( $sql->{print}{aggregate_stmt} ) {
        $cols_sql .= ',' if $cols_sql;
        $cols_sql .= $sql->{print}{aggregate_stmt};
    }
    $cols_sql = ' *' if ! $cols_sql;
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
    my %lyt_stmt = %{$info->{lyt_h}};
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
        @{$sql->{print}}{ @{$sql->{list_keys}} } = map{ [] } @{$sql->{list_keys}};
        @{$sql->{quote}}{ @{$sql->{list_keys}} } = map{ [] } @{$sql->{list_keys}};
    }

    CUSTOMIZE: while ( 1 ) {
        print_select_statement( $sql, $table );
        # Choose
        my $custom = choose(
            [ undef, @customize{@keys} ],
            { prompt => 'Customize:', layout => 3, undef => $info->{back} }
        );
        for ( $custom ) {
            when ( ! defined ) {
                last CUSTOMIZE;
            }
            when ( $customize{'lock'} ) {
                if ( $info->{lock} == 1 ) {
                    $info->{lock} = 0;
                    $customize{lock} = $lock->[0];
                    delete @{$quote_cols}{@{$sql->{quote}{aliases}}};
                    @{$sql->{print}}{ @{$sql->{stmt_keys}} } = ( '' ) x @{$sql->{stmt_keys}};
                    @{$sql->{quote}}{ @{$sql->{stmt_keys}} } = ( '' ) x @{$sql->{stmt_keys}};
                    @{$sql->{print}}{ @{$sql->{list_keys}} } = map{ [] } @{$sql->{list_keys}};
                    @{$sql->{quote}}{ @{$sql->{list_keys}} } = map{ [] } @{$sql->{list_keys}};
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
                    my $choices = [ $info->{ok}, @cols ];
                    unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
                    my $print_col = choose(
                        $choices,
                        { prompt => 'Choose: ', %lyt_stmt }
                    );
                    if ( ! defined $print_col ) {
                        if ( @{$sql->{quote}{chosen_columns}} ) {
                            $sql->{quote}{chosen_columns} = [];
                            $sql->{print}{chosen_columns} = [];
                            next COLUMNS;
                        }
                        else {
                            $sql->{quote}{chosen_columns} = [];
                            $sql->{print}{chosen_columns} = [];
                            last COLUMNS;
                        }
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
                    my $choices = [ $info->{ok}, $DISTINCT, $ALL ];
                    unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
                    my $select_distinct = choose(
                        $choices,
                        { prompt => 'Choose: ', %lyt_stmt }
                    );
                    if ( ! defined $select_distinct ) {
                        if ( $sql->{quote}{distinct_stmt} ) {
                            $sql->{quote}{distinct_stmt} = '';
                            $sql->{print}{distinct_stmt} = '';
                            next DISTINCT;
                        }
                        else {
                            $sql->{quote}{distinct_stmt} = '';
                            $sql->{print}{distinct_stmt} = '';
                            last DISTINCT;
                        }
                    }
                    if ( $select_distinct eq $info->{ok} ) {
                        last DISTINCT;
                    }
                    $select_distinct =~ s/^\s+|\s+\z//g;
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
                    my $choices = [ $info->{ok}, @cols ];
                    unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
                    my $print_col = choose(
                        $choices,
                        { prompt => 'Choose: ', %lyt_stmt }
                    );
                    if ( ! defined $print_col ) {
                        if ( $sql->{quote}{where_stmt} ne " WHERE" ) {
                            $sql->{quote}{where_args} = [];
                            $sql->{quote}{where_stmt} = " WHERE";
                            $sql->{print}{where_stmt} = " WHERE";
                            $count = 0;
                            $AND_OR = '';
                            next WHERE;
                        }
                        else {
                            $sql->{quote}{where_args} = [];
                            $sql->{quote}{where_stmt} = '';
                            $sql->{print}{where_stmt} = '';
                            last WHERE;
                        }
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
                        my $choices = [ $AND, $OR ];
                        unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
                        $AND_OR = choose(
                            $choices,
                            { prompt => 'Choose:', %lyt_stmt }
                        );
                        if ( ! defined $AND_OR ) {
                            if ( $sql->{quote}{where_stmt} ) {
                                $sql->{quote}{where_args} = [];
                                $sql->{quote}{where_stmt} = " WHERE";
                                $sql->{print}{where_stmt} = " WHERE";
                                $count = 0;
                                $AND_OR = '';
                                next WHERE;
                            }
                            else {
                                $sql->{quote}{where_args} = [];
                                $sql->{quote}{where_stmt} = '';
                                $sql->{print}{where_stmt} = '';
                                last WHERE;
                            }
                        }
                        $AND_OR =~ s/^\s+|\s+\z//g;
                        $AND_OR = ' ' . $AND_OR;
                    }
                    ( my $quote_col = $quote_cols->{$print_col} ) =~ s/\sAS\s\S+\z//;
                    $sql->{quote}{where_stmt} .= $AND_OR . ' ' . $quote_col;
                    $sql->{print}{where_stmt} .= $AND_OR . ' ' . $print_col;
                    set_operator_sql( $info, $opt, $sql, 'where', $table, \@cols, $quote_cols, $quote_col );
                    if ( ! $sql->{quote}{where_stmt} ) {
                        $sql->{quote}{where_args} = [];
                        $sql->{quote}{where_stmt} = " WHERE";
                        $sql->{print}{where_stmt} = " WHERE";
                        $count = 0;
                        $AND_OR = '';
                        next WHERE;
                    }
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
                    my $choices = [ $info->{ok}, @{$info->{aggregate_functions}} ];
                    unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
                    my $func = choose(
                        $choices,
                        { prompt => 'Choose:', %lyt_stmt }
                    );
                    if ( ! defined $func ) {
                        if ( $sql->{quote}{aggregate_stmt} ) {
                            delete @{$quote_cols}{@{$sql->{quote}{aliases}}};
                            $sql->{quote}{aliases}        = [];
                            $sql->{quote}{aggregate_stmt} = '';
                            $sql->{print}{aggregate_stmt} = '';
                            next AGGREGATE;
                        }
                        else {
                            delete @{$quote_cols}{@{$sql->{quote}{aliases}}};
                            $sql->{quote}{aliases}        = [];
                            $sql->{quote}{aggregate_stmt} = '';
                            $sql->{print}{aggregate_stmt} = '';
                            last AGGREGATE;
                        }
                    }
                    if ( $func eq $info->{ok} ) {
                        last AGGREGATE;
                    }
                    my ( $print_col, $quote_col );
                    if ( $func =~ /^count\s*\(\s*\*\s*\)\z/i ) {
                        $print_col = '*';
                        $quote_col = '*';
                    }
                    $func =~ s/\s*\(\s*\S\s*\)\z//;
                    $sql->{quote}{aggregate_stmt} .= $info->{col_sep} . $func . '(';
                    $sql->{print}{aggregate_stmt} .= $info->{col_sep} . $func . '(';
                    if ( ! defined $print_col ) {
                        print_select_statement( $sql, $table );
                        # Choose
                        my $choices = [ @cols ];
                        unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
                        $print_col = choose(
                            $choices,
                            { prompt => 'Choose:', %lyt_stmt }
                        );
                        if ( ! defined $print_col ) {
                            if ( $sql->{quote}{aggregate_stmt} ) {
                                delete @{$quote_cols}{@{$sql->{quote}{aliases}}};
                                $sql->{quote}{aliases}        = [];
                                $sql->{quote}{aggregate_stmt} = '';
                                $sql->{print}{aggregate_stmt} = '';
                                next AGGREGATE;
                            }
                            else {
                                delete @{$quote_cols}{@{$sql->{quote}{aliases}}};
                                $sql->{quote}{aliases}        = [];
                                $sql->{quote}{aggregate_stmt} = '';
                                $sql->{print}{aggregate_stmt} = '';
                                last AGGREGATE;
                            }
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
                    my $choices = [ $info->{ok}, @cols ];
                    unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
                    my $print_col = choose(
                        $choices,
                        { prompt => 'Choose: ', %lyt_stmt }
                    );
                    if ( ! defined $print_col ) {
                       if ( $sql->{quote}{group_by_cols} ) {
                            $sql->{quote}{group_by_stmt} = " GROUP BY";
                            $sql->{print}{group_by_stmt} = " GROUP BY";
                            $sql->{quote}{group_by_cols} = '';
                            $sql->{print}{group_by_cols} = '';
                            $info->{col_sep} = ' ';
                            next GROUP_BY;
                        }
                        else {
                            $sql->{quote}{group_by_stmt} = '';
                            $sql->{print}{group_by_stmt} = '';
                            $sql->{quote}{group_by_cols} = '';
                            $sql->{print}{group_by_cols} = '';
                            last GROUP_BY;
                        }
                    }
                    if ( $print_col eq $info->{ok} ) {
                        if ( $info->{col_sep} eq ' ' ) {
                            $sql->{quote}{group_by_stmt} = '';
                            $sql->{print}{group_by_stmt} = '';
                        }
                        last GROUP_BY;
                    }
                    ( my $quote_col = $quote_cols->{$print_col} ) =~ s/\sAS\s\S+\z//;
                    if ( ! @{$sql->{quote}{chosen_columns}} ) {
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
                    my $choices = [ $info->{ok}, @{$info->{aggregate_functions}}, @{$sql->{quote}{aliases}} ];
                    unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
                    my $func = choose(
                        $choices,
                        { prompt => 'Choose:', %lyt_stmt }
                    );
                    if ( ! defined $func ) {
                        if ( $sql->{quote}{having_stmt} ne " HAVING" ) {
                            $sql->{quote}{having_args} = [];
                            $sql->{quote}{having_stmt} = " HAVING";
                            $sql->{print}{having_stmt} = " HAVING";
                            $count = 0;
                            $AND_OR = '';
                            next HAVING;
                        }
                        else {
                            $sql->{quote}{having_args} = [];
                            $sql->{quote}{having_stmt} = '';
                            $sql->{print}{having_stmt} = '';
                            last HAVING;
                        }
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
                        my $choices = [ $AND, $OR ];
                        unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
                        $AND_OR = choose(
                            $choices,
                            { prompt => 'Choose:', %lyt_stmt }
                        );
                        if ( ! defined $AND_OR ) {
                            $sql->{quote}{having_args} = [];
                            $sql->{quote}{having_stmt} = '';
                            $sql->{print}{having_stmt} = '';
                            last HAVING;
                            if ( $sql->{quote}{having_stmt} ) {
                                $sql->{quote}{having_args} = [];
                                $sql->{quote}{having_stmt} = " HAVING";
                                $sql->{print}{having_stmt} = " HAVING";
                                $count = 0;
                                $AND_OR = '';
                                next HAVING;
                            }
                            else {
                                $sql->{quote}{having_args} = [];
                                $sql->{quote}{having_stmt} = '';
                                $sql->{print}{having_stmt} = '';
                                last HAVING;
                            }
                        }
                        $AND_OR =~ s/^\s+|\s+\z//g;
                        $AND_OR = ' ' . $AND_OR;
                    }
                    my ( $print_col, $quote_col );
                    if ( any { $_ eq $func } @{$sql->{quote}{aliases}} ) {
                        $sql->{quote}{having_stmt} .= $AND_OR . ' ' . $quote_cols->{$func};  #
                        $sql->{print}{having_stmt} .= $AND_OR . ' ' . $func;
                    }
                    else {
                        if ( $func =~ /^count\s*\(\s*\*\s*\)\z/i ) {
                            $print_col = '*';
                            $quote_col = '*';
                        }
                        $func =~ s/\s*\(\s*\S\s*\)\z//;
                        $sql->{quote}{having_stmt} .= $AND_OR . ' ' . $func . '(';
                        $sql->{print}{having_stmt} .= $AND_OR . ' ' . $func . '(';
                        if ( ! defined $print_col ) {
                            print_select_statement( $sql, $table );
                            # Choose
                            my $choices = [ @cols ];
                            unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
                            $print_col = choose(
                                $choices,
                                { prompt => 'Choose:', %lyt_stmt }
                            );
                            if ( ! defined $print_col ) {
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
                    set_operator_sql( $info, $opt, $sql, 'having', $table, \@cols, $quote_cols, $quote_col );
                    if ( ! $sql->{quote}{where_stmt} ) {
                        $sql->{quote}{where_args} = [];
                        $sql->{quote}{where_stmt} = " HAVING";
                        $sql->{print}{where_stmt} = " HAVING";
                        $count = 0;
                        $AND_OR = '';
                        next WHERE;
                    }
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
                    my $choices = [ $info->{ok}, @cols ];
                    unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
                    my $print_col = choose(
                        $choices,
                        { prompt => 'Choose: ', %lyt_stmt }
                    );
                    if ( ! defined $print_col ) {
                        if ( $sql->{quote}{order_by_stmt} ne " ORDER BY" ) {
                            $sql->{quote}{order_by_args} = [];
                            $sql->{quote}{order_by_stmt} = " ORDER BY";
                            $sql->{print}{order_by_stmt} = " ORDER BY";
                            $info->{col_sep} = ' ';
                            next ORDER_BY;
                        }
                        else {
                            $sql->{quote}{order_by_args} = [];
                            $sql->{quote}{order_by_stmt} = '';
                            $sql->{print}{order_by_stmt} = '';
                            last ORDER_BY;
                        }
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
                    $choices = [ $ASC, $DESC ];
                    unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
                    my $direction = choose(
                        $choices,
                        { prompt => 'Choose:', %lyt_stmt }
                    );
                    if ( ! defined $direction ){
                        $sql->{quote}{order_by_args} = [];
                        $sql->{quote}{order_by_stmt} = " ORDER BY";
                        $sql->{print}{order_by_stmt} = " ORDER BY";
                        $info->{col_sep} = ' ';
                        next ORDER_BY;
                    }
                    $direction =~ s/^\s+|\s+\z//g;
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
                    my $choices = [ $info->{ok}, $only_limit, $offset_and_limit ];
                    unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
                    my $choice = choose(
                        $choices,
                        { prompt => 'Choose: ', %lyt_stmt }
                    );
                    if ( ! defined $choice ) {
                        $sql->{quote}{limit_args} = [];
                        $sql->{quote}{limit_stmt} = '';
                        $sql->{print}{limit_stmt} = '';
                        last LIMIT;
                    }
                    if ( $choice eq $info->{ok} ) {
                        if ( ! @{$sql->{quote}{limit_args}} ) {
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
                        if ( ! defined $offset ) {
                            next LIMIT;
                        }
                        push @{$sql->{quote}{limit_args}}, $offset;
                        $sql->{quote}{limit_stmt} .= ' ' . '?'     . ',';
                        $sql->{print}{limit_stmt} .= ' ' . $offset . ',';
                    }
                    print_select_statement( $sql, $table );
                    # Choose
                    my $limit = choose_a_number( $info, $opt, $digits, 'Compose LIMIT:' );
                    if ( ! defined $limit ) {
                        $sql->{quote}{limit_args} = [];
                        $sql->{quote}{limit_stmt} = " LIMIT";
                        $sql->{print}{limit_stmt} = " LIMIT";
                        next LIMIT;
                    }
                    push @{$sql->{quote}{limit_args}}, $limit;
                    $sql->{quote}{limit_stmt} .= ' ' . '?';
                    $sql->{print}{limit_stmt} .= ' ' . $limit;
                }
            }
            when( $customize{'print_table'} ) {
                my $cols_sql = '';
                if ( @{$sql->{quote}{chosen_columns}} ) {
                    $cols_sql = ' ' . join( ', ', @{$sql->{quote}{chosen_columns}} );
                }
                elsif ( ! @{$sql->{quote}{chosen_columns}} && $sql->{print}{group_by_cols} ) {
                    $cols_sql = $sql->{print}{group_by_cols};
                }
                if ( $sql->{quote}{aggregate_stmt} ) {
                    $cols_sql .= ',' if $cols_sql;
                    $cols_sql .= $sql->{quote}{aggregate_stmt};
                }
                $cols_sql = $default_cols_sql if ! $cols_sql;
                my $select .= "SELECT" . $sql->{quote}{distinct_stmt} . $cols_sql . $from_stmt;
                $select .= $sql->{quote}{where_stmt};
                $select .= $sql->{quote}{group_by_stmt};
                $select .= $sql->{quote}{having_stmt};
                $select .= $sql->{quote}{order_by_stmt};
                $select .= $sql->{quote}{limit_stmt};
                my @arguments = ( @{$sql->{quote}{where_args}}, @{$sql->{quote}{having_args}}, @{$sql->{quote}{limit_args}} );
                # $dbh->{LongReadLen} = (GetTerminalSize)[0] * 4;
                # $dbh->{LongTruncOk} = 1;
                my $work_around = 0; # https://rt.cpan.org/Public/Bug/Display.html?id=62458
                if ( $info->{db_type} eq 'mysql' && $dbh->{mysql_bind_type_guessing} &&  any { /^[0-9]*e[0-9]*\z/ } @arguments ) {
                    $dbh->{mysql_bind_type_guessing} = 0;
                    $work_around = 1;
                }
                my $sth = $dbh->prepare( $select );
                $sth->execute( @arguments );
                if ( $work_around ) {
                    $dbh->{mysql_bind_type_guessing} = 1;
                    $work_around = 0;
                }
                my $col_names = $sth->{NAME};
                my $col_types = $sth->{db_TYPE_string( $info )};
                if ( $info->{db_type} eq 'sqlite' && $table eq 'union_tables' && @{$sql->{quote}{chosen_columns}} ) {
                    $col_names = [ map { s/^"([^"]+)"\z/$1/; $_ } @$col_names ];
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


sub set_operator_sql {
    my ( $info, $opt, $sql, $clause, $table, $cols, $quote_cols, $quote_col ) = @_;
    my ( $stmt, $args );
    my %lyt_stmt = %{$info->{lyt_h}};
    if ( $clause eq 'where' ) {
        $stmt = 'where_stmt';
        $args = 'where_args';
    }
    elsif ( $clause eq 'having' ) {
        $stmt = 'having_stmt';
        $args = 'having_args';
    }
    print_select_statement( $sql, $table );
    # Choose
    my $choices = [ @{$opt->{menu}{operators}[v]} ];
    unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
    my $operator = choose(
        $choices,
        { prompt => 'Choose:', %lyt_stmt }
    );
    if ( ! defined $operator ) {
        $sql->{quote}{$args} = [];
        $sql->{quote}{$stmt} = '';
        $sql->{print}{$stmt} = '';
        return;
    }
    $operator =~ s/^\s+|\s+\z//g;
    if ( $operator !~ /REGEXP\z/ ) {
        $sql->{quote}{$stmt} .= ' ' . $operator;
        $sql->{print}{$stmt} .= ' ' . $operator;
    }
    if ( $operator =~ /NULL\z/ ) {
        # do nothing
    }
    elsif ( $operator =~ /^(?:NOT\s)?IN\z/ ) {
        $info->{col_sep} = ' ';
        $sql->{quote}{$stmt} .= '(';
        $sql->{print}{$stmt} .= '(';

        IN: while ( 1 ) {
            print_select_statement( $sql, $table );
            # Choose
            my $choices = [ $info->{ok}, @$cols ];
            unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
            my $print_col = choose(
                $choices,
                { prompt => "$operator: ", %lyt_stmt }
            );
            if ( ! defined $print_col ) {
                $sql->{quote}{$args} = [];
                $sql->{quote}{$stmt} = '';
                $sql->{print}{$stmt} = '';
                return;
            }
            if ( $print_col eq $info->{ok} ) {
                if ( $info->{col_sep} eq ' ' ) {
                    $sql->{quote}{$args} = [];
                    $sql->{quote}{$stmt} = '';
                    $sql->{print}{$stmt} = '';
                    return;
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
    elsif ( $operator =~ /^(?:NOT\s)?BETWEEN\z/ ) {
        print_select_statement( $sql, $table );
        # Readline
        my $value_1 = local_read_line( prompt => 'Value_1: ' );
        $sql->{quote}{$stmt} .= ' ' . '?' .      ' AND';
        $sql->{print}{$stmt} .= ' ' . $value_1 . ' AND';
        push @{$sql->{quote}{$args}}, $value_1;
        print_select_statement( $sql, $table );
        # Readline
        my $value_2 = local_read_line( prompt => 'Value_2: ' );
        $sql->{quote}{$stmt} .= ' ' . '?';
        $sql->{print}{$stmt} .= ' ' . $value_2;
        push @{$sql->{quote}{$args}}, $value_2;
    }
    elsif ( $operator =~ /REGEXP\z/ ) {
        $sql->{print}{$stmt} .= ' ' . $operator;
        print_select_statement( $sql, $table );
        # Readline
        my $value = local_read_line( prompt => 'Pattern: ' );
        $value = '^$' if ! length $value;
        if ( $info->{db_type} eq 'sqlite' ) {
            $value = qr/$value/i if ! $opt->{sql}{regexp_case}[v];
            $value = qr/$value/  if   $opt->{sql}{regexp_case}[v];
        }
        $sql->{quote}{$stmt} =~ s/.*\K\s\Q$quote_col\E//;
        $sql->{quote}{$stmt} .= sql_regexp( $info, $opt, $quote_col, $operator =~ /^NOT/ ? 1 : 0 );
        $sql->{print}{$stmt} .= ' ' . "'$value'";
        push @{$sql->{quote}{$args}}, $value;
    }
    else {
        print_select_statement( $sql, $table );
        # Readline
        my $value = local_read_line( prompt => $operator =~ /LIKE\z/ ? 'Pattern: ' : 'Value: ' );
        $sql->{quote}{$stmt} .= ' ' . '?';
        $sql->{print}{$stmt} .= ' ' . "'$value'";
        push @{$sql->{quote}{$args}}, $value;
    }
    return;
}


#----------------------------------------------------------------------------------------------------#
#-- 444 -----------------------------   prepare print routines   ------------------------------------#
#----------------------------------------------------------------------------------------------------#


sub print_loop {
    my ( $info, $opt, $dbh, $all_arrayref, $col_names, $col_types ) = @_;
    my $begin = 0;
    my $end = $opt->{print}{limit}[v] - 1;
    my @choices;
    my $rows = @$all_arrayref;
    if ( $rows > $opt->{print}{limit}[v] ) {
        my $lr = length $rows;
        push @choices, sprintf "  %${lr}d - %${lr}d  ", $begin, $end;
        $rows -= $opt->{print}{limit}[v];
        while ( $rows > 0 ) {
            $begin += $opt->{print}{limit}[v];
            $end   += ( $rows > $opt->{print}{limit}[v] ) ? $opt->{print}{limit}[v] : $rows;
            push @choices, sprintf "  %${lr}d - %${lr}d  ", $begin, $end;
            $rows -= $opt->{print}{limit}[v];
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
            last PRINT if ! defined $choice;
            $start = ( split /\s*-\s*/, $choice )[0];
            $start =~ s/^\s+//;
            $stop = $start + $opt->{print}{limit}[v] - 1;
            $stop = $#$all_arrayref if $stop > $#$all_arrayref;
            print_table( $info, $opt, [ @{$all_arrayref}[ $start .. $stop ] ], $col_names, $col_types );
        }
        else {
            print_table( $info, $opt, $all_arrayref, $col_names, $col_types );
            last PRINT;
        }
    }
}


sub calc_widths {
    my ( $info, $opt, $a_ref, $col_types, $terminal_width ) = @_;
    my ( $cols_head_width, $width_columns, $is_binary_datatype, $not_a_number );
    my $count = 0;
    say 'Computing: ...';
    for my $row ( @$a_ref ) {
        $count++;
        for my $i ( 0 .. $#$row ) {
            $width_columns->[$i] ||= 1;
            $row->[$i] = $opt->{print}{undef}[v] if ! defined $row->[$i];
            if ( $count == 1 ) {
                # column name
                $row->[$i] =~ s/\p{Space}+/ /g;
                $row->[$i] =~ s/\p{Cntrl}//g;
                utf8::upgrade( $row->[$i] );
                my $gcstring = Unicode::GCString->new( $row->[$i] );
                $cols_head_width->[$i] = $gcstring->columns();
                my $binary_datatype_regexp = binary_datatypes_regexp( $info );
                if ( $col_types->[$i] =~ $binary_datatype_regexp ) {
                    $width_columns->[$i] = $info->{binary_length} if $info->{binary_length} > $width_columns->[$i];
                    $is_binary_datatype->[$i] = 1;
                }
            }
            else {
                # normal row
                if ( $opt->{print}{binary_filter}[v] ) {
                    if ( $is_binary_datatype->[$i] ) {
                        $row->[$i] = $info->{binary_string};
                    }
                    elsif ( $info->{db_type} eq 'sqlite' && substr( $row->[$i], 0, 100 ) =~ /\0/ ) {
                        $row->[$i] = $info->{binary_string};
                        $width_columns->[$i] = $info->{binary_length} if $info->{binary_length} > $width_columns->[$i];
                    }
                    else {
                        $row->[$i] =~ s/\p{Space}+/ /g;
                        $row->[$i] =~ s/\p{Cntrl}//g;
                        utf8::upgrade( $row->[$i] );
                        my $gcstring = Unicode::GCString->new( $row->[$i] );
                        $width_columns->[$i] = $gcstring->columns() if $gcstring->columns() > $width_columns->[$i];
                    }
                }
                else {
                    $row->[$i] =~ s/\p{Space}+/ /g;
                    $row->[$i] =~ s/\p{Cntrl}//g;
                    utf8::upgrade( $row->[$i] );
                    my $gcstring = Unicode::GCString->new( $row->[$i] );
                    $width_columns->[$i] = $gcstring->columns() if $gcstring->columns() > $width_columns->[$i];
                }
                ++$not_a_number->[$i] if ! looks_like_number $row->[$i];
            }
        }
    }
    if ( sum( @$width_columns ) + $opt->{print}{tab_width}[v] * ( @$width_columns - 1 ) < $terminal_width ) {
        # auto cut
        MAX: while ( 1 ) {
            my $count = 0;
            my $sum = sum( @$width_columns ) + $opt->{print}{tab_width}[v] * ( @$width_columns - 1 );
            for my $i ( 0 .. $#$cols_head_width ) {
                if ( $cols_head_width->[$i] > $width_columns->[$i] ) {
                    $width_columns->[$i]++;
                    $count++;
                    last MAX if ( $sum + $count ) == $terminal_width;
                }
            }
            last MAX if $count == 0;
        }
    }
    return $width_columns, $not_a_number;
}


sub minus_x_percent {
    my ( $value, $percent ) = @_;
    return int $value - ( $value * 1/100 * $percent );
}

sub recalc_widths {
    my ( $info, $opt, $terminal_width, $a_ref, $col_types ) = @_;
    my ( $width_columns, $not_a_number ) = calc_widths( $info, $opt, $a_ref, $col_types, $terminal_width );
    return if ! defined $width_columns || ! @$width_columns;
    my $sum = sum( @$width_columns ) + $opt->{print}{tab_width}[v] * ( @$width_columns - 1 );
    my @tmp_width_columns = @$width_columns;
    my $percent = 0;
    my $minimum_with = $opt->{print}{min_col_width}[v];
    while ( $sum > $terminal_width ) {
        $percent += 0.5;
        if ( $percent >= 100 ) {
            say 'Terminal window is not wide enough to print this table.';
            choose( [ 'Press ENTER to show the column names' ], { prompt => 0 } );
            choose( $a_ref->[0], { prompt => 'Column names (close with ENTER):', layout => 0 } );
            return;
        }
        my $count = 0;
        for my $i ( 0 .. $#tmp_width_columns ) {
            next if $minimum_with >= $tmp_width_columns[$i];
            if ( $minimum_with >= minus_x_percent( $tmp_width_columns[$i], $percent ) ) {
                $tmp_width_columns[$i] = $minimum_with;
            }
            else {
                $tmp_width_columns[$i] = minus_x_percent( $tmp_width_columns[$i], $percent );
            }
            $count++;
            last if $sum <= $terminal_width;
        }
        $minimum_with-- if $count == 0 && $minimum_with > 1;
        $sum = sum( @tmp_width_columns ) + $opt->{print}{tab_width}[v] * ( @tmp_width_columns - 1 );
    }
    my $rest = $terminal_width - $sum;
    while ( $rest > 0 ) {
        my $count = 0;
        for my $i ( 0 .. $#tmp_width_columns ) {
            if ( $tmp_width_columns[$i] < $width_columns->[$i] ) {
                $tmp_width_columns[$i]++;
                $rest--;
                $count++;
                last if $rest < 1;
            }
        }
        last if $count == 0;
    }
    $width_columns = [ @tmp_width_columns ] if @tmp_width_columns;
    return $width_columns, $not_a_number;
}


#----------------------------------------------------------------------------------------------------#
#-- 555 ----------------------------------   print routine   ----------------------------------------#
#----------------------------------------------------------------------------------------------------#


sub print_table {
    my ( $info, $opt, $a_ref, $col_names, $col_types ) = @_;
    my ( $terminal_width ) = GetTerminalSize( *STDOUT );
    return if ! defined $a_ref;
    unshift @$a_ref, $col_names if defined $col_names;
    my ( $width_columns, $not_a_number ) = recalc_widths( $info, $opt, $terminal_width, $a_ref, $col_types );
    return if ! defined $width_columns;
    my $items = @$a_ref * @{$a_ref->[0]};     #
    my $start = 10_000;                       #
    my $total = $#{$a_ref};                   #
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
    for my $row ( @$a_ref ) {
        my $string;
        for my $i ( 0 .. $#$width_columns ) {
            my $word = $row->[$i];
            my $right_justify = $not_a_number->[$i] ? 0 : 1;
            $string .= unicode_sprintf(
                        $width_columns->[$i],
                        $word,
                        $right_justify,
            );
            $string .= ' ' x $opt->{print}{tab_width}[v] if $i != $#$width_columns;
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
    $progress->update( $total ) if $total >= $next_update && $items > $start; #
    say 'Computing: ...' if $items > $start * 2;                              #
    choose( \@list, {
        prompt => 0, layout => 3, clear_screen => 1, limit => $opt->{print}{limit}[v] + 1,
        length_longest => sum( @$width_columns, $opt->{print}{tab_width}[v] * $#{$width_columns} ) }
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
            if ( ! defined $opt->{$section}{$key}[chs] ) {
                delete $opt->{$section}{$key};
            }
            else {
                push @choices, $opt->{$section}{$key}[chs];
            }
        }
    }
    my $change = 0;

    OPTIONS: while ( 1 ) {
        # Choose
        my $option = choose(
            [ undef, $info->{_help}, @choices, $info->{_confirm} ],
            { undef => $info->{_exit}, layout => 3, clear_screen => 1 }
        );
        my %lyt_nr = %{$info->{lyt_nr}};
        my %lyt_bol = %{$info->{lyt_h}};
        my ( $true, $false ) = ( 'YES', 'NO' );
        SWITCH: for ( $option ) {
            when ( ! defined ) { exit(); }
            when ( $info->{_confirm} ) { last OPTIONS; }
            when ( $info->{_help} ) { help(); choose( [ ' Close with ENTER ' ], { prompt => 0 } ) }
            when ( $opt->{"print"}{'tab_width'}[chs] ) {
                # Choose
                my $current_value = $opt->{print}{tab_width}[v];
                my $choice = choose(
                    [ undef, 0 .. 99 ],
                    { prompt => 'Tab width [' . $current_value . ']:',  %lyt_nr }
                );
                next SWITCH if ! defined $choice;
                $opt->{print}{tab_width}[v] = $choice;
                $change++;
            }
            when ( $opt->{"print"}{'min_col_width'}[chs] ) {
                # Choose
                my $current_value = $opt->{print}{min_col_width}[v];
                my $choice = choose(
                    [ undef, 0 .. 99 ],
                    { prompt => 'Minimum Column width [' . $current_value . ']:',  %lyt_nr }
                );
                next SWITCH if ! defined $choice;
                $opt->{print}{min_col_width}[v] = $choice;
                $change++;
            }
            when ( $opt->{"print"}{'undef'}[chs] ) {
                # Readline
                my $current_value = $opt->{print}{undef}[v];
                my $choice = local_read_line( prompt => 'Print replacement for undefined table vales ["' . $current_value . '"]: ' );
                next SWITCH if ! defined $choice;
                $opt->{print}{undef}[v] = $choice;
                $change++;
            }
            when ( $opt->{"print"}{'binary_filter'}[chs] ) {
                # Choose
                my $current_value = $opt->{print}{binary_filter}[v] ? 'YES' : 'NO';
                my $choice = choose(
                    [ undef, $true, $false ],
                    { prompt => 'Enable Binary Filter [' . $current_value . ']:', %lyt_bol }
                );
                next SWITCH if ! defined $choice;
                $opt->{print}{binary_filter}[v] = ( $choice eq $true ) ? 1 : 0;
                $change++;
            }
            when ( $opt->{"print"}{'limit'}[chs] ) {
                my $current_value = $opt->{print}{limit}[v];
                $current_value =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{menu}{kilo_sep}[v]/g;
                my $prompt = 'Compose new "limit" [' . $current_value . ']:';
                # Choose
                my $choice = choose_a_number( $info, $opt, 7, $prompt );
                next SWITCH if ! defined $choice;
                $opt->{print}{limit}[v] = $choice;
                $change++;
            }
            when ( $opt->{"sql"}{'lock_stmt'}[chs] ) {
                # Choose
                my ( $lk0, $lk1 ) = ( 'Lk0', 'Lk1' );
                my $current_value = $opt->{sql}{lock_stmt}[v] ? $lk1 : $lk0;
                my $choice = choose(
                    [ undef, $lk0, $lk1 ],
                    { prompt => 'Keep statement: set the default value [' . $current_value . ']:', %lyt_bol }
                );
                next SWITCH if ! defined $choice;
                $opt->{sql}{lock_stmt}[v] = ( $choice eq $lk1 ) ? 1 : 0;
                $change++;
            }
            when ( $opt->{"sql"}{'system_info'}[chs] ) {
                # Choose
                my $current_value = $opt->{sql}{system_info}[v] ? 'YES' : 'NO';
                my $choice = choose(
                    [ undef, $true, $false ],
                    { prompt => 'Enable system DBs/schemas/tables [' . $current_value . ']:', %lyt_bol }
                );
                next SWITCH if ! defined $choice;
                $opt->{sql}{system_info}[v] = ( $choice eq $true ) ? 1 : 0;
                $change++;
            }
            when ( $opt->{"sql"}{'regexp_case'}[chs] ) {
                # Choose
                my $current_value = $opt->{sql}{regexp_case}[v] ? 'YES' : 'NO';
                my $choice = choose(
                    [ undef, $true, $false ],
                    { prompt => 'REGEXP matches case sensitiv [' . $current_value . ']:', %lyt_bol }
                );
                next SWITCH if ! defined $choice;
                $opt->{sql}{regexp_case}[v] = ( $choice eq $true ) ? 1 : 0;
                $change++;
            }
            when ( $opt->{"dbs"}{'db_login'}[chs] ) {
                # Choose
                my $current_value = $opt->{dbs}{db_login}[v] ? 'YES' : 'NO';
                my $choice = choose(
                    [ undef, $true, $false ],
                    { prompt => 'Ask for every new DB connection [' . $current_value . ']:', %lyt_bol }
                );
                next SWITCH if ! defined $choice;
                $opt->{dbs}{db_login}[v] = ( $choice eq $true ) ? 1 : 0;
                $change++;
            }
            when ( $opt->{"dbs"}{'db_defaults'}[chs] ) {
                my $ret = database_setting( $info, $opt );
                $change++ if $ret;
            }
            when ( $opt->{"menu"}{'kilo_sep'}[chs] ) {
                my ( $comma, $full_stop, $underscore, $space, $none ) = ( 'comma', 'full stop', 'underscore', 'space', 'none' );
                my %sep_h = (
                    $comma      => ',',
                    $full_stop  => '.',
                    $underscore => '_',
                    $space      => ' ',
                    $none       => '',
                );
                # Choose
                my $current_value = $opt->{menu}{kilo_sep}[v];
                my $choice = choose(
                    [ undef, $comma, $full_stop, $underscore, $space, $none ],
                    { prompt => 'Thousands separator [' . $current_value . ']:',  %lyt_bol }
                );
                next SWITCH if ! defined $choice;
                $opt->{menu}{kilo_sep}[v] = $sep_h{$choice};
                $change++;
            }
            when ( $opt->{"menu"}{'sssc_mode'}[chs] ) {
                my ( $simple, $compat ) = ( 'simple', 'compat' );
                # Choose
                my $current_value = $opt->{menu}{sssc_mode}[v] ? $compat : $simple;
                my $choice = choose(
                    [ undef, $simple, $compat ],
                    { prompt => 'sssc mode [' . $current_value . ']:',  %lyt_bol }
                );
                next SWITCH if ! defined $choice;
                $opt->{menu}{sssc_mode}[v] = $choice ? $compat : $simple;
                $change++;
            }
            when ( $opt->{"menu"}{'operators'}[chs] ) {
                my $current   = $opt->{menu}{operators}[v];
                my $available = $info->{avilable_operators};
                # Choose
                my $list = choose_list( $info, $current, $available );
                next SWITCH if ! defined $list;
                next SWITCH if ! @$list;
                $opt->{menu}{operators}[v] = $list;
                $change++;
            }
            when ( $opt->{"menu"}{'database_types'}[chs] ) {
                my $current   = $opt->{menu}{database_types}[v];
                my $available = $info->{avilable_db_types};
                # Choose
                my $list = choose_list( $info, $current, $available );
                next SWITCH if ! defined $list;
                next SWITCH if ! @$list;
                $opt->{menu}{database_types}[v] = $list;
                $change++;
            }
            default { die "$option: no such value in the hash \%opt"; }
        }
    }
    if ( $change ) {
        write_config_file( $opt, $info->{config_file} );
    }
    return $opt;
}


sub database_setting {
    my ( $info, $opt, $db ) = @_;
    my @choices = ();
    my $db_type;
    if ( ! defined $db ) {
        $db_type = choose( $opt->{menu}{database_types}[v], { layout => 1 } );
        return if ! defined $db_type;
    }
    else {
        $db_type = $info->{db_type};
    }
    for my $key ( @{$info->{$db_type}{keys}} ) {
        if ( ! defined $opt->{$db_type}{$key}[chs] ) {
            delete $opt->{$db_type}{$key};
        }
        else {
            push @choices, $opt->{$db_type}{$key}[chs];
        }
    }
    if ( defined $info->{$db_type}{commandline_only} ) {
        for my $key ( @{$info->{$db_type}{commandline_only}} ) {
            my $idx = first_index { $_ eq $opt->{$db_type}{$key}[chs] } @choices;
            splice( @choices, $idx, 1 );
        }
    }

    my $section = $db // $db_type;
    my $change = 0;
    my $new = {};
    my ( $true, $false ) = ( 'YES', 'NO' );

    OPTIONS_DB: while ( 1 ) {
        # Choose
        my $option = choose(
            [ undef, @choices, $info->{_confirm} ],
            { undef => $info->{_back}, layout => 3, clear_screen => 1 }
        );
        if ( ! defined $option ) {
            $change = 0;
            return $change;
        }
        if ( $option eq $info->{_confirm} ) {
            if ( $change ) {
                for my $key ( keys %{$new->{$section}} ) {
                    $opt->{$section}{$key}[v] = $new->{$section}{$key};
                }
                write_config_file( $opt, $info->{config_file} );
            }
            return $change;
        }
        my %lyt_nr = %{$info->{lyt_nr}};
        my %lyt_bol = %{$info->{lyt_h}};
        if ( $db_type eq 'sqlite' ) {
            SQLITE: for ( $option ) {
                when ( $opt->{'sqlite'}{unicode}[chs] ) {
                    # Choose
                    my $key = 'unicode';
                    my $unicode = current_value( $key, $db_type, $db );
                    my $choice = choose(
                        [ undef, $true, $false ],
                        { prompt => 'Unicode [' . ( $unicode ? 'YES' : 'NO' ) . ']:', %lyt_bol }
                    );
                    if ( ! defined $choice ) {
                        delete $new->{$section}{$key};
                        next SQLITE;
                    }
                    $new->{$section}{$key} = ( $choice eq $true ) ? 1 : 0;
                    $change++;
                }
                when ( $opt->{'sqlite'}{see_if_its_a_number}[chs] ) {
                    # Choose
                    my $key = 'see_if_its_a_number';
                    my $see_if_its_a_number = current_value( $key, $db_type, $db );
                    my $choice = choose(
                        [ undef, $true, $false ],
                        { prompt => 'See if its a number [' . ( $see_if_its_a_number ? 'YES' : 'NO' ) . ']:', %lyt_bol }
                    );
                    if ( ! defined $choice ) {
                        delete $new->{$section}{$key};
                        next SQLITE;
                    }
                    $new->{$section}{$key} = ( $choice eq $true ) ? 1 : 0;
                    $change++;
                }
                when ( $opt->{'sqlite'}{busy_timeout}[chs] ) {
                    my $key = 'busy_timeout';
                    my $busy_timeout = current_value( $key, $db_type, $db );
                    $busy_timeout =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{menu}{kilo_sep}[v]/g;
                    my $prompt = 'Compose new "Busy timeout (ms)" [' . $busy_timeout . ']:';
                    # Choose
                    my $choice = choose_a_number( $info, $opt, 6, $prompt );
                    if ( ! defined $choice ) {
                        delete $new->{$section}{$key};
                        next SQLITE;
                    }
                    $new->{$section}{$key} = $choice;
                    $change++;
                }
                when ( $opt->{'sqlite'}{cache_size}[chs] ) {
                    my $key = 'cache_size';
                    my $cache_size = current_value( $key, $db_type, $db );
                    $cache_size =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{menu}{kilo_sep}[v]/g;
                    my $prompt = 'Compose new "Cache size (kb)" [' . $cache_size . ']:';
                    # Choose
                    my $choice = choose_a_number( $info, $opt, 8, $prompt );
                    if ( ! defined $choice ) {
                        delete $new->{$section}{$key};
                        next SQLITE;
                    }
                    $new->{$section}{$key} = $choice;
                    $change++;
                }
                default { die "$option: no such value in the hash \%opt"; }
            }
        }
        elsif ( $db_type eq 'mysql' ) {
            MYSQL: for ( $option ) {
                when ( $opt->{'mysql'}{enable_utf8}[chs] ) {
                    # Choose
                    my $key = 'enable_utf8';
                    my $utf8 = current_value( $key, $db_type, $db );
                    my $choice = choose(
                        [ undef, $true, $false ],
                        { prompt => 'Enable utf8 [' . ( $utf8 ? 'YES' : 'NO' ) . ']:', %lyt_bol }
                    );
                    if ( ! defined $choice ) {
                        delete $new->{$section}{$key};
                        next MYSQL;
                    }
                    $new->{$section}{$key} = ( $choice eq $true ) ? 1 : 0;
                    $change++;
                }
                when ( $opt->{'mysql'}{bind_type_guessing}[chs] ) {
                    # Choose
                    my $key = 'bind_type_guessing';
                    my $bind_type_guessing = current_value( $key, $db_type, $db );
                    my $choice = choose(
                        [ undef, $true, $false ],
                        { prompt => 'Bind type guessing [' . ( $bind_type_guessing ? 'YES' : 'NO' ) . ']:', %lyt_bol }
                    );
                    if ( ! defined $choice ) {
                        delete $new->{$section}{$key};
                        next MYSQL;
                    }
                    $new->{$section}{$key} = ( $choice eq $true ) ? 1 : 0;
                    $change++;
                }
                when ( $opt->{'mysql'}{ChopBlanks}[chs] ) {
                    # Choose
                    my $key = 'ChopBlanks';
                    my $chop_blanks = current_value( $key, $db_type, $db );
                    my $choice = choose(
                        [ undef, $true, $false ],
                        { prompt => 'Chop blanks off [' . ( $chop_blanks ? 'YES' : 'NO' ) . ']:', %lyt_bol }
                    );
                    if ( ! defined $choice ) {
                        delete $new->{$section}{$key};
                        next MYSQL;
                    }
                    $new->{$section}{$key} = ( $choice eq $true ) ? 1 : 0;
                    $change++;
                }
                when ( $opt->{'mysql'}{connect_timeout}[chs] ) {
                    my $key = 'connect_timeout';
                    my $connect_timeout = current_value( $key, $db_type, $db );
                    $connect_timeout =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{menu}{kilo_sep}[v]/g;
                    my $prompt = 'Compose new "Busy timeout (s)" [' . $connect_timeout . ']:';
                    # Choose
                    my $choice = choose_a_number( $info, $opt, 4, $prompt );
                    if ( ! defined $choice ) {
                        delete $new->{$section}{$key};
                        next MYSQL;
                    }
                    $new->{$section}{$key} = $choice;
                    $change++;
                }
                default { die "$option: no such value in the hash \%opt"; }
            }
        }
        elsif ( $db_type eq 'postgres' ) {
            POSTGRES: for ( $option ) {
                when ( $opt->{'postgres'}{pg_enable_utf8}[chs] ) {
                    # Choose
                    my $key = 'pg_enable_utf8';
                    my $utf8 = current_value( $key, $db_type, $db );
                    my $choice = choose(
                        [ undef, $true, $false ],
                        { prompt => 'Enable utf8 [' . ( $utf8 ? 'YES' : 'NO' ) . ']:', %lyt_bol }
                    );
                    if ( ! defined $choice ) {
                        delete $new->{$section}{$key};
                        next POSTGRES;
                    }
                    $new->{$section}{$key} = ( $choice eq $true ) ? 1 : 0;
                    $change++;
                }
                default { die "$option: no such value in the hash \%opt"; }
            }
        }
    }
}


sub current_value {
    my ( $key, $db_type, $db ) = @_;
    if ( ! defined $db ) {
        return $opt->{$db_type}{$key}[v];
    }
    else {
        return $opt->{$db}{$key}[v] // $opt->{$db_type}{$key}[v];
    }
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
    while ( ! defined( $line ) ) {
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
    my ( $file, $h_ref ) = @_;
    my $json = JSON::XS->new->pretty->encode( $h_ref );
    open my $fh, '>', encode_utf8( $file ) or die $!;
    print $fh $json;
    close $fh or die $!;
}


sub read_json {
    my ( $file ) = @_;
    return {} if ! -f encode_utf8( $file );
    my $json;
    {
        local $/ = undef;
        open my $fh, '<', encode_utf8( $file ) or die $!;
        $json = readline $fh;
        close $fh or die $!;
    }
    my $h_ref = JSON::XS->new->pretty->decode( $json ) if $json;
    return $h_ref;
}


sub choose_a_number {
    my ( $info, $opt, $digits, $prompt ) = @_;
    my %hash;
    my $number;
    my $reset = 'reset';

    NUMBER: while ( 1 ) {
        my $longest = $digits;
        $longest += int( ( $digits - 1 ) / 3 ) if $opt->{menu}{kilo_sep}[v] ne '';
        my @list = ();
        for my $di ( 0 .. $digits - 1 ) {
            my $begin = 1 . '0' x $di;
            $begin =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{menu}{kilo_sep}[v]/g;
            ( my $end = $begin ) =~ s/^1/9/;
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
        return if ! defined $range;
        last if $confirm && $range eq $confirm;
        $range = ( split /\s*-\s*/, $range )[0];
        $range =~ s/^\s*\d//;
        ( my $range_no_sep = $range ) =~ s/\Q$opt->{menu}{kilo_sep}[v]\E//g if $opt->{menu}{kilo_sep}[v] ne '';
        my $key = length $range_no_sep;
        # Choose
        my $choice = choose(
            [ undef, map( $_ . $range, 1 .. 9 ), $reset ],
            { %{$info->{lyt_h}}, undef => '<<' }
        );
        next if ! defined $choice;
        if ( $choice eq $reset ) {
            $hash{$key} = 0;
        }
        else {
            $choice =~ s/\Q$opt->{menu}{kilo_sep}[v]\E//g if $opt->{menu}{kilo_sep}[v] ne '';
            $hash{$key} = $choice;
        }
        $number = sum( @hash{keys %hash} );
        $number =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{menu}{kilo_sep}[v]/g;
    }
    $number =~ s/\Q$opt->{menu}{kilo_sep}[v]\E//g if $opt->{menu}{kilo_sep}[v] ne '';
    return $number;
}


sub choose_list {
    my ( $info, $current, $available ) = @_;
    my $new = [];
    while ( 1 ) {
        my $length_key = 9;
        my ( $terminal_width ) = GetTerminalSize( *STDOUT );
        my $col_max = $terminal_width - $length_key;
        my $line_fold = Text::LineFold->new(
            Charset       => 'utf-8',
            ColMax        => $col_max,
            OutputCharset => '_UNICODE_',
            Urgent        => 'FORCE',
        );
        my $key1 = 'Current > ';
        my $key2 = '    New > ';
        my $text1 = $line_fold->fold( '' , '', join ', ', map { "\"$_\"" } @$current );
        my $text2 = $line_fold->fold( '' , '', join ', ', map { "\"$_\"" } @$new );
        $text1 = ' ' if ! $text1;
        $text2 = ' ' if ! $text2;
        print GO_TO_TOP_LEFT;
        print CLEAR_EOS;
        for my $row ( split /\R+/, $text1 ) {
            printf "%${length_key}s%s\n", $key1, $row;
            $key1 = '';
        }
        for my $row ( split /\R+/, $text2 ) {
            printf "%${length_key}s%s\n", $key2, $row;
            $key2 = '';
        }
        say '';
        # Choose
        my $filter_type = choose(
            [ undef, map( "- $_", @$available ), $info->{_confirm} ],
            { prompt => 'Choose:', layout => 3, undef => $info->{_back} }
        );
        return if ! defined $filter_type;
        if ( $filter_type eq $info->{_confirm} ) {
            return $new if @$new;
            return;
        }
        $filter_type =~ s/^-\s//;
        push @$new, $filter_type;
    }
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


sub set_credentials {
    my ( $info, $opt, $db ) = @_;
    my $db_type = $info->{db_type};
    if ( $opt->{dbs}{db_login}[v] ) {
        print GO_TO_TOP_LEFT;
        print CLEAR_EOS;
        say "Database: $db";
        $info->{$db_type}{$db}{user}   = local_read_line( prompt => 'Username: ' )               if ! defined $info->{$db_type}{$db}{user};
        $info->{$db_type}{$db}{passwd} = local_read_line( prompt => 'Password: ', no_echo => 1 ) if ! defined $info->{$db_type}{$db}{passwd};
        return $info->{$db_type}{$db}{user}, $info->{$db_type}{$db}{passwd};
    }
    else {
        $info->{$db_type}{user}   = local_read_line( prompt => 'Enter username: ' )               if ! defined $info->{$db_type}{user};
        $info->{$db_type}{passwd} = local_read_line( prompt => 'Enter password: ', no_echo => 1 ) if ! defined $info->{$db_type}{passwd};
        return $info->{$db_type}{user}, $info->{$db_type}{passwd};
    }
}


##########################################################################################################################
###########################################   database specific subroutines   ############################################
##########################################################################################################################


sub get_db_handle {
    my ( $info, $opt, $db ) = @_;
    my $dbh;
    if ( $info->{db_type} eq 'sqlite' ) {
        die "\"$db\": $!. Maybe the cached data is not up to date." if ! -f $db;
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
        my ( $user, $passwd ) = set_credentials( $info, $opt, $db );
        $dbh = DBI->connect( "DBI:mysql:dbname=$db", $user, $passwd, {
            PrintError  => 0,
            RaiseError  => 1,
            AutoCommit  => 1,
            mysql_enable_utf8        => $opt->{$db}{enable_utf8}[v]        // $opt->{mysql}{enable_utf8}[v],
            mysql_connect_timeout    => $opt->{$db}{connect_timeout}[v]    // $opt->{mysql}{connect_timeout}[v],
            mysql_bind_type_guessing => $opt->{$db}{bind_type_guessing}[v] // $opt->{mysql}{bind_type_guessing}[v],
            ChopBlanks               => $opt->{$db}{ChopBlanks}[v]         // $opt->{mysql}{ChopBlanks}[v],
        } ) or die DBI->errstr;
    }
    elsif ( $info->{db_type} eq 'postgres' ) {
        my ( $user, $passwd ) = set_credentials( $info, $opt, $db );
        $dbh = DBI->connect( "DBI:Pg:dbname=$db", $user, $passwd, {
            PrintError => 0,
            RaiseError => 1,
            AutoCommit => 1,
            pg_enable_utf8 => $opt->{$db}{pg_enable_utf8}[v] // $opt->{postgres}{pg_enable_utf8}[v],
        } ) or die DBI->errstr;
    }
    else {
        no_entry_db_type( $info );;
    }
    return $dbh;
}


sub available_databases {
    my ( $info, $opt ) = @_;
    my $databases = [];
    if ( $info->{db_type} eq 'sqlite' ) {
        my @dirs = map { decode_utf8( $_ ) } @ARGV ? @ARGV : ( $info->{home} );
        my $c_dirs  = join ' ', @dirs;
        my $c_depth = $opt->{sqlite}{max_depth}[v] // '';
        $info->{cache} = read_json( $info->{db_cache_file} );
        if ( $opt->{sqlite}{reset_cache}[v] ) {
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
        if ( ! $info->{cached} ) {
            say 'Searching...';
            for my $dir ( @dirs ) {
                my $max_depth;
                if ( defined $opt->{sqlite}{max_depth}[v] ) {
                    $max_depth = $opt->{sqlite}{max_depth}[v];
                    $dir = rel2abs $dir;
                    $max_depth += $dir =~ tr[/][];
                    $max_depth--;
                }
                find( {
                    preprocess => sub {
                        if ( defined $max_depth ) {
                            my $depth = $File::Find::dir =~ tr[/][];
                            return @_ if $depth < $max_depth;
                            return grep { ! -d } @_ if $depth == $max_depth;
                            return;
                        }
                        else {
                            return @_;
                        }
                    },
                    wanted     => sub {
                        my $file = $File::Find::name;
                        return if ! -f $file;
                        return if ! -s $file;
                        return if ! -r $file;
                        #say $file;
                        if ( ! eval {
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
        if ( ! $opt->{sql}{system_info}[v] ) {
            $stmt = "SELECT schema_name FROM information_schema.schemata
                     WHERE schema_name != 'mysql' AND schema_name != 'information_schema' AND schema_name != 'performance_schema'
                     ORDER BY schema_name";
        }
        elsif ( $opt->{sql}{system_info}[v] ) {
            $stmt = "SELECT schema_name FROM information_schema.schemata
                     ORDER BY schema_name";
        }
        $databases = $dbh->selectcol_arrayref( $stmt, {} );
    }
    elsif( $info->{db_type} eq 'postgres' ) {
        my $dbh = get_db_handle( $info, $opt, 'postgres' );
        my $stmt;
        if ( ! $opt->{sql}{system_info}[v] ) {
            $stmt = "SELECT datname FROM pg_database
                     WHERE datistemplate = false AND datname != 'postgres'
                     ORDER BY datname";
        }
        elsif ( $opt->{sql}{system_info}[v] ) {
            $stmt = "SELECT datname FROM pg_database
                     ORDER BY datname";
        }
        $databases = $dbh->selectcol_arrayref( $stmt, {} );
    }
    else {
        no_entry_for_db_type( $info );;
    }
    return $databases;
}


sub get_schema_names {
    my ( $info, $opt, $dbh, $db ) = @_;
    return [ 'main' ] if $info->{db_type} eq 'sqlite';
    return [ $db ]    if $info->{db_type} eq 'mysql';
    if ( $info->{db_type} eq 'postgres' ) {
        # Schema names beginning with pg_ are reserved for system purposes and cannot be created by users.
        my $stmt;
        if ( ! $opt->{sql}{system_info}[v] ) {
            $stmt = "SELECT schema_name FROM information_schema.schemata
                     WHERE schema_name != 'information_schema' AND schema_name !~ '^pg_'
                     ORDER BY schema_name";
        }
        elsif ( $opt->{sql}{system_info}[v] ) {
            $stmt = "SELECT schema_name FROM information_schema.schemata
                     ORDER BY schema_name";
        }
        my $schemas = $dbh->selectcol_arrayref( $stmt, {} );
        return $schemas;
    }
    no_entry_for_db_type( $info );;
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


sub system_databases {
    my ( $info ) = @_;
    my $system_databases;
    if ( $info->{db_type} eq 'mysql' ) {
        $system_databases = [ qw( mysql information_schema performance_schema ) ];
    }
    elsif ( $info->{db_type} eq 'postgres' ) {
        $system_databases = [ qw( postgres template0 template1 ) ];
    }
    return $system_databases;
}


sub db_TYPE_string {
    my ( $info ) = @_;
    return 'TYPE'            if $info->{db_type} eq 'sqlite';
    return 'mysql_type_name' if $info->{db_type} eq 'mysql';
    return 'pg_type'         if $info->{db_type} eq 'postgres';
    no_entry_for_db_type( $info );;
}


sub column_names_and_types {
    my ( $info, $dbh, $db, $schema, $data ) = @_;
    if ( $info->{db_type} eq 'sqlite' ) {
        for my $table ( @{$data->{$db}{$schema}{tables}} ) {
            my $sth = $dbh->prepare( "SELECT * FROM " . $dbh->quote_identifier( undef, undef, $table ) );
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
            my $a_ref = $sth->fetchrow_arrayref();
            my @references = grep { /FOREIGN\sKEY/ } split /\s*\n\s*/, $a_ref->[0];
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
                $foreign_keys->{$table}{$row->{constraint_name}}{reference_table} = $row->{referenced_table_name} if ! $foreign_keys->{$table}{$row->{constraint_name}}{reference_table};
            }
            $primary_key_columns->{$table} = [ $dbh->primary_key( undef, $schema, $table ) ];
        }
        elsif ( $info->{db_type} eq 'postgres' ) {
            my $sth = $dbh->foreign_key_info( undef, undef, undef, undef, $schema, $table );
            if ( defined $sth ) {
                while ( my $row = $sth->fetchrow_hashref ) {
                    push @{$foreign_keys->{$table}{$row->{FK_NAME}}{foreign_key_col  }}, $row->{FK_COLUMN_NAME};
                    push @{$foreign_keys->{$table}{$row->{FK_NAME}}{reference_key_col}}, $row->{UK_COLUMN_NAME};
                    $foreign_keys->{$table}{$row->{FK_NAME}}{reference_table} = $row->{UK_TABLE_NAME} if ! $foreign_keys->{$table}{$row->{FK_NAME}}{reference_table};
                }
            }
            $primary_key_columns->{$table} = [ $dbh->primary_key( undef, $schema, $table ) ];
        }
    }
    return $primary_key_columns, $foreign_keys;
}


sub sql_regexp {
    my ( $info, $opt, $quote_col, $not ) = @_;
    if ( $not ) {
        if ( $info->{db_type} eq 'sqlite' ) {
            return ' '. $quote_col . ' NOT REGEXP ?';
        }
        if ( $info->{db_type} eq 'mysql' ) {
            return ' '. $quote_col . ' NOT REGEXP ?'        if ! $opt->{sql}{regexp_case}[v];
            return ' '. $quote_col . ' NOT REGEXP BINARY ?' if   $opt->{sql}{regexp_case}[v];
        }
        if ( $info->{db_type} eq 'postgres' ) {
            return ' '. $quote_col . ' !~* ?' if ! $opt->{sql}{regexp_case}[v];
            return ' '. $quote_col . ' !~ ?'  if   $opt->{sql}{regexp_case}[v];
        }
        if ( $info->{db_type} eq 'oracle' ) {
            return ' NOT REGEXP_LIKE( ' . $quote_col . ', ?, \'i\' )' if ! $opt->{sql}{regexp_case}[v];
            return ' NOT REGEXP_LIKE( ' . $quote_col . ', ? )'        if   $opt->{sql}{regexp_case}[v];
        }

    }
    else {
        if ( $info->{db_type} eq 'sqlite' ) {
            return ' '. $quote_col . ' REGEXP ?';
        }
        if ( $info->{db_type} eq 'mysql' ) {
            return ' '. $quote_col . ' REGEXP ?'        if ! $opt->{sql}{regexp_case}[v];
            return ' '. $quote_col . ' REGEXP BINARY ?' if   $opt->{sql}{regexp_case}[v];
        }
        if ( $info->{db_type} eq 'postgres' ) {
            return ' '. $quote_col . ' ~* ?' if ! $opt->{sql}{regexp_case}[v];
            return ' '. $quote_col . ' ~ ?'  if   $opt->{sql}{regexp_case}[v];
        }
        if ( $info->{db_type} eq 'oracle' ) {
            return ' REGEXP_LIKE( ' . $quote_col . ', ?, \'i\' )' if ! $opt->{sql}{regexp_case}[v];
            return ' REGEXP_LIKE( ' . $quote_col . ', ? )'        if   $opt->{sql}{regexp_case}[v];
        }
    }
    no_entry_for_db_type( $info );;
}


sub binary_datatypes_regexp {
    my ( $info ) = @_;
    return qr/^blob\z/i           if $info->{db_type} eq 'sqlite';
    return qr/binary|blob\z/i     if $info->{db_type} eq 'mysql';
    return qr/^(?:bytea|oid)\z/i  if $info->{db_type} eq 'postgres';
    no_entry_for_db_type( $info );
}


sub no_entry_for_db_type {
    my ( $info ) = @_;
    die "No entry for \"$info->{db_type}\"!";
}



__DATA__



