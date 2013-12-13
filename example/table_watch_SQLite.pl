#!/usr/bin/env perl
use warnings FATAL => qw(all);
use strict;
use 5.10.0;
use open qw(:std :utf8);
no warnings 'utf8';

# Version 1.065

use Encode                 qw(encode_utf8 decode_utf8);
use File::Basename         qw(basename);
use File::Find             qw(find);
use File::Spec::Functions  qw(catfile catdir rel2abs);
use Getopt::Long           qw(GetOptions);
use List::Util             qw(sum);
use Pod::Usage             qw(pod2usage);
use Scalar::Util           qw(looks_like_number);

use Clone                  qw(clone);
use DBI                    qw();
use JSON::XS               qw();
use File::HomeDir          qw(my_home);
use List::MoreUtils        qw(any none first_index pairwise);
use Term::Choose           qw(choose);
use Term::ProgressBar      qw();
use Term::ReadKey          qw(GetTerminalSize ReadKey ReadMode);
use Text::LineFold         qw();
use Unicode::GCString      qw();

END { ReadMode 0 }

use constant {
    BSPACE         => 0x7f,
    GO_TO_TOP_LEFT => "\e[1;1H",
    CLEAR_EOS      => "\e[0J",
    v              => 0,
    chs            => 1,
};

my $home = File::HomeDir->my_home();
my $config_dir = '.table_watch_conf';

if ( $home ) {
    $config_dir = catdir( $home, $config_dir );
    mkdir $config_dir or die $! if ! -d $config_dir;
}
else {
    say "Could not find the home directory!";
    exit;
}

my $info = {
    home                => $home,
    config_file         => catfile( $config_dir, 'tw_config.json' ),
    db_cache_file       => catfile( $config_dir, 'tw_cache_db_search.json' ),
    lyt_stmt            => { layout => 1, order => 0, justify => 2, clear_screen => 1, undef => '<<', lf => [0,4]  },
    lyt_1               => { layout => 1, order => 0, justify => 2, clear_screen => 0, undef => '<<' },
    lyt_stop            => { layout => 0, order => 1, justify => 0, clear_screen => 0 },
    lyt_3_cs            => { layout => 3, order => 1, justify => 0, clear_screen => 1, undef => '  BACK' },
    line_fold           => { Charset=> 'utf8', OutputCharset => '_UNICODE_', Urgent => 'FORCE' },
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
                             "BETWEEN", "NOT BETWEEN", " = ", " != ", " <> ", " < ", " > ", " >= ", " <= ",
                             "LIKE col", "NOT LIKE col", "LIKE %col%", "NOT LIKE %col%",
                             # "LIKE col%", "NOT LIKE col%", "LIKE %col", "NOT LIKE %col",
                             " = col", " != col", " <> col", " < col", " > col", " >= col", " <= col" ],
    avilable_db_types   => [ 'sqlite', 'mysql', 'postgres' ],
};

my $default_db_types  = [ 'sqlite', 'mysql', 'postgres' ];
my $default_operators = [ "REGEXP", " = ", " != ", " < ", " > ", "IS NULL", "IS NOT NULL", "IN" ];

my $opt = {
    print => {
        tab_width       => [ 2,       '- Tabwidth' ],
        min_col_width   => [ 30,      '- Colwidth' ],
        undef           => [ '',      '- Undef' ],
        limit           => [ 100_000, '- Rows' ],
        progress_bar    => [ 20_000,  '- ProgressBar' ],
    },
    sql => {
        lock_stmt       => [ 0, '- Lock' ],
        system_info     => [ 0, '- Metadata' ],
        regexp_case     => [ 0, '- Regexp case' ],
    },
    menu => {
        thsd_sep        => [ ',',                '- Separator' ],
        database_types  => [ $default_db_types,  '- DB types' ],
        operators       => [ $default_operators, '- Operators' ],
        sssc_mode       => [ 0,                  '- Sssc mode' ],
        mouse           => [ 0,                  '- Mouse mode' ],
        expand          => [ 1,                  '- Expand' ],
          keep_db_choice     => [ 0, '- Choose Database' ],
          keep_schema_choice => [ 0, '- Choose Schema' ],
          keep_table_choice  => [ 0, '- Choose Table' ],
          table_expand       => [ 1, '- Print  Table' ],
    },
    dbs => {
        login               => [ 0,       '- DB login' ],
        db_defaults         => [ 'Dummy', '- DB defaults' ],
    },
    sqlite => {
        reset_cache         => [ 0,       '- New search' ],
        unicode             => [ 1,       '- Unicode' ],
        see_if_its_a_number => [ 1,       '- See if its a number' ],
        busy_timeout        => [ 3_000,   '- Busy timeout (ms)' ],
        cache_size          => [ 500_000, '- Cache size (kb)' ],
        binary_filter       => [ 0,       '- Binary filter' ],
    },
    mysql => {
        default_user        => [ '',    '- Default user' ],
        enable_utf8         => [ 1,     '- Enable utf8' ],
        connect_timeout     => [ 4,     '- Connect timeout' ],
        bind_type_guessing  => [ 1,     '- Bind type guessing' ],
        ChopBlanks          => [ 1,     '- Chop blanks off' ],
        binary_filter       => [ 0,     '- Binary filter' ],
    },
    postgres => {
        default_user        => [ '',    '- Default user' ],
        pg_enable_utf8      => [ 1,     '- Enable utf8' ],
        binary_filter       => [ 0,     '- Binary filter' ],
    }
};

$info->{option_sections} = [ qw( print sql menu dbs ) ];
$info->{print}{keys}     = [ qw( tab_width min_col_width undef limit progress_bar ) ];
$info->{sql}{keys}       = [ qw( lock_stmt system_info regexp_case ) ];
$info->{dbs}{keys}       = [ qw( login db_defaults ) ];
$info->{menu}{keys}      = [ qw( thsd_sep database_types operators sssc_mode expand keep_db_choice
                                 keep_schema_choice keep_table_choice table_expand mouse ) ];
$info->{db_sections}     = [ qw( sqlite mysql postgres ) ];
$info->{sqlite}{keys}    = [ qw( reset_cache unicode see_if_its_a_number busy_timeout cache_size binary_filter ) ];
$info->{mysql}{keys}     = [ qw( default_user enable_utf8 connect_timeout bind_type_guessing ChopBlanks binary_filter ) ];
$info->{postgres}{keys}  = [ qw( default_user pg_enable_utf8 binary_filter ) ];

$info->{sub_expand}{menu}     = [ qw( keep_db_choice keep_schema_choice keep_table_choice table_expand ) ];
$info->{cmdline_only}{sqlite} = [ qw( reset_cache ) ];


if ( ! eval {
    $opt = read_config_file( $opt, $info->{config_file} );
    my $help;
    GetOptions (
        'h|?|help' => \$help,
        's|search' => \$opt->{sqlite}{reset_cache}[v],
    );
    $opt = options( $info, $opt ) if $help;
    1 }
) {
    say 'Configfile/Options:';
    print_error_message( $@ );
}

for my $lyt ( grep { /^lyt_/ } keys %$info ) {
    $info->{$lyt}{mouse} = $opt->{menu}{mouse}[v];
}

$info->{ok} = '<OK>' if $opt->{menu}{sssc_mode}[v];
my $sqlite_search = $opt->{sqlite}{reset_cache}[v] ? 1 : 0;


DB_TYPES: while ( 1 ) {

    if ( $sqlite_search ) {
        $info->{db_type} = 'sqlite';
        $sqlite_search = 0;
    }
    else {
        if ( @{$opt->{menu}{database_types}[v]} == 1 ) {
            $info->{one_db_type} = 1;
            $info->{db_type} = $opt->{menu}{database_types}[v][0];
        }
        else {
            $info->{one_db_type} = 0;
            # Choose
            $info->{db_type} = choose(
                [ undef, @{$opt->{menu}{database_types}[v]} ],
                { prompt => 'Database Type: ', %{$info->{lyt_1}}, undef => 'Quit' }
            );
            last DB_TYPES if ! defined $info->{db_type};
        }
    }

    $info->{cached} = '';
    my $db_type = $info->{db_type};
    my $databases = [];
    if ( ! eval {
        $databases = available_databases( $info, $opt );
        1 }
    ) {
        say 'Available databases:';
        delete $info->{login}{$db_type};
        print_error_message( $@ );
        next DB_TYPES;
    }
    if ( ! @$databases ) {
        print_error_message( "no $db_type-databases found\n" );
        next DB_TYPES;
    }
    if ( $opt->{sql}{system_info}[v] ) {
        my $system_databases = system_databases( $db_type );
        my @system;
        for my $system_db ( @$system_databases ) {
            my $i = first_index{ $_ eq $system_db } @$databases;
            push @system, splice( @$databases, $i, 1 );
        }
        if ( $db_type eq 'sqlite' ) {
            @$databases = ( @$databases, @system );
        }
        else {
            @$databases = ( map( "- $_", @$databases ), map( "  $_", @system ) );
        }
    }
    else {
        if ( $db_type ne 'sqlite' ) {
            @$databases = map{ "- $_" } @$databases;
        }
    }

    my $new_db_settings = 0;
    my $db;
    my $data = {};
    my %lyt_v_cs = ( %{$info->{lyt_3_cs}}, index => 1 );
    my $old_idx_db = 0;

    DATABASES: while ( 1 ) {

        if ( ! $new_db_settings ) {
            # $data = {};
            my $back = ( $db_type eq 'sqlite' ? '' : ' ' x 2 ) . ( $info->{one_db_type} ? 'Quit' : 'BACK' );
            my $choices = [ undef, @$databases ];
            # Choose
            my $idx_db = choose(
                $choices,
                { prompt => 'Choose Database' . $info->{cached}, default => $old_idx_db, %lyt_v_cs, undef => $back }
            );
            $db = $choices->[$idx_db] if defined $idx_db;
            if ( ! defined $db ) {
                last DB_TYPES if   $info->{one_db_type};
                next DB_TYPES if ! $info->{one_db_type};
            }
            if ( $opt->{menu}{keep_db_choice}[v] ) {
                if ( $old_idx_db == $idx_db ) {
                    $old_idx_db = 0;
                    next DATABASES;
                }
                else {
                    $old_idx_db = $idx_db;
                }
            }
            $db =~ s/^[-\ ]\s// if $db_type ne 'sqlite';
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
            delete $info->{login}{$db_type}{$db};
            print_error_message( $@ );
            # remove database from @databases
            next DATABASES;
        }
        my $old_idx_sch = 0;

        SCHEMA: while ( 1 ) {

            my $schema;
            if ( @{$data->{$db}{schemas}} == 1 ) {
                $schema = $data->{$db}{schemas}[0];
            }
            elsif ( @{$data->{$db}{schemas}} > 1 ) {
                my $choices;
                my $idx_sch;
                if ( $opt->{sql}{system_info}[v] && $db_type eq 'postgres' ) {
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
                    $choices = [ undef, map( "- $_", @normal ), map( "  $_", @system ) ];
                }
                else {
                    $choices = [ undef, map( "- $_", @{$data->{$db}{schemas}} ) ];
                }
                # Choose
                $idx_sch = choose(
                    $choices,
                    { prompt => 'DB "'. basename( $db ) . '" - choose Schema:', default => $old_idx_sch, %lyt_v_cs }
                );
                $schema = $choices->[$idx_sch] if defined $idx_sch;
                next DATABASES if ! defined $schema;
                if ( $opt->{menu}{keep_schema_choice}[v] ) {
                    if ( $old_idx_sch == $idx_sch ) {
                        $old_idx_sch = 0;
                        next SCHEMA;
                    }
                    else {
                        $old_idx_sch = $idx_sch;
                    }
                }
                $schema =~ s/^[-\ ]\s//;
            }

            if ( ! eval {
                if ( ! defined $data->{$db}{$schema}{tables} ) {
                    $data->{$db}{$schema}{tables} = get_table_names( $dbh, $db, $schema );
                }
                1 }
            ) {
                say 'Get table names:';
                print_error_message( $@ );
                next DATABASES;
            }

            my $join       = '  Join';
            my $union      = '  Union';
            my $db_setting = '  Database settings';
            my @tables = ();
            push @tables, map { "- $_" } @{$data->{$db}{$schema}{tables}};
            push @tables, '  sqlite_master' if $db_type eq 'sqlite' && $opt->{sql}{system_info}[v];
            my $old_idx_tbl = 0;

            TABLES: while ( 1 ) {

                my $prompt = 'DB: "'. basename( $db );
                $prompt .= '.' . $schema if defined $data->{$db}{schemas} && @{$data->{$db}{schemas}} > 1;
                $prompt .= '"';
                my $choices = [ undef, @tables, $join, $union, $db_setting ];
                # Choose
                my $idx_tbl = choose(
                    $choices,
                    { prompt => $prompt, default => $old_idx_tbl, %lyt_v_cs }
                );
                my $table;
                $table = $choices->[$idx_tbl] if defined $idx_tbl;
                if ( ! defined $table ) {
                    next SCHEMA if defined $data->{$db}{schemas} && @{$data->{$db}{schemas}} > 1;
                    next DATABASES;
                }
                if ( $opt->{menu}{keep_table_choice}[v] ) {
                    if ( $old_idx_tbl == $idx_tbl ) {
                        $old_idx_tbl = 0;
                        next TABLES;
                    }
                    else {
                        $old_idx_tbl = $idx_tbl;
                    }
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
                    }
                    next DATABASES if $new_db_settings;
                    next TABLES;
                }
                elsif ( $table eq $join ) {
                    if ( ! eval {
                        ( $select_from_stmt, $print_and_quote_cols ) = join_tables( $info, $dbh, $db, $schema, $data );
                        $table = 'joined_tables';
                        1 }
                    ) {
                        say 'Join tables:';
                        print_error_message( $@ );
                    }
                    next TABLES if ! defined $select_from_stmt;
                }
                elsif ( $table eq $union ) {
                    if ( ! eval {
                        ( $select_from_stmt, $print_and_quote_cols ) = union_tables( $info, $dbh, $db, $schema, $data );
                        $table = 'union_tables';
                        1 }
                    ) {
                        say 'Union tables:';
                        print_error_message( $@ );
                    }
                    next TABLES if ! defined $select_from_stmt;
                }
                else {
                    $table =~ s/^[-\ ]\s//;
                }
                if ( ! eval {
                    my $qt_columns = {};
                    my $pr_columns = [];
                    my $sql;
                    $sql->{stmt_keys}    = [ qw( distinct_stmt where_stmt group_by_stmt having_stmt order_by_stmt limit_stmt ) ];
                    $sql->{list_keys}    = [ qw( chosen_cols aggr_cols where_args group_by_cols having_args limit_args ) ];
                    $sql->{pr_func_keys} = [ qw( aggr hidd ) ];
                    reset_stmts( $sql, $qt_columns );

                    $info->{lock} = $opt->{sql}{lock_stmt}[v];

                    if ( $select_from_stmt ) {
                        for my $a_ref ( @$print_and_quote_cols ) {
                            $qt_columns->{$a_ref->[0]} = $a_ref->[1];
                            push @$pr_columns, $a_ref->[0];
                        }
                    }
                    else {
                        $select_from_stmt = "SELECT * FROM " . $dbh->quote_identifier( undef, $schema, $table );
                        my $sth = $dbh->prepare( $select_from_stmt );
                        $sth->execute();
                        for my $col ( @{$sth->{NAME}} ) {
                            $qt_columns->{$col} = $dbh->quote_identifier( $col );
                            push @$pr_columns, $col;
                        }
                    }
                    $info->{set_limit} = 0;
                    if ( defined $opt->{print}{limit}[v] ) {
                        my $statement = "SELECT COUNT(*) FROM ( " . $select_from_stmt . " LIMIT ? ) AS limit_part";
                        my ( $rows ) = $dbh->selectrow_array( $statement, undef, $opt->{print}{limit}[v] );
                        $info->{set_limit} = 1 if $rows == $opt->{print}{limit}[v];
                    }

                    PRINT_TABLE: while ( 1 ) {
                        my ( $all_arrayref ) = read_table( $info, $opt, $sql, $dbh, $table, $select_from_stmt, $qt_columns, $pr_columns );
                        last PRINT_TABLE if ! defined $all_arrayref;
                        delete @{$info}{qw(width_head width_cols not_a_number)};
                        func_print_tbl( $info, $opt, $db, $all_arrayref );
                    }

                    1 }
                ) {
                    say 'Print table:';
                    print_error_message( $@ );
                }
            }
        }
    }
}



sub union_tables {
    my ( $info, $dbh, $db, $schema, $data ) = @_;
    my $ds = $data->{$db}{$schema};
    if ( ! defined $ds->{col_names} || ! defined $ds->{col_types} ) {
        $data = column_names_and_types( $info, $dbh, $db, $schema, $data );
    }
    my $enough_tables = '  Enough TABLES';
    my @tables_unused = map { "- $_" } @{$ds->{tables}};
    my $used_tables   = [];
    my $cols          = {};
    my $saved_cols    = [];

    UNION_TABLE: while ( 1 ) {
        my $prompt = print_union_statement( $info, $used_tables, $cols, 'Choose UNION table:' );
        # Choose
        my $union_table = choose(
            [ undef, map( "+ $_", @$used_tables ), @tables_unused, $info->{_info}, $enough_tables ],
            { prompt => $prompt, lf => [0,4], %{$info->{lyt_3_cs}} }
        );
        return if ! defined $union_table;
        if ( $union_table eq $info->{_info} ) {
            if ( ! defined $ds->{tables_info} ) {
                $ds->{tables_info} = get_tables_info( $info, $dbh, $db, $schema, $data );
            }
            my $tbls_info = print_tables_info( $ds );
            choose( $tbls_info, { prompt => '', %{$info->{lyt_3_cs}} } );
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
            my @short_cuts = ( ( @$saved_cols ? $privious_cols : $void ), $all_cols );
            my $prompt = print_union_statement( $info, $used_tables, $cols, 'Choose Column:' );
            my $choices = [ $info->{ok}, @short_cuts, @{$ds->{col_names}{$union_table}} ];
            unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
            # Choose
            my $col = choose(
                $choices,
                { prompt => $prompt, lf => [0,4], %{$info->{lyt_1}}, clear_screen => 1 }
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
                $cols->{$union_table} = $saved_cols;
                next UNION_COLUMNS if $opt->{menu}{sssc_mode}[v];
                last UNION_COLUMNS;
            }
            if ( $col eq $all_cols ) {
                @{$cols->{$union_table}} = @{$ds->{col_names}{$union_table}};
                next UNION_COLUMNS if $opt->{menu}{sssc_mode}[v];
                last UNION_COLUMNS;
            }
            else {
                push @{$cols->{$union_table}}, $col;
            }
        }
        $saved_cols = $cols->{$union_table} if defined $cols->{$union_table};
    }
    my $print_and_quote_cols = [];
    my $union_statement = "SELECT * FROM (";
    my $count = 0;
    for my $table ( @$used_tables ) {
        $count++;
        if ( $count == 1 ) {
            # column names in the result-set of a UNION are taken from the first query.
            if ( ${$cols->{$table}}[0] eq '*' ) {
                for my $col ( @{$ds->{col_names}{$table}} ) {
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
        $union_statement .= $count < @$used_tables ? " UNION ALL " : " )";
    }
    my $derived_table_name = join '_', @$used_tables;
    $union_statement .= " AS $derived_table_name";
    return $union_statement, $print_and_quote_cols;
}


sub print_union_statement {
    my ( $info, $used_tables, $cols, $prompt ) = @_;
    my $str = "SELECT * FROM (\n";
    my $c = 0;
    for my $table ( @$used_tables ) {
        ++$c;
        $str .= "  SELECT ";
        $str .= defined $cols->{$table} ? join( ', ', @{$cols->{$table}} ) : '?';
        $str .= " FROM $table";
        $str .= $c < @$used_tables ? " UNION ALL\n" : "\n";
    }
    my $derived_table_name = join '_', @$used_tables;
    $str .= ") AS $derived_table_name\n" if @$used_tables;
    return $str . "\n$prompt";
}


sub get_tables_info {
    my ( $info, $dbh, $db, $schema, $data ) = @_;
    my $tables_info = {};
    my $sth;
    my ( $pk, $fk ) = primary_and_foreign_keys( $info, $dbh, $db, $schema, $data );
    for my $table ( @{$data->{$db}{$schema}{tables}}  ) {
        push @{$tables_info->{$table}}, [ 'Table: ', '== ' . $table . ' ==' ];
        push @{$tables_info->{$table}}, [
            'Columns: ',
            join( ' | ',
                pairwise { no warnings q(once); lc( $a ) . ' ' . $b }
                    @{$data->{$db}{$schema}{col_types}{$table}},
                    @{$data->{$db}{$schema}{col_names}{$table}}
            )
        ];
        if ( @{$pk->{$table}} ) {
            push @{$tables_info->{$table}}, [ 'PK: ', 'primary key (' . join( ',', @{$pk->{$table}} ) . ')' ];
        }
        for my $fk_name ( sort keys %{$fk->{$table}} ) {
            if ( $fk->{$table}{$fk_name} ) {
                push @{$tables_info->{$table}}, [
                    'FK: ',
                    'foreign key (' . join( ',', @{$fk->{$table}{$fk_name}{foreign_key_col}} ) .
                    ') references ' . $fk->{$table}{$fk_name}{reference_table} .
                    '(' . join( ',', @{$fk->{$table}{$fk_name}{reference_key_col}} ) .')'
                ];
            }
        }
    }
    return $tables_info;
}


sub print_tables_info {
    my ( $ds ) = @_;
    my $len_key = 10;
    my $col_max = ( GetTerminalSize )[0] - $len_key;
    my $line_fold = Text::LineFold->new( %{$info->{line_fold}} );
    $line_fold->config( 'ColMax', $col_max > 140 ? 140 : $col_max );
    my $ch_info = [ 'Close with ENTER' ];
    for my $table ( @{$ds->{tables}} ) {
        push @{$ch_info}, " ";
        for my $line ( @{$ds->{tables_info}{$table}} ) {
            my $text = sprintf "%*s%s", $len_key, @$line;
            $text = $line_fold->fold( '' , ' ' x $len_key, $text );
            push @{$ch_info}, split /\R+/, $text;
        }
    }
    return $ch_info;
}


sub join_tables {
    my ( $info, $dbh, $db, $schema, $data ) = @_;
    my $ds = $data->{$db}{$schema};
    if ( ! defined $ds->{col_names} || ! defined $ds->{col_types} ) {
        $data = column_names_and_types( $info, $dbh, $db, $schema, $data );
    }
    my $join_stmt_qt = "SELECT * FROM";
    my $join_stmt_pr = "SELECT * FROM";
    my @tables = map { "- $_" } @{$ds->{tables}};
    my ( $master, @used_tables, @pks, @fks );

    MASTER: while ( 1 ) {
        my $prompt = print_join_statement( $info, $join_stmt_pr, 'Choose MASTER table:' );
        # Choose
        $master = choose(
            [ undef, @tables, $info->{_info} ],
            { prompt => $prompt, lf => [0,4], %{$info->{lyt_3_cs}} }
        );
        return if ! defined $master;
        if ( $master eq $info->{_info} ) {
            if ( ! defined $ds->{tables_info} ) {
                $ds->{tables_info} = get_tables_info( $info, $dbh, $db, $schema, $data );
            }
            my $tbls_info = print_tables_info( $ds );
            choose( $tbls_info, { prompt => '', %{$info->{lyt_3_cs}} } );
            next MASTER;
        }
        my $idx = first_index { $_ eq $master } @tables;
        splice( @tables, $idx, 1 );
        $master =~ s/^[-\ ]\s//;
        @used_tables = ( $master );
        my @avail_tables = @tables;
        my $master_q = $dbh->quote_identifier( undef, $schema, $master );
        $join_stmt_qt = "SELECT * FROM " . $master_q;
        $join_stmt_pr = "SELECT * FROM " . $master;
        my ( @old_pks, @old_fks, @old_used_tables, @old_avail_tables );
        my $old_stmt_qt = '';
        my $old_stmt_pr = '';

        SLAVE_TABLES: while ( 1 ) {
            my $enough_slaves = '  Enough SLAVES';
            my $slave;
            @old_pks          = @pks;
            @old_fks          = @fks;
            $old_stmt_qt      = $join_stmt_qt;
            $old_stmt_pr      = $join_stmt_pr;
            @old_used_tables  = @used_tables;
            @old_avail_tables = @avail_tables;

            SLAVE: while ( 1 ) {
                my $prompt = print_join_statement( $info, $join_stmt_pr, 'Add a SLAVE table:' );
                # Choose
                $slave = choose(
                    [ undef, @avail_tables, $info->{_info}, $enough_slaves ],
                    { prompt => $prompt, lf => [0,4], %{$info->{lyt_3_cs}}, undef => $info->{_reset} }
                );
                if ( ! defined $slave ) {
                    if ( @used_tables == 1 ) {
                        $join_stmt_qt = "SELECT * FROM";
                        $join_stmt_pr = "SELECT * FROM";
                        @tables = map { "- $_" } @{$ds->{tables}};
                        next MASTER;
                    }
                    else {
                        @used_tables = ( $master );
                        @avail_tables = @tables;
                        $join_stmt_qt = "SELECT * FROM " . $master_q;
                        $join_stmt_pr = "SELECT * FROM " . $master;
                        @pks = ();
                        @fks = ();
                        next SLAVE_TABLES;
                    }
                }
                last SLAVE_TABLES if $slave eq $enough_slaves;
                if ( $slave eq $info->{_info} ) {
                    if ( ! defined $ds->{tables_info} ) {
                        $ds->{tables_info} = get_tables_info( $info, $dbh, $db, $schema, $data );
                    }
                    my $tbls_info = print_tables_info( $ds );
                    choose( $tbls_info, { prompt => '', %{$info->{lyt_3_cs}} } );
                    next SLAVE;
                }
                last SLAVE;
            }
            my $idx = first_index { $_ eq $slave } @avail_tables;
            splice( @avail_tables, $idx, 1 );
            $slave =~ s/^[-\ ]\s//;
            my $slave_q = $dbh->quote_identifier( undef, $schema, $slave );
            $join_stmt_qt .= " LEFT OUTER JOIN " . $slave_q . " ON";
            $join_stmt_pr .= " LEFT OUTER JOIN " . $slave   . " ON";
            my %avail_pk_cols = ();
            for my $used_table ( @used_tables ) {
                for my $col ( @{$ds->{col_names}{$used_table}} ) {
                    $avail_pk_cols{"$used_table.$col"} = $dbh->quote_identifier( undef, $used_table, $col );
                }
            }
            my %avail_fk_cols = ();
            for my $col ( @{$ds->{col_names}{$slave}} ) {
                $avail_fk_cols{"$slave.$col"} = $dbh->quote_identifier( undef, $slave, $col );
            }
            my $AND = '';

            ON: while ( 1 ) {
                my $prompt = print_join_statement( $info, $join_stmt_pr, 'Choose PRIMARY KEY column:' );
                # Choose
                my $pk_col = choose(
                    [ undef, map( "- $_", sort keys %avail_pk_cols ), $info->{_continue} ],
                    { prompt => $prompt, lf => [0,4], %{$info->{lyt_3_cs}}, undef => $info->{_reset} }
                );
                if ( ! defined $pk_col ) {
                    @pks          = @old_pks;
                    @fks          = @old_fks;
                    $join_stmt_qt = $old_stmt_qt;
                    $join_stmt_pr = $old_stmt_pr;
                    @used_tables  = @old_used_tables;
                    @avail_tables = @old_avail_tables;
                    next SLAVE_TABLES;
                }
                if ( $pk_col eq $info->{_continue} ) {
                    if ( @pks == @old_pks ) {
                        $join_stmt_qt = $old_stmt_qt;
                        $join_stmt_pr = $old_stmt_pr;
                        @used_tables  = @old_used_tables;
                        @avail_tables = @old_avail_tables;
                        next SLAVE_TABLES;
                    }
                    last ON;
                }
                $pk_col =~ s/^[-\ ]\s//;
                push @pks, $avail_pk_cols{$pk_col};
                $join_stmt_qt .= $AND;
                $join_stmt_pr .= $AND;
                $join_stmt_qt .= ' ' . $avail_pk_cols{$pk_col} . " =";
                $join_stmt_pr .= ' ' . $pk_col                          . " =";
                $prompt = print_join_statement( $info, $join_stmt_pr, 'Choose FOREIGN KEY column:' );
                # Choose
                my $fk_col = choose(
                    [ undef, map{ "- $_" } sort keys %avail_fk_cols ],
                    { prompt => $prompt, lf => [0,4], %{$info->{lyt_3_cs}}, undef => $info->{_reset} }
                );
                if ( ! defined $fk_col ) {
                    @pks          = @old_pks;
                    @fks          = @old_fks;
                    $join_stmt_qt = $old_stmt_qt;
                    $join_stmt_pr = $old_stmt_pr;
                    @used_tables  = @old_used_tables;
                    @avail_tables = @old_avail_tables;
                    next SLAVE_TABLES;
                }
                $fk_col =~ s/^[-\ ]\s//;
                push @fks, $avail_fk_cols{$fk_col};
                $join_stmt_qt .= ' ' . $avail_fk_cols{$fk_col};
                $join_stmt_pr .= ' ' . $fk_col;
                $AND = " AND";
            }
            push @used_tables, $slave;
        }
        last MASTER;
    }

    my $length_uniq = 2;
    AB: while ( 1 ) {
        $length_uniq++;
        my %abbr;
        for my $table ( @used_tables, ) {
            next AB if $abbr{ substr $table, 0, $length_uniq }++;
        }
        last AB;
    }
    my @dup;
    my %seen;
    for my $table ( keys %{$ds->{col_names}} ) {
        next if none { $_ eq $table } @used_tables;
        for my $col ( @{$ds->{col_names}{$table}} ) {
            $seen{$col}++;
            push @dup, $col if $seen{$col} == 2;
        }
    }
    my @columns_sql;
    my $print_and_quote_cols;
    for my $table ( @{$ds->{tables}} ) {
        next if none { $_ eq $table } @used_tables;
        for my $col ( @{$ds->{col_names}{$table}} ) {
            my $tbl_col_q = $dbh->quote_identifier( undef, $table, $col );
            next if any { $_ eq $tbl_col_q } @fks;
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
    $join_stmt_qt =~ s/\s\*\s/ $col_statement /;
    return $join_stmt_qt, $print_and_quote_cols;
}


sub print_join_statement {
    my ( $info, $join_stmt_pr, $prompt ) = @_;
    $join_stmt_pr =~ s/(?=\sLEFT\sOUTER\sJOIN)/\n\ /g;
    return $join_stmt_pr . "\n\n$prompt";
}



sub print_select_statement {
    my ( $info, $sql, $table, $prompt ) = @_;
    my $cols_sql = '';
    if ( @{$sql->{print}{chosen_cols}} ) {
        $cols_sql = ' ' . join( ', ', @{$sql->{print}{chosen_cols}} );
    }
    if ( ! @{$sql->{print}{chosen_cols}} && @{$sql->{print}{group_by_cols}} ) {
        $cols_sql = ' ' . join( ', ', @{$sql->{print}{group_by_cols}} );
    }
    if ( @{$sql->{print}{aggr_cols}} ) {
        $cols_sql .= ',' if $cols_sql;
        $cols_sql .= ' ' . join( ', ', @{$sql->{print}{aggr_cols}} );
    }
    $cols_sql = ' *' if ! $cols_sql;
    my $str = "SELECT";
    $str .= $sql->{print}{distinct_stmt} if $sql->{print}{distinct_stmt};
    $str .= $cols_sql . "\n";
    $str .= "  FROM $table" . "\n";
    $str .= ' ' . $sql->{print}{where_stmt}    . "\n" if $sql->{print}{where_stmt};
    $str .= ' ' . $sql->{print}{group_by_stmt} . "\n" if $sql->{print}{group_by_stmt};
    $str .= ' ' . $sql->{print}{having_stmt}   . "\n" if $sql->{print}{having_stmt};
    $str .= ' ' . $sql->{print}{order_by_stmt} . "\n" if $sql->{print}{order_by_stmt};
    $str .= ' ' . $sql->{print}{limit_stmt}    . "\n" if $sql->{print}{limit_stmt};
    $str .= "\n";
    $str .= $prompt if $prompt;
    return $str;
}


sub reset_stmts {
    my ( $sql, $qt_columns ) = @_;
    for my $pr_func_key ( @{$sql->{pr_func_keys}} ) {
        delete @{$qt_columns}{@{$sql->{pr_func}{$pr_func_key}//[]}};
    }
    @{$sql->{pr_func}}{ @{$sql->{pr_func_keys}} } = map{ [] } @{$sql->{pr_func_keys}};
    @{$sql->{print}}{ @{$sql->{stmt_keys}} }      = ( '' ) x  @{$sql->{stmt_keys}};
    @{$sql->{quote}}{ @{$sql->{stmt_keys}} }      = ( '' ) x  @{$sql->{stmt_keys}};
    @{$sql->{print}}{ @{$sql->{list_keys}} }      = map{ [] } @{$sql->{list_keys}};
    @{$sql->{quote}}{ @{$sql->{list_keys}} }      = map{ [] } @{$sql->{list_keys}};
    delete $sql->{backup};
}


sub read_table {
    my ( $info, $opt, $sql, $dbh, $table, $select_from_stmt, $qt_columns, $pr_columns  ) = @_;
    my %lyt_stmt = %{$info->{lyt_stmt}};
    my @keys = ( qw( print_table columns aggregate distinct where group_by having order_by limit lock ) );
    my $lk = [ '  Lk0', '  Lk1' ];
    my %customize = (
        hidden          => 'Customize:',
        print_table     => 'Print TABLE',
        columns         => '- COLUMNS',
        aggregate       => '- AGGREGATE',
        distinct        => '- DISTINCT',
        where           => '- WHERE',
        group_by        => '- GROUP BY',
        having          => '- HAVING',
        order_by        => '- ORDER BY',
        limit           => '- LIMIT',
        lock            => $lk->[$info->{lock}],
    );
    my ( $DISTINCT, $ALL, $ASC, $DESC, $AND, $OR ) = ( "DISTINCT", "ALL", "ASC", "DESC", "AND", "OR" );
    if ( $info->{lock} == 0 ) {
        reset_stmts( $sql, $qt_columns );
    }
    my $auto_limit = '';
    if ( $info->{set_limit} && ! $sql->{print}{limit_stmt} ) {
        $auto_limit = " LIMIT " . insert_sep( $opt->{print}{limit}[v], $opt->{menu}{thsd_sep}[v] );
        $sql->{print}{limit_stmt} = $auto_limit;
    }

    CUSTOMIZE: while ( 1 ) {
        my $prompt = print_select_statement( $info, $sql, $table );
        # Choose
        my $custom = choose(
            [ $customize{hidden}, undef, @customize{@keys} ],
            { prompt => $prompt, lf => [0,4], %{$info->{lyt_3_cs}}, default => 1, undef => $info->{back} }
        );
        if ( ! defined $custom ) {
            last CUSTOMIZE;
        }
        elsif ( $custom eq $customize{'lock'} ) {
            if ( $info->{lock} == 1 ) {
                $info->{lock} = 0;
                $customize{lock} = $lk->[0];
                reset_stmts( $sql, $qt_columns );
            }
            elsif ( $info->{lock} == 0 )   {
                $info->{lock} = 1;
                $customize{lock} = $lk->[1];
            }
        }
        elsif( $custom eq $customize{'columns'} ) {
            my @cols = @$pr_columns;
            $sql->{quote}{chosen_cols} = [];
            $sql->{print}{chosen_cols} = [];
            delete @{$qt_columns}{@{$sql->{pr_func}{hidd}}};
            $sql->{pr_func}{hidd} = [];

            COLUMNS: while ( 1 ) {
                my $prompt = print_select_statement( $info, $sql, $table, 'Choose:' );
                my $choices = [ $info->{ok}, @cols ];
                unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
                # Choose
                my $print_col = choose(
                    $choices,
                    { prompt => $prompt, %lyt_stmt }
                );
                if ( ! defined $print_col ) {
                    if ( @{$sql->{quote}{chosen_cols}} ) {
                        $sql->{quote}{chosen_cols} = [];
                        $sql->{print}{chosen_cols} = [];
                        next COLUMNS;
                    }
                    else {
                        $sql->{quote}{chosen_cols} = [];
                        $sql->{print}{chosen_cols} = [];
                        last COLUMNS;
                    }
                }
                if ( $print_col eq $info->{ok} ) {
                    delete $sql->{backup}{print}{chosen_cols};
                    last COLUMNS;
                }
                push @{$sql->{quote}{chosen_cols}}, $qt_columns->{$print_col};
                push @{$sql->{print}{chosen_cols}}, $print_col;
            }
        }
        elsif( $custom eq $customize{'distinct'} ) {
            $sql->{quote}{distinct_stmt} = '';
            $sql->{print}{distinct_stmt} = '';

            DISTINCT: while ( 1 ) {
                my $prompt = print_select_statement( $info, $sql, $table, 'Choose:' );
                my $choices = [ $info->{ok}, $DISTINCT, $ALL ];
                unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
                # Choose
                my $select_distinct = choose(
                    $choices,
                    { prompt => $prompt, %lyt_stmt }
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
                $sql->{quote}{distinct_stmt} = ' ' . $select_distinct;
                $sql->{print}{distinct_stmt} = ' ' . $select_distinct;
            }
        }
        elsif( $custom eq $customize{'aggregate'} ) {
            my @cols = ( @$pr_columns );
            delete @{$qt_columns}{@{$sql->{pr_func}{aggr}}};
            $sql->{pr_func}{aggr}    = [];
            $sql->{quote}{aggr_cols} = [];
            $sql->{print}{aggr_cols} = [];

            AGGREGATE: while ( 1 ) {
                my $prompt = print_select_statement( $info, $sql, $table, 'Choose:' );
                my $choices = [ $info->{ok}, @{$info->{aggregate_functions}} ];
                unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
                # Choose
                my $func = choose(
                    $choices,
                    { prompt => $prompt, %lyt_stmt }
                );
                if ( ! defined $func ) {
                    if ( @{$sql->{quote}{aggr_cols}} ) {
                        delete @{$qt_columns}{@{$sql->{pr_func}{aggr}}};
                        $sql->{pr_func}{aggr}    = [];
                        $sql->{quote}{aggr_cols} = [];
                        $sql->{print}{aggr_cols} = [];
                        next AGGREGATE;
                    }
                    else {
                        delete @{$qt_columns}{@{$sql->{pr_func}{aggr}}};
                        $sql->{pr_func}{aggr}    = [];
                        $sql->{quote}{aggr_cols} = [];
                        $sql->{print}{aggr_cols} = [];
                        last AGGREGATE;
                    }
                }
                if ( $func eq $info->{ok} ) {
                    delete $sql->{backup}{print}{aggr_cols};
                    last AGGREGATE;
                }
                my $i = @{$sql->{quote}{aggr_cols}};
                if ( $func =~ /^count\(\*\)\z/i ) {
                    $sql->{print}{aggr_cols}[$i] = $func;
                    $sql->{quote}{aggr_cols}[$i] = $func;
                }
                else {
                    $func =~ s/\(\S\)\z//;
                    $sql->{quote}{aggr_cols}[$i] = $func . '(';
                    $sql->{print}{aggr_cols}[$i] = $func . '(';
                    my $prompt = print_select_statement( $info, $sql, $table, 'Choose:' );
                    my $choices = [ @cols ];
                    unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
                    # Choose
                    my $print_col = choose(
                        $choices,
                        { prompt => $prompt, %lyt_stmt }
                    );
                    if ( ! defined $print_col ) {
                        delete @{$qt_columns}{@{$sql->{pr_func}{aggr}}};
                        $sql->{pr_func}{aggr}         = [];
                        $sql->{quote}{aggr_cols} = [];
                        $sql->{print}{aggr_cols} = [];
                        next AGGREGATE;
                    }
                    ( my $quote_col = $qt_columns->{$print_col} ) =~ s/\sAS\s\S+\z//;
                    $sql->{print}{aggr_cols}[$i] .= $print_col . ')';
                    $sql->{quote}{aggr_cols}[$i] .= $quote_col . ')';
                }
                my $pr_func             = $sql->{print}{aggr_cols}[$i];
                $qt_columns->{$pr_func} = $sql->{quote}{aggr_cols}[$i];
                push @{$sql->{pr_func}{aggr}}, $pr_func;
            }
        }
        elsif ( $custom eq $customize{'where'} ) {
            my @cols = ( @$pr_columns, @{$sql->{pr_func}{hidd}} );
            my $AND_OR = '';
            $sql->{quote}{where_args} = [];
            $sql->{quote}{where_stmt} = " WHERE";
            $sql->{print}{where_stmt} = " WHERE";
            my $count = 0;

            WHERE: while ( 1 ) {
                my $prompt = print_select_statement( $info, $sql, $table, 'Choose:' );
                my $choices = [ $info->{ok}, @cols ];
                unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
                # Choose
                my $print_col = choose(
                    $choices,
                    { prompt => $prompt, %lyt_stmt }
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
                    my $prompt = print_select_statement( $info, $sql, $table, 'Choose:' );
                    my $choices = [ $AND, $OR ];
                    unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
                    # Choose
                    $AND_OR = choose(
                        $choices,
                        { prompt => $prompt, %lyt_stmt }
                    );
                    if ( ! defined $AND_OR ) {
                        $sql->{quote}{where_args} = [];
                        $sql->{quote}{where_stmt} = " WHERE";
                        $sql->{print}{where_stmt} = " WHERE";
                        $count = 0;
                        $AND_OR = '';
                        next WHERE;
                    }
                    $AND_OR = ' ' . $AND_OR;
                }
                ( my $quote_col = $qt_columns->{$print_col} ) =~ s/\sAS\s\S+\z//;
                $sql->{quote}{where_stmt} .= $AND_OR . ' ' . $quote_col;
                $sql->{print}{where_stmt} .= $AND_OR . ' ' . $print_col;
                set_operator_sql( $info, $opt, $sql, 'where', $table, \@cols, $qt_columns, $quote_col );
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
        elsif( $custom eq $customize{'group_by'} ) {
            my @cols = ( @$pr_columns, @{$sql->{pr_func}{hidd}} );
            my $col_sep = ' ';
            $sql->{quote}{group_by_stmt} = " GROUP BY";
            $sql->{print}{group_by_stmt} = " GROUP BY";
            $sql->{quote}{group_by_cols} = [];
            $sql->{print}{group_by_cols} = [];

            GROUP_BY: while ( 1 ) {
                my $prompt = print_select_statement( $info, $sql, $table, 'Choose:' );
                my $choices = [ $info->{ok}, @cols ];
                unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
                # Choose
                my $print_col = choose(
                    $choices,
                    { prompt => $prompt, %lyt_stmt }
                );
                if ( ! defined $print_col ) {
                    if ( @{$sql->{quote}{group_by_cols}} ) {
                        $sql->{quote}{group_by_stmt} = " GROUP BY";
                        $sql->{print}{group_by_stmt} = " GROUP BY";
                        $sql->{quote}{group_by_cols} = [];
                        $sql->{print}{group_by_cols} = [];
                        $col_sep = ' ';
                        next GROUP_BY;
                    }
                    else {
                        $sql->{quote}{group_by_stmt} = '';
                        $sql->{print}{group_by_stmt} = '';
                        $sql->{quote}{group_by_cols} = [];
                        $sql->{print}{group_by_cols} = [];
                        last GROUP_BY;
                    }
                }
                if ( $print_col eq $info->{ok} ) {
                    if ( $col_sep eq ' ' ) {
                        $sql->{quote}{group_by_stmt} = '';
                        $sql->{print}{group_by_stmt} = '';
                    }
                    last GROUP_BY;
                }
                ( my $quote_col = $qt_columns->{$print_col} ) =~ s/\sAS\s\S+\z//;
                if ( ! @{$sql->{quote}{chosen_cols}} ) {
                    push @{$sql->{quote}{group_by_cols}}, $quote_col;
                    push @{$sql->{print}{group_by_cols}}, $print_col;
                }
                $sql->{quote}{group_by_stmt} .= $col_sep . $quote_col;
                $sql->{print}{group_by_stmt} .= $col_sep . $print_col;
                $col_sep = ', ';
            }
        }
        elsif( $custom eq $customize{'having'} ) {
            my @cols = ( @$pr_columns ); #, @{$sql->{pr_func}{hidd}} );
            my $AND_OR = '';
            $sql->{quote}{having_args} = [];
            $sql->{quote}{having_stmt} = " HAVING";
            $sql->{print}{having_stmt} = " HAVING";
            my $count = 0;

            HAVING: while ( 1 ) {
                my $prompt = print_select_statement( $info, $sql, $table, 'Choose:' );
                my $choices = [ $info->{ok}, @{$info->{aggregate_functions}}, map( '@' . $_, @{$sql->{pr_func}{aggr}} ) ];
                unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
                # Choose
                my $func = choose(
                    $choices,
                    { prompt => $prompt, %lyt_stmt }
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
                    my $prompt = print_select_statement( $info, $sql, $table, 'Choose:' );
                    my $choices = [ $AND, $OR ];
                    unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
                    # Choose
                    $AND_OR = choose(
                        $choices,
                        { prompt => $prompt, %lyt_stmt }
                    );
                    if ( ! defined $AND_OR ) {
                        $sql->{quote}{having_args} = [];
                        $sql->{quote}{having_stmt} = " HAVING";
                        $sql->{print}{having_stmt} = " HAVING";
                        $count = 0;
                        $AND_OR = '';
                        next HAVING;
                    }
                    $AND_OR = ' ' . $AND_OR;
                }
                my ( $print_having_func, $quote_having_func );
                my ( $print_col, $quote_col );
                if ( ( any { '@' . $_ eq $func } @{$sql->{pr_func}{aggr}} ) ) {
                    ( my $print_func = $func ) =~ s/^\@//;
                    my $quote_func = $qt_columns->{$print_func};
                    $sql->{quote}{having_stmt} .= $AND_OR . ' ' . $quote_func;
                    $sql->{print}{having_stmt} .= $AND_OR . ' ' . $print_func;
                    $quote_col = $qt_columns->{$print_func};
                }
                elsif ( $func =~ /^count\(\*\)\z/i ) {
                    $print_col = '*';
                    $quote_col = '*';
                    $sql->{quote}{having_stmt} .= $AND_OR . ' ' . $func;
                    $sql->{print}{having_stmt} .= $AND_OR . ' ' . $func;
                }
                else {
                    $func =~ s/\(\S\)\z//;
                    $sql->{quote}{having_stmt} .= $AND_OR . ' ' . $func . '(';
                    $sql->{print}{having_stmt} .= $AND_OR . ' ' . $func . '(';;
                    my $prompt = print_select_statement( $info, $sql, $table, 'Choose:' );
                    my $choices = [ @cols ];
                    unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
                    # Choose
                    $print_col = choose(
                        $choices,
                        { prompt => $prompt, %lyt_stmt }
                    );
                    if ( ! defined $print_col ) {
                        $sql->{quote}{having_args} = [];
                        $sql->{quote}{having_stmt} = " HAVING";
                        $sql->{print}{having_stmt} = " HAVING";
                        $count = 0;
                        $AND_OR = '';
                        next HAVING;
                    }
                    ( $quote_col = $qt_columns->{$print_col} ) =~ s/\sAS\s\S+\z//;
                    $sql->{quote}{having_stmt} .= $quote_col . ')';
                    $sql->{print}{having_stmt} .= $print_col . ')';
                }
                set_operator_sql( $info, $opt, $sql, 'having', $table, \@cols, $qt_columns, $quote_col );
                if ( ! $sql->{quote}{having_stmt} ) {
                    $sql->{quote}{having_args} = [];
                    $sql->{quote}{having_stmt} = " HAVING";
                    $sql->{print}{having_stmt} = " HAVING";
                    $count = 0;
                    $AND_OR = '';
                    next HAVING;
                }
                $count++;
            }
        }
        elsif( $custom eq $customize{'order_by'} ) {
            my @cols = ( @$pr_columns, map( '@' . $_, @{$sql->{pr_func}{aggr}},
                                                      @{$sql->{pr_func}{hidd}} ) );
            my $col_sep = ' ';
            $sql->{quote}{order_by_stmt} = " ORDER BY";
            $sql->{print}{order_by_stmt} = " ORDER BY";

            ORDER_BY: while ( 1 ) {
                my $prompt = print_select_statement( $info, $sql, $table, 'Choose:' );
                my $choices = [ $info->{ok}, @cols ];
                unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
                # Choose
                my $print_col = choose(
                    $choices,
                    { prompt => $prompt, %lyt_stmt }
                );
                if ( ! defined $print_col ) {
                    if ( $sql->{quote}{order_by_stmt} ne " ORDER BY" ) {
                        $sql->{quote}{order_by_args} = [];
                        $sql->{quote}{order_by_stmt} = " ORDER BY";
                        $sql->{print}{order_by_stmt} = " ORDER BY";
                        $col_sep = ' ';
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
                    if ( $col_sep eq ' ' ) {
                        $sql->{quote}{order_by_stmt} = '';
                        $sql->{print}{order_by_stmt} = '';
                    }
                    last ORDER_BY;
                }
                if ( any { '@' . $_ eq $print_col } @{$sql->{pr_func}{aggr}},
                                                    @{$sql->{pr_func}{hidd}}
                ) {
                    $print_col =~ s/^\@//;
                }
                ( my $quote_col = $qt_columns->{$print_col} ) =~ s/\sAS\s\S+\z//;
                $sql->{quote}{order_by_stmt} .= $col_sep . $quote_col;
                $sql->{print}{order_by_stmt} .= $col_sep . $print_col;
                $prompt = print_select_statement( $info, $sql, $table, 'Choose:' );
                $choices = [ $ASC, $DESC ];
                unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
                # Choose
                my $direction = choose(
                    $choices,
                    { prompt => $prompt, %lyt_stmt }
                );
                if ( ! defined $direction ){
                    $sql->{quote}{order_by_args} = [];
                    $sql->{quote}{order_by_stmt} = " ORDER BY";
                    $sql->{print}{order_by_stmt} = " ORDER BY";
                    $col_sep = ' ';
                    next ORDER_BY;
                }
                $sql->{quote}{order_by_stmt} .= ' ' . $direction;
                $sql->{print}{order_by_stmt} .= ' ' . $direction;
                $col_sep = ', ';
            }
        }
        elsif( $custom eq $customize{'limit'} ) {
            $sql->{quote}{limit_args} = [];
            $sql->{quote}{limit_stmt} = " LIMIT";
            $sql->{print}{limit_stmt} = " LIMIT";
            my $digits = 7;
            my ( $only_limit, $offset_and_limit ) = ( 'LIMIT', 'OFFSET-LIMIT' );

            LIMIT: while ( 1 ) {
                my $prompt = print_select_statement( $info, $sql, $table, 'Choose:' );
                my $choices = [ $info->{ok}, $only_limit, $offset_and_limit ];
                unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
                # Choose
                my $choice = choose(
                    $choices,
                    { prompt => $prompt, %lyt_stmt }
                );
                if ( ! defined $choice ) {
                    if ( @{$sql->{quote}{limit_args}} ) {
                        $sql->{quote}{limit_args} = [];
                        $sql->{quote}{limit_stmt} = " LIMIT";
                        $sql->{print}{limit_stmt} = " LIMIT";
                        next LIMIT;
                    }
                    else {
                        $sql->{quote}{limit_stmt} = '';
                        $sql->{print}{limit_stmt} = $auto_limit;
                        last LIMIT;
                    }
                }
                if ( $choice eq $info->{ok} ) {
                    if ( ! @{$sql->{quote}{limit_args}} ) {
                        $sql->{quote}{limit_stmt} = '';
                        $sql->{print}{limit_stmt} = $auto_limit;
                    }
                    last LIMIT;
                }
                $sql->{quote}{limit_args} = [];
                $sql->{quote}{limit_stmt} = " LIMIT";
                $sql->{print}{limit_stmt} = " LIMIT";
                # Choose_a_number
                my $limit = choose_a_number( $info, $opt, $digits, '"LIMIT"' );
                next LIMIT if ! defined $limit || $limit eq '--';
                push @{$sql->{quote}{limit_args}}, $limit;
                $sql->{quote}{limit_stmt} .= ' ' . '?';
                $sql->{print}{limit_stmt} .= ' ' . insert_sep( $limit, $opt->{menu}{thsd_sep}[v] );
                if ( $choice eq $offset_and_limit ) {
                    # Choose_a_number
                    my $offset = choose_a_number( $info, $opt, $digits, '"OFFSET"' );
                    if ( ! defined $offset || $offset eq '--' ) {
                        $sql->{quote}{limit_args} = [];
                        $sql->{quote}{limit_stmt} = " LIMIT";
                        $sql->{print}{limit_stmt} = " LIMIT";
                        next LIMIT;
                    }
                    push @{$sql->{quote}{limit_args}}, $offset;
                    $sql->{quote}{limit_stmt} .= " OFFSET " . '?';
                    $sql->{print}{limit_stmt} .= " OFFSET " . insert_sep( $offset, $opt->{menu}{thsd_sep}[v] );
                }
            }
        }
        elsif ( $custom eq $customize{'hidden'} ) {
            my @functions = ( qw( Epoch_to_Date Truncate Epoch_to_DateTime ) );
            delete @{$qt_columns}{@{$sql->{pr_func}{hidd}}};
            $sql->{pr_func}{hidd} = [];
            my $default_cols_sql = 0;
            if ( ! @{$sql->{quote}{chosen_cols}} && ! @{$sql->{quote}{aggr_cols}} ) {
                @{$sql->{quote}{chosen_cols}} = map { $qt_columns->{$_} } @$pr_columns;
                @{$sql->{print}{chosen_cols}} = @$pr_columns;
                $default_cols_sql = 1;
            }
            if ( ! exists $sql->{backup}{print}{chosen_cols} ) {
                @{$sql->{backup}{print}{chosen_cols}} = @{$sql->{print}{chosen_cols}};
            }
            if ( ! exists $sql->{backup}{print}{aggr_cols} ) {
                @{$sql->{backup}{print}{aggr_cols}} = @{$sql->{print}{aggr_cols}};
            }

            HIDDEN: while ( 1 ) {
                my $items_cc = @{$sql->{quote}{chosen_cols}};
                my $prompt = print_select_statement( $info, $sql, $table, 'Choose:' );
                my @cols = map { "- $_" } @{$sql->{print}{chosen_cols}}, @{$sql->{print}{aggr_cols}};
                my $choices = [ undef, @cols, $info->{_confirm} ];
                # Choose
                my $i = choose(
                    $choices,
                    { prompt => $prompt, lf => [0,4], %{$info->{lyt_3_cs}}, index => 1 }
                );
                my $print_col;
                $print_col = $choices->[$i] if defined $i;
                if ( ! defined $print_col ) {
                    for my $stmt_key ( qw( chosen_cols aggr_cols ) ) {
                        for my $i ( 0 .. $#{$sql->{print}{$stmt_key}} ) {
                            if ( $sql->{print}{$stmt_key}[$i] ne $sql->{backup}{print}{$stmt_key}[$i] ) {
                                $sql->{print}{$stmt_key}[$i] = $sql->{backup}{print}{$stmt_key}[$i];
                                $sql->{quote}{$stmt_key}[$i] = $qt_columns->{$sql->{backup}{print}{$stmt_key}[$i]};
                            }
                        }
                    }
                    if ( $default_cols_sql ) {
                        $sql->{quote}{chosen_cols} = [];
                        $sql->{print}{chosen_cols} = [];
                    }
                    delete @{$qt_columns}{@{$sql->{pr_func}{hidd}}};
                    $sql->{pr_func}{hidd} = [];
                    last HIDDEN;
                }
                if ( $print_col eq $info->{_confirm} ) {
                    last HIDDEN;
                }
                $print_col =~ s/^\-\s//;
                $i--;
                my $stmt_key;
                if ( $i <= $items_cc - 1 ) {
                    $stmt_key = 'chosen_cols';
                }
                else {
                    $stmt_key = 'aggr_cols';
                    $i -= $items_cc;
                }
                if ( $sql->{print}{$stmt_key}[$i] ne $sql->{backup}{print}{$stmt_key}[$i] ) {
                    $sql->{print}{$stmt_key}[$i] = $sql->{backup}{print}{$stmt_key}[$i];
                    $sql->{quote}{$stmt_key}[$i] = $qt_columns->{$sql->{backup}{print}{$stmt_key}[$i]};
                    next HIDDEN;
                }
                $prompt = print_select_statement( $info, $sql, $table, 'Choose:' );
                # Choose
                my $function = choose(
                    [ undef, map( "  $_", @functions ) ],
                    { prompt => $prompt, lf => [0,4], %{$info->{lyt_3_cs}} }
                );
                if ( ! defined $function ) {
                    next HIDDEN;
                }
                $function =~ s/^\s\s//;
                ( my $quote_col = $qt_columns->{$print_col} ) =~ s/\sAS\s\S+\z//;
                my ( $quote_func, $print_func ) = col_functions( $info, $sql, $table, $function, $quote_col, $print_col );
                if ( ! defined $quote_func ) {
                    next HIDDEN;
                }
                $sql->{quote}{$stmt_key}[$i] = $quote_func;
                $sql->{print}{$stmt_key}[$i] = $print_func;
                my $pr_func             = $sql->{print}{$stmt_key}[$i];
                $qt_columns->{$pr_func} = $sql->{quote}{$stmt_key}[$i];
                if ( none { $print_col =~ /^\Q$_\E/ } map { /^([^(]+\()/ } @{$info->{aggregate_functions}} ) {
                    push @{$sql->{pr_func}{hidd}}, $pr_func;
                }
            }
        }
        elsif( $custom eq $customize{'print_table'} ) {
            my ( $default_cols_sql, $from_stmt ) = $select_from_stmt =~ /^SELECT\s(.*?)(\sFROM\s.*)\z/;
            my $cols_sql = '';
            if ( @{$sql->{quote}{chosen_cols}} ) {
                $cols_sql = ' ' . join( ', ', @{$sql->{quote}{chosen_cols}} );
            }
            elsif ( ! @{$sql->{quote}{chosen_cols}} && @{$sql->{quote}{group_by_cols}} ) {
                $cols_sql = ' ' . join( ', ', @{$sql->{quote}{group_by_cols}} );
            }
            if ( @{$sql->{quote}{aggr_cols}} ) {
                $cols_sql .= ',' if $cols_sql;
                $cols_sql .= ' ' . join( ', ', @{$sql->{quote}{aggr_cols}} );
            }
            $cols_sql = ' ' . $default_cols_sql if ! $cols_sql;
            my $select .= "SELECT" . $sql->{quote}{distinct_stmt} . $cols_sql . $from_stmt;
            $select .= $sql->{quote}{where_stmt};
            $select .= $sql->{quote}{group_by_stmt};
            $select .= $sql->{quote}{having_stmt};
            $select .= $sql->{quote}{order_by_stmt};
            $select .= $sql->{quote}{limit_stmt};
            my @arguments = ( @{$sql->{quote}{where_args}}, @{$sql->{quote}{having_args}}, @{$sql->{quote}{limit_args}} );
            if ( ! $sql->{quote}{limit_stmt} && $info->{set_limit} ) {
                $select .= " LIMIT ?";
                push @arguments, $opt->{print}{limit}[v];
            }
            my $work_around = 0; # https://rt.cpan.org/Public/Bug/Display.html?id=62458
            if ( $info->{db_type} eq 'mysql' && $dbh->{mysql_bind_type_guessing} && any { /^[0-9]*e[0-9]*\z/ } @arguments ) {
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
            if ( $info->{db_type} eq 'sqlite' && $table eq 'union_tables' && @{$sql->{quote}{chosen_cols}} ) {
                $col_names = [ map { s/^"([^"]+)"\z/$1/; $_ } @$col_names ]; #
            }
            my $all_arrayref = $sth->fetchall_arrayref;
            unshift @$all_arrayref, $col_names;
            local $| = 1;
            print GO_TO_TOP_LEFT;
            print CLEAR_EOS;
            return $all_arrayref;
        }
        else {
            die "$custom: no such value in the hash \%customize";
        }
    }
    return;
}


sub set_operator_sql {
    my ( $info, $opt, $sql, $clause, $table, $cols, $qt_columns, $quote_col ) = @_;
    my ( $stmt, $args );
    my %lyt_stmt = %{$info->{lyt_stmt}};
    if ( $clause eq 'where' ) {
        $stmt = 'where_stmt';
        $args = 'where_args';
    }
    elsif ( $clause eq 'having' ) {
        $stmt = 'having_stmt';
        $args = 'having_args';
    }
    my $prompt = print_select_statement( $info, $sql, $table, 'Choose:' );
    my $choices = [ @{$opt->{menu}{operators}[v]} ];
    unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
    # Choose
    my $operator = choose(
        $choices,
        { prompt => $prompt, %lyt_stmt }
    );
    if ( ! defined $operator ) {
        $sql->{quote}{$args} = [];
        $sql->{quote}{$stmt} = '';
        $sql->{print}{$stmt} = '';
        return;
    }
    $operator =~ s/^\s+|\s+\z//g;
    if ( $operator !~ /\s%?col%?\z/ ) {
        if ( $operator !~ /REGEXP\z/ ) {
            $sql->{quote}{$stmt} .= ' ' . $operator;
            $sql->{print}{$stmt} .= ' ' . $operator;
        }
        if ( $operator =~ /NULL\z/ ) {
            # do nothing
        }
        elsif ( $operator =~ /^(?:NOT\s)?IN\z/ ) {
            my $col_sep = '';
            $sql->{quote}{$stmt} .= '(';
            $sql->{print}{$stmt} .= '(';

            IN: while ( 1 ) {
                my $status = print_select_statement( $info, $sql, $table );
                # Readline
                my $value = local_readline( $info, { status => $status, prompt => 'Value: ' } );
                if ( ! defined $value ) {
                    $sql->{quote}{$args} = [];
                    $sql->{quote}{$stmt} = '';
                    $sql->{print}{$stmt} = '';
                    return;
                }
                if ( $value eq '' ) {
                    if ( $col_sep eq ' ' ) {
                        $sql->{quote}{$args} = [];
                        $sql->{quote}{$stmt} = '';
                        $sql->{print}{$stmt} = '';
                        return;
                    }
                    $sql->{quote}{$stmt} .= ')';
                    $sql->{print}{$stmt} .= ')';
                    last IN;
                }
                $sql->{quote}{$stmt} .= $col_sep . '?';
                $sql->{print}{$stmt} .= $col_sep . $value;
                push @{$sql->{quote}{$args}}, $value;
                $col_sep = ',';
            }
        }
        elsif ( $operator =~ /^(?:NOT\s)?BETWEEN\z/ ) {
            my $status1 = print_select_statement( $info, $sql, $table );
            # Readline
            my $value_1 = local_readline( $info, { status => $status1, prompt => 'Value: ' } );
            if ( ! defined $value_1 ) {
                $sql->{quote}{$args} = [];
                $sql->{quote}{$stmt} = '';
                $sql->{print}{$stmt} = '';
                return;
            }
            $sql->{quote}{$stmt} .= ' ' . '?' .      ' AND';
            $sql->{print}{$stmt} .= ' ' . $value_1 . ' AND';
            push @{$sql->{quote}{$args}}, $value_1;
            my $status2 = print_select_statement( $info, $sql, $table );
            # Readline
            my $value_2 = local_readline( $info, { status => $status2, prompt => 'Value: ' } );
            if ( ! defined $value_2 ) {
                $sql->{quote}{$args} = [];
                $sql->{quote}{$stmt} = '';
                $sql->{print}{$stmt} = '';
                return;
            }
            $sql->{quote}{$stmt} .= ' ' . '?';
            $sql->{print}{$stmt} .= ' ' . $value_2;
            push @{$sql->{quote}{$args}}, $value_2;
        }
        elsif ( $operator =~ /REGEXP\z/ ) {
            $sql->{print}{$stmt} .= ' ' . $operator;
            my $status = print_select_statement( $info, $sql, $table );
            # Readline
            my $value = local_readline( $info, { status => $status, prompt => 'Pattern: ' } );
            if ( ! defined $value ) {
                $sql->{quote}{$args} = [];
                $sql->{quote}{$stmt} = '';
                $sql->{print}{$stmt} = '';
                return;
            }
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
            my $status = print_select_statement( $info, $sql, $table );
            my $prompt = $operator =~ /LIKE\z/ ? 'Pattern: ' : 'Value: ';
            # Readline
            my $value = local_readline( $info, { status => $status, prompt => $prompt } );
            if ( ! defined $value ) {
                $sql->{quote}{$args} = [];
                $sql->{quote}{$stmt} = '';
                $sql->{print}{$stmt} = '';
                return;
            }
            $sql->{quote}{$stmt} .= ' ' . '?';
            $sql->{print}{$stmt} .= ' ' . "'$value'";
            push @{$sql->{quote}{$args}}, $value;
        }
    }
    elsif ( $operator =~ /\s%?col%?\z/ ) {
        my $arg;
        if ( $operator =~ /^(.+)\s(%?col%?)\z/ ) {
            $operator = $1;
            $arg = $2;
        }
        $operator =~ s/^\s+|\s+\z//g;
        $sql->{quote}{$stmt} .= ' ' . $operator;
        $sql->{print}{$stmt} .= ' ' . $operator;
        my $status = print_select_statement( $info, $sql, $table, "$operator:" );
        my $choices = [ @$cols ];
        unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
        # Choose
        my $print_col = choose(
            $choices,
            { prompt => $prompt, %lyt_stmt }
        );
        if ( ! defined $print_col ) {
            $sql->{quote}{$stmt} = '';
            $sql->{print}{$stmt} = '';
            return;
        }
        ( my $quote_col = $qt_columns->{$print_col} ) =~ s/\sAS\s\S+\z//;
        my ( @qt_args, @pr_args );
        if ( $arg =~ /^(%)col/ ) {
            push @qt_args, "'$1'";
            push @pr_args, "'$1'";
        }
        push @qt_args, $quote_col;
        push @pr_args, $print_col;
        if ( $arg =~ /col(%)\z/ ) {
            push @qt_args, "'$1'";
            push @pr_args, "'$1'";
        }
        if ( $operator =~ /LIKE\z/ ) {
            $sql->{quote}{$stmt} .= ' ' . concatenate( $info->{db_type}, \@qt_args );
            $sql->{print}{$stmt} .= ' ' . join( '+', @pr_args );
        }
        else {
            $sql->{quote}{$stmt} .= ' ' . $quote_col;
            $sql->{print}{$stmt} .= ' ' . $print_col;
        }
    }
    return;
}


sub calc_col_width {
    my ( $info, $opt, $a_ref, $term_width, $binary_filter ) = @_;
    my ( $width_head, $width_cols, $not_a_number );
    if ( defined $info->{width_head} && defined $info->{width_cols} && defined $info->{not_a_number} ) {
        return @{$info}{qw(width_head width_cols not_a_number)};
    }
    my $gcs_bnry = Unicode::GCString->new( $info->{binary_string} );
    my $binary_length = $gcs_bnry->columns;
    my $binray_regexp = qr/[\x00-\x08\x0B-\x0C\x0E-\x1F]/;
    my $count = 0;
    for my $row ( @$a_ref ) {
        ++$count;
        for my $i ( 0 .. $#$row ) {
            $width_cols->[$i] ||= 1;
            $row->[$i] = $opt->{print}{undef}[v] if ! defined $row->[$i];
            my $width;
            if ( $binary_filter && substr( $row->[$i], 0, 100 ) =~ $binray_regexp ) {
                $row->[$i] = $info->{binary_string};
                $width = $binary_length;
            }
            else {
                $row->[$i] =~ s/\p{Space}+/ /g;
                $row->[$i] =~ s/\p{C}//g;
                my $gcs = Unicode::GCString->new( $row->[$i] );
                $width = $gcs->columns;
            }
            if ( $count == 1 ) {
                # column name
                $width_head->[$i] = $width;
            }
            else {
                # normal row
                $width_cols->[$i] = $width if $width > $width_cols->[$i];
                ++$not_a_number->[$i] if ! looks_like_number $row->[$i];
            }
        }
    }
    return @{$info}{qw(width_head width_cols not_a_number)} = ( $width_head, $width_cols, $not_a_number );
}


sub minus_x_percent {
    my ( $value, $percent ) = @_;
    my $new = int( $value - ( $value / 100 * $percent ) );
    return $new > 0 ? $new : 1;
}

sub calc_avail_width {
    my ( $info, $opt, $a_ref, $term_width, $width_head, $width_cols  ) = @_;
    my $avail_width = $term_width - $opt->{print}{tab_width}[v] * $#$width_cols;
    my $sum = sum( @$width_cols );
    if ( $sum < $avail_width ) {
        # auto cut
        HEAD: while ( 1 ) {
            my $count = 0;
            for my $i ( 0 .. $#$width_head ) {
                if ( $width_head->[$i] > $width_cols->[$i] ) {
                    ++$width_cols->[$i];
                    ++$count;
                    last HEAD if ( $sum + $count ) == $avail_width;
                }
            }
            last HEAD if $count == 0;
            $sum += $count;
        }
        return $width_head, $width_cols;
    }
    elsif ( $sum > $avail_width ) {
        my $minimum_with = $opt->{print}{min_col_width}[v] || 1;
        if ( @$width_head > $avail_width ) {
            say 'Terminal window is not wide enough to print this table.';
            choose( [ 'Press ENTER to show the column names' ], { prompt => '', %{$info->{lyt_stop}} } );
            choose( $a_ref->[0], { prompt => 'Column names (close with ENTER):', %{$info->{lyt_stop}} } );
            return;
        }
        my @width_cols_tmp = @$width_cols;
        my $percent = 0;

        MIN: while ( $sum > $avail_width ) {
            ++$percent;
            my $count = 0;
            for my $i ( 0 .. $#width_cols_tmp ) {
                next if $minimum_with >= $width_cols_tmp[$i];
                if ( $minimum_with >= minus_x_percent( $width_cols_tmp[$i], $percent ) ) {
                    $width_cols_tmp[$i] = $minimum_with;
                }
                else {
                    $width_cols_tmp[$i] = minus_x_percent( $width_cols_tmp[$i], $percent );
                }
                ++$count;
            }
            $sum = sum( @width_cols_tmp );
            $minimum_with-- if $count == 0;
            #last MIN if $minimum_with == 0;
        }
        my $rest = $avail_width - $sum;
        if ( $rest ) {

            REST: while ( 1 ) {
                my $count = 0;
                for my $i ( 0 .. $#width_cols_tmp ) {
                    if ( $width_cols_tmp[$i] < $width_cols->[$i] ) {
                        $width_cols_tmp[$i]++;
                        $rest--;
                        $count++;
                        last REST if $rest == 0;
                    }
                }
                last REST if $count == 0;
            }
        }
        $width_cols = [ @width_cols_tmp ] if @width_cols_tmp;
    }
    return $width_head, $width_cols;
}


sub trunk_col_to_avail_width {
    my ( $info, $opt, $a_ref, $width_cols, $not_a_number, $show_progress ) = @_;
    my $total = $#{$a_ref};                   #
    my $next_update = 0;                      #
    my $c = 0;                                #
    my $progress;                             #
    if ( $show_progress ) {                   #
        local $| = 1;                         #
        print GO_TO_TOP_LEFT;                 #
        print CLEAR_EOS;                      #
        $progress = Term::ProgressBar->new( { #
            name => 'Computing',              #
            count => $total,                  #
            remove => 1 } );                  #
        $progress->minor( 0 );                #
        $show_progress = 1;                   #
    }                                         #
    my $list;
    my $tab = ' ' x $opt->{print}{tab_width}[v];
    for my $row ( @$a_ref ) {
        my $str = '';
        for my $i ( 0 .. $#$width_cols ) {
            $str .= unicode_sprintf( $width_cols->[$i], $row->[$i], $not_a_number->[$i] ? 0 : 1 );
            $str .= $tab if $i != $#$width_cols;
        }
        push @$list, $str;
        if ( $show_progress ) {                                              #
            my $is_power = 0;                                                #
            for ( my $i = 0; 2 ** $i <= $c; ++$i ) {                         #
                $is_power = 1 if 2 ** $i == $c;                              #
            }                                                                #
            $next_update = $progress->update( $c ) if $c >= $next_update;    #
            ++$c;                                                            #
        }                                                                    #
    }
    $progress->update( $total ) if $show_progress && $total >= $next_update; #
    my $len = sum( @$width_cols, $opt->{print}{tab_width}[v] * $#{$width_cols} );
    return $list, $len;
}


sub func_print_tbl {
    my ( $info, $opt, $db, $a_ref ) = @_;
    my ( $term_width ) = GetTerminalSize;
    my $binary_filter = $opt->{$info->{db_type} . '_' . $db}{binary_filter}[v] // $opt->{$info->{db_type}}{binary_filter}[v];
    my $show_progress = 0;
    if ( defined $opt->{print}{progress_bar}[v] && @$a_ref * @{$a_ref->[0]}  > $opt->{print}{progress_bar}[v] ) {
        $show_progress = 1;
    }
    say 'Computing: ...' if $show_progress;
    my ( $width_head, $width_cols, $not_a_number ) = calc_col_width( $info, $opt, $a_ref, $term_width, $binary_filter );
    ( $width_head, $width_cols ) = calc_avail_width( $info, $opt, $a_ref, $term_width, $width_head, $width_cols );
    return if ! $width_head || ! $width_cols;
    my ( $list, $len ) = trunk_col_to_avail_width( $info, $opt, $a_ref, $width_cols, $not_a_number, $show_progress );
    my $limit = 100_000;
    $limit = $opt->{print}{limit}[v] + 1 if defined $opt->{print}{limit}[v] && $opt->{print}{limit}[v] + 1 > $limit;

    if ( $opt->{menu}{table_expand}[v] ) {
        my $length_key = 0;
        for my $width ( @$width_head ) {
            $length_key = $width if $width > $length_key;
        }
        $length_key += 1;
        my $separator = ' : ';
        my $gcs = Unicode::GCString->new( $separator );
        my $length_sep = $gcs->columns;
        my $old_row = 0;

        my $size_changed = 0;
        my $orig_sigwinch = $SIG{'WINCH'};
        local $SIG{'WINCH'} = sub {
            $orig_sigwinch->() if $orig_sigwinch && ref $orig_sigwinch eq 'CODE';
            $size_changed = 1;
        };

        while ( 1 ) {
            if ( $size_changed ) {
                $size_changed = 0;
                func_print_tbl( $info, $opt, $db, $a_ref );
                return;
            }
            my $row = choose(
                $list,
                { prompt => '', %{$info->{lyt_3_cs}}, index => 1, default => $old_row,
                  limit => $limit, ll => $len }
            );
            return if ! defined $row;
            return if $row == 0;
            if ( $old_row != 0 && $old_row == $row ) {
                $old_row = 0;
                next;
            }
            $old_row = $row;
            my ( $term_width ) = GetTerminalSize;
            $length_key = int( $term_width / 100 * 33 ) if $length_key > int( $term_width / 100 * 33 );
            my $col_max = $term_width - ( $length_key + $length_sep );
            my $line_fold = Text::LineFold->new( %{$info->{line_fold}} );
            $line_fold->config( 'ColMax', $col_max );
            my $row_data = [ ' Close with ENTER' ];
            for my $col ( 0 .. $#{$a_ref->[0]} ) {
                push @{$row_data}, ' ';
                my $key = $a_ref->[0][$col];
                my $sep = $separator;
                if ( ! defined $a_ref->[$row][$col] || $a_ref->[$row][$col] eq '' ) {
                    push @{$row_data}, sprintf "%*.*s%*s%s", $length_key, $length_key, $key, $length_sep, $sep, '';
                }
                else {
                    my $text = $line_fold->fold( '' , '', $a_ref->[$row][$col] );
                    for my $line ( split /\R+/, $text ) {
                        push @{$row_data}, sprintf "%*.*s%*s%s", $length_key, $length_key, $key, $length_sep, $sep, $line;
                        $key = '' if $key;
                        $sep = '' if $sep;
                    }
                }
            }
            choose(
                $row_data,
                { prompt => '', %{$info->{lyt_3_cs}} }
            );
        }
    }
    else {
        choose(
            $list,
            { prompt => '', %{$info->{lyt_3_cs}}, limit => $limit, ll => $len }
        );
        return;
    }
}


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
    for my $section ( keys %{$info->{sub_expand}} ) {
        for my $key ( @{$info->{sub_expand}{$section}} ) {
            my $idx = first_index { $_ eq $opt->{$section}{$key}[chs] } @choices;
            splice( @choices, $idx, 1 );
        }
    }

    OPTION: while ( 1 ) {
        # Choose
        my $option = choose(
            [ undef, $info->{_help}, @choices, $info->{_continue} ],
            { %{$info->{lyt_3_cs}}, undef => $info->{_exit} }
        );
        if ( ! defined $option ) {
            if ( $info->{write_config} ) {
                write_config_file( $opt, $info->{config_file} );
                delete $info->{write_config};
            }
            exit();
        }
        elsif ( $option eq $info->{_continue} ) {
            if ( $info->{write_config} ) {
                write_config_file( $opt, $info->{config_file} );
                delete $info->{write_config};
            }
            return $opt;
        }
        elsif ( $option eq $info->{_help} ) {
            pod2usage( { -exitval => 'NOEXIT', -verbose => 2 } );
        }
        elsif ( $option eq $opt->{"print"}{'tab_width'}[chs] ) {
            my $max = 99;
            my ( $section, $key, $prompt ) = ( 'print', 'tab_width', 'Tab width' );
            opt_number( $info, $opt, $section, $key, $prompt, $max );
        }
        elsif ( $option eq $opt->{"print"}{'min_col_width'}[chs] ) {
            my $max = 99;
            my ( $section, $key, $prompt ) = ( 'print', 'min_col_width', 'Minimum Column width' );
            opt_number( $info, $opt, $section, $key, $prompt, $max );
        }
        elsif ( $option eq $opt->{"print"}{'undef'}[chs] ) {
            my ( $section, $key, $prompt ) = ( 'print', 'undef', 'Print replacement for undefined table vales' );
            opt_readline( $info, $opt, $section, $key, $prompt );
        }
        elsif ( $option eq $opt->{"print"}{'limit'}[chs] ) {
            my $digits = 7;
            my ( $section, $key, $prompt ) = ( 'print', 'limit', '"Max rows"' );
            opt_number_range( $info, $opt, $section, $key, $prompt, $digits );
        }
        elsif ( $option eq $opt->{"print"}{'progress_bar'}[chs] ) {
            my $digits = 7;
            my ( $section, $key, $prompt ) = ( 'print', 'progress_bar', '"Threshold ProgressBar"' );
            opt_number_range( $info, $opt, $section, $key, $prompt, $digits );
        }
        elsif ( $option eq $opt->{"sql"}{'lock_stmt'}[chs] ) {
            my $list = [ 'Lk0', 'Lk1' ];
            my ( $section, $key, $prompt ) = ( 'sql', 'lock_stmt', 'Keep statement' );
            opt_choose_list_index( $info, $opt, $section, $key, $prompt, $list );
        }
        elsif ( $option eq $opt->{"sql"}{'system_info'}[chs] ) {
            my ( $section, $key, $prompt ) = ( 'sql', 'system_info', 'Enable Metadata' );
            opt_yes_no( $info, $opt, $section, $key, $prompt );
        }
        elsif ( $option eq $opt->{"sql"}{'regexp_case'}[chs] ) {
            my ( $section, $key, $prompt ) = ( 'sql', 'regexp_case', 'REGEXP case sensitiv' );
            opt_yes_no( $info, $opt, $section, $key, $prompt );
        }
        elsif ( $option eq $opt->{"dbs"}{'login'}[chs] ) {
            my $list = [ 'for every new connection', 'once per database', 'only once' ];
            my ( $section, $key, $prompt ) = ( 'dbs', 'login', 'Ask for credentials' );
            opt_choose_list_index( $info, $opt, $section, $key, $prompt, $list );
        }
        elsif ( $option eq $opt->{"dbs"}{'db_defaults'}[chs] ) {
            database_setting( $info, $opt );
        }
        elsif ( $option eq $opt->{"menu"}{'thsd_sep'}[chs] ) {
            my $list = [ "','", "'.'", "'_'", "' '", "''" ];
            my ( $section, $key, $prompt ) = ( 'menu', 'thsd_sep', 'Thousands separator' );
            opt_choose_list_value( $info, $opt, $section, $key, $prompt, $list );
        }
        elsif ( $option eq $opt->{"menu"}{'sssc_mode'}[chs] ) {
            my $list = [ 'simple', 'compat' ];
            my ( $section, $key, $prompt ) = ( 'menu', 'sssc_mode', 'Sssc mode' );
            opt_choose_list_index( $info, $opt, $section, $key, $prompt, $list );
        }
        elsif ( $option eq $opt->{"menu"}{'operators'}[chs] ) {
            my $available = $info->{avilable_operators};
            my ( $section, $key ) = ( 'menu', 'operators' );
            opt_choose_a_list( $info, $opt, $section, $key, $available );
        }
        elsif ( $option eq $opt->{"menu"}{'database_types'}[chs] ) {
            my $available = $info->{avilable_db_types};
            my ( $section, $key ) = ( 'menu', 'database_types'  );
            opt_choose_a_list( $info, $opt, $section, $key, $available );
        }
        elsif ( $option eq $opt->{"menu"}{'mouse'}[chs] ) {
            my $max = 4;
            my ( $section, $key, $prompt ) = ( 'menu', 'mouse', 'Mouse mode' );
            opt_number( $info, $opt, $section, $key, $prompt, $max );
        }
        elsif ( $option eq $opt->{"menu"}{'expand'}[chs] ) {
            my $count;
            my ( $memory, $simple ) = ( 'Enchanted', 'Simple' );
            my @keys = @{$info->{sub_expand}{menu}};
            my $tmp = {};
            for my $key ( @keys ) {
                $tmp->{menu}{$key}[v] = $opt->{menu}{$key}[v];
            }
            while ( 1 ) {
                my $choices = [ undef ];
                for my $key ( @keys ) {
                    my $current = $tmp->{menu}{$key}[v] ? $memory : $simple;
                    push @$choices, sprintf "%-18s[%s]", $opt->{menu}{$key}[chs], $current;
                }
                push @$choices, $info->{_confirm};
                # Choose
                my $idx = choose(
                    $choices,
                    { prompt => 'Choose:', %{$info->{lyt_3_cs}}, index => 1 }
                );
                next OPTION if ! defined $idx;
                my $choice = $choices->[$idx];
                next OPTION if ! defined $choice;
                if ( $choice eq $info->{_confirm} ) {
                    if ( $count ) {
                        for my $key ( @keys ) {
                            $opt->{menu}{$key}[v] = $tmp->{menu}{$key}[v];
                        }
                        $info->{write_config}++;
                    }
                    next OPTION;
                }
                my $key = $keys[$idx-1];
                $tmp->{menu}{$key}[v] = $tmp->{menu}{$key}[v] ? 0 : 1;
                $count++;
            }
        }
        else { die $option }
    }
}

sub opt_yes_no {
    my ( $info, $opt, $section, $key, $prompt ) = @_;
    my ( $yes, $no ) = ( 'YES', 'NO' );
    my $current = $opt->{$section}{$key}[v];
    # Choose
    my $choice = choose(
        [ undef, $yes, $no ],
        { prompt => $prompt . ' [' . ( $current ? 'YES' : 'NO' ) . ']:', %{$info->{lyt_1}} }
    );
    return if ! defined $choice;
    $opt->{$section}{$key}[v] = $choice eq $yes ? 1 : 0;
    $info->{write_config}++;
    return;
}

sub opt_choose_list_value {
    my ( $info, $opt, $section, $key, $prompt, $list ) = @_;
    my $current = $opt->{$section}{$key}[v];
    # Choose
    my $choice = choose(
        [ undef, @$list ],
        { prompt => $prompt . ' [' . $current . ']:', %{$info->{lyt_1}} }
    );
    return if ! defined $choice;
    $choice =~ s/^'|'\z//g;
    $opt->{$section}{$key}[v] = $choice;
    $info->{write_config}++;
    return;
}

sub opt_choose_list_index {
    my ( $info, $opt, $section, $key, $prompt, $list ) = @_;
    my $current = $list->[$opt->{$section}{$key}[v]];
    # Choose
    my $idx = choose(
        [ undef, @$list ],
        { prompt => $prompt . ' [' . $current . ']:', index => 1, %{$info->{lyt_1}} }
    );
    return if ! defined $idx;
    $idx--;
    $opt->{$section}{$key}[v] = $idx;
    $info->{write_config}++;
    return;
}

sub opt_choose_a_list {
    my ( $info, $opt, $section, $key, $available ) = @_;
    my $current = $opt->{$section}{$key}[v];
    # Choose_list
    my $list = choose_list( $info, $current, $available );
    return if ! defined $list;
    $opt->{$section}{$key}[v] = $list;
    $info->{write_config}++;
    return;
}

sub opt_number {
    my ( $info, $opt, $section, $key, $prompt, $max ) = @_;
    my $current = $opt->{$section}{$key}[v];
    # Choose
    my $choice = choose(
        [ undef, 0 .. $max ],
        { prompt => $prompt . ' [' . $current . ']:', %{$info->{lyt_1}}, justify => 1 }
    );
    return if ! defined $choice;
    $opt->{$section}{$key}[v] = $choice;
    $info->{write_config}++;
    return;
}

sub opt_number_range {
    my ( $info, $opt, $section, $key, $prompt, $digits ) = @_;
    my $current = $opt->{$section}{$key}[v];
    $current = insert_sep( $current, $opt->{menu}{thsd_sep}[v] );
    # Choose_a_number
    my $choice = choose_a_number( $info, $opt, $digits, $prompt, $current );
    return if ! defined $choice;
    $opt->{$section}{$key}[v] = $choice eq '--' ? undef : $choice;
    $info->{write_config}++;
    return;
}

sub opt_readline {
    my ( $info, $opt, $section, $key, $prompt ) = @_;
    my $current = $opt->{$section}{$key}[v];
    # Readline
    my $choice = local_readline( $info,
        { prompt => $prompt . ' ["' . $current . '"]: ' }
    );
    return if ! defined $choice;
    $opt->{$section}{$key}[v]  = $choice;
    $info->{write_config}++;
    return;
}

sub database_setting {
    my ( $info, $opt, $db ) = @_;
    my @choices = ();
    my $db_type;
    if ( ! defined $db ) {
        # Choose
        $db_type = choose(
            [ undef, @{$opt->{menu}{database_types}[v]} ],
            { %{$info->{lyt_1}} }
        );
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
    if ( defined $info->{cmdline_only}{$db_type} ) {
        for my $key ( @{$info->{cmdline_only}{$db_type}} ) {
            my $idx = first_index { $_ eq $opt->{$db_type}{$key}[chs] } @choices;
            splice( @choices, $idx, 1 );
        }
    }
    my $section = $db_type;
    if ( defined $db ) {
        $section .= '_' . $db;
        for my $key ( keys %{$opt->{$section}} ) {
            $opt->{$section}{$key}[v] //= $opt->{$db_type}{$key}[v];
        }
    }
    my $orig = clone( $opt );

    DB_OPTION: while ( 1 ) {
        # Choose
        my $option = choose(
            [ undef, @choices, $info->{_confirm} ],
            { %{$info->{lyt_3_cs}} }
        );
        if ( ! defined $option ) {
            if ( $info->{write_config} ) {
                $opt = clone( $orig );
            }
            return;
        }
        if ( $option eq $info->{_confirm} ) {
            if ( $info->{write_config} ) {
                write_config_file( $opt, $info->{config_file} );
                delete $info->{write_config};
            }
            return;
        }
        if ( $db_type eq 'sqlite' ) {
            if ( $option eq $opt->{"sqlite"}{unicode}[chs] ) {
                my ( $key, $prompt ) = ( 'unicode', 'Unicode' );
                opt_yes_no( $info, $opt, $section, $key, $prompt );
            }
            elsif ( $option eq $opt->{"sqlite"}{see_if_its_a_number}[chs] ) {
                my ( $key, $prompt ) = ( 'see_if_its_a_number', 'See if its a number' );
                opt_yes_no( $info, $opt, $section, $key, $prompt );
            }
            elsif ( $option eq $opt->{"sqlite"}{busy_timeout}[chs] ) {
                my $digits = 6;
                my ( $key, $prompt ) = ( 'busy_timeout', '"Busy timeout (ms)"' );
                opt_number_range( $info, $opt, $section, $key, $prompt, $digits );
            }
            elsif ( $option eq $opt->{"sqlite"}{cache_size}[chs] ) {
                my $digits = 8;
                my ( $key, $prompt ) = ( 'cache_size', '"Cache size (kb)"' );
                opt_number_range( $info, $opt, $section, $key, $prompt, $digits );
            }
            elsif ( $option eq $opt->{"sqlite"}{binary_filter}[chs] ) {
                my ( $key, $prompt ) = ( 'binary_filter', 'Enable Binary Filter' );
                opt_yes_no( $info, $opt, $section, $key, $prompt );
            }
            else { die $option }
        }
        elsif ( $db_type eq 'mysql' ) {
            if ( $option eq $opt->{"mysql"}{enable_utf8}[chs] ) {
                my ( $key, $prompt ) = ( 'enable_utf8', 'Enable utf8' );
                opt_yes_no( $info, $opt, $section, $key, $prompt );
            }
            elsif ( $option eq $opt->{"mysql"}{default_user}[chs] ) {
                my ( $key, $prompt ) = ( 'default_user', 'Default user' );
                opt_readline( $info, $opt, $section, $key, $prompt );
            }
            elsif ( $option eq $opt->{"mysql"}{bind_type_guessing}[chs] ) {
                my ( $key, $prompt ) = ( 'bind_type_guessing', 'Bind type guessing' );
                opt_yes_no( $info, $opt, $section, $key, $prompt );
            }
            elsif ( $option eq $opt->{"mysql"}{ChopBlanks}[chs] ) {
                my ( $key, $prompt ) = ( 'ChopBlanks', 'Chop blanks off' );
                opt_yes_no( $info, $opt, $section, $key, $prompt );
            }
            elsif ( $option eq $opt->{"mysql"}{connect_timeout}[chs] ) {
                my $digits = 4;
                my ( $key, $prompt ) = ( 'connect_timeout', '"Busy timeout"' );
                opt_number_range( $info, $opt, $section, $key, $prompt, $digits );
            }
            elsif ( $option eq $opt->{"mysql"}{binary_filter}[chs] ) {
                my ( $key, $prompt ) = ( 'binary_filter', 'Enable Binary Filter' );
                opt_yes_no( $info, $opt, $section, $key, $prompt );
            }
            else { die $option }
        }
        elsif ( $db_type eq 'postgres' ) {
            if ( $option eq $opt->{"postgres"}{pg_enable_utf8}[chs] ) {
                my ( $key, $prompt ) = ( 'pg_enable_utf8', 'Enable utf8' );
                opt_yes_no( $info, $opt, $section, $key, $prompt );
            }
            elsif ( $option eq $opt->{"postgres"}{default_user}[chs] ) {
                my ( $key, $prompt ) = ( 'default_user', 'Default user' );
                opt_readline( $info, $opt, $section, $key, $prompt );
            }
            elsif ( $option eq $opt->{"postgres"}{binary_filter}[chs] ) {
                my ( $key, $prompt ) = ( 'binary_filter', 'Enable Binary Filter' );
                opt_yes_no( $info, $opt, $section, $key, $prompt );
            }
            else { die $option }
        }
    }
}


sub print_error_message {
    my ( $message ) = @_;
    utf8::decode( $message );
    print $message;
    choose( [ 'Press ENTER to continue' ], { prompt => '', %{$info->{lyt_stop}} } );
}


sub local_readline {
    my ( $info, $args ) = @_;
    my $str = '';
    local_readline_print( $info, $args, $str );
    ReadMode 4;
    while ( 1 ) {
        my $key = ReadKey;
        return if ! defined $key;
        if ( $key eq "\cC" ) {
            print "\n";
            say "^C";
            exit 1;
        }
        elsif ( $key eq "\cD" ) {
            print "\n";
            return;
        }
        elsif ( $key eq "\n" ) {
            print "\n";
            return $str;
        }
        elsif ( ord( $key ) == BSPACE || $key eq "\cH" ) {
            $str =~ s/.\z// if $str;
            local_readline_print( $info, $args, $str );
            next;
        }
        elsif ( $key !~ /^\p{Print}\z/ ) {
            local_readline_print( $info, $args, $str );
            next;
        }
        $str .= $key;
        local_readline_print( $info, $args, $str );
    }
    ReadMode 0;
    return $str;
}

sub local_readline_print {
    my ( $info, $args, $str ) = @_;
    print GO_TO_TOP_LEFT;
    print CLEAR_EOS;
    if ( defined $args->{status} ) {
        my $line_fold = Text::LineFold->new(
            %{$info->{line_fold}},
            ColMax => ( GetTerminalSize )[0],
        );
        print $line_fold->fold( '', ' ' x 4, $args->{status} );
    }
    print $args->{prompt} . ( $args->{no_echo} ? '' : $str );
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
    close $fh;
}


sub read_json {
    my ( $file ) = @_;
    return {} if ! -f encode_utf8( $file );
    open my $fh, '<', encode_utf8( $file ) or die $!;
    my $json = do { local $/; <$fh> };
    close $fh;
    my $h_ref = JSON::XS->new->pretty->decode( $json ) if $json;
    return $h_ref;
}


sub choose_a_number {
    my ( $info, $opt, $digits, $name, $current ) = @_;
    my $sep = $opt->{menu}{thsd_sep}[v];
    my $tab = '  -  ';
    my $gcs_tab = Unicode::GCString->new( $tab );
    my $length_tab = $gcs_tab->columns;
    my $longest = $digits;
    $longest += int( ( $digits - 1 ) / 3 ) if $sep ne '';
    my @choices_range = ();
    for my $di ( 0 .. $digits - 1 ) {
        my $begin = 1 . '0' x $di;
        $begin = 0 if $di == 0;
        $begin = insert_sep( $begin, $sep );
        ( my $end = $begin ) =~ s/^[01]/9/;
        unshift @choices_range, sprintf " %*s%s%*s", $longest, $begin, $tab, $longest, $end;
    }
    my $confirm = sprintf "%-*s", $longest * 2 + $length_tab, $info->{confirm};
    my $back    = sprintf "%-*s", $longest * 2 + $length_tab, $info->{back};
    my ( $term_width ) = GetTerminalSize;
    my $gcs_longest_range = Unicode::GCString->new( $choices_range[0] );
    if ( $gcs_longest_range->columns > $term_width ) {
        @choices_range = ();
        for my $di ( 0 .. $digits - 1 ) {
            my $begin = 1 . '0' x $di;
            $begin = 0 if $di == 0;
            $begin = insert_sep( $begin, $sep );
            unshift @choices_range, sprintf "%*s", $longest, $begin;
        }
        $confirm = $info->{confirm};
        $back    = $info->{back};
    }
    my $reset = 'reset';
    my %numbers;
    my $result;

    NUMBER: while ( 1 ) {
        my $new_result = $result // '--';
        my $prompt = '';
        if ( @_ == 5 ) {
            $prompt .= sprintf "%s%*s\n",   'Current ' . $name . ': ', $longest, $current // '--';
            $prompt .= sprintf "%s%*s\n\n", '    New ' . $name . ': ', $longest, $new_result;
        }
        else {
            $prompt = sprintf "%s%*s\n\n", $name . ': ', $longest, $new_result;
        }
        # Choose
        my $range = choose(
            [ undef, @choices_range, $confirm ],
            { prompt => $prompt, %{$info->{lyt_3_cs}}, justify => 1, undef => $back }
        );
        return if ! defined $range;
        if ( $range eq $confirm ) {
            return '--' if ! defined $result;
            $result =~ s/\Q$sep\E//g if $sep ne '';
            return $result;
        }
        my $zeros = ( split /\s*-\s*/, $range )[0];
        $zeros =~ s/^\s*\d//;
        ( my $zeros_no_sep = $zeros ) =~ s/\Q$sep\E//g if $sep ne '';
        my $count_zeros = length $zeros_no_sep;
        my @choices = $count_zeros ? map( $_ . $zeros,  1 .. 9 ) : ( 0 .. 9 );
        # Choose
        my $number = choose(
            [ undef, @choices, $reset ],
            { prompt => $prompt, %{$info->{lyt_1}}, clear_screen => 1, undef => '<<' }
        );
        next if ! defined $number;
        if ( $number eq $reset ) {
            delete $numbers{$count_zeros};
        }
        else {
            $number =~ s/\Q$sep\E//g if $sep ne '';
            $numbers{$count_zeros} = $number;
        }
        $result = sum( @numbers{keys %numbers} );
        $result = insert_sep( $result, $sep );
    }
}


sub insert_sep {
    my ( $number, $separator ) = @_;
    return if ! defined $number;
    return $number if ! $separator;
    $number =~ s/(\d)(?=(?:\d{3})+\b)/$1$separator/g;
    return $number;
}


sub choose_list {
    my ( $info, $current, $available ) = @_;
    my $new = [];
    my $key_cur = 'Current > ';
    my $key_new = '    New > ';
    my $gcs_cur = Unicode::GCString->new( $key_cur );
    my $gcs_new = Unicode::GCString->new( $key_new );
    my $length_key = $gcs_cur->columns > $gcs_new->columns ? $gcs_cur->columns : $gcs_new->columns;
    my $line_fold = Text::LineFold->new( %{$info->{line_fold}} );
    while ( 1 ) {
        my $prompt = $key_cur . join( ', ', map { "\"$_\"" } @$current ) . "\n";
        $prompt   .= $key_new . join( ', ', map { "\"$_\"" } @$new )     . "\n\n";
        $prompt   .= 'Choose:';
        # Choose
        my $choice = choose(
            [ undef, map( "- $_", @$available ), $info->{_confirm} ],
            { prompt => $prompt, lf => [0,$length_key], %{$info->{lyt_3_cs}} }
        );
        return if ! defined $choice;
        if ( $choice eq $info->{_confirm} ) {
            return $new if @$new;
            return;
        }
        $choice =~ s/^-\s//;
        push @$new, $choice;
    }
}


sub unicode_sprintf {
    my ( $avail_width, $unicode, $right_justify ) = @_;
    my $gcs = Unicode::GCString->new( $unicode );
    my $colwidth = $gcs->columns;
    if ( $colwidth > $avail_width ) {
        my $pos = $gcs->pos;
        $gcs->pos( 0 );
        my $cols = 0;
        my $gc;
        while ( defined( $gc = $gcs->next ) ) {
            if ( $avail_width < ( $cols += $gc->columns ) ) {
                my $ret = $gcs->substr( 0, $gcs->pos - 1 );
                $gcs->pos( $pos );
                return $ret->as_string;
            }
        }
    }
    elsif ( $colwidth < $avail_width ) {
        if ( $right_justify ) {
            $unicode = " " x ( $avail_width - $colwidth ) . $unicode;
        }
        else {
            $unicode = $unicode . " " x ( $avail_width - $colwidth );
        }
    }
    return $unicode;
}


sub set_credentials {
    my ( $info, $opt, $db ) = @_;
    my ( $user, $passwd );
    if ( length( $opt->{$info->{db_type}}{default_user}[v] // '' ) ) {
        $user = $opt->{$info->{db_type}}{default_user}[v];
    }
    my $db_key = $info->{db_type} . '_' . $db;
    my $login = $opt->{dbs}{login}[v];
    if( $login == 1 ) {
        $user //= $info->{login}{$info->{db_type}}{$db}{user};
        $passwd = $info->{login}{$info->{db_type}}{$db}{passwd};
    }
    elsif ( $login == 2 ) {
        $user //= $info->{login}{$info->{db_type}}{user};
        $passwd = $info->{login}{$info->{db_type}}{passwd};
    }
    # Readline
    if ( ! defined $user ) {
        $user = local_readline(
            $info,
            { prompt => 'User    : ', status => "\"$db\"\n" }
        );
    }
    if ( ! defined $passwd ) {
        $passwd = local_readline(
            $info,
            { prompt => 'Password: ', status => "\"$db\"\n\nUser    : $user", no_echo => 1 }
        );
    }
    if ( $login == 1 ) {
        $info->{login}{$info->{db_type}}{$db}{user}   = $user;
        $info->{login}{$info->{db_type}}{$db}{passwd} = $passwd;
    }
    elsif ( $login == 2 ) {
        $info->{login}{$info->{db_type}}{user}   = $user;
        $info->{login}{$info->{db_type}}{passwd} = $passwd;
    }
    return $user, $passwd;
}


###########################################   database specific subroutines   ############################################


sub get_db_handle {
    my ( $info, $opt, $db ) = @_;
    my $dbh;
    my $db_key = $info->{db_type} . '_' . $db;
    if ( $info->{db_type} eq 'sqlite' ) {
        my $unicode       = $opt->{$db_key}{unicode}[v]             // $opt->{sqlite}{unicode}[v];
        my $see_if_number = $opt->{$db_key}{see_if_its_a_number}[v] // $opt->{sqlite}{see_if_its_a_number}[v];
        my $busy_timeout  = $opt->{$db_key}{busy_timeout}[v]        // $opt->{sqlite}{busy_timeout}[v];
        my $cache_size    = $opt->{$db_key}{cache_size}[v]          // $opt->{sqlite}{cache_size}[v];

        die "\"$db\": $!. Maybe the cached data is not up to date." if ! -f $db;
        $dbh = DBI->connect( "DBI:SQLite:dbname=$db", '', '', {
            PrintError => 0,
            RaiseError => 1,
            AutoCommit => 1,
            sqlite_unicode => $unicode,
            sqlite_see_if_its_a_number => $see_if_number,
        } ) or die DBI->errstr;
        $dbh->sqlite_busy_timeout( $busy_timeout )       if defined $busy_timeout;
        $dbh->do( 'PRAGMA cache_size = ' . $cache_size ) if defined $cache_size;
        $dbh->sqlite_create_function( 'regexp', 2, sub {
            my ( $regex, $string ) = @_;
            $string //= '';
            return $string =~ m/$regex/ism }
        );
        $dbh->sqlite_create_function( 'truncate', 2, sub { # round
            my ( $number, $precision ) = @_;
            return if ! defined $number;
            die "Argument isn't numeric in TRUNCATE" if ! looks_like_number( $number );
            return sprintf( "%.*f", $precision // 2, $number ) }
        );
    }
    elsif ( $info->{db_type} eq 'mysql' )  {
        my $enable_utf8        = $opt->{$db_key}{enable_utf8}[v]        // $opt->{mysql}{enable_utf8}[v];
        my $connect_timeout    = $opt->{$db_key}{connect_timeout}[v]    // $opt->{mysql}{connect_timeout}[v];
        my $bind_type_guessing = $opt->{$db_key}{bind_type_guessing}[v] // $opt->{mysql}{bind_type_guessing}[v];
        my $chop_blanks        = $opt->{$db_key}{ChopBlanks}[v]         // $opt->{mysql}{ChopBlanks}[v];
        my ( $user, $passwd ) = set_credentials( $info, $opt, $db );
        $dbh = DBI->connect( "DBI:mysql:dbname=$db", $user, $passwd, {
            PrintError  => 0,
            RaiseError  => 1,
            AutoCommit  => 1,
            mysql_enable_utf8        => $enable_utf8,
            mysql_connect_timeout    => $connect_timeout,
            mysql_bind_type_guessing => $bind_type_guessing,
            ChopBlanks               => $chop_blanks,
        } ) or die DBI->errstr;
    }
    elsif ( $info->{db_type} eq 'postgres' ) {
        my $enable_utf8 = $opt->{$db_key}{pg_enable_utf8}[v] // $opt->{postgres}{pg_enable_utf8}[v];
        my ( $user, $passwd ) = set_credentials( $info, $opt, $db );
        $dbh = DBI->connect( "DBI:Pg:dbname=$db", $user, $passwd, {
            PrintError => 0,
            RaiseError => 1,
            AutoCommit => 1,
            pg_enable_utf8 => $enable_utf8,
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
        $info->{cache} = read_json( $info->{db_cache_file} );
        if ( $opt->{sqlite}{reset_cache}[v] ) {
            delete $info->{cache}{$c_dirs};
            $info->{cached} = '';
        }
        else {
            if ( $info->{cache}{$c_dirs} ) {
                $databases = $info->{cache}{$c_dirs};
                $info->{cached} = ' (cached)';
            }
            else {
                $info->{cached} = '';
            }
        }
        if ( ! $info->{cached} ) {
            say 'Searching...';
            for my $dir ( @dirs ) {
                find( {
                    wanted     => sub {
                        my $file = $File::Find::name;
                        return if ! -f $file;
                        return if ! -s $file;
                        return if ! -r $file;
                        #say $file;
                        if ( ! eval {
                            open my $fh, '<:raw', $file or die "$file: $!";
                            defined( read $fh, my $string, 13 ) or die "$file: $!";
                            close $fh;
                            push @$databases, decode_utf8( $file ) if $string eq 'SQLite format';
                            1 }
                        ) {
                            utf8::decode( $@ );
                            print $@;
                        }
                    },
                    no_chdir   => 1,
                },
                encode_utf8( $dir ) );
            }
            $info->{cache}{$c_dirs} = $databases;
            write_json( $info->{db_cache_file}, $info->{cache} );
            say 'Ended searching';
        }
    }
    elsif( $info->{db_type} eq 'mysql' ) {
        my $dbh = get_db_handle( $info, $opt, 'information_schema' );
        my $stmt;
        if ( ! $opt->{sql}{system_info}[v] ) {
            $stmt = "SELECT schema_name FROM information_schema.schemata
                        WHERE schema_name != 'mysql'
                          AND schema_name != 'information_schema'
                          AND schema_name != 'performance_schema'
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
                        WHERE datistemplate = false
                          AND datname != 'postgres'
                        ORDER BY datname";
        }
        elsif ( $opt->{sql}{system_info}[v] ) {
            $stmt = "SELECT datname FROM pg_database
                     ORDER BY datname";
        }
        $databases = $dbh->selectcol_arrayref( $stmt, {} );
    }
    else {
        no_entry_for_db_type( $info->{db_type} );
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
                        WHERE schema_name != 'information_schema'
                          AND schema_name !~ '^pg_'
                        ORDER BY schema_name";
        }
        elsif ( $opt->{sql}{system_info}[v] ) {
            $stmt = "SELECT schema_name FROM information_schema.schemata
                     ORDER BY schema_name";
        }
        my $schemas = $dbh->selectcol_arrayref( $stmt, {} );
        return $schemas;
    }
    no_entry_for_db_type( $info->{db_type} );
}


sub get_table_names {
    my ( $dbh, $db, $schema ) = @_;
    my $tables = [];
    if ( $info->{db_type} eq 'sqlite' ) {
        my $stmt = "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name";
        $tables = $dbh->selectcol_arrayref( $stmt );
    }
    else {
        my $stmt = "SELECT table_name FROM information_schema.tables
                        WHERE table_schema = ?
                        ORDER BY table_name";
                        # AND table_type = 'BASE TABLE'
        $tables = $dbh->selectcol_arrayref( $stmt, {}, ( $schema ) );
    }
    return $tables;
}


sub system_databases {
    my ( $db_type ) = @_;
    my $system_databases;
    if ( $db_type eq 'mysql' ) {
        $system_databases = [ qw( mysql information_schema performance_schema ) ];
    }
    elsif ( $db_type eq 'postgres' ) {
        $system_databases = [ qw( postgres template0 template1 ) ];
    }
    return $system_databases;
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
        $stmt = "SELECT table_name, column_name, column_type
                    FROM information_schema.columns
                    WHERE table_schema = ?" if $info->{db_type} eq 'mysql';
        $stmt = "SELECT table_name, column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = ?" if $info->{db_type} eq 'postgres';
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
    my $fks     = {};
    my $pk_cols = {};
    my $sth;
    if ( $info->{db_type} eq 'mysql' ) {
        my $stmt = "SELECT constraint_name, table_name, column_name, referenced_table_name,
                           referenced_column_name, position_in_unique_constraint
                    FROM information_schema.key_column_usage
                    WHERE table_schema = ?
                      AND table_name = ?
                      AND referenced_table_name IS NOT NULL";
        $sth = $dbh->prepare( $stmt );
    }
    elsif ( $info->{db_type} eq 'sqlite' ) {
        my $stmt = "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?";
        $sth = $dbh->prepare( $stmt );
    }
    for my $tbl ( @{$data->{$db}{$schema}{tables}} ) {
        if ( $info->{db_type} eq 'sqlite' ) {
            $sth->execute( $tbl );
            my $a_ref = $sth->fetchrow_arrayref();
            my @references = grep { /FOREIGN\sKEY/ } split /\s*\n\s*/, $a_ref->[0];
            for my $i ( 0 .. $#references ) {
                if ( $references[$i] =~ /FOREIGN\sKEY\s?\(([^)]+)\)\sREFERENCES\s([^(]+)\(([^)]+)\),?/ ) {
                    $fks->{$tbl}{$i}{foreign_key_col}   = [ split /\s*,\s*/, $1 ];
                    $fks->{$tbl}{$i}{reference_key_col} = [ split /\s*,\s*/, $3 ];
                    $fks->{$tbl}{$i}{reference_table}   = $2;
                }
            }
        }
        elsif ( $info->{db_type} eq 'mysql' ) {
            $sth->execute( $schema, $tbl );
            while ( my $row = $sth->fetchrow_hashref ) {
                $fks->{$tbl}
                      {$row->{constraint_name}}
                      {foreign_key_col}
                      [$row->{position_in_unique_constraint}-1] = $row->{column_name};
                $fks->{$tbl}
                      {$row->{constraint_name}}
                      {reference_key_col}
                      [$row->{position_in_unique_constraint}-1] = $row->{referenced_column_name};
                if ( ! $fks->{$tbl}{$row->{constraint_name}}{reference_table} ) {
                    $fks->{$tbl}
                          {$row->{constraint_name}}
                          {reference_table} = $row->{referenced_table_name};
                }
            }
        }
        elsif ( $info->{db_type} eq 'postgres' ) {
            my $sth = $dbh->foreign_key_info( undef, undef, undef, undef, $schema, $tbl );
            if ( defined $sth ) {
                while ( my $row = $sth->fetchrow_hashref ) {
                    push @{$fks->{$tbl}{$row->{FK_NAME}}{foreign_key_col  }}, $row->{FK_COLUMN_NAME};
                    push @{$fks->{$tbl}{$row->{FK_NAME}}{reference_key_col}}, $row->{UK_COLUMN_NAME};
                    if ( ! $fks->{$tbl}{$row->{FK_NAME}}{reference_table} ) {
                        $fks->{$tbl}
                              {$row->{FK_NAME}}
                              {reference_table} = $row->{UK_TABLE_NAME};
                    }
                }
            }
        }
        $pk_cols->{$tbl} = [ $dbh->primary_key( undef, $schema, $tbl ) ];
    }
    return $pk_cols, $fks;
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
            return ' NOT REGEXP_LIKE(' . $quote_col . ',?,\'i\')' if ! $opt->{sql}{regexp_case}[v];
            return ' NOT REGEXP_LIKE(' . $quote_col . ',?)'       if   $opt->{sql}{regexp_case}[v];
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
            return ' REGEXP_LIKE(' . $quote_col . ',?,\'i\')' if ! $opt->{sql}{regexp_case}[v];
            return ' REGEXP_LIKE(' . $quote_col . ',?)'       if   $opt->{sql}{regexp_case}[v];
        }
    }
    no_entry_for_db_type( $info->{db_type} );
}


sub concatenate {
    my ( $db_type, $arg ) = @_;
    return 'concat(' . join( ',', @$arg ) . ')' if $db_type eq 'mysql';
    return join( ' || ', @$arg );
}


sub col_functions {
    my ( $info, $sql, $table, $func, $quote_col, $print_col ) = @_;
    my %lyt_stmt = %{$info->{lyt_stmt}};
    my $db_type = $info->{db_type};
    my ( $quote_f, $print_f );
    if ( $func =~ /^epoch_to_date(?:time)?\z/i ) {
        $print_f = $func =~ /^epoch_to_date\z/i ? "DATE($print_col)" : "DATETIME($print_col)";
        my $prompt = print_select_statement( $info, $sql, $table, "$print_f\nInterval:" );
        my ( $seconds, $milliseconds, $microseconds ) = ( '1 Second', '1 Millisecond', '1 Microsecond' );
        my $choices =  [ $seconds, $milliseconds, $microseconds ];
        unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
        # Choose
        my $interval = choose( $choices, { prompt => $prompt, %lyt_stmt } );
        return if ! defined $interval;
        my $div = $interval eq $microseconds ? 1000000 :
                  $interval eq $milliseconds ? 1000 : 1;
        if ( $func =~ /^epoch_to_datetime\z/i ) {
            $quote_f = "FROM_UNIXTIME($quote_col/$div,'%Y-%m-%d %H:%i:%s')"    if $db_type eq 'mysql';
            $quote_f = "( TO_TIMESTAMP(${quote_col}::bigint/$div))::timestamp" if $db_type eq 'postgres';
            $quote_f = "DATETIME($quote_col/$div,'unixepoch','localtime')"     if $db_type eq 'sqlite';
        }
        else {
            # mysql: FROM_UNIXTIME doesn't work with negative timestamps
            $quote_f = "FROM_UNIXTIME($quote_col/$div,'%Y-%m-%d')"       if $db_type eq 'mysql';
            $quote_f = "(TO_TIMESTAMP(${quote_col}::bigint/$div))::date" if $db_type eq 'postgres';
            $quote_f = "DATE($quote_col/$div,'unixepoch','localtime')"   if $db_type eq 'sqlite';
        }
    }
    elsif ( $func =~ /^TRUNCATE\z/i ) {
        my $prompt = print_select_statement( $info, $sql, $table, "TRUNC $print_col\nDecimal places:" );
        my $choices = [ 0 .. 9 ];
        unshift @$choices, undef if $opt->{menu}{sssc_mode}[v];
        my $precision = choose( $choices, { prompt => $prompt, %lyt_stmt } );
        return if ! defined $precision;
        $quote_f = "TRUNCATE($quote_col,$precision)" if $db_type eq 'mysql';
        $quote_f = "TRUNC($quote_col,$precision)"    if $db_type eq 'postgres';
        $quote_f = "TRUNCATE($quote_col,$precision)" if $db_type eq 'sqlite'; # round
        $print_f = "TRUNC($print_col,$precision)";
    }
    return $quote_f, $print_f;
}


sub no_entry_for_db_type {
    my ( $db_type ) = @_;
    die "No entry for \"$db_type\"!";
}





__END__

=pod

=encoding UTF-8

=head1 NAME

table_watch_SQLite.pl - Read SQLite/MySQL/PostgreSQL databases.

=head1 VERSION

Version 1.065

=cut

=head1 SYNOPSIS

=head2 SQLite/MySQL/PostgreSQL:

table_watch_SQLite.pl

table_watch_SQLite.pl -h|--help

    -h|--help    show the config menu (which offers also this help text).

=head2 SQLite:

table_watch_SQLite.pl [-s|--search] [directories to be searched]

    If no directories are passed the home directory is searched for SQLite databases.

    -s|--search    new search of SQLite databases (don't use cached data).

=head1 DESCRIPTION

Search and read SQLite/MySQL/PostgreSQL databases.

table_watch_SQLite.pl expects an "UTF-8" environment

"q" key goes back ("Ctrl-D" instead of "q" if prompted for a string).

=head1 Options

=head2 Help

Show this Info.

=head2 Tabwidth

Set the number of spaces between columns.

=head2 Colwidth

Set the width the columns should have at least when printed.

=head2 Undef

Set the string that will be shown on the screen if a table value is undefined.

=head2 Rows

Set the maximum number of fetched table rows. Can be overwritten by setting the SQL-statement "LIMIT".

=head2 ProgressBar

Set the progress bar threshold. The threshold refers to the list size.

=head2 DB types

Choose the needed database types.

=head2 Operators

Choose the needed operators.

=head2 Separator

Choose the thousands separator displayed in menus.

=head2 Sssc mode

With the Sssc mode "compat" enabled back-arrows are offered in the SQL "sub-statement" menus.

To reset a SQL "sub-statement" (e.g WHERE) in the "simple" mode re-enter into the "sub-statement" and choose '- OK -' or use the "q" key.

=head2 Expand

Set the behavior of different menus.

=head2 Mouse mode

Set the mouse mode (see Term::Choose option "mouse").

=head2 Lock

Set the default value: Lk0 or Lk1.

    Lk0: Reset the SQL-statement after each "PrintTable".

    Lk1: Reset the SQL-statement only when a table is selected.

=head2 Metadata

If enabled system tables/schemas/databases are appended to the respective list.

=head2 Regexp case

If enabled REGEXP will match case sensitive.

With MySQL the sensitive match is achieved be enabling the BINARY operator.

=head2 "Binary filter"

Print "BNRY" instead of arbitrary binary data.

Printing arbitrary binary data could break the output.

=head2 DB defaults

Set Database defaults.

Different Database settings ...

Database defaults can be overwritten for each Database with the "Database settings".

=head2 DB login

Determine how often I<table_watch_SQLite.pl> asks for the login data:

  -for every new connection: log in data is asked for every new database connection.

  -once per database: log in data is asked only once per database.

  -only one: log in data is asked only once and then used for all connections.

=head1 AUTHOR

Matthäus Kiem <cuer2s@gmail.com>

=head1 LICENSE AND COPYRIGHT

Copyright 2012-2013 Matthäus Kiem.

This program  is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

=cut
