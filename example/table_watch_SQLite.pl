#!/usr/local/bin/perl
use warnings;
use 5.10.1;
use utf8;
binmode STDOUT, ':encoding(utf-8)';
binmode STDIN,  ':encoding(utf-8)';

#use warnings FATAL => qw(all);
#use Data::Dumper;
# Version 0.21

use File::Basename;
use File::Spec::Functions qw(catfile catdir tmpdir);
use Getopt::Long qw(GetOptions :config bundling);
use List::Util qw(sum);
use Scalar::Util qw(looks_like_number);
use Term::ReadLine;

use CHI;
use Config::Tiny;
use DBI;
use JSON;
use File::Find::Rule;
use File::HomeDir qw(my_home);
use List::MoreUtils qw(any none first_index);
use Term::Choose qw(choose);
use Term::ProgressBar;
use Term::ReadKey qw(GetTerminalSize);
use Text::LineFold;
use Unicode::GCString;

my $term = Term::ReadLine->new( 'table_watch', *STDIN, *STDOUT );
$term->ornaments( ',,,' );

use constant {
    GO_TO_TOP_LEFT  => "\e[1;1H",
    CLEAR_EOS       => "\e[0J",
    UP              => "\e[A"
};

my $home = File::HomeDir->my_home;
my $config_dir = catdir( $home, '.table_watch_conf' );
mkdir $config_dir or die $! if not -d $config_dir;

my $arg = {
    back                => 'BACK',
    _back               => '  BACK',
    confirm             => 'CONFIRM',
    _confirm            => '  CONFIRM',
    home                => $home,
    cached              => '',
    cache_rootdir       => tmpdir(),
    config_file         => catfile( $config_dir, '.table_watch.conf' ),
    temp_table_file     => catfile( $config_dir, '.table_watch_join.json' ),   
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
    
    Search and read SQLite databases.

Usage:
    table_watch_SQLite.pl [-h|--help or options -m and/or -s ] [directories to be searched]
    If no directories are passed the home directory is searched for SQLite databases.
    Options with the parenthesis at the end can be used on the command line too.

Options:
    Help          : Show this Info.
    Settings      : Show settings.
    Cache expire  : Days until data expires. The cache holds the names of the found databases.
    Reset cache   : Reset the cache.  (-s|--no-cache)
    Maxdepth      : Levels to descend at most when searching in directories for databases.  (-m|--max-depth)
    Limit         : Set the maximum number of table rows printed in one time.
    Binary filter : Print "BNRY" instead of binary data (printing binary data could break the output).
    Tab           : Set the number of spaces between columns.
    Min-Width     : Set the width the columns should have at least when printed.
    Undef         : Set the string that will be shown on the screen if a table value is undefined.
    Thousands sep : Choose the thousands separator.

    "q" key goes back.
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
    cache => {  
        expire        => [ '7d', '- Cache expire' ], 
        reset         => [ 0, '- Reset cache' ],
    },
    search => {   
        max_depth     => [ undef, '- Maxdepth' ], 
    },
    all => {    
        limit         => [ 50_000, '- Limit' ], 
        binary_filter => [ 1, '- Binary filter' ], 
        tab           => [ 2, '- Tab' ],
        min_width     => [ 30, '- Min-Width' ], 
        undef         => [ '', '- Undef' ], 
        kilo_sep      => [ ',', '- Thousands sep' ], 
    },
    
    sqlite => { 
        unicode             => [ 1, '- Unicode' ], 
        see_if_its_a_number => [ 1, '- See if its a number' ], 
        busy_timeout        => [ 3_000, '- Busy timeout (ms)' ], 
        cache_size          => [ 400_000, '- Cache size (kb)' ], 
    },
    db_all => {
        delete_join         => [ 'dummy', '- Delete JOIN statement' ],
    },
};
$arg->{option_sections} = [ qw( cache search all ) ];



if ( not eval {
    $opt = read_config_file( $arg->{config_file}, $opt );
    my $help;
    GetOptions (
        'h|help'        => \$help,
        's|no-cache'    => \$opt->{cache}{reset}[v],
        'm|max-depth:i' => \$opt->{search}{max_depth}[v],
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
    my ( $opt, $database ) = @_;
    die "\"$database\": $!. Maybe the cached data is not up to date." if not -f $database;
    my $dbh = DBI->connect( "DBI:SQLite:dbname=$database", '', '', {
        RaiseError                  => 1,
        PrintError                  => 0,
        AutoCommit                  => 1,
        sqlite_unicode              => $opt->{$database}{unicode}[v]             // $opt->{sqlite}{unicode}[v],
        sqlite_see_if_its_a_number  => $opt->{$database}{see_if_its_a_number}[v] // $opt->{sqlite}{see_if_its_a_number}[v],
    } ) or die DBI->errstr;
    $dbh->sqlite_busy_timeout( $opt->{$database}{busy_timeout}[v] // $opt->{sqlite}{busy_timeout}[v] );
    $dbh->do( 'PRAGMA cache_size = ' . ( $opt->{$database}{cache_size}[v] // $opt->{sqlite}{cache_size}[v] ) );
    return $dbh;
}

sub available_databases {
    my ( $arg, $opt ) = @_;
    my @dirs = @ARGV ? @ARGV : ( $arg->{home} );
    $arg->{cache_key} = join ' ', @dirs, '|', $opt->{search}{max_depth}[v] // '';
    $arg->{cached} = ' (cached)';
    $arg->{cache} = CHI->new (
        namespace        => 'table_watch_SQLite',
        driver           => 'File',
        root_dir         => $arg->{cache_rootdir},
        expires_in       => $opt->{cache}{expire}[v],
        expires_variance => 0.25,
    );
    $arg->{cache}->remove( $arg->{cache_key} ) if $opt->{cache}{reset}[v];

    my @databases = $arg->{cache}->compute(
        $arg->{cache_key},
        $opt->{cache}{expire}[v],
        sub {
            $arg->{cached} = '';
            say 'searching...';
            my $rule = File::Find::Rule->new();
            $rule->file();
            $rule->maxdepth( $opt->{search}{max_depth}[v] ) if defined $opt->{search}{max_depth}[v];
            $rule->exec( sub{ # return $File_LibMagic->describe_filename( $_ ) =~ /\ASQLite/ } );
                open my $fh, '<', $_ or die $!;
                return ( readline $fh // '' ) =~ /\ASQLite\sformat/;
            } );   
            my @databases = $rule->in( @dirs );
            say 'ended searching';
            return @databases;
        }
    );
    return \@databases;
}

sub get_table_names {
    my ( $dbh, $database ) = @_;
    my $tables = $dbh->selectcol_arrayref( "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name" );
    return $tables;
}


#----------------------------------------------------------------------------------------------------#
#-- 333 --------------------------------------   main   ---------------------------------------------#
#----------------------------------------------------------------------------------------------------#


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
        $dbh = get_database_handle( $opt, $database );
        $arg->{db_type} = lc $dbh->{Driver}{Name};
        $tables = get_table_names( $dbh, $database );
        1 }
    ) {
        say 'Get database handle and table names:';
        print $@;
        choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
        # remove database from @databases
        next DATABASES;
    }
    my @all_tables = ();
    my $temp_tmp_tbl = {};
    my $save_tmp_tbl = read_json( $arg->{temp_table_file} );
    my $temp_tables = [ keys %{$save_tmp_tbl->{$database}} ];
    my $join_tables = '  Join tables';
    my $db_setting = '  Database settings';
    my @append;
    push @append, '  sqlite_master' if $arg->{db_type} eq 'sqlite';   # '  sqlite_temp_master'
    push @append, $join_tables, $db_setting;
    push @all_tables, map { "- $_" } @$tables, sort @$temp_tables; 
    push @all_tables, @append;

    TABLES: while ( 1 ) {
        # Choose
        my $table = choose( [ undef, @all_tables ], { prompt => 'db: "'. basename( $database ) . '"', %lyt, undef => $arg->{_back} } );
        last TABLES if not defined $table;
        my $join_statement = '';
        my $columns = [];
        my $join = 0;
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
                ( $join_statement, $columns ) = join_tables( $arg, $opt, $dbh, $tables, $temp_tables );
                $join = 1;
                $table = 'joined_tables';
                
                1 }
            ) {
                say 'Join tables:';
                print $@;
                choose( [ 'Press ENTER to continue' ], { prompt => 0 } );
            }
            next TABLES if not defined $join_statement;
        }
        else {
            $table =~ s/\A..//;
        }    
        if ( not eval {  
            if ( $table =~ /\AJ_.*_temp\z/ ) {
                if ( exists $temp_tmp_tbl->{$table}{join_stmt} and $temp_tmp_tbl->{$table}{join_stmt} ) {
                    $join_statement = $temp_tmp_tbl->{$table}{join_stmt};
                    $columns        = $temp_tmp_tbl->{$table}{columns};
                }
                elsif ( exists $save_tmp_tbl->{$database}{$table}{join_stmt} and  $save_tmp_tbl->{$database}{$table}{join_stmt} ) {
                    $join_statement = $save_tmp_tbl->{$database}{$table}{join_stmt};
                    $columns        = $save_tmp_tbl->{$database}{$table}{columns};
                }
                else {
                    die "No JOIN statement!";
                }
            }

            CUSTOMIZE: while ( 1 ) {
                my ( $total_ref, $col_names, $col_types ) = read_table( $arg, $opt, $dbh, $table, $join_statement, $columns );
                last CUSTOMIZE if not defined $total_ref;
                print_loop( $arg, $opt, $dbh, $total_ref, $col_names, $col_types );
            }
            if ( $join_statement ) {
                if ( $join ) {
                    $join = 0;
                    my $temp_name;
                    print GO_TO_TOP_LEFT;
                    print CLEAR_EOS;

                    TEMP_NAME: while ( 1 ) {
                        # Read
                        $temp_name = $term->readline( 'Joined Table Name: ' );
                        chomp $temp_name;
                        my $allowed_regex = qr/\A\p{Word}+\z/;
                        if ( $temp_name eq '' ) {
                            last TEMP_NAME;
                        }
                        elsif ( $temp_name !~ $allowed_regex ) {
                            say "Allowed: $allowed_regex";
                            print "Try again: ";
                        }
                        else {
                            $temp_name = 'J_' . $temp_name . '_temp';
                            if ( any { $_ eq "- $temp_name" } @all_tables ) {
                                say "A table with this name exits allready";
                                print "Try another name: ";
                            }
                            else {
                                push @$temp_tables, $temp_name;
                                $temp_tmp_tbl->{$temp_name}{join_stmt} = $join_statement;
                                $temp_tmp_tbl->{$temp_name}{columns}   = $columns;
                                my ( $no, $save ) = ( ' NO ', ' YES ' );
                                my $choice = choose( [ $no, $save ], { prompt => 'Save JOIN statement?', layout => 1,  pad_one_row => 1 } );
                                if ( $choice eq $save ) {
                                    $save_tmp_tbl->{$database}{$temp_name}{join_stmt} = $join_statement;
                                    $save_tmp_tbl->{$database}{$temp_name}{columns}   = $columns;
                                    write_json( $arg->{temp_table_file}, $save_tmp_tbl );
                                }
                                last TEMP_NAME;
                            }
                        }
                    }
                    @all_tables = ();
                    push @all_tables, map { "- $_" } @$tables, sort @$temp_tables;
                    push @all_tables, @append;
                }
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
#-- 444 ---------------------------------   join routines   -----------------------------------------#
#----------------------------------------------------------------------------------------------------#


sub join_tables_info {
    my ( $arg, $opt, $dbh, $tables ) = @_;
    my $table_overview = {};
    for my $table ( @$tables ) {
        my $table_q = $dbh->quote_identifier( $table );
        my $sth = $dbh->prepare( "SELECT * FROM $table_q" );
        $sth->execute();
        $table_overview->{$table} = $sth->{NAME};
    }
    my %print_hash;
    for my $table ( @$tables ) {
        push @{$print_hash{$table}}, [ 'TABLE: ', '<' . $table . '>' ];        
        push @{$print_hash{$table}}, [ 'COLUMNS: ', join( ' | ', @{$table_overview->{$table}} ) ];
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
        else {
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
    return $table_overview, $print_info_array_ref;
}


sub join_tables {
    my ( $arg, $opt, $dbh, $tables, $temp_tables ) = @_;
    my ( $table_overview, $print_info_array_ref ) = join_tables_info( $arg, $opt, $dbh, $tables );
    my @table_names = ( @$tables, @$temp_tables );
    my $reset = '  RESET';
    my $info  = '  INFO';
    my $join_statement_quote = "SELECT * FROM";
    my $join_statement_print = "SELECT * FROM";    
    my @tables = map { "° $_" } @$tables;
    my $mastertable;
    INFO_M: while ( 1 ) {
        join_tables_print_info( $print_info_array_ref, $join_statement_print );
        # Choose
        $mastertable = choose( [ undef, $info, @tables ], { prompt => 'Choose MASTER table:', layout => 3, undef => $arg->{_back} } );
        return if not defined $mastertable;
        if ( $mastertable eq $info ) {
            choose( $print_info_array_ref, { prompt => 0, layout => 3, clear_screen => 1 } );
            next INFO_M;
        }
        last INFO_M;
    }
    my $idx = first_index { $_ eq $mastertable } @tables;
    splice( @tables, $idx, 1 );
    $mastertable =~ s/\A..//;
    my @used_tables = ( $mastertable );
    my @av_slave_tables = map { s/\A°\s/+ /; $_ } @tables;
    die "No slave tables" if not @av_slave_tables;
    my $mastertable_q = $dbh->quote_identifier( $mastertable ); 
    $join_statement_quote = "SELECT * FROM " . $mastertable_q;
    $join_statement_print = "SELECT * FROM " . $mastertable;    
    my ( @primary_keys, @foreign_keys, @old_pks, @old_fks, @old_used, @old_slave );
    my $old_stmt_qot = '';
    my $old_stmt_prt = '';
    SLAVE_TABLES: while ( 1 ) {   
        last SLAVE_TABLES if not @av_slave_tables;
        my $confirm = '  Enough SLAVES';
        my $slave_table;
        @old_pks      = @primary_keys;
        @old_fks      = @foreign_keys;
        $old_stmt_qot = $join_statement_quote;
        $old_stmt_prt = $join_statement_print;
        @old_used     = @used_tables;
        @old_slave    = @av_slave_tables;
        INFO_S: while ( 1 ) {
            join_tables_print_info( $print_info_array_ref, $join_statement_print );
            # Choose
            $slave_table = choose( [ undef, $info, @av_slave_tables, $confirm ], { prompt => 'Add a SLAVE table:', layout => 3, undef => $reset } );
            if ( not defined $slave_table ) {
                return if @used_tables == 1;
                @used_tables = ( $mastertable );
                @av_slave_tables = map { s/\A°\s/+ /; $_ } @tables;
                $join_statement_quote = "SELECT * FROM " . $mastertable_q;
                $join_statement_print = "SELECT * FROM " . $mastertable; 
                @primary_keys = ();
                @foreign_keys = ();
                next SLAVE_TABLES;
            }
            last SLAVE_TABLES if $slave_table eq $confirm;
            if ( $slave_table eq $info ) {
                choose( $print_info_array_ref, { prompt => 0, layout => 3, clear_screen => 1 } );
                next INFO_S;
            }
            last INFO_S;
        }
        my $idx = first_index { $_ eq $slave_table } @av_slave_tables;
        splice( @av_slave_tables, $idx, 1 );   
        $slave_table =~ s/\A..//;
        my $slave_table_q = $dbh->quote_identifier( $slave_table );
        $join_statement_quote .= " LEFT OUTER JOIN " . $slave_table_q . " ON"; 
        $join_statement_print .= " LEFT OUTER JOIN " . $slave_table   . " ON";        
        my %primary_key_columns = ();
        for my $used_table ( @used_tables ) {
            for my $col ( @{$table_overview->{$used_table}} ) {
                $primary_key_columns{"$used_table.$col"} = $dbh->quote_identifier( undef, $used_table, $col );
            }
        }        
        my %foreign_key_columns = ();
        for my $col ( @{$table_overview->{$slave_table}} ) {
            $foreign_key_columns{"$slave_table.$col"} = $dbh->quote_identifier( undef, $slave_table, $col );
        }
        my $AND = '';
        my $count = 0;
        
        ON: while ( 1 ) {
            join_tables_print_info( $print_info_array_ref, $join_statement_print );
            # Choose
            my $continue  = '  CONTINUE';
            my $more_cols = '  More columns';
            my $choices;
            if ( $count == 0 ) {
                $choices = [ undef, map{ "- $_" } sort keys %primary_key_columns ];
                $count = 1;
            }
            elsif ( $count == 1 ) {
                $choices = [ undef, $more_cols, $continue ];
                $count = 0;
            }            
            my $pkc_choise = choose( $choices, { prompt => 'Choose PRIMARY KEY column:', layout => 3, undef => $reset } );
            if ( not defined $pkc_choise ) {
                @primary_keys         = @old_pks;
                @foreign_keys         = @old_fks;
                $join_statement_quote = $old_stmt_qot;
                $join_statement_print = $old_stmt_prt;                
                @used_tables          = @old_used;
                @av_slave_tables      = @old_slave;
                next SLAVE_TABLES;
            }
            if ( $pkc_choise eq $more_cols ) {
                next ON;
            }
            if ( $pkc_choise eq $continue ) {
                last ON;
            }
            $pkc_choise =~ s/\A..//;
            push @primary_keys, $primary_key_columns{$pkc_choise};
            $join_statement_quote .= $AND;
            $join_statement_print .= $AND;
            $join_statement_quote .= ' ' . $primary_key_columns{$pkc_choise} . " ="; 
            $join_statement_print .= ' ' . $pkc_choise                       . " =";                
            join_tables_print_info( $print_info_array_ref, $join_statement_print );
            # Choose
            my $fkc_choice = choose( [ undef, map{ "- $_" } sort keys %foreign_key_columns ], { prompt => 'Choose FOREIGN KEY column:', layout => 3, undef => $reset } );
            if ( not defined $fkc_choice ) {
                @primary_keys         = @old_pks;
                @foreign_keys         = @old_fks;
                $join_statement_quote = $old_stmt_qot;
                $join_statement_print = $old_stmt_prt;                
                @used_tables          = @old_used;
                @av_slave_tables      = @old_slave;                
                next SLAVE_TABLES;
            }
            $fkc_choice =~ s/\A..//;
            push @foreign_keys, $foreign_key_columns{$fkc_choice};
            $join_statement_quote .= ' ' . $foreign_key_columns{$fkc_choice};   
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
    for my $table ( keys %{$table_overview} ) {
        next if none { $_ eq $table } @used_tables;
        for my $col ( @{$table_overview->{$table}} ) {
            $seen{$col}++;
            push @dup, $col if $seen{$col} == 2;
        }
    }
    my @col_stmts;
    my $columns;
    for my $table ( @$tables ) {
        next if none { $_ eq $table } @used_tables;
        for my $col ( @{$table_overview->{$table}} ) {
            my $q_table_col = $dbh->quote_identifier( undef, $table, $col );
            next if any { $_ eq $q_table_col } @foreign_keys;
            my ( $alias, $col_stmt );
            if ( any { $_ eq $col } @dup ) {
                if ( any { $_ eq $q_table_col } @primary_keys ) {
                    $alias = $col . '_PK';
                }
                else {
                    $alias = $col . '_' . substr $table, 0, $length_uniq;
                }
                $col_stmt = $q_table_col . " AS " . $alias;
            }
            else {
                $alias = $col;
                $col_stmt = $q_table_col;
            }
            push @col_stmts, $col_stmt;
            push @$columns , [ $alias, $col_stmt ];
        }
    }
    my $col_statement = join ', ', @col_stmts;
    $join_statement_quote =~ s/\s\*\s/ $col_statement /; 
    return $join_statement_quote, $columns;  
}


sub join_tables_print_info {
    my ( $print_info_array_ref, $join_statement_print ) = @_;
    print GO_TO_TOP_LEFT;
    print CLEAR_EOS;
    if ( $join_statement_print ) {
        my @array = split /(?=\sLEFT\sOUTER\sJOIN)/, $join_statement_print;
        say shift @array;
        for my $join ( @array ) {
            say "  $join";
        }
        say "";
    }
}


#----------------------------------------------------------------------------------------------------#
#-- 555 -------------------------------   statement routine   ---------------------------------------#
#----------------------------------------------------------------------------------------------------#


sub print_select {
    my ( $arg, $opt, $table, $chosen_cols_print, $print ) = @_;
    my $cols_str = '';
    $cols_str = ' '. join( ', ', @$chosen_cols_print ) if @$chosen_cols_print;
    $cols_str = $print->{group_by_cols}                if not @$chosen_cols_print and $print->{group_by_cols};
    if ( $print->{aggregate_stmt} ) {
        $cols_str .= ',' if $cols_str;
        $cols_str .= $print->{aggregate_stmt};
    }
    $cols_str = ' *' if not $cols_str;
    print GO_TO_TOP_LEFT;
    print CLEAR_EOS;
    print "SELECT";
    print $print->{distinct_stmt} if $print->{distinct_stmt};
    say $cols_str;
    say " FROM $table";
    say $print->{where_stmt}    if $print->{where_stmt};
    say $print->{group_by_stmt} if $print->{group_by_stmt};
    say $print->{having_stmt}   if $print->{having_stmt};
    say $print->{order_by_stmt} if $print->{order_by_stmt};
    say $print->{limit_stmt} if $print->{limit_stmt};    
    say "";
}


sub read_table {
    my ( $arg, $opt, $dbh, $table, $join_statement, $columns ) = @_;
    $dbh->func( 'regexp', 2, sub { my ( $regex, $string ) = @_; $string //= ''; return $string =~ m/$regex/ism }, 'create_function' ) if $arg->{db_type} eq 'sqlite';
    my $continue = '- OK -';
    my $back = '- reset -';
    my %pad_one = ( layout => 1, pad_one_row => 1, undef => $back );
    my %pad_tow = ( layout => 1, pad_one_row => 2, undef => $back );
    my $table_q = $dbh->quote_identifier( $table );
    my @keys = ( qw( print_table columns aggregate distinct where group_by having order_by limit ) );
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
    );
    my $FROM            = '';
    my $col_default_str = '';
    my $col_stmts       = {};
    my $col_names       = [];
    if ( $join_statement ) {
        if ( $join_statement =~ /\ASELECT\s(.*)(\sFROM\s.*)\z/ ) {
            $col_default_str = $1;
            $FROM            = $2;
            for my $ref ( @$columns ) {
                $col_stmts->{$ref->[0]} = $ref->[1];
                push @$col_names, $ref->[0];
            }
        } else { die $join_statement; }
    }
    else {
        $col_default_str = ' *';
        $FROM = " FROM $table_q";
        my $sth = $dbh->prepare( "SELECT *" . $FROM );
        $sth->execute();
        for my $col ( @{$sth->{NAME}} ) {
            $col_stmts->{$col} = $dbh->quote_identifier( $col );
            push @$col_names, $col;
        }
    } 
    my $print             = {};
    my $quote             = {};
    my $chosen_cols_quote = [];
    my $chosen_cols_print = [];
    my @aliases           = ();
    my $before_col        = ' ';
    my $between_col       = ', ';
    my ( $DISTINCT, $ALL, $ASC, $DESC, $AND, $OR ) = ( " DISTINCT ", " ALL ", " ASC ", " DESC ", " AND ", " OR " );
    my @stmt_keys = ( qw( distinct_stmt group_by_cols aggregate_stmt where_stmt group_by_stmt having_stmt order_by_stmt limit_stmt ) );
    @$print{@stmt_keys} = ( '' ) x @stmt_keys;
    @$quote{@stmt_keys} = ( '' ) x @stmt_keys;
    $quote->{where_args}  = [];
    $quote->{having_args} = [];
    $quote->{limit_args}  = [];

    CUSTOMIZE: while ( 1 ) {
        print_select( $arg, $opt, $table, $chosen_cols_print, $print );
        # Choose
        my $custom = choose( [ undef, @customize{@keys} ], { prompt => 'Customize:', layout => 3, undef => $arg->{back} } );
        for ( $custom ) {
            when ( not defined ) {
                last CUSTOMIZE;
            }
            when( $customize{columns} ) {
                my @cols = @$col_names;
                $chosen_cols_quote = [];
                $chosen_cols_print = [];
                
                COLUMNS: while ( @cols ) {
                    print_select( $arg, $opt, $table, $chosen_cols_print, $print );
                    # Choose
                    my $col = choose( [ $continue, @cols ], { prompt => 'Choose: ', %pad_tow } );
                    if ( not defined $col ) {
                        $chosen_cols_quote = [];
                        $chosen_cols_print = [];
                        last COLUMNS;
                    }
                    if ( $col eq $continue ) {
                        last COLUMNS;
                    }
                    push @$chosen_cols_quote, $col_stmts->{$col};
                    push @$chosen_cols_print, $col;
                }
            }
            when( $customize{distinct} ) {
                $quote->{distinct_stmt} = '';
                $print->{distinct_stmt} = '';
                
                DISTINCT: while ( 1 ) {
                    print_select( $arg, $opt, $table, $chosen_cols_print, $print );
                    # Choose
                    my $select_distinct = choose( [ $continue, $DISTINCT, $ALL ], { prompt => 'Choose: ', %pad_one } );
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
                my @cols = @$col_names;
                my $AND_OR = '';
                $quote->{where_args} = [];
                $quote->{where_stmt} = " WHERE";
                $print->{where_stmt} = " WHERE";
                my $count = 0;
                
                WHERE: while ( 1 ) {
                    print_select( $arg, $opt, $table, $chosen_cols_print, $print );
                    # Choose
                    my $col = choose( [ $continue, @cols ], { prompt => 'Choose: ', %pad_tow } );
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
                        print_select( $arg, $opt, $table, $chosen_cols_print, $print );
                        # Choose
                        $AND_OR = choose( [ $AND, $OR ], { prompt => 'Choose:', %pad_one } );
                        if ( not defined $AND_OR ) {
                            $quote->{where_args} = [];
                            $quote->{where_stmt} = '';
                            $print->{where_stmt} = '';
                            last WHERE;
                        }
                        $AND_OR =~ s/\A\s+|\s+\z//g;
                        $AND_OR = ' ' . $AND_OR;
                    }
                    ( my $col_stmt = $col_stmts->{$col} ) =~ s/\sAS\s\S+\z//;               
                    $quote->{where_stmt} .= $AND_OR . ' ' . $col_stmt;                    
                    $print->{where_stmt} .= $AND_OR . ' ' . $col;
                    print_select( $arg, $opt, $table, $chosen_cols_print, $print );
                    # Choose
                    my $filter_type = choose( [ @{$arg->{filter_types}} ], { prompt => 'Choose:', %pad_one } );
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
                            print_select( $arg, $opt, $table, $chosen_cols_print, $print );
                            # Choose
                            my $col = choose( [ $continue, @cols ], { prompt => 'Choose: ', %pad_tow } );
                            if ( not defined $col ) {
                                $quote->{where_args} = [];
                                $quote->{where_stmt} = '';
                                $print->{where_stmt} = '';
                                last WHERE;
                            }
                            if ( $col eq $continue ) {
                                if ( $arg->{col_sep} eq $before_col ) {
                                    $quote->{where_args} = [];
                                    $quote->{where_stmt} = '';
                                    $print->{where_stmt} = '';
                                    last WHERE;
                                }
                                $quote->{where_stmt} .= ' )';
                                $print->{where_stmt} .= ' )';
                                last IN;
                            }
                            ( my $col_stmt = $col_stmts->{$col} ) =~ s/\sAS\s\S+\z//; 
                            $quote->{where_stmt} .= $arg->{col_sep} . $col_stmt;                            
                            $print->{where_stmt} .= $arg->{col_sep} . $col;
                            $arg->{col_sep} = $between_col;
                        }
                    }
                    else {
                        print_select( $arg, $opt, $table, $chosen_cols_print, $print ); 
                        # Read
                        my $pattern = $term->readline( $filter_type =~ /REGEXP\z/ ? 'Regexp: ' : 'Arg: ' );
#                        if ( not defined $pattern or ( $pattern eq "\n" and $filter_type =~ /REGEXP\z/ ) ) {
                        if ( not defined $pattern ) {
                            $quote->{where_args} = [];
                            $quote->{where_stmt} = '';
                            $print->{where_stmt} = '';
                            last WHERE;
                        }
                        $pattern= '^$' if not length $pattern and $filter_type =~ /REGEXP\z/;
                        $pattern= qr/$pattern/i if $arg->{db_type} eq 'sqlite' and $filter_type =~ /REGEXP\z/;
                        $quote->{where_stmt} .= ' ' . '?';
                        $print->{where_stmt} .= ' ' . $dbh->quote( $pattern );
                        push @{$quote->{where_args}}, $pattern;
                    }
                    $count++;
                }
            }
            when( $customize{aggregate} ) {
                my @cols = @$col_names;
                $arg->{col_sep} = $before_col;
                @aliases                 = ();
                $quote->{aggregate_stmt} = '';
                $print->{aggregate_stmt} = '';
                
                AGGREGATE: while ( 1 ) {
                    print_select( $arg, $opt, $table, $chosen_cols_print, $print );
                    # Choose
                    my $func = choose( [ $continue, @{$arg->{aggregate_functions}} ], { prompt => 'Choose:', %pad_tow } );
                    if ( not defined $func ) {
                        @aliases                 = ();
                        $quote->{aggregate_stmt} = '';
                        $print->{aggregate_stmt} = '';
                        last AGGREGATE;
                    }
                    if ( $func eq $continue ) {
                        last AGGREGATE;
                    }
                    my ( $col, $col_stmt );
                    if ( $func =~ /\Acount\s*\(\s*\*\s*\)\z/i ) {
                        $col      = '*';
                        $col_stmt = '*';
                    }
                    $func =~ s/\s*\(\s*\S\s*\)\z//;
                    $quote->{aggregate_stmt} .= $arg->{col_sep} . $func . '(';
                    $print->{aggregate_stmt} .= $arg->{col_sep} . $func . '(';
                    if ( not defined $col ) {
                        print_select( $arg, $opt, $table, $chosen_cols_print, $print );
                        # Choose
                        $col = choose( [ @cols ], { prompt => 'Choose:', %pad_tow } );
                        if ( not defined $col ) {
                            @aliases                 = ();
                            $quote->{aggregate_stmt} = '';
                            $print->{aggregate_stmt} = '';
                            last AGGREGATE;
                        }
                        ( $col_stmt = $col_stmts->{$col} ) =~ s/\sAS\s\S+\z//;
                    }
                    my $alias = '@' . $func . '_' . $col; # ( $col eq '*' ? 'ROWS' : $col );
                    $quote->{aggregate_stmt} .= $col_stmt . ') AS ' . $dbh->quote_identifier( $alias );                
                    $print->{aggregate_stmt} .= $col      . ') AS ' .                         $alias  ;
                    push @aliases, $alias;
                    $col_stmts->{$alias} = $func . '(' . $col_stmt . ')';
                    $arg->{col_sep} = $between_col;
                }
            }
            when( $customize{group_by} ) {
                my @cols = @$col_names;
                $arg->{col_sep} = $before_col;
                $quote->{group_by_stmt} = " GROUP BY";
                $print->{group_by_stmt} = " GROUP BY";
                
                GROUP_BY: while ( 1 ) {
                    print_select( $arg, $opt, $table, $chosen_cols_print, $print );
                    # Choose
                    my $col = choose( [ $continue, @cols ], { prompt => 'Choose: ', %pad_tow } );
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
                    ( my $col_stmt = $col_stmts->{$col} ) =~ s/\sAS\s\S+\z//;
                    if ( not @$chosen_cols_quote ) {
                        $quote->{group_by_cols} .= $arg->{col_sep} . $col_stmt;
                        $print->{group_by_cols} .= $arg->{col_sep} . $col;
                    }
                    $quote->{group_by_stmt} .= $arg->{col_sep} . $col_stmt;                    
                    $print->{group_by_stmt} .= $arg->{col_sep} . $col;
                    $arg->{col_sep} = $between_col;
                }
            }
            when( $customize{having} ) {
                my @cols = @$col_names;
                my $AND_OR = '';
                $quote->{having_args} = [];
                $quote->{having_stmt} = " HAVING";
                $print->{having_stmt} = " HAVING";
                my $count = 0;
                
                HAVING: while ( 1 ) {
                    print_select( $arg, $opt, $table, $chosen_cols_print, $print );
                    # Choose
                    my $func = choose( [ $continue, @{$arg->{aggregate_functions}}, @aliases ], { prompt => 'Choose:', %pad_tow } );
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
                        print_select( $arg, $opt, $table, $chosen_cols_print, $print );
                        # Choose
                        $AND_OR = choose( [ $AND, $OR ], { prompt => 'Choose:', %pad_one } );
                        last HAVING if not defined $AND_OR;
                        $AND_OR =~ s/\A\s+|\s+\z//g;
                        $AND_OR = ' ' . $AND_OR;
                    }
                    if ( any { $_ eq $func } @aliases ) {
                        $quote->{having_stmt} .= $AND_OR . ' ' . $col_stmts->{$func};
                        $print->{having_stmt} .= $AND_OR . ' ' . $func; 
                    }
                    else {
                        my ( $col, $col_stmt );
                        if ( $func =~ /\Acount\s*\(\s*\*\s*\)\z/i ) {
                            $col      = '*';
                            $col_stmt = '*';
                        }
                        $func =~ s/\s*\(\s*\S\s*\)\z//;
                        $quote->{having_stmt} .= $AND_OR . ' ' . $func . '(';
                        $print->{having_stmt} .= $AND_OR . ' ' . $func . '(';
                        if ( not defined $col ) {
                            print_select( $arg, $opt, $table, $chosen_cols_print, $print );
                            # Choose
                            $col = choose( [ @cols ], { prompt => 'Choose:', %pad_tow } );
                            if ( not defined $col ) {
                                $quote->{having_args} = [];
                                $quote->{having_stmt} = '';
                                $print->{having_stmt} = '';
                                last HAVING;
                            }
                            ( $col_stmt = $col_stmts->{$col} ) =~ s/\sAS\s\S+\z//;
                        }
                        $quote->{having_stmt} .= $col_stmt . ')';
                        $print->{having_stmt} .= $col      . ')';
                    }
                    print_select( $arg, $opt, $table, $chosen_cols_print, $print );
                    # Choose
                    my $filter_type = choose( [ @{$arg->{filter_types}} ], { prompt => 'Choose:', %pad_one } );
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
                            print_select( $arg, $opt, $table, $chosen_cols_print, $print );
                            # Choose
                            my $col = choose( [ $continue, @cols ], { prompt => 'Choose: ', %pad_tow } );
                            if ( not defined $col ) {
                                $quote->{having_args} = [];
                                $quote->{having_stmt} = '';
                                $print->{having_stmt} = '';
                                last HAVING;
                            }
                            if ( $col eq $continue ) {
                                if ( $arg->{col_sep} eq $before_col ) {
                                    $quote->{having_args} = [];
                                    $quote->{having_stmt} = '';
                                    $print->{having_stmt} = '';
                                    last HAVING;
                                }
                                $quote->{having_stmt} .= ' )';
                                $print->{having_stmt} .= ' )';
                                last IN;
                            }
                            ( my $col_stmt = $col_stmts->{$col} ) =~ s/\sAS\s\S+\z//;
                            $quote->{having_stmt} .= $arg->{col_sep} . $col_stmt;
                            $print->{having_stmt} .= $arg->{col_sep} . $col;
                            $arg->{col_sep} = $between_col;
                        }
                    }
                    else {
                        print_select( $arg, $opt, $table, $chosen_cols_print, $print );
                        # Read
                        my $pattern = $term->readline( $filter_type =~ /REGEXP\z/ ? 'Regexp: ' : 'Arg: ' );
                        #if ( not defined $pattern or ( $pattern eq "\n" and $filter_type =~ /REGEXP\z/ ) ) {
                        if ( not defined $pattern ) {                  
                            $quote->{having_args} = [];
                            $quote->{having_stmt} = '';
                            $print->{having_stmt} = '';
                            last HAVING;
                        }
                        $pattern= '^$' if not length $pattern and $filter_type =~ /REGEXP\z/;
                        $pattern= qr/$pattern/i if $arg->{db_type} eq 'sqlite' and $filter_type =~ /REGEXP\z/;
                        $quote->{having_stmt} .= ' ' . '?';
                        $print->{having_stmt} .= ' ' . $dbh->quote( $pattern );
                        push @{$quote->{having_args}}, $pattern;
                    }
                    $count++;
                }
            }
            when( $customize{order_by} ) {
                my @cols = ( @$col_names, @aliases );
                $arg->{col_sep} = $before_col;
                $quote->{order_by_stmt} = " ORDER BY";
                $print->{order_by_stmt} = " ORDER BY";
                
                ORDER_BY: while ( 1 ) {
                    print_select( $arg, $opt, $table, $chosen_cols_print, $print );
                    # Choose
                    my $col = choose( [ $continue, @cols ], { prompt => 'Choose: ', %pad_tow } );
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
                    ( my $col_stmt = $col_stmts->{$col} ) =~ s/\sAS\s\S+\z//;
                    $quote->{order_by_stmt} .= $arg->{col_sep} . $col_stmt;
                    $print->{order_by_stmt} .= $arg->{col_sep} . $col;
                    print_select( $arg, $opt, $table, $chosen_cols_print, $print );
                    # Choose
                    my $direction = choose( [ $ASC, $DESC ], { prompt => 'Choose:', %pad_one } );
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
            when( $customize{limit} ) {
                $quote->{limit_stmt} = " LIMIT";
                $print->{limit_stmt} = " LIMIT";
                my $rows = $dbh->selectrow_array( "SELECT COUNT(*)" . $FROM, {} );
                my $digits = length $rows;
                my ( $only_limit, $offset_and_limit ) = ( '        LIMIT', 'OFFSET, LIMIT' );
                LIMIT: {
                    print_select( $arg, $opt, $table, $chosen_cols_print, $print );
                    # Choose
                    my $choice = choose( [ $only_limit, $offset_and_limit ], { prompt => 'Choose: ', layout => 3 } );
                    if ( $choice eq $offset_and_limit ) {
                        print_select( $arg, $opt, $table, $chosen_cols_print, $print );
                        # Choose
                        my $offset = choose_a_number( $arg, $opt, $digits, 'Compose OFFSET:' );
                        if ( not defined $offset ) {
                            $quote->{limit_stmt} = '';
                            $print->{limit_stmt} = '';
                            last LIMIT;
                        }
                        push @{$quote->{limit_args}}, $offset;
                        $quote->{limit_stmt} .= ' ' . '?'     . ', ';
                        $print->{limit_stmt} .= ' ' . $offset . ', ';
                    }
                    print_select( $arg, $opt, $table, $chosen_cols_print, $print );
                    # Choose
                    my $limit = choose_a_number( $arg, $opt, $digits, 'Compose LIMIT:' ); 
                    if ( not defined $limit ) {
                        $quote->{limit_args} = [];
                        $quote->{limit_stmt} = '';
                        $print->{limit_stmt} = '';
                        last LIMIT;
                    }
                    push @{$quote->{limit_args}}, $limit;
                    $quote->{limit_stmt} .= ' ' . '?';
                    $print->{limit_stmt} .= ' ' . $limit;
                }
            }
            when( $customize{print_table} ) {
                my $cols_str = '';
                $cols_str = ' ' . join( ', ', @$chosen_cols_quote ) if @$chosen_cols_quote;
                $cols_str = $print->{group_by_cols} if not @$chosen_cols_quote and $print->{group_by_cols};
                if ( $quote->{aggregate_stmt} ) {
                    $cols_str .= ',' if $cols_str;
                    $cols_str .= $quote->{aggregate_stmt};
                }
                $cols_str = $col_default_str if not $cols_str;
                my $select .= "SELECT" . $quote->{distinct_stmt} . $cols_str . $FROM;
                $select .= $quote->{where_stmt};
                $select .= $quote->{group_by_stmt};
                $select .= $quote->{having_stmt};
                $select .= $quote->{order_by_stmt};
                $select .= $quote->{limit_stmt};
                my @arguments = ( @{$quote->{where_args}}, @{$quote->{having_args}}, @{$quote->{limit_args}} );
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


#----------------------------------------------------------------------------------------------------#
#-- 666 -----------------------------   prepare print routines   ------------------------------------#
#----------------------------------------------------------------------------------------------------#


sub print_loop {
    my ( $arg, $opt, $dbh, $total_ref, $col_names, $col_types ) = @_;
    my $offset = 0;
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

    PRINT: while ( 1 ) {
        if ( @choices ) {
            # Choose
            my $choice = choose( [ undef, @choices ], { layout => 3, undef => $arg->{_back} } );
            last PRINT if not defined $choice;
            $offset = ( split /\s*-\s*/, $choice )[0];
            $offset =~ s/\A\s+//;
            print_table( $arg, $opt, [ @{$total_ref}[ $offset .. $offset + $opt->{all}{limit}[v] - 1 ] ], $col_names, $col_types );
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
            # next if not defined $row->[$i];
            $row->[$i] = $opt->{all}{undef}[v] if not defined $row->[$i]; 
            if ( $count == 1 ) { # column name
                $row->[$i] =~ s/\p{Space}/ /g;
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
#-- 777 ----------------------------------   print routine   ----------------------------------------#
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
            #my $word = $row->[$i] // $opt->{all}{undef}[v];
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
#-- 888 --------------------------------    option routines     -------------------------------------#
#----------------------------------------------------------------------------------------------------#

sub options {
    my ( $arg, $opt ) = @_;
    my @choices = ();
    for my $section ( @{$arg->{option_sections}} ) {
        for my $key ( sort keys %{$opt->{$section}} ) {
            if ( not defined $opt->{$section}{$key}[chs] ) {
                # invalid key in config_file
            }
            else {
                push @choices, $opt->{$section}{$key}[chs];
            }
        }                
    }
    my $change = 0;
    OPTIONS: while ( 1 ) {
        my ( $exit, $help, $show_settings, $continue ) = ( '  EXIT', '  HELP', '  SHOW SETTINGS', '  CONTINUE' );
        # Choose
        my $option = choose( [ $exit, $help, @choices, $show_settings, undef ], { undef => $continue, layout => 3, clear_screen => 1 } );
        my $back = '<<';
        my %number_lyt = ( layout => 1, vertical => 0, right_justify => 1, undef => $back );
        my %bol = ( undef => " $back ", pad_one_row => 1 );
        my ( $true, $false ) = ( ' YES ', ' NO ' );
        given ( $option ) {
            when ( not defined ) { last OPTIONS; }
            when ( $exit ) { exit() }
            when ( $help ) { help(); choose( [ '  Close with ENTER  ' ], { prompt => 0 } ) }
            when ( $show_settings ) {
                my @choices;
                for my $section ( @{$arg->{option_sections}} ) {
                    for my $key ( sort keys %{$opt->{$section}} ) {
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
                        elsif ( $section eq 'cache' and $key eq 'reset' or  $section eq 'all' and $key eq 'binary_filter' ) {
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
            when ( $opt->{cache}{expire}[chs] ) {
                # Choose
                my $number = choose( [ undef, 0 .. 99 ], { prompt => 'Days until data expires [' . $opt->{cache}{expire}[v] . ']:', %number_lyt } );
                break if not defined $number;
                $opt->{cache}{expire}[v] = $number.'d';
                $change++;
            }
            when ( $opt->{cache}{reset}[chs] ) {
                # Choose
                my $choice = choose( [ undef, $true, $false ], { prompt => 'Reset cache [' . ( $opt->{cache}{reset}[v] ? 'YES' : 'NO' ) . ']:', %bol } );
                break if not defined $choice;
                $opt->{cache}{reset}[v] = ( $choice eq $true ) ? 1 : 0;
                $change++;
            }
            when ( $opt->{search}{max_depth}[chs] ) {
                # Choose
                my $number = choose( [ undef, '--', 0 .. 99 ], { prompt => 'Levels to descend at most [' . ( $opt->{search}{max_depth}[v] // 'undef' ) . ']:', %number_lyt } );
                break if not defined $number;
                if ( $number eq '--' ) {
                    $opt->{search}{max_depth}[v] = undef;
                }
                else {
                    $opt->{search}{max_depth}[v] = $number;
                }
                $change++;
            }
            when ( $opt->{all}{tab}[chs] ) {
                # Choose
                my $number = choose( [ undef, 0 .. 99 ], { prompt => 'Tab width [' . $opt->{all}{tab}[v] . ']:',  %number_lyt } );
                break if not defined $number;
                $opt->{all}{tab}[v] = $number;
                $change++;
            }
            when ( $opt->{all}{kilo_sep}[chs] ) {
                my ( $comma, $full_stop, $underscore, $space, $none ) = ( ' comma ', ' full stop ', ' underscore ', ' space ', ' none ' );
                my %sep_h = ( 
                    $comma      => ',',
                    $full_stop  => '.',
                    $underscore => '_',
                    $space      => ' ',
                    $none       => '',
                );
                # Choose
                my $sep = choose( [ undef, $comma, $full_stop, $underscore, $space, $none ], { prompt => 'Thousands separator [' . $opt->{all}{kilo_sep}[v] . ']:',  %bol } );
                break if not defined $sep;
                $opt->{all}{kilo_sep}[v] = $sep_h{$sep};
                $change++;
            }
            when ( $opt->{all}{min_width}[chs] ) {
                # Choose
                my $number = choose( [ undef, 0 .. 99 ], { prompt => 'Minimum Column width [' . $opt->{all}{min_width}[v] . ']:',  %number_lyt } );
                break if not defined $number;
                $opt->{all}{min_width}[v] = $number;
                $change++;
            }
            when ( $opt->{all}{limit}[chs] ) {
                my $number_now = $opt->{all}{limit}[v];
                $number_now =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{all}{kilo_sep}[v]/g;
                my $prompt = 'Compose new "limit" [' . $number_now . ']:';
                # Choose
                my $limit = choose_a_number( $arg, $opt, 7, $prompt );
                break if not defined $limit;
                $opt->{all}{limit}[v] = $limit;
                $change++;
            }
            when ( $opt->{all}{binary_filter}[chs] ) {
                # Choose
                my $choice = choose( [ undef, $true, $false ], { prompt => 'Enable Binary Filter [' . ( $opt->{all}{binary_filter}[v] ? 'YES' : 'NO' ) . ']:', %bol } );
                break if not defined $choice;
                $opt->{all}{binary_filter}[v] = ( $choice eq $true ) ? 1 : 0;
                $change++;
            }
            when ( $opt->{all}{undef}[chs] ) {
                # Read     
                my $undef = $term->readline( 'Choose a replacement-string for undefined table vales ["' . $opt->{all}{undef}[v] . '"]: ' );
                break if not defined $undef;         
                $opt->{all}{undef}[v] = $undef;
                $change++;
            }
            default { die "$option: no such value in the hash \%opt"; }
        }
    }
    if ( $change ) {
        my ( $true, $false ) = ( ' Make changes permanent ', ' Use changes only this time ' );
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
    for my $section ( $arg->{db_type}, 'db_all' ) {
        for my $key ( sort keys %{$opt->{$section}} ) {
            if ( not defined $opt->{$section}{$key}[chs] ) {
                # invalid key in config_file
            }
            else {
                push @choices, $opt->{$section}{$key}[chs];
            }
        }
    }
    my $change;
    my ( $true, $false ) = ( ' YES ', ' NO ' );
    DB_OPTIONS: while ( 1 ) {
        # Choose
        my $option = choose( [ undef, @choices ], { undef => $arg->{_back}, layout => 3, clear_screen => 1 } );
        last DB_OPTIONS if not defined $option;
        my $back = '<<';
        my %number_lyt = ( layout => 1, vertical => 0, right_justify => 1, undef => $back );
        my %bol = ( undef => " $back ", pad_one_row => 1 );
        given ( $option ) {
            when ( $opt->{sqlite}{unicode}[chs] ) {
                # Choose
                my $unicode = $opt->{$database}{unicode}[v] // $opt->{$arg->{db_type}}{unicode}[v];
                my $choice = choose( [ undef, $true, $false ], { prompt => 'Unicode [' . ( $unicode ? 'YES' : 'NO' ) . ']:', %bol } );
                break if not defined $choice;
                $opt->{$database}{unicode}[v] = ( $choice eq $true ) ? 1 : 0;
                write_config_file( $arg->{config_file}, $opt );
                $change++;
            }
            when ( $opt->{sqlite}{see_if_its_a_number}[chs] ) {
                # Choose
                my $see_if_its_a_number = $opt->{$database}{see_if_its_a_number}[v] // $opt->{$arg->{db_type}}{see_if_its_a_number}[v];
                my $choice = choose( [ undef, $true, $false ], { prompt => 'See if its a number [' . ( $see_if_its_a_number ? 'YES' : 'NO' ) . ']:', %bol } );
                break if not defined $choice;
                $opt->{$database}{see_if_its_a_number}[v] = ( $choice eq $true ) ? 1 : 0;
                write_config_file( $arg->{config_file}, $opt );
                $change++;
            }
            when ( $opt->{sqlite}{busy_timeout}[chs] ) {
                my $number_now = $opt->{$database}{busy_timeout}[v] // $opt->{$arg->{db_type}}{busy_timeout}[v];
                $number_now =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{all}{kilo_sep}[v]/g;
                my $prompt = 'Compose new "Busy timeout (ms)" [' . $number_now . ']:';
                # Choose
                my $timeout = choose_a_number( $arg, $opt, 6, $prompt );
                break if not defined $timeout;
                $opt->{$database}{busy_timeout}[v] = $timeout;
                write_config_file( $arg->{config_file}, $opt );
                $change++;
            }
            when ( $opt->{sqlite}{cache_size}[chs] ) {
                my $number_now = $opt->{$database}{cache_size}[v] // $opt->{$arg->{db_type}}{cache_size}[v];
                $number_now =~ s/(\d)(?=(?:\d{3})+\b)/$1$opt->{all}{kilo_sep}[v]/g;
                my $prompt = 'Compose new "Cache size (kb)" [' . $number_now . ']:';
                # Choose
                my $cache_size = choose_a_number( $arg, $opt, 8, $prompt );
                break if not defined $cache_size;
                $opt->{$database}{cache_size}[v] = $cache_size;
                write_config_file( $arg->{config_file}, $opt );
                $change++;
            }
            when ( $opt->{db_all}{delete_join}[chs] ) {
                my $save_tmp_tbl = read_json( $arg->{temp_table_file} );
                my @temp_tables = map{ "- $_" } sort keys %{$save_tmp_tbl->{$database}};
                my @removed;
                while ( 1 ) {
                    my $table = choose( [ undef, @temp_tables, $arg->{_confirm} ], { layout => 3, undef => '  ' . $arg->{back} } );
                    break if not defined $table;
                    last if $table eq $arg->{_confirm};
                    my $idx = first_index { $_ eq $table } @temp_tables;
                    push @removed, splice( @temp_tables, $idx, 1 );
                }
                for my $table ( @removed ) {
                    $table =~ s/\A..//;
                    delete $save_tmp_tbl->{$database}{$table};
                }
                write_json( $arg->{temp_table_file}, $save_tmp_tbl );
                $change++;
            }            
            default { die "$option: no such value in the hash \%opt"; }
        }
    }
    return $change ? 1 : 0;
}


#----------------------------------------------------------------------------------------------------#
#-- 999 --------------------------------    helper routines     -------------------------------------#
#----------------------------------------------------------------------------------------------------#


sub write_config_file {
    my ( $file, $opt ) = @_;
    my $ini = Config::Tiny->new;
    for my $section ( keys %$opt ) {
        for my $key ( keys %{$opt->{$section}} ) {
            if ( not defined $opt->{$section}{$key}[v] ) {
                $ini->{$section}{$key} = '';
            }
            elsif ( $opt->{$section}{$key}[v] eq '' ) {
                $ini->{$section}{$key} = "''";
            }
            else {
                $ini->{$section}{$key} = $opt->{$section}{$key}[v];
            }
        }
    }
    $ini->write( $file ) or die Config::Tiny->errstr;
}

sub read_config_file {
    my ( $file, $opt ) = @_;
    if ( not -f $file ) {
        open my $fh, '>', $file or die $!;
        close $fh or die $!;
    }
    my $ini = Config::Tiny->new;
    $ini = Config::Tiny->read( $file ) or die Config::Tiny->errstr;
    for my $section ( keys %$ini ) {
        for my $key ( keys %{$ini->{$section}} ) {
            if ( $ini->{$section}{$key} eq '' ) {
                $opt->{$section}{$key}[v] = undef;
            }
            elsif ( $ini->{$section}{$key} eq "''" ) {
                $opt->{$section}{$key}[v] = '';
            }
            else {
                $opt->{$section}{$key}[v] = $ini->{$section}{$key};
            }
        }
    }
    return $opt;
}


sub write_json {
    my ( $file, $ref ) = @_;
    my $json = encode_json $ref;   
    open my $fh, '>', $file or die $!;
    print $fh $json;
    close $fh or die $!;
}


sub read_json {
    my ( $file ) = @_;
    return {} if not -f $file;
    open my $fh, '<', $file or die $!;
    my $json = readline $fh;
    close $fh or die $!;    
    my $ref = decode_json $json;
    return $ref;
}


sub choose_a_number {
    my ( $arg, $opt, $digits, $prompt ) = @_;
    my %hash;
    my $number;
    while ( 1 ) {
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
        my $choice = choose( [ undef, @list ], { prompt => $prompt, layout => 3, right_justify => 1, undef => $arg->{back} . ' ' x ( $longest * 2 + 1 ) } );
        return if not defined $choice;
        last if $confirm and $choice eq $confirm;
        $choice = ( split /\s*-\s*/, $choice )[0];
        $choice =~ s/\A\s*\d//;
        my $reset = 'reset';
        # Choose
        my $c = choose( [ undef, map( $_ . $choice, 1 .. 9 ), $reset ], { pad_one_row => 2, undef => '<<' } );
        next if not defined $c;
        if ( $c eq $reset ) {
            delete $hash{length $c};
        }
        else {
            $c =~ s/\Q$opt->{all}{kilo_sep}[v]\E//g if $opt->{all}{kilo_sep}[v] ne '';
            $hash{length $c} = $c;
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
