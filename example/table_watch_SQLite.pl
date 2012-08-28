#!/usr/bin/env perl
use warnings;
use 5.10.1;
use utf8;
binmode STDOUT, ':utf8';
# Version 0.09

use File::Find qw(find);
use File::Path qw(make_path);
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
	GO_TO_TOP_LEFT 	=> "\e[1;1H", 
	CLEAR_EOS 		=> "\e[0J", 
	UP 				=> "\e[A" 
};

my $home = File::HomeDir->my_home;

sub help {
    print << 'HELP';

Usage:
    table_info.pl [help or options] [directories to be searched]
    say rel2abs( $0 );
<>;
    If no directories are passed the home directory is searched for databases.
    Options with the parenthesis at the end can be used on the comandline too.
    Customized Table - REGEXP: for case sensitivity prefix pattern with (?-i).

Options:
    Help          : Show this Info.  (-h|--help)
    Settings      : Show settings.
    Cache expire  : Days until data expires. The cache holds the names of the found databases.
    Reset cache   : Reset the cache. (-s|--no-cache)
    Cache rootdir : Set the cache root directory.   
    Maxdepth      : Levels to descend at most when searching in directories for databases.  (-m|max-depth) 
    Limit         : Set the maximum number of table rows read in one time.  (-l|--limit)
    No BLOB       : Do not print columns with the column-type BLOB.
    Tab           : Set the number of spaces between columns.
    Min-Width     : Set the width the columns should have at least when printed.
    Delete        : Enable the option "delete table" and "delete database".  (-d|--delete)
    Undef         : Set the string, that will be shown instead of undefined table values when printed.
    Table colors  : Set the colors for the columns.
    Head colors   : Set the fore- and background color for the table-head.
    Cut col names : When should column names be cut.
    
HELP
}

my $opt = {
    cache_expire   => '7d',
    reset_cache    => 0,
    cache_rootdir  => tmpdir(),
    max_depth      => undef,
    limit          => 10_000,
    no_blob        => 1,
    tab            => 2,
    min_width      => 30,
    delete         => 0,
    undef          => '',
    # colors on a terminal with green font and black background:
    table_colors   => [ 'cyan', 'default', 'magenta', 'white', 'green', 'blue', 'yellow', 'red' ],
    head_colors    => 'white reverse',
    cut_col_names  => -1,
};

my $help;
GetOptions (
    'h|help'        => \$help,
    's|no-cache'    => \$opt->{reset_cache},
    'l|limit:i'     => \$opt->{limit},
    'm|max-depth:i' => \$opt->{max_depth},
    'd|delete'      => \$opt->{delete},
);

my $config_file = catfile $home, '.table_info.conf';

if ( not -e $config_file ) {
	open my $fh, '>', $config_file or warn $!;
	close $fh or warn $!;
}

if ( -e $config_file and -s $config_file ) {
    my $section = 'sqlite';
    my $ini = Config::Tiny->new;
    $ini = Config::Tiny->read( $config_file );
    for my $key ( keys %{$ini->{$section}} ) {
        if ( $key eq 'table_colors' ) {
            $opt->{$key} = [ split / /, $ini->{$section}{$key} ];
        }
        else {
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
}

$opt = options( $opt, $config_file ) if $help;

my @dirs = @ARGV ? @ARGV : ( $home );

my $key = join ' ', @dirs, '|', $opt->{max_depth} // '';

my $cached = ' (cached)';

my $cache = CHI->new ( 
    namespace => 'table_watch_SQLite', 
    driver => 'File', 
    root_dir => $opt->{cache_rootdir},  
    expires_in => $opt->{cache_expire}, 
    expires_variance => 0.25, 
);

$cache->remove( $key ) if $opt->{reset_cache};

my @databases = $cache->compute( $key, $opt->{cache_expire}, sub { return search_databases->( @dirs ) } );

sub search_databases {
    my @dirs = @_;
    my @databases;
    $cached = '';
    say "searching...";
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
                push @databases, $file if $flm->describe_filename( $file ) =~ /\ASQLite/; 
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
my %lyt = ( layout => 3, clear_screen => 1 );

#my %auswahl = ( 
#    color_customize => '°°° table °°°',
#    row_customize 	=> '*** table ***',
#    count_rows     	=> 'count rows',
#    delete_table   	=> 'delete table',
#);
#my @aw_keys = ( qw( color_customize row_customize count_rows ) );

my %auswahl = ( 
    color_auto      => '°° table auto', 
    color_customize => '°° table customized',
    row_auto        => '** table auto', 
    row_customize   => '** table customized',
    count_rows      => '   count rows',
    delete_table    => '   delete table',
);
my @aw_keys = ( qw( color_auto color_customize count_rows row_auto row_customize ) );

push @aw_keys, 'delete_table' if $opt->{delete}; 

DATABASES: while ( 1 ) {
    my $db = choose( [ undef, @databases ], { prompt => 'Choose Database' . $cached, %lyt, undef => $quit } );
    last DATABASES if not defined $db;
	my $dbh;
	eval {
		$dbh = DBI->connect( "DBI:SQLite:dbname=$db", '', '', { 
			RaiseError => 1, 
			PrintError => 0, 
			AutoCommit => 1, 
			sqlite_unicode => 1,
		} ) or die DBI->errstr;
		$dbh->sqlite_busy_timeout( 3000 );
		$dbh->do( "PRAGMA cache_size = 400000" );
		$dbh->do( "PRAGMA synchronous = OFF" );   
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
        push @tables, '  delete database' if $opt->{delete};
        my $table = choose( [ undef, @tables ], { prompt => 'Choose Table', %lyt, undef => "  $back" } );
        last TABLES if not defined $table;
        $table =~ s/\A..//;
        if ( $table eq 'delete database' ) {
            say "\nRealy delete database ", colored( "\"$db\"", 'red' ), "?\n";
            my $c = choose( [ ' No ', ' Yes ' ], { prompt => 0, pad_one_row => 1 } );
            if ( $c eq ' Yes ' ) {
                eval { unlink $db or die $! };
                if ( $@ ) {
                    say "Could not remove database \"$db\"";
                    print $@;
                }
                else {
                    $cache->remove( $key );
                    @databases = grep { $_ ne $db } @databases;
                }
                last TABLES;
            }
        }

        CHOOSE: while ( 1 ) {
            my $choice = choose( [ undef, @auswahl{@aw_keys} ], { %lyt, undef => "  $back" } );
            given ( $choice ) {
                when ( not defined ) {
                    last CHOOSE;
                }
                when ( $auswahl{color_auto} ) {
                    my $type = 'color';
                    my $rows = $dbh->selectrow_array( "SELECT COUNT(*) FROM [$table]" );
                    my $select = "SELECT * FROM [$table]";
                    my $arguments;
                    PRINT: while ( 1 ) {
						my ( $begin, $end, $last ) = choose_table_range( $rows );
						last PRINT if $last == 1;	
						my ( $ref, $col_types ) = read_db_table( $dbh, $begin, $end, $select, $arguments );
						last PRINT if not defined $ref;
						print_table( $ref, $col_types, $type );
						last PRINT if $last == 2;
					}
                }
                when ( $auswahl{color_customize} ) {
                    my $type = 'color';
                    my ( $rows, $select, $arguments ) = prepare_read_table( $dbh, $table );
                    next CHOOSE if not defined $rows;
                    PRINT: while ( 1 ) {
						my ( $begin, $end, $last ) = choose_table_range( $rows );
						last PRINT if $last == 1;	
						my ( $ref, $col_types ) = read_db_table( $dbh, $begin, $end, $select, $arguments );
						last PRINT if not defined $ref;
						print_table( $ref, $col_types, $type );
						last PRINT if $last == 2;
					}
                }
                when ( $auswahl{row_auto} ) {
                    my $type = 'row';
                    my $rows = $dbh->selectrow_array( "SELECT COUNT(*) FROM [$table]" );
                    my $select = "SELECT * FROM [$table]";
                    my $arguments;
                    PRINT: while ( 1 ) {
						my ( $begin, $end, $last ) = choose_table_range( $rows );
						last PRINT if $last == 1;	
						my ( $ref, $col_types ) = read_db_table( $dbh, $begin, $end, $select, $arguments );
						last PRINT if not defined $ref;
						print_table( $ref, $col_types, $type );
						last PRINT if $last == 2;
					}
                }
                when ( $auswahl{row_customize} ) {
                    my $type = 'row';
                    my ( $rows, $select, $arguments ) = prepare_read_table( $dbh, $table );
                    next CHOOSE if not defined $rows;
                    PRINT: while ( 1 ) {
						my ( $begin, $end, $last ) = choose_table_range( $rows );	
						last PRINT if $last == 1;
						my ( $ref, $col_types ) = read_db_table( $dbh, $begin, $end, $select, $arguments );
						last PRINT if not defined $ref;
						print_table( $ref, $col_types, $type );
						last PRINT if $last == 2;
					}
                }
                when ( $auswahl{count_rows} ) {
                    my $rows = $dbh->selectrow_array( "SELECT COUNT(*) FROM [$table]" );
                    $rows =~ s/(\d)(?=(?:\d{3})+\b)/$1_/g;
                    say "\n$db";
                    say "\nTable \"$table\":  $rows Rows\n"; 
                    choose( [ 'Press ENTER to continue' ], { prompt => 0 } ); 
                }
                when ( $auswahl{delete_table} ) {
                    say "\n$db";
                    say "\nRealy delete table ", colored( "\"$table\"", 'red' ), "?\n";
                    my $c = choose( [ ' No ', ' Yes ' ], { prompt => 0, pad_one_row => 1 } );
                    if ( $c eq ' Yes ' ) {
                        eval { $dbh->do( "DROP TABLE [$table]" ) };
                        if ( $@ ) {
                            say "Could not drop table \"$table\"";
                            print $@;
                        }
                        else {
                            @tables = grep { $_ ne $table } @tables;
                        }
                        last CHOOSE;
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

sub choose_table_range {
	my ( $rows ) = @_;
	my $last = 0;
	my $begin = 1;
	my $end = $opt->{limit};    
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
		my $choice = choose( [ undef, @choices ], { layout => 3, undef => $back } );
		if ( defined $choice ) {
			( $begin, $end ) = split /\s*-\s*/, $choice;
			$begin =~ s/\A\s+//;
		}
		else { 
			$last = 1; 
		}
    }
    else { 
		$last = 2;
	}
    return $begin, $end, $last;
}


sub read_db_table {
    my ( $dbh, $begin, $end, $select, $arguments ) = @_;
    my ( $ref, $col_types );
    eval {
        my $sth = $dbh->prepare( $select . " LIMIT ?, ?" );
        $sth->execute( defined $arguments ? @$arguments : (), $begin, $end );
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
	my ( $table, $columns, $chosen_columns, $cols_order_by_tmp, $cols_regexp_tmp, $hash_regexp ) = @_;
	print GO_TO_TOP_LEFT;
	print CLEAR_EOS;
	$chosen_columns = @$chosen_columns ? $chosen_columns : $columns;
	my ( @cols_regexp, @cols_order_by );
	@cols_order_by = grep { $_ ~~ @$chosen_columns } @$cols_order_by_tmp if @$cols_order_by_tmp;
	@cols_regexp   = grep { $_ ~~ @$chosen_columns } @$cols_regexp_tmp   if @$cols_regexp_tmp;
	say "SELECT ", ( @$chosen_columns ~~ @$columns ) ? '*' : join( ', ', @$chosen_columns );
	say " FROM $table";
	say " WHERE ", join " AND ", map { "$_ REGEXP $hash_regexp->{$_}" } @cols_regexp if @cols_regexp;
	say " ODER BY ", join ', ', @cols_order_by	if @cols_order_by;
	say "";
}


sub prepare_read_table {
	my ( $dbh, $table ) = @_;
	$dbh->func( 'regexp', 2, sub { my ( $regex, $string ) = @_; $string //= ''; return $string =~ m/$regex/ism }, 'create_function' );
	my $continue = ' OK ';
	my @keys = ( qw( print_table columns order_by regexp ) );
	my %customize = ( print_table => 'Print TABLE', columns => '- Columns', order_by => '- Order by', regexp => '- Regexp' ); 
	my $sth = $dbh->prepare( "SELECT * FROM [$table]" );
	my $columns	= $sth->{NAME};
	my $chosen_columns    = [];
	my $cols_order_by_tmp = [];
	my $cols_regexp_tmp   = [];	
	my $hash_regexp       = {};	
	
	CUSTOMIZE: while ( 1 ) {
		print_select( $table, $columns, $chosen_columns, $cols_order_by_tmp, $cols_regexp_tmp, $hash_regexp );
		my $custom = choose( [ undef, @customize{@keys} ], { prompt => "Customize:", layout => 3, undef => $back } );
        #my $custom = choose( [ @customize{@keys}, undef ], { prompt => "Customize:", layout => 3, undef => $back } );		
		for ( $custom ) {
			when ( not defined ) {
				last CUSTOMIZE;	
			}
			when( $customize{columns} ) {
				my @cols = @$columns;
				$chosen_columns = [];
				while ( @cols ) {
					print_select( $table, $columns, $chosen_columns, $cols_order_by_tmp, $cols_regexp_tmp, $hash_regexp );
					my $col = choose( [ $continue, @cols ], { prompt => "Choose: ", pad_one_row => 2 } );
					last if $col eq $continue;
					push @$chosen_columns, $col;
					my $idx = first_index { $_ eq $col } @cols;
					splice @cols, $idx, 1;
				}
				if ( not @$chosen_columns ) {
					@$chosen_columns = @$columns;
				}
			}
			when( $customize{order_by} ) {
				my @cols = @$chosen_columns ? @$chosen_columns : @$columns;
				$cols_order_by_tmp = [];
				while ( @cols ) {
					print_select( $table, $columns, $chosen_columns, $cols_order_by_tmp, $cols_regexp_tmp, $hash_regexp );
					my $col = choose( [ $continue, @cols ], { prompt => "Choose: ", pad_one_row => 2 } );
					last if $col eq $continue;
					push @$cols_order_by_tmp, $col;
					my $idx = first_index { $_ eq $col } @cols;
					splice @cols, $idx, 1;
				}
			}
			when ( $customize{regexp} ) {
				my @cols = @$chosen_columns ? @$chosen_columns : @$columns;
				$hash_regexp = {};
				$cols_regexp_tmp = [];
				while ( @cols ) {
					print_select( $table, $columns, $chosen_columns, $cols_order_by_tmp, $cols_regexp_tmp, $hash_regexp );
					my $col = choose( [ $continue, @cols ], { prompt => "Choose: ", pad_one_row => 2 } );
					last if $col eq $continue;
					print "$col: ";
					my $regex = <>;
					chomp $regex;
					$hash_regexp->{$col} = $regex;
					push @$cols_regexp_tmp, $col;
					my $idx = first_index { $_ eq $col } @cols;
					splice @cols, $idx, 1;
				}
			}
			when( $customize{print_table} ) {
				my $order_by_str = '';
				$chosen_columns = @$chosen_columns ? $chosen_columns : $columns;
				my @cols_order_by;
				@cols_order_by = grep { $_ ~~ @$chosen_columns } @$cols_order_by_tmp if @$cols_order_by_tmp;
				$order_by_str = ' ORDER BY ' . '[' . join( '], [', @cols_order_by ) . ']' if @cols_order_by;
				my $regexp_str = '';
				my @cols_regexp;
				@cols_regexp = grep { $_ ~~ @$chosen_columns } @$cols_regexp_tmp if @$cols_regexp_tmp;
				if ( @cols_regexp ) {
					my $n;
					for my $col ( @cols_regexp ) {
						++$n;
						$regexp_str = " WHERE [$col] REGEXP ?" if $n == 1;
						$regexp_str .= " AND [$col] REGEXP ?"  if $n > 1;
					}
				}
				my $str = '[' . join( '], [', @$chosen_columns ? @$chosen_columns : @$columns ) . ']';
				my $select = "SELECT $str FROM [$table]" . $regexp_str . $order_by_str;
				my @arguments = ( @$hash_regexp{@cols_regexp} );
				my $rows = $dbh->selectrow_array( "SELECT COUNT(*) FROM [$table]" . $regexp_str, {}, @$hash_regexp{@cols_regexp} );
				return $rows, $select, \@arguments;
			}
			default {
				say "Something is wrong!";
			}
		}
	}
	return;
}


sub calc_widths {
    my ( $ref, $col_types, $cut, $maxcols ) = @_; 
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
    my ( $maxcols, $ref, $col_types ) = @_;
    my $cut = $opt->{cut_col_names};
    my ( $max, $not_a_number );
    eval {
        ( $max, $not_a_number ) = calc_widths( $ref, $col_types, $cut, $maxcols );
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
            say "Terminal window is not wide enough to print this table.";
            choose( [ 'Press ENTER to show the column names' ], { prompt => 0 } );
            choose( $ref->[0], { prompt => "Column names (close with ENTER):", layout => 0 } );
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
    my ( $ref, $col_types, $type ) = @_;
    my ( $maxcols, $maxrows ) = GetTerminalSize( *STDOUT );
    return if not defined $ref;
    my ( $max, $not_a_number ) = recalc_widths( $maxcols, $ref, $col_types );
    return if not defined $max;
    print GO_TO_TOP_LEFT;
    print CLEAR_EOS;
#################################################################################
    if ( $type eq 'row' ) {
        my $items = @$ref * @{$ref->[0]};
        my $start = 8_000;
        my $total = $#{$ref};
        my $next_update = 0;
        my $c = 0;
        my $progress;
        if ( $items > $start ) {
            $progress = Term::ProgressBar->new( { name => 'Computing', count => $total, remove => 1 } );
            $progress->minor( 0 );
        }
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
            if ( $items > $start ) {
                my $is_power = 0;
                for ( my $i = 0; 2 ** $i <= $c; $i++) {
                    $is_power = 1 if 2 ** $i == $c;
                }
                $next_update = $progress->update( $c ) if $c >= $next_update;
                ++$c;
            }
        }
        $progress->update( $total ) if $total >= $next_update and $items > $start;
        choose( \@list, { prompt => 0, layout => 3, length_longest => sum( @$max, $opt->{tab} * $#{$max} ), max_list => $opt->{limit} + 1 } );
        return;
    }
    ########################################################################################
    my $first_row = '';
    my $f_row = shift @$ref;
    for my $i ( 0 .. $#$max ) {
        my $word = $f_row->[$i] // $opt->{undef};
        my $right_justify = $not_a_number->[$i] ? 0 : 1;
        $word = unicode_sprintf( 
                    $max->[$i], 
                    $word, 
                    $right_justify, 
                    $maxcols + int( $maxcols / 10 )
        );
        $word .= ' ' x $opt->{tab} if not $i == $#$max;
        $first_row .= colored( $word, $opt->{head_colors} );
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
                my $word = $row->[$i] // $opt->{undef};
                my $right_justify = $not_a_number->[$i] ? 0 : 1;
                $word  = unicode_sprintf( 
                            $max->[$i], 
                            $word, 
                            $right_justify, 
                            $maxcols + int( $maxcols / 10 ) 
                );
                my $color = ( $opt->{table_colors}[$ci] eq 'default' ) ? '' : $opt->{table_colors}[$ci];
                $string .=  colored( $word, $color );
                $string .= ' ' x $opt->{tab} if not $i == $#$max;
                $ci++;
                $ci = 0 if $ci >= @{$opt->{table_colors}};
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


#########################################################################
############################     options     ############################
#########################################################################



sub options {
    my ( $opt, $config_file ) = @_;
    my $oh = {
        cache_rootdir   => '- Cache rootdir', 
        cache_expire    => '- Cache expire', 
        reset_cache     => '- Reset cache', 
        max_depth       => '- Maxdepth', 
        limit           => '- Limit', 
        delete          => '- Delete', 
        tab             => '- Tab', 
        min_width       => '- Min-Width', 
        no_blob         => '- No BLOB',
        undef           => '- Undef', 
        table_colors    => '- Table colors', 
        head_colors     => '- Head colors',
        cut_col_names   => '- Cut col names',
    };
    my @keys = ( qw( cache_rootdir cache_expire reset_cache max_depth limit delete no_blob min_width tab undef table_colors head_colors cut_col_names ) );
    my $change;
    
    OPTIONS: while ( 1 ) {
        my ( $exit, $help, $show_settings, $continue ) = ( '  EXIT', '  HELP', '  SHOW SETTINGS', '  CONTINUE' );
        my $option = choose( [ $exit, $help, @{$oh}{@keys}, $show_settings, undef ], { undef => $continue, layout => 3, clear_screen => 1 } );
        my @colors = ( 'black', 'blue', 'cyan', 'green', 'magenta', 'red', 'white', 'yellow', 'default' );
        my @background = ( 'on_black', 'on_blue', 'on_cyan', 'on_green', 'on_magenta', 'on_red', 'on_white', 'on yellow' );
        my $back = '<<';
        my %number_lyt = ( layout => 1, vertical => 0, right_justify => 1, undef => $back );
        my %bol = ( undef => " $back ", pad_one_row => 1 );
        
        given ( $option ) {
            when ( not defined ) { last OPTIONS; }
            when ( $exit ) { exit() }
            when ( $help ) { help(); choose( [ '  Close with ENTER  ' ], { prompt => 0 } ) }
            when ( $show_settings ) { 
                say "";
                for my $key ( @keys ) {
                    my $value;
                    if ( not defined $opt->{$key} ) {
                        $value = 'undef';
                    } 
                    elsif ( $key eq 'undef' && $opt->{$key} eq '' ) {
                        $value = "''";
                    }
                    elsif ( $key eq 'table_colors' ) {
                        $value = "@{$opt->{$key}}";
                    }
                    elsif ( $key eq 'reset_cache' or $key eq 'delete' or $key eq 'no_blob' ) {
                        $value = $opt->{$key} ? 'true' : 'false';
                    }
                    elsif ( $key eq 'cut_col_names' ) {
                        $value = "auto"    if $opt->{$key} == -1;
                        $value = "no cut"  if $opt->{$key} ==  0;
                        $value = "cut col" if $opt->{$key} ==  1; 
                    }                    
                    else {
                        $value = $opt->{$key};
                    }
                    ( my $name = $oh->{$key} ) =~ s/\A..//;
                    printf "%-16s : %s\n", "  $name", $value;
                }
                say "";
                choose( [ '  Close with ENTER  ' ], { prompt => 0 } )
            }            
            when ( $oh->{cache_expire} ) { 
                my $number = choose( [ 0 .. 99, undef ], { prompt => 'Days until data expires:', %number_lyt } ); 
                break if not defined $number;
                $opt->{cache_expire} = $number.'d';
                $change++;
            }
            when ( $oh->{reset_cache} ) {
                my ( $true, $false ) = ( ' Remove cache ', ' Keep cache ' );
                my $choice = choose( [ $true, $false, undef ], { prompt => 'Cache:', %bol } );
                break if not defined $choice;
                $opt->{reset_cache} = ( $choice eq $true ) ? 1 : 0;
                $change++;                
            }           
            when ( $oh->{max_depth} ) { 
                my $number = choose( [ 0 .. 99, undef ], { prompt => 'Levels to descend at most:', %number_lyt } ); 
                break if not defined $number;
                $opt->{max_depth} = $number;
                $change++;
            }
            when ( $oh->{tab} ) { 
                my $number = choose( [ 0 .. 99, undef ], { prompt => 'Tab width',  %number_lyt } );
                break if not defined $number;
                $opt->{tab} = $number;
                $change++;
            }
            when ( $oh->{min_width} ) { 
                my $number = choose( [ 0 .. 99, undef ], { prompt => 'Minimum Column width:',  %number_lyt } );
                break if not defined $number;
                $opt->{min_width} = $number;
                $change++;
            }
            when ( $oh->{limit} ) { 
                my $number = set_number( 'Maximal number of rows read from a Table: ', 1, 99999 );
                break if not defined $number;
                $opt->{limit} = $number;
                $change++;
            }
            when ( $oh->{delete} ) { 
                my ( $true, $false ) = ( ' Enable delete options ', ' Disable delete options ' );
                my $choice = choose( [ $true, $false, undef ], { prompt => 'Delete" options:', %bol } );
                break if not defined $choice;
                $opt->{delete} = ( $choice eq $true ) ? 1 : 0;
                $change++;
            }
            when ( $oh->{no_blob} ) { 
                my ( $true, $false ) = ( ' Don\'t print BLOB columns ', ' Print BLOB columns ' );
                my $choice = choose( [ $true, $false, undef ], { prompt => 'BLOB columns:', %bol } );
                break if not defined $choice;
                $opt->{no_blob} = ( $choice eq $true ) ? 1 : 0;
                $change++;
            }
            when ( $oh->{undef} ) { 
                print "Choose a replacement-string for undefined table vales: ";
                my $undef = <>;
                chomp $undef;
                break if not $undef;
                $opt->{undef} = $undef;
                $change++;
            }
            when ( $oh->{cache_rootdir} ) {
                my $cache_rootdir = set_dir( "Enter cache root directory: " );
                break if not defined $cache_rootdir;
                $opt->{cache_rootdir} = $cache_rootdir;
                $change++;
            }
            when ( $oh->{table_colors} ) {
                my $columns = [];
                my $c = 0;
                while ( $c < 50 ) {
                    say "-> @$columns" if @$columns;
                    my $color = choose( [ @colors, 'CONFIRM', undef ], { prompt => 'Choose ' . ($c + 1) . '. color:', undef => $back } );
                    break if not defined $color;                    
                    last if $color eq 'CONFIRM';
                    $columns->[$c] = $color;
                    $c++;
                    print UP if @$columns > 1;
                }
                break if not @$columns;
                $opt->{table_colors} = $columns;
                $change++;
            }
            when ( $oh->{head_colors} ) {
                my $color_bg = choose( [ @background, undef ], { prompt => 'Choose table-head background color:', undef => $back } );
                break if not defined $color_bg;
                my $color_fg = choose( [ @colors, undef ], { prompt => 'Choose table-head foreground color:', undef => $back } );
                break if not defined $color_fg;
                $opt->{head_colors} = "$color_fg $color_bg";
                $change++;
            }
            when ( $oh->{cut_col_names} ) {
				my $default = 'Auto: cut column names if the sum of length of column names is greater than screen width.';
				my $cut_col = 'Cut the column names to the length of the longest column value.';
				my $no_cut  = 'Do not cut column names deliberately, instead treat column names as normal column valules.';
                my $cut = choose( [ $default, $cut_col, $no_cut, undef ], { prompt => 'Column names:', undef => $back } );
                break if not defined $cut;
				if    ( $cut eq $cut_col ) { $opt->{cut_col_names} =  1 }
				elsif ( $cut eq $no_cut )  { $opt->{cut_col_names} =  0 }
				else                       { $opt->{cut_col_names} = -1 }
				$change++;
			}
            default { die "Something is wrong $!"; }
        }
    }
    if ( $change ) {
        my ( $true, $false ) = ( ' Make changes permanent ', ' Use changes only this time ' );
        my $permanent = choose( [ $false, $true ], { prompt => 0, layout => 3, pad_one_row => 1 } );
        exit if not defined $permanent;
        if ( $permanent eq $true and -e $config_file ) {
            my $section = 'sqlite';
			my $ini = Config::Tiny->new;
            for my $key ( keys %$opt ) {
                if ( $key eq 'table_colors' ) {
                    $ini->{$section}->{$key} = "@{$opt->{table_colors}}";
                }
                else {
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
            }
            $ini->write( $config_file );
        }
        if ( $permanent eq $true ) {
			my $continue = choose( [ ' CONTINUE ', undef ], { prompt => 0, layout => 1, undef => ' QUIT ', pad_one_row => 1 } );
			exit() if not defined $continue;
		}
    }
    return $opt;
}


sub set_number {
    my ( $prompt, $min, $max ) = @_;
    my $number;
    while ( 1 ) {
        print $prompt;
        $number = <>;
        chomp $number;
        return if $number eq '';
        if ( $number !~ /\A\+?\d+\z/ ) {
            say "$number is not a positive integer!";
            say "A positive integer is needed.";
            next;
        }
        $min //= 0;
        if ( $number < $min ) {
            say "$number is less than $min!";
            say "The smalles allowed number is $min.";
            next;
        }    
        if ( defined $max and $number > $max ) {
            say "$number is greather than $max!";
            say "Largest allowed number: $max.";
            next;
        }     
        last;
    }
    $number =~ s/\A\+//;
    return $number;
}


sub set_dir {
    my ( $prompt ) = @_;
    print $prompt;
    my $dir = <>;
    chomp $dir;
    return if not $dir;
    if ( not -e $dir or not -d $dir ) {
        say "Creating $dir:";
        make_path( $dir, { error => \my $error } );
        if ( @$error ) {
            for my $diag (@$error) {
                my ( $file, $message ) = %$diag;
                if ( $file ) {
                    say "problem creating $file: $message";
                    return;
                }
                else {
                    say "general error: $message";
                    return;
                }
            }
        }
        else {
            return $dir;
        }
    }
    elsif ( -w $dir ) {
        return $dir;
    }
    else {
        say "No permissions to write to $dir";
        return;
    }
}



__DATA__

