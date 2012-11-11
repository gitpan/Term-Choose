#!perl -T

use 5.10.1;
use strict;
use warnings;
use autodie;
use Test::More;

unless ( $ENV{RELEASE_TESTING} ) {
    plan( skip_all => "Author tests not required for installation" );
}
else {
    plan tests => 3;
    
}


my $log = 0;
open my $fh1, '<', 'lib/Term/Choose.pm';
while ( my $line = readline $fh1 ) {
	if ( $line =~ /(?:\A\s*|\s+)use\s+Log::Log4perl/ ) {
		$log++;
	}
}
close $fh1;

is( $log, 0, 'OK - logging in Choose.pm disabled.' );



my $test_env = 0;
open my $fh2, '<', 'example/table_watch_SQLite.pl';
while ( my $line = readline $fh2 ) {
    if ( $line =~ /\A\s*use\s+warnings\s+FATAL/s ) {
        $test_env++;
    }
    if ( $line =~ /\A\s*use\s+Data::Dumper/s ) {
        $test_env++;
    }
}
close $fh2;

is( $test_env, 0, 'OK - test environment in table_watch_SQLite.pl disabled.' );

my $data = 0;
open my $fh3, '<', 'example/table_watch_SQLite.pl';
my $whole_file = do { 
    local $/ = undef; 
    <$fh3> 
};
close $fh3;
if ( $whole_file !~ /__DATA__\s*\z/ ) {
    $data = 1;
}
is( $data, 0, 'OK - __DATA__ section is clean' );
