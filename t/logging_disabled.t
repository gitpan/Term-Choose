use 5.010001;
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


my $test_env_1 = 0;
open my $fh1, '<', 'lib/Term/Choose.pm';
while ( my $line = readline $fh1 ) {
    if ( $line =~ /\A\s*use\s+warnings\s+FATAL/s ) {
        $test_env_1++;
    }
	if ( $line =~ /(?:\A\s*|\s+)use\s+Log::Log4perl/ ) {
		$test_env_1++;
	}
}
close $fh1;

is( $test_env_1, 0, 'OK - test environment in Choose.pm disabled.' );



my $test_env_2 = 0;
open my $fh2, '<', 'example/table_watch_SQLite.pl';
while ( my $line = readline $fh2 ) {
    if ( $line =~ /\A\s*use\s+warnings\s+FATAL/s ) {
        $test_env_2++;
    }
    if ( $line =~ /\A\s*use\s+Data::Dumper/s ) {
        $test_env_2++;
    }
}
close $fh2;

is( $test_env_2, 0, 'OK - test environment in table_watch_SQLite.pl disabled.' );

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
