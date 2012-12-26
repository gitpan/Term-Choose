use 5.010001;
use strict;
use warnings;
use autodie;
use Test::More;

unless ( $ENV{RELEASE_TESTING} ) {
    plan( skip_all => "Author tests not required for installation" );
}
else {
    plan tests => 1;
    
}


my $file = 'lib/Term/Choose.pm';

my $test_env = 0;
open my $fh1, '<', $file;
while ( my $line = readline $fh1 ) {
    if ( $line =~ /\A\s*use\s+warnings\s+FATAL/s ) {
        $test_env++;
    }
	if ( $line =~ /(?:\A\s*|\s+)use\s+Log::Log4perl/ ) {
		$test_env++;
	}
}
close $fh1;
is( $test_env, 0, "OK - test environment in $file disabled." );

