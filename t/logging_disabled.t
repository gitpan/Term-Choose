#!perl -T

use 5.10.1;
use strict;
use warnings;
use Test::More;

unless ( $ENV{RELEASE_TESTING} ) {
    plan( skip_all => "Author tests not required for installation" );
}
else {
    plan tests => 2;
    
}


my $log_1 = 0;

open my $fh1, '<', 'lib/Term/Choose.pm' or die $!;
while ( my $line = readline $fh1 ) {
	if ( $line =~ /\$log\s*->/ ) {
		$log_1++;
	}
    if ( $line =~ /(?:\A\s*|\s+)my\s*\$log/ ) {
		$log_1++;
	}
	if ( $line =~ /(?:\A\s*|\s+)use\s+Log::Log4perl/ ) {
		$log_1++;
	}
}
close $fh1 or die $!;

is( $log_1, 0, 'OK - all logging in Choose.pm disabled.' );


my $log_2 = 0;

open my $fh2, '<', 'lib/Term/Choose/GC.pm' or die $!;
while ( my $line = readline $fh2 ) {
	if ( $line =~ /\$log\s*->/ ) {
		$log_2++;
	}
    if ( $line =~ /(?:\A\s*|\s+)my\s*\$log/ ) {
		$log_2++;
	}
	if ( $line =~ /(?:\A\s*|\s+)use\s+Log::Log4perl/ ) {
		$log_2++;
	}
}
close $fh2 or die $!;


is( $log_2, 0, 'OK - all logging in GC.pm disabled.' );


