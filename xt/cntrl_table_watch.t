use 5.010001;
use strict;
use warnings;
use Test::More tests => 1;


my $file = 'example/table_watch_SQLite.pl';



my $test_env = 0;

open my $fh1, '<', $file or die $!;
while ( my $line = readline $fh1 ) {
    if ( $line =~ /\A\s*use\s+Data::Dumper/s ) {
        $test_env++;
    }
}
close $fh1;

is( $test_env, 0, 'OK - test environment in $file disabled.' );
