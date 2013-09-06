use 5.010000;
use strict;
use warnings;
use Test::More tests => 2;


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




my $test_username_passwd = 0;

open my $fh3, '<', $file or die $!;
while ( my $line = readline $fh3 ) {
    if ( $line =~ /\Amy\s\$info\s=\s\{\s*\z/ .. $line =~ /\A\};\s*\z/ ) {
        if ( $line =~ /\A\s+user\s+=>\sundef,/ ) {
            $test_username_passwd++;
        }
        if ( $line =~ /\A\s+passwd\s+=>\sundef,/ ) {
            $test_username_passwd++;
        }
    }
}
close $fh3;

is( $test_username_passwd , 4, "OK - default username and default password not defined in $file." );
