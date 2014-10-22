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

my $file = 'example/table_watch_SQLite.pl';

my $test_env = 0;
open my $fh1, '<', $file;
while ( my $line = readline $fh1 ) {
    if ( $line =~ /\A\s*use\s+warnings\s+FATAL/s ) {
        $test_env++;
    }
    if ( $line =~ /\A\s*use\s+Data::Dumper/s ) {
        $test_env++;
    }
}
close $fh1;
is( $test_env, 0, 'OK - test environment in $file disabled.' );



my $data = 0;
open my $fh2, '<', $file;
my $whole_file = do { 
    local $/ = undef; 
    <$fh2> 
};
close $fh2;
if ( $whole_file !~ /__DATA__\s*\z/ ) {
    $data = 1;
}
is( $data, 0, 'OK - __DATA__ section in $file is clean' );



my $test_username_passwd = 0;
open my $fh3, '<', $file;
while ( my $line = readline $fh3 ) {
    if ( $line =~ /\A\s+mysql_user\s+=>\sundef,/ ) {
        $test_username_passwd++;
    }
    if ( $line =~ /\A\s+mysql_passwd\s+=>\sundef,/ ) {
        $test_username_passwd++;
    }    
    if ( $line =~ /\A\s+postgres_user\s+=>\sundef,/ ) {
        $test_username_passwd++;
    }    
    if ( $line =~ /\A\s+postgres_passwd\s+=>\sundef,/ ) {
        $test_username_passwd++;
    }
}
close $fh3;
is( $test_username_passwd , 4, "OK - default username and default password not defined in $file." );







