#!perl -T

use 5.006;
use strict;
use warnings;
use Test::More;

unless ( $ENV{RELEASE_TESTING} ) {
    plan( skip_all => "Author tests not required for installation" );
}
else {
    plan tests => 3;
}

use POSIX qw(strftime);
use Term::Choose;

my $v = $Term::Choose::VERSION;
my $v_pod = -1;
my $v_changes = -1;
my $release_date = -1;

open my $fh1, '<', 'lib/Term/Choose.pm' or die $!;

while ( my $line = readline $fh1 ) {
    if ( $line =~ /\A=pod/ .. $line =~ /\A=cut/ ) {
        if ( $line =~ m/\A\s*Version\s+(\S+)/m ) {
            $v_pod = $1;
        }
    }
}

close $fh1 or die $!;



open my $fh2, '<', 'Changes' or die $!;

while ( my $line = readline $fh2 ) {
    if ( $line =~ m/\A\s*([0-9][0-9.]*)\s+(\d\d\d\d-\d\d-\d\d)\s*\Z/m ) {
        $v_changes = $1;
        $release_date = $2;
        last;
    }
}

close $fh2 or die $!;


my $today = strftime "%Y-%m-%d", localtime();


is( $v, $v_pod, 'Version in POD OK' );
is( $v, $v_changes, 'Version in Changes OK' );
is( $release_date, $today, 'Release date in Changes is date from today' );


