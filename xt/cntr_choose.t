use 5.010000;
use strict;
use warnings;
use Test::More tests => 2;


my $file = 'lib/Term/Choose.pm';


my $test_env = 0;

open my $fh1, '<', $file or die $!;
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



my $pad_before_pad_one_row = 0;
my $c = 0;

open my $fh2, '<', $file or die $!;
while ( my $line = readline $fh2 ) {
    if ( $line =~ /\Asub _set_defaults/ .. $line =~ /\A\}/ ) {
        $c++ if $line =~ /\A\s*\$\Qconfig->{pad}\E/;
        if ( $line =~ /\A\s*\$\Qconfig->{pad_one_row}\E/ ) {
            $pad_before_pad_one_row = 1 if $c;
            last;
        }
    }
}
close $fh2;

is( $pad_before_pad_one_row, 1, "OK - option \"pad\" is set before option \"pad_one_row\"." );
