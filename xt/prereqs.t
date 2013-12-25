use 5.010001;
use warnings;
use strict;
use File::Find;
use Test::More;
use Perl::PrereqScanner;


my %prereqs_make;
open my $fh_m, '<', 'Makefile.PL' or die $!;
while ( my $line = <$fh_m> ) {
    my $module;
    if ( $line =~ /^\s*BUILD_REQUIRES/ .. $line =~ /^\s*\},/ ) {
        if ( $line =~ /^\s*'([^']+)'\s*=>/ ) {
            $prereqs_make{$1} = $1;
        }
    }
    if ( $line =~ /^\s*PREREQ_PM/ .. $line =~ /^\s*\},/ ) {
        if ( $line =~ /^\s*'([^']+)'\s*=>/ ) {
            $prereqs_make{$1} = $1;
        }
    }
}
close $fh_m or die $!;


my @files;
for my $dir ( 'lib', 't' ) {
    find( {
        wanted => sub {
            my $file = $File::Find::name;
            return if ! -f $file;
            push @files, $file;
        },
        no_chdir => 1,
    }, $dir );
}
my %modules;
for my $file ( @files ) {
    my $scanner = Perl::PrereqScanner->new;
    my $prereqs = $scanner->scan_file( $file );
    for my $module ( keys %{$prereqs->{requirements}} ) {
        next if $module =~ /^\p{Lowercase}/;
        $modules{$module} = $module;
    }
}



for my $module ( sort keys %modules ) {
    is( $prereqs_make{$module}, $modules{$module} );
}

cmp_ok( keys %modules, '==', keys %prereqs_make, 'keys %modules == keys %prereqs_make' );

done_testing();


