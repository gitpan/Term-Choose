use 5.010001;
use strict;
use warnings;
use autodie;
use utf8;
use Test::More;

my $build = 'Build.PL';
my %build;
open my $fh_build, '<:encoding(UTF-8)', $build;
while ( my $line = readline $fh_build ) {
    chomp $line;
    $build{one}   = $line if $. == 1 and $line =~ /\Ause/;
    $build{two}   = $line if $. == 2 and $line eq 'use warnings';
    $build{three} = $line if $. == 3 and $line eq 'use strict;';
    $build{four}  = $line if $. == 4 and $line eq 'use utf8;';    
    $build{five}  = $line if $. == 5 and $line eq q{die "No support for OS" if $^O eq 'MSWin32';};
    $build{module_name}       = $1 if $line =~ /\A\s*module_name\s*=>\s*'([^']+)'/;
    $build{license}           = $1 if $line =~ /\A\s*license\s*=>\s*'([^']+)'/;
    $build{dist_author}       = $1 if $line =~ /\A\s*dist_author\s*=>\s*'([^']+)'/;
    $build{dist_version_from} = $1 if $line =~ /\A\s*dist_version_from\s*=>\s*'([^']+)'/;
    $build{Test_More    }     = $1 if $line =~ /\A\s*'?Test::More'?\s*=>\s*([^\s,]+)/;
    $build{perl}              = $1 if $line =~ /\A\s*'?perl'?\s*=>\s*([^\s,]+)/;
    $build{Carp}              = $1 if $line =~ /\A\s*'?Carp'?\s*=>\s*([^\s,]+)/;
    $build{Scalar_Util}       = $1 if $line =~ /\A\s*'?Scalar::Util'?\s*=>\s*([^\s,]+)/;
    $build{Term_ReadKey}      = $1 if $line =~ /\A\s*'?Term::ReadKey'?\s*=>\s*([^\s,]+)/;
    $build{Unicode_GCString}  = $1 if $line =~ /\A\s*'?Scalar::Util'?\s*=>\s*([^\s,]+)/;
}
close $fh_build;

my $make = 'Makefile.PL';
my %make;
open my $fh_make, '<:encoding(UTF-8)', $make;
while ( my $line = readline $fh_make ) {
    chomp $line;
    $make{one}   = $line if $. == 1 and $line =~ /\Ause/;
    $make{two}   = $line if $. == 2 and $line eq 'use warnings';
    $make{three} = $line if $. == 3 and $line eq 'use strict;';
    $make{four}  = $line if $. == 4 and $line eq 'use utf8;';    
    $make{five}  = $line if $. == 5 and $line eq q{die "No support for OS" if $^O eq 'MSWin32';};
    $make{module_name}       = $1 if $line =~ /\A\s*NAME\s*=>\s*'([^']+)'/;
    $make{license}           = $1 if $line =~ /\A\s*LICENSE\s*=>\s*'([^']+)'/;
    $make{dist_author}       = $1 if $line =~ /\A\s*AUTHOR\s*=>\s*'([^']+)'/;
    $make{dist_version_from} = $1 if $line =~ /\A\s*VERSION_FROM\s*=>\s*'([^']+)'/;
    $make{Test_More    }     = $1 if $line =~ /\A\s*'?Test::More'?\s*=>\s*([^\s,]+)/;
    $make{perl}              = $1 if $line =~ /\A\s*'?MIN_PERL_VERSION'?\s*=>\s*([^\s,]+)/;
    $make{Carp}              = $1 if $line =~ /\A\s*'?Carp'?\s*=>\s*([^\s,]+)/;
    $make{Scalar_Util}       = $1 if $line =~ /\A\s*'?Scalar::Util'?\s*=>\s*([^\s,]+)/;
    $make{Term_ReadKey}      = $1 if $line =~ /\A\s*'?Term::ReadKey'?\s*=>\s*([^\s,]+)/;
    $make{Unicode_GCString}  = $1 if $line =~ /\A\s*'?Scalar::Util'?\s*=>\s*([^\s,]+)/;
}
close $fh_make;


my %keys;
for my $key ( keys %build ) {
    $keys{$key}++;
}
for my $key ( keys %make ) {
    $keys{$key}++;
}

unless ( $ENV{RELEASE_TESTING} ) {
    plan( skip_all => "Author tests not required for installation" );
}
else {
    plan tests => scalar keys %keys;    
}

for my $key ( sort keys %keys ) {
    ok( $build{$key} eq $make{$key}, "Key: $key" );
}










































__DATA__

my $str;
my $ord = ord $str;

if ( not defined $ord ) {
    say "Not defined";
}
else { 
    say "|$ord|"; 
}

