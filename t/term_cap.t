use 5.010001;
use warnings;
use strict;
use Test::More;
use Test::Fatal;
use POSIX     qw();
use Term::Cap qw();

if ( $^O eq 'MSWin32' ) {
    plan skip_all => "MSWin32: no escape sequences.";
}

my $termios = POSIX::Termios->new();
$termios->getattr;

my $terminal = Term::Cap->Tgetent( { TERM => undef, OSPEED => $termios->getospeed } );

my %seq = (
    cl => "\e[H\e[2J",
    cd => "\e[J",
    cm => "\e[%i%d;%dH",
    do => "\n",
    le => "\x{08}",
    nd => "\e[C",
    up => "\e[A",
    sc => "\0337",
    rc => "\0338",

    me => "\e[0m",
    md => "\e[1m",
    mr => "\e[7m",
    ms => "\0\0\0\0\0",

    ve => "\e[?12l\e[?25h",
    vi => "\e[?25l",
);

diag( $ENV{TERM} );

for my $cap ( sort keys %seq ) {
    my $exception = exception { $terminal->Trequire( $cap ) };
    ok( ! defined $exception, "Trequire( '$cap' ) OK" ) or delete $seq{$cap};
}

for my $cap ( sort keys %seq ) {
    next if $seq{$cap} eq '';
    my $d = $terminal->Tputs( $cap );
    my $ok = ok( $d eq $seq{$cap}, $cap );
    if ( ! $ok ) {
        $d =~ s/\e/\\e/g;
        $seq{$cap} =~ s/\e/\\e/g;
        diag( qq{> '$cap': "$seq{$cap}" - "$d"} );
    }
}

done_testing;