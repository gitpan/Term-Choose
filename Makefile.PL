use 5.010001;
use warnings;
use strict;
use ExtUtils::MakeMaker;


my %prereqs_os_specific;
if ( $^O eq 'MSWin32' ) {
    %prereqs_os_specific = (
        'Term::Size::Win32'    => 0,
        'Win32::Console'       => 0,
        'Win32::Console::ANSI' => 0,
    );
}
else {
    %prereqs_os_specific = (
        'Term::ReadKey'        => 0,
    );
}


WriteMakefile(
    PL_FILES         => {},
    MIN_PERL_VERSION => '5.010001',
    LICENSE          => 'perl',
    AUTHOR           => 'Matthaeus Kiem <cuer2s@gmail.com>',
    NAME             => 'Term::Choose',
    VERSION_FROM     => 'lib/Term/Choose.pm',
    EXE_FILES        => [],
    BUILD_REQUIRES   => {
        'Test::More'        => 0,
    },
    PREREQ_PM => {
        'constant'          => 0,
        'strict'            => 0,
        'warnings'          => 0,
        'Carp'              => 0,
        'Exporter'          => 0,
        'Text::LineFold'    => 0,
        'Unicode::GCString' => 0,
        %prereqs_os_specific,
    },
    clean => {
        FILES => 'Term-Choose-*',
    },
);
