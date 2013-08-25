use 5.010000;
use warnings;
use strict;
use utf8;
die "No support for OS" if $^O eq 'MSWin32';
use ExtUtils::MakeMaker;


WriteMakefile(
    PL_FILES       => {},
    MIN_PERL_VERSION => '5.010000',
    LICENSE        => 'perl',
    AUTHOR         => 'Matthaeus Kiem <cuer2s@gmail.com>',
    NAME           => 'Term::Choose',
    VERSION_FROM   => 'lib/Term/Choose.pm',
    EXE_FILES      => [],
    BUILD_REQUIRES => {
        'Test::More' => 0,
    },
    PREREQ_PM => {
        'Carp' => 0,
        'Term::ReadKey' => '2.30',
        'Text::LineFold' => '2012.04',
        'Unicode::GCString' => '2012.10',
    },
    clean => { 
        FILES => 'Term-Choose-*',
    },
);
