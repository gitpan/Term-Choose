use 5.010001;
use warnings;
use strict;
use ExtUtils::MakeMaker;


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
        'Carp'              => 0,
        'Exporter'          => 0,
        'Term::ReadKey'     => 0,
        'Text::LineFold'    => 0,
        'Unicode::GCString' => 0,
    },
    clean => {
        FILES => 'Term-Choose-*',
    },
);
