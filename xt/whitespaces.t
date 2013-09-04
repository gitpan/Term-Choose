use 5.010000;
use strict;
use warnings;

use Test::Whitespaces {

    dirs => [
        'lib',
        't',
        'example',
    ],

    files => [
        'README',
        'Makefile.PL',
        'Build.PL',
        'Changes',
    ],

};
