use 5.010000;
use warnings;
use strict;
use utf8;
die "No support for OS" if $^O eq 'MSWin32';
use Module::Build;


my $builder = Module::Build->new(
    module_name         => 'Term::Choose',
    license             => 'perl',
    dist_author         => 'Matthäus Kiem <cuer2s@gmail.com>',
    dist_version_from   => 'lib/Term/Choose.pm',
    configure_requires => {
        'Module::Build' => 0,
    },
    build_requires => {
        'Test::More' => 0,
    },
    requires => {
        'perl'              => '5.010000',
        'Carp'              => 0,
        'Term::ReadKey'     => '2.30',
        'Text::LineFold'    => '2012.04',
        'Unicode::GCString' => '2012.10',
    },
    add_to_cleanup      => [ 'Term-Choose-*' ],
    #create_makefile_pl => 'traditional',
);

$builder->create_build_script();



