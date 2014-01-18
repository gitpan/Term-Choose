use 5.010001;
use warnings;
use strict;
use Module::Build;


my $builder = Module::Build->new(
    module_name         => 'Term::Choose',
    license             => 'perl',
    dist_author         => 'Matthaeus Kiem <cuer2s@gmail.com>',
    dist_version_from   => 'lib/Term/Choose.pm',
    configure_requires => {
        'Module::Build'     => 0,
    },
    build_requires => {
        'Test::More'        => 0,
    },
    requires => {
        'perl'              => '5.010001',
        'Carp'              => 0,
        'Exporter'          => 0,
        'Term::ReadKey'     => 0,
        'Text::LineFold'    => 0,
        'Unicode::GCString' => 0,
    },
    add_to_cleanup      => [ 'Term-Choose-*' ],
    #create_makefile_pl => 'traditional',
);

$builder->create_build_script();
