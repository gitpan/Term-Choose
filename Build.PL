use 5.10.1;
use strict;
use warnings;
use Module::Build;

my $builder = Module::Build->new(
    module_name         => 'Term::Choose',
    license             => 'perl',
    dist_author         => q{Kürbis <kuerbis@server>},
    dist_version_from   => 'lib/Term/Choose.pm',
    configure_requires => {
        'Module::Build' => 0,
    },
    build_requires => {
        'Test::More' => 0,
    },
    requires => {
        'perl'              => '5.10.1',
        'Carp'              => 0,
        'Scalar::Util'      => 1.25,
        'Term::ReadKey'     => 2.30, 
        'Unicode::GCString' => 2012.04
    },
    add_to_cleanup      => [ 'Term-Choose-*' ],
    create_makefile_pl => 'traditional',
);

$builder->create_build_script();
