use Test::More;

use Test::Spelling;

plan( skip_all => 'skip tests on windows' ) if $^O eq 'MSWin32';

add_stopwords(<DATA>);
all_pod_files_spelling_ok();


__DATA__
BackSpace
Ctrl
EOT
Kiem
Matthäus
PageDown
PageUp
ReadKey
SGR
SpaceBar
hjkl
lf
ll
stackoverflow
