use 5.010001;
use strict;
use warnings;
use autodie;
use Test::More;

unless ( $ENV{RELEASE_TESTING} ) {
    plan( skip_all => "Author tests not required for installation" );
}

my @long = ( qw( pad pad_one_row empty_string undef length_longest default limit screen_width ) );
my @simple = ( qw( justify layout order clear_screen page mouse_mode beep hide_cursor index ) ); # prompt
my @all = ( @long, @simple );

plan tests => 2 + scalar @all;


my $file = 'lib/Term/Choose.pm';
my $fh;
my %option_default;

open $fh, '<', $file;
while ( my $line = readline $fh ) {
    if ( $line =~ /\Asub _set_layout {/ .. $line =~ /\A\s+return\s\$config;/ ) {
        if ( $line =~ m|\A\s+#?\s*\$config->{(\w+)}\s+//=\s(.*);| ) {
            next if $1 eq 'prompt';
            $option_default{$1} = $2;
         }
    }
}
close $fh;

   
my %pod_default;
my %pod;

for my $key ( @all ) {
    open $fh, '<', $file;
    while ( my $line = readline $fh ) {
        if ( $line =~ /\A=head4\s\Q$key\E/ ... $line =~ /\A=head/ ) {
            chomp $line;
            next if $line =~ /\A\s*\z/;
            push @{$pod{$key}}, $line;
        }
    }
    close $fh;
}

for my $key ( @simple ) {
    my $opt;
    for my $line ( @{$pod{$key}} ) {
        if ( $line =~ /(\d).*\(default\)/ ) {
            $pod_default{$key} = $1;
            last;
        }
    }
}

for my $key ( @long ) {          
    if ( $key eq 'pad_one_row' ) {
        for my $line ( @{$pod{$key}} ) {
            if ( $line =~ /default:\s([^)]+)\)/ ) {
                $pod_default{$key} = $1;
                last;
            }
        }   
    }
    else {
        for my $line ( @{$pod{$key}} ) {
            if ( $line =~ /default:\s('[^']+'|\w+)(?:\)|\s*)/ ) {
                $pod_default{$key} = $1;
                last;
            }
        }
    }
}
 
 
 
is( scalar @all, scalar keys %option_default, 'scalar @all == scalar keys %option_default' );
is( scalar keys %pod_default, scalar keys %option_default, 'scalar keys %pod_default == scalar keys %option_default' );
 
 
for my $key ( sort keys %option_default ) {
    if ( $key eq 'pad_one_row' ) {
        my $por = 0;
        if ( $option_default{$key} eq '$config->{pad}' and $pod_default{$key} eq 'value of the option I<pad>' ) {
            $por = 1;
        }
        is( $por, '1', "option $key: default value in pod OK" );
    }
    else {
        is( $option_default{$key}, $pod_default{$key}, "option $key: default value in pod matches default value in code" );
    }
}


