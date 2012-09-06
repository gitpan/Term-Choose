#!perl -T

use 5.10.1;
use strict;
use warnings;
use Test::More;

unless ( $ENV{RELEASE_TESTING} ) {
    plan( skip_all => "Author tests not required for installation" );
}

my @long = ( qw( pad pad_one_row empty_string undef length_longest cursor max_list screen_width ) );
my @simple = ( qw( right_justify layout vertical clear_screen page mouse_mode beep hide_cursor ) ); # prompt
my @all = ( @long, @simple );

plan tests => 2 + scalar @all;


my $file = 'lib/Term/Choose.pm';
my $fh;
my %option_default;

open $fh, '<', $file or die $!;
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
    open $fh, '<', $file or die $!;
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
    for my $line ( @{$pod{$key}} ) {
        if ( $line =~ /default:\s('[^']+'|[\w]+)(?:\)|\s*)/ ) {
            $pod_default{$key} = $1;
            last;
        }
    }
}
 
 
 
is( scalar @all, scalar keys %option_default, 'scalar @all == scalar keys %option_default' );
is( scalar keys %pod_default, scalar keys %option_default, 'scalar keys %pod_default == scalar keys %option_default' );
 
 
for my $key ( sort keys %option_default ) {
    is( $option_default{$key}, $pod_default{$key}, "option $key: default value in pod matches default value in code" );
}


