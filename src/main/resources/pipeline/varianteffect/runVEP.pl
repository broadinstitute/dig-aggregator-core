#!/usr/bin/perl

use strict;
use warnings;

# spawn the shell script for each part
foreach my $part (@ARGV) {
    my $pid = fork();

    die "fork: $!" unless defined($pid);

    if (not $pid) {
        exec("bash ./runVEP.sh $part");
    }
}

# wait for all processes to finish
while ((my $pid = wait()) >= 0) {
    die "$pid failed with status: $?" if $? != 0;
}
