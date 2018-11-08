#!/usr/bin/env perl

use strict;
use Getopt::Long;

my $instanceId;
my $help = 0;
my $dryRun = 0;

GetOptions(
  'instance-id=s' => \$instanceId,
  'help|?' => \$help,
  'dry-run!' => \$dryRun);

if($help || !$instanceId) {
  die("Terminate an EC2 instance.\nUsage: $ARGV[0] --instance-id <instance-id> [options]\nOptions:\n--help\n--dry-run\n");
}

my $cmd = "aws ec2 terminate-instances --instance-ids $instanceId";

if($dryRun) {
  print "Would have ran: '$cmd'\n";
} else {
  my $output = `$cmd`;

  print $output, "\n";
}
