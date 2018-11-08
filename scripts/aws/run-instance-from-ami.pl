#!/usr/bin/env perl

use strict;
use Getopt::Long;

my $amiId;
my $instanceCount = 1;
my $instanceType = "m3.medium";
my @securityGroups = ("sg-2b58c961");
my $subnet = "subnet-ab89bbf3";
my $help = 0;
my $dryRun = 0;

GetOptions(
  'ami=s' => \$amiId,
  'instance-count=i' => \$instanceCount,
  'instance-type=s' => \$instanceType,
  'security-group=s' => \@securityGroups,
  'subnet' => \$subnet,
  'help|?' => \$help,
  'dry-run!' => \$dryRun);

if($help || !$amiId) {
  die("Run an EC2 instance.\nUsage: $ARGV[0] --ami <ami-id> [options]\nOptions:\n--instance-count N\n--instance-type ...\n--security-group ...\n--subnet ...\n--help\n--dry-run\n");
}

# = ${1?"Run an instance from an AMI.  Usage: $0 <AMI id>"}

my $amazonLinuxAmi = "ami-f316478c";

my $securityGroupString = join(',', @securityGroups);

my $cmd = "aws ec2 run-instances --count $instanceCount --image-id $amiId --instance-type $instanceType --security-group-ids $securityGroupString --subnet-id $subnet --key-name \"GenomeStore REST\" --associate-public-ip-address";

if($dryRun) {
  print "Would have ran: '$cmd'\n";
} else {
  my $output = `$cmd`;

  print $output, "\n";
}
