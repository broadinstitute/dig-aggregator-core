#!/bin/bash -xe

# GCC, make, and perl utils
sudo yum group install -y "Development Tools"
sudo yum install -y perl-CPAN
sudo yum install -y perl-DBD-MySQL
sudo yum install -y perlbrew
sudo yum install -y htop

# add perlbrew installed apps to path (for cpanm)
export PATH=$PATH:$HOME/perl5/perlbrew/bin

# install cpanm
perlbrew install-cpanm

# install required perl modules
cpanm --sudo Archive::Zip
cpanm --sudo DBD::mysql
cpanm --sudo DBI
cpanm --sudo JSON
cpanm --sudo PerlIO::gzip
cpanm --sudo Try::Tiny
cpanm --sudo autodie
