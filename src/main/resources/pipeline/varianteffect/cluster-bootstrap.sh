#!/bin/bash -xe

# VEP requires GCC, make 
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
cpanm --sudo --force Archive::Zip
cpanm --sudo --force DBD::mysql
cpanm --sudo --force DBI
cpanm --sudo --force JSON
cpanm --sudo --force PerlIO::gzip
cpanm --sudo --force Try::Tiny
cpanm --sudo --force autodie
