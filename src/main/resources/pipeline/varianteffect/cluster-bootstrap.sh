#!/bin/bash -xe

# install utils required for mounting EFS
sudo yum install -y amazon-efs-utils

# mount VEP EFS
sudo mkdir -p /mnt/vep
sudo chown hadoop:hadoop /mnt/vep
sudo mount -t efs fs-35f069d5:/ /mnt/vep

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
cpanm --sudo Archive::Zip
cpanm --sudo DBD::mysql
cpanm --sudo DBI
cpanm --sudo JSON
cpanm --sudo PerlIO::gzip
cpanm --sudo Try::Tiny
cpanm --sudo autodie

# create a vep directory in ~ to copy data locally
mkdir -p /mnt/var/vep
chmod 775 /mnt/var/vep

# copy everything needed for VEP into it (takes a LONG time)
cp -R /mnt/vep/* /mnt/var/vep

# build and install ensembl-xs locally
cd /mnt/var/vep/ensembl-xs
perl Makefile.PL
make
sudo make install
