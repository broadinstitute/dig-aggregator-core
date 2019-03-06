#!/bin/bash -xe

# NOTE: This is performed as a STEP instead of a bootstrap step, because AWS
#       will timeout the cluster if the bootstrap takes over 1 hour to run.

# install utils required for mounting EFS
sudo yum install -y amazon-efs-utils

# mount VEP EFS
sudo mkdir -p /mnt/vep
sudo chown hadoop:hadoop /mnt/vep
sudo mount -t efs fs-35f069d5:/ /mnt/vep

# create a vep directory in /mnt/var to copy data locally
mkdir -p /mnt/var/vep
chmod 775 /mnt/var/vep

# copy everything needed for VEP into it (takes a LONG time)
cp -R /mnt/vep/* /mnt/var/vep

# build and install ensembl-xs locally
cd /mnt/var/vep/ensembl-xs
perl Makefile.PL
make
sudo make install
