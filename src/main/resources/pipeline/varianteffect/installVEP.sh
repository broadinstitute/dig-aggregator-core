#!/bin/bash -xe

# NOTE: This is performed as a STEP instead of a bootstrap step, because AWS
#       will timeout the cluster if the bootstrap takes over 1 hour to run.

# copy the runVEP.sh script from S3 for use by runVEP.pl
aws s3 cp s3://dig-analysis-data/resources/pipeline/varianteffect/runVEP.sh .

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
