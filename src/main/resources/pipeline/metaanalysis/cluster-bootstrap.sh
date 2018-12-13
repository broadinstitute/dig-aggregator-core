#!/bin/bash -xe

# install utils required for mounting EFS
sudo yum install -y amazon-efs-utils

# mount EFS for large local processes (e.g. METAL)
sudo mkdir -p /mnt/efs
sudo chown hadoop:hadoop /mnt/efs
sudo chmod 777 /mnt/efs
sudo mount -t efs fs-06254a4d:/ /mnt/efs

# make sure a bin folder exists for the hadoop user
mkdir -p /home/hadoop/bin
mkdir -p /home/hadoop/scripts

# get a pre-built version of metal from S3
aws s3 cp s3://dig-analysis-data/bin/metal/metal-2018-08-28.tgz /home/hadoop
tar zxf /home/hadoop/metal-2018-08-28.tgz -C /home/hadoop

# install it to the bin folder for use and make it executable
ln -s /home/hadoop/metal/build/metal/metal /home/hadoop/bin/metal
