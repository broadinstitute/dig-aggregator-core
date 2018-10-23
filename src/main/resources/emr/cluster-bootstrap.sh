#!/bin/bash -xe
      
sudo yum install -y amazon-efs-utils

sudo mkdir -p /mnt/efs
sudo mount -t efs fs-06254a4d:/ /mnt/efs

sudo pip install boto==2.39.0
sudo pip install neo4j-driver==1.6.1
sudo pip install scipy==1.1.0