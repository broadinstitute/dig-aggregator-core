#!/bin/bash -xe

# install utils required for mounting EFS
sudo yum install -y amazon-efs-utils

# mount EFS for large local processes (e.g. METAL)
sudo mkdir -p /mnt/efs
sudo chown hadoop:hadoop /mnt/efs
sudo chmod a+rwx /mnt/efs
sudo mount -t efs fs-06254a4d:/ /mnt/efs
