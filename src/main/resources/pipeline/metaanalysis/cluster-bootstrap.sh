#!/bin/bash -xe

sudo mkdir -p /mnt/efs
sudo mount -t efs fs-06254a4d:/ /mnt/efs
