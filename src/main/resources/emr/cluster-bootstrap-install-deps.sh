#!/bin/bash -xe

#
#amazon-efs-utils already installed in custom AMI      
sudo yum install -y amazon-efs-utils

#TODO: activate ldsc

#
#Pip steps already done in custom AMI
sudo pip install boto==2.39.0
sudo pip install neo4j-driver==1.6.1
sudo pip install scipy==1.1.0