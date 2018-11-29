#!/bin/bash -xe

# hack solution to the is-master-node question
#
# see: https://forums.aws.amazon.com/thread.jspa?threadID=222418
#
if [ `grep 'isMaster' /mnt/var/lib/info/instance.json | awk -F ':' '{print $2}' | awk -F ',' '{print $1}'` = 'false' ]
then
	echo "This is not the master node, exiting."
	exit 0
fi

# install docker and start the daemon
sudo yum install -y docker
sudo service docker start

# fetch VEP from docker hub
sudo docker pull ensemblorg/ensembl-vep

# enable users to run docker commands
sudo usermod -a -G docker hadoop

# which reference genome to install
GENOME=homo_sapiens_vep_94_GRCh37.tar.gz

# where the VEP reference genome cache will be installed
VEP_DATA=/mnt/efs/varianteffect/vep_data

# copy the VEP cache from S3 and unpack it if needed
if [ ! -d "$VEP_DATA/homo_sapiens/94_GRCh37" ] 
then
    mkdir -p "$VEP_DATA"
    cd "$VEP_DATA"
    
    # download the reference genome cache from S3 and unpack it
    aws s3 cp "s3://dig-analysis-data/bin/vep/$GENOME" "$VEP_DATA"
    tar xzf "$GENOME"

    # save money and nuke the genome file
    rm "$GENOME"
fi
