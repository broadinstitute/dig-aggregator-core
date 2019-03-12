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

# which version of PLINK to use
set PLINK=plink2_linux_x86_64_20190102.zip

# copy plink to the home directory and extract it
aws s3 cp "s3://dig-analysis-data/bin/plink/$PLINK" "$HOME"
unzip "$HOME/$PLINK" -d "$HOME/plink2"

set WORKDIR=/mnt/efs/bin/1kg
set PHASE=phase1

# copy 1000 genomes if it isn't on EFS yet
if [ ! -d "$WORKDIR" ] 
then
    mkdir -p "$WORKDIR"
    cd "$WORKDIR"
    
    # download the common variants from 1kg
    aws s3 cp "s3://dig-analysis-data/bin/1kg/$PHASE/1kg_${PHASE}_common.bed" .
    aws s3 cp "s3://dig-analysis-data/bin/1kg/$PHASE/1kg_${PHASE}_common.bim" .
    aws s3 cp "s3://dig-analysis-data/bin/1kg/$PHASE/1kg_${PHASE}_common.fam" .

    # save money and nuke the genome file
    rm "$GENOME"
fi
