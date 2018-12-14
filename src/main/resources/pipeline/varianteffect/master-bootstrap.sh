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

# which reference genome to install (see: https://www.gencodegenes.org/human/release_29lift37.html)
GENOME=homo_sapiens_vep_94_GRCh37.tar.gz
FASTA=GRCh37.primary_assembly.genome.fa.gz

# where the VEP reference genome cache and plugins will be installed
VEP_DATA=/mnt/efs/bin/vep_data

# install VCF reader
sudo python3 -m pip install PyVCF

# copy the VEP cache from S3 and unpack it if needed
if [ ! -d "$VEP_DATA/homo_sapiens/94_GRCh37" ] 
then
    mkdir -p "$VEP_DATA"
    cd "$VEP_DATA"
    
    # download the reference genome cache from S3 and unpack it
    aws s3 cp "s3://dig-analysis-data/bin/GRCh37/vep/$GENOME" "$VEP_DATA"
    tar xzf "$GENOME"

    # save money and nuke the genome file
    rm "$GENOME"
fi

# install samtools if needed
if [ ! -d "$VEP_DATA/samtools-1.9" ]
then
    cd "$VEP_DATA"
    
    # download and extract the built version
    aws s3 cp "s3://dig-analysis-data/bin/samtools/samtools-1.9.tar.gz" .
    tar zxvf "samtools-1.9.tar.gz"
fi

# copy the FASTA file and unpack it if needed
if [ ! -d "$VEP_DATA/fasta" ]
then
    mkdir -p "$VEP_DATA/fasta"
    cd "$VEP_DATA/fasta"

    # needs write access for docker to write lock file
    chmod a+w "$VEP_DATA/fasta"

    # download and unpack
    aws s3 cp "s3://dig-analysis-data/bin/GRCh37/fasta/$FASTA" .
    gunzip "$FASTA"
fi

# install the dbNSFP database
if [ ! -d "$VEP_DATA/dbNSFP" ]
then
    mkdir -p "$VEP_DATA/dbNSFP"
    cd "$VEP_DATA/dbNSFP"

    # the database and tabix index file
    aws s3 cp "s3://dig-analysis-data/bin/GRCh37/dbNSFP/dbNSFP_hg19.gz" .
    aws s3 cp "s3://dig-analysis-data/bin/GRCh37/dbNSFP/dbNSFP_hg19.gz.tbi" .
fi

# where VEP plugins will be placed (must be within VEP_DATA)
VEP_PLUGINS="$VEP_DATA/Plugins"

if [ ! -d "$VEP_PLUGINS" ]
then
    sudo yum install -y wget zip

    # create the plugins directory
    mkdir -p "$VEP_PLUGINS"
    cd "$VEP_PLUGINS"

    # download dbNSFP plugin from S3
    aws s3 cp s3://dig-analysis-data/bin/vep/plugins/dbNSFP.pm .

    # download LOF plugin from GitHub, unpack, and link it
    aws s3 cp s3://dig-analysis-data/bin/vep/plugins/loftee-0.3-beta.tar.gz .
    tar zxf loftee-0.3-beta.tar.gz
    ln -s loftee-0.3-beta/LoF.pm .
fi
