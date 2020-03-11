#!/bin/bash -xe

BASSET_ROOT=/mnt/var/basset

# create a gregor directory in /mnt/var to copy data locally
mkdir -p "${BASSET_ROOT}"
chmod 775 "${BASSET_ROOT}"

# install to the metal directory
cd "${BASSET_ROOT}"

# install git and wget
sudo yum -y install git wget

# install HDF5
sudo yum -y install epel-release
sudo yum-config-manager --enable epel
sudo yum -y install hdf5-devel

# where extra tools will be installed to
mkdir -p /home/hadoop/bin
cd /home/hadoop
export PATH=/home/hadoop/bin:$PATH

# download bedtools
wget https://github.com/arq5x/bedtools2/releases/download/v2.29.2/bedtools.static.binary
mv bedtools.static.binary /home/hadoop/bin/bedtools
chmod a+x /home/hadoop/bin/bedtools

# install pre-compiled samtools
aws s3 cp s3://dig-analysis-data/bin/samtools/samtools-1.9.tar.gz .
tar zxf samtools-1.9.tar.gz
ln -s /home/hadoop/samtools-1.9/samtools /home/hadoop/bin/samtools

# install pre-compiled htslib
aws s3 cp s3://dig-analysis-data/bin/samtools/htslib-1.9.tar.gz .
tar zxf htslib-1.9.tar.gz
ln -s /home/hadoop/htslib/tabix /home/hadoop/bin/tabix
ln -s /home/hadoop/htslib/htsfile /home/hadoop/bin/htsfile
ln -s /home/hadoop/htslib/bgzip /home/hadoop/bin/bgzip

# install CUDA
wget http://developer.download.nvidia.com/compute/cuda/10.2/Prod/local_installers/cuda_10.2.89_440.33.01_linux.run
sudo sh cuda_10.2.89_440.33.01_linux.run
mkdir installers
sudo ./cuda_7.5.18_linux.run -extract=`pwd`/installers
cd installers
sudo ./NVIDIA-Linux-x86_64-352.39.run
modprobe nvidia
sudo ./cuda-linux64-rel-7.5.18-19867135.run
sudo ./cuda-samples-linux-7.5.18-19867135.run

# clone the torch repository, build, and install it
git clone https://github.com/torch/distro.git torch --recursive
cd torch
bash install-deps
./install.sh -b -s
ln -s /home/hadoop/torch/install/bin/* /home/hadoop/bin/

# install hdf5 package for torch
cd /home/hadoop
git clone https://github.com/deepmind/torch-hdf5
cd torch-hdf5
luarocks make hdf5-0-0.rockspec

# install inn package for torch
cd /home/hadoop
git clone https://github.com/szagoruyko/imagine-nn
cd imagine-nn
Torch_DIR=/home/hadoop/torch/install/share/cmake/torch/ luarocks make inn-1.0-0.rockspec

# install dp
cd /home/hadoop
luarocks install nn
luarocks install optim
luarocks install cutorch
luarocks install cunn
luarocks install lfs
luarocks install dp
luarocks install dpnn

# install python requirements
sudo python3 -m pip install numpy
sudo python3 -m pip install matplotlib
sudo python3 -m pip install seaborn
sudo python3 -m pip install pandas
sudo python3 -m pip install h5py
sudo python3 -m pip install pysam
sudo python3 -m pip install weblogo

# clone the basset repository

# get a pre-built version of metal from S3
aws s3 cp s3://dig-analysis-data/bin/metal/metal-2018-08-28.tgz /home/hadoop
tar zxf /home/hadoop/metal-2018-08-28.tgz -C /home/hadoop

# install it to the bin folder for use and make it executable
ln -s /home/hadoop/metal/build/metal/metal /home/hadoop/bin/metal

# copy the getmerge-strip-headers shell script from S3
aws s3 cp "s3://dig-analysis-data/resources/scripts/getmerge-strip-headers.sh" /home/hadoop/bin
chmod +x /home/hadoop/bin/getmerge-strip-headers.sh

# copy the runMETAL script from S3
aws s3 cp "s3://dig-analysis-data/resources/pipeline/metaanalysis/runMETAL.sh" /home/hadoop/bin
chmod +x /home/hadoop/bin/runMETAL.sh
