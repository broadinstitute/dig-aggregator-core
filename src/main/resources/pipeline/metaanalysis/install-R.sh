#!/bin/bash -xe

INSTALL_ROOT=/mnt/var/install

sudo yum groupinstall "Development Tools" -y
sudo yum install zlib-devel bzip2-devel xz-devel -y
sudo yum install pcre-devel curl-devel readline-devel -y
sudo yum install libtiff-devel libjpeg-devel libpng-devel -y
sudo yum install pango-devel cairo-devel ghostscript-devel -y

# install to home directory
mkdir -p "${INSTALL_ROOT}"
chmod 775 "${INSTALL_ROOT}"
cd "${INSTALL_ROOT}"

# build R from sources
wget https://cloud.r-project.org/src/base/R-3/R-3.6.3.tar.gz
tar zxf R-3.6.3.tar.gz
cd R-3.6.3
./configure --with-x=no
make

# install packages
sudo "${INSTALL_ROOT}/R-3.6.3/bin/R" -e "install.packages('qqman', dependencies=TRUE, repo='http://cran.r-project.org')"
