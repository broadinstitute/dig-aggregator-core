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

# install PCR2, which R-4.0.0 needs
sudo yum install -y http://mirror.centos.org/centos/7/os/x86_64/Packages/pcre2-10.23-2.el7.x86_64.rpm
sudo yum install -y http://mirror.centos.org/centos/7/os/x86_64/Packages/pcre2-utf16-10.23-2.el7.x86_64.rpm
sudo yum install -y http://mirror.centos.org/centos/7/os/x86_64/Packages/pcre2-utf32-10.23-2.el7.x86_64.rpm
sudo yum install -y http://mirror.centos.org/centos/7/os/x86_64/Packages/pcre2-devel-10.23-2.el7.x86_64.rpm

# build R from sources
wget https://cloud.r-project.org/src/base/R-4/R-4.0.0.tar.gz
tar zxf R-4.0.0.tar.gz
cd R-4.0.0
./configure --with-x=no
make

# install packages
sudo "${INSTALL_ROOT}/R-4.0.0/bin/R" -e "install.packages('qqman', dependencies=TRUE, repo='http://cran.r-project.org')"
