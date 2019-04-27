#!/bin/bash -xe
#
# usage: installGREGOR.sh <ancestry> [r2]
#           where
#               ancestry = "AFR" | "AMR" | "ASN" | "EUR" | "SAN"
#               r2       = "0.2" | "0.7" (default = "0.7")
#

GREGOR_ROOT=/mnt/var/gregor
S3_BUCKET="s3://dig-analysis-data"

# NOTE: This is performed as a STEP instead of a bootstrap step, because AWS
#       will timeout the cluster if the bootstrap takes over 1 hour to run.

# create a gregor directory in /mnt/var to copy data locally
mkdir -p "${GREGOR_ROOT}"
chmod 775 "${GREGOR_ROOT}"

# install to the VEP directory
cd "${GREGOR_ROOT}"

# get the ancestry and r2 value to use
ANCESTRY=$1
R2=${2:-"0.7"}
REF="GREGOR.ref.${ANCESTRY}.LD.ge.${R2}.tar.gz"

# create the ref directory
mkdir -p ref
cd ref

# download and extract the tarball for the 1000g reference
aws s3 cp "${S3_BUCKET}/bin/gregor/${REF}" .
tar zxf "${REF}"

# download and extract GREGOR itself
cd ${GREGOR_ROOT}
aws s3 cp "${S3_BUCKET}/bin/gregor/GREGOR.v1.4.0.tar.gz" .
tar zxf GREGOR.v1.4.0.tar.gz

# download the getmerge-strip-headers script and make it executable
aws s3 cp "${S3_BUCKET}/resources/scripts/getmerge-strip-headers.sh" .
chmod +x getmerge-strip-headers.sh

# NOTE: The SNP list is going to be the same across all regions that it is run
#       against. So, instead of downloading it for every single region, it is
#       downloaded once and re-used over and over.

hadoop fs -getmerge -skip-empty-file "${S3_BUCKET}/out/gregor/snp/part-*" snplist.txt
