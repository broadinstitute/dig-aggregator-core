#!/bin/bash -xe
#
# usage: installGREGOR.sh <ancestry> [r2]
#           where
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

# get the r2 value to use
R2=${1:-"0.7"}

# all ancestry REF files need to be installed
ANCESTRIES=(AFR AMR ASN EUR SAN)

# create the ref directory
mkdir -p ref
cd ref

# install each REF file
for ANCESTRY in "ANCESTRIES[@]"; do
    REF="GREGOR.ref.${ANCESTRY}.LD.ge.${R2}.tar.gz"

    # download and extract the tarball for the 1000g reference
    aws s3 cp "${S3_BUCKET}/bin/gregor/${REF}" .
    tar zxf "${REF}"
done

# download and extract GREGOR itself
cd ${GREGOR_ROOT}
aws s3 cp "${S3_BUCKET}/bin/gregor/GREGOR.v1.4.0.tar.gz" .
tar zxf GREGOR.v1.4.0.tar.gz

# download the getmerge-strip-headers script and make it executable
aws s3 cp "${S3_BUCKET}/resources/scripts/getmerge-strip-headers.sh" .
chmod +x getmerge-strip-headers.sh
