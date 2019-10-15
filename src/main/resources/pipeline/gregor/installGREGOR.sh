#!/bin/bash -xe
#
# usage: installGREGOR.sh <ancestry> [r2]
#           where
#               r2       = "0.2" | "0.7" (default = "0.7")
#

GREGOR_ROOT=/mnt/var/gregor

S3_BUCKET="s3://dig-analysis-data"
S3_DIR="${S3_BUCKET}/out/gregor"

# locations for the region files
BED_INDEX_FILE="${GREGOR_ROOT}/bed.file.index"
REGIONS_DIR="${GREGOR_ROOT}/regions"

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
for ANCESTRY in "${ANCESTRIES[@]}"; do
    REF="GREGOR.ref.${ANCESTRY}.LD.ge.${R2}.tar.gz"

    # download and extract the tarball for the 1000g reference
    aws s3 cp "${S3_BUCKET}/bin/gregor/${REF}" .
    tar zxf "${REF}"
done

# download and extract GREGOR itself
cd ${GREGOR_ROOT}
aws s3 cp "${S3_BUCKET}/bin/gregor/GREGOR.v1.4.0.tar.gz" .
tar zxf GREGOR.v1.4.0.tar.gz

#
# At this point, the output of the SortRegionsProcessor will be the same for every
# run of GREGOR, so we can copy it from HDFS to the local machine once for all the
# subsequent phenotype/ancestry combinations to use.
#
# The BED_INDEX_FILE will be used in every configuration file sent to GREGOR.
#

# ensure that the regions directory exists and bed index file
mkdir -p "${REGIONS_DIR}"
touch "${BED_INDEX_FILE}"

# find all the unique triplet directories of tissue/method/annotation
S3_PATHS=($(hadoop fs -ls -C "${S3_DIR}/regions/sorted/*/*/*/part-*" | xargs dirname | sort | uniq))

echo "Unique paths:"
echo "-------------"
echo "${PATHS[@]}" | tr ' ' '\n'
echo "-------------"

# for each path, parse and fetch
for S3_PATH in "${S3_PATHS[@]}"; do
  BIOSAMPLE=$(dirname "${S3_PATH}" | xargs dirname | xargs basename | awk -F "=" '{print $2}')
  METHOD=$(dirname "${S3_PATH}" | xargs basename | awk -F "=" '{print $2}')
  ANNOTATION=$(basename "${S3_PATH}" | awk -F "=" '{print $2}')

  # create a temporary file to merge into
  TMP_FILE=$(mktemp bed.XXXXXX)

  # merge all the part files together into the temp file
  hadoop fs -getmerge -skip-empty-file "${S3_PATH}/part-*" "${TMP_FILE}"

  # don't use .bed extension as we'll use the column value of the output for Neo4j
  BED_FILE="${REGIONS_DIR}/${BIOSAMPLE}___${METHOD}___${ANNOTATION}"

  # only keep the first 3 columns (chrom, start, end)
  cut -f1,2,3 "${TMP_FILE}" > "${BED_FILE}"

  # add the bed file to the index
  echo "${BED_FILE}" >> "${BED_INDEX_FILE}"
done
