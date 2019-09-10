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

# find all the unique tissues from the part files
TISSUES=($(hadoop fs -ls -C "${S3_DIR}/regions/*/*/*/part-*" | xargs dirname | xargs dirname | xargs dirname | xargs -I @ basename "@" | sed -r 's/^biosample=//' | sort | uniq))

# debug to STDOUT all the unique tissues
echo "Unique bio-samples:"
echo "-------------------"
echo "${TISSUES[@]}" | tr ' ' '\n'
echo "-------------------"

# for each tissue, get all the methods used
for TISSUE in "${TISSUES[@]}"; do
  METHODS=($(hadoop fs -ls -C "${S3_DIR}/regions/biosample=${TISSUE}/*/*/part-*" | xargs dirname | xargs dirname | xargs -I @ basename "@" | sed -r 's/^method=([0-9]+_)?//' | sort | uniq))

  # for each method, get all the annotations
  for METHOD in "${METHODS[@]}"; do
    ANNOTATIONS=($(hadoop fs -ls -C "${S3_DIR}/regions/biosample=${TISSUE}/method=${METHOD}/*/part-*" | xargs dirname | xargs -I @ basename "@" | sed -r 's/^annotation=([0-9]+_)?//' | sort | uniq))

    # for each annotation, merge all the part files into a single BED
    for ANNOTATION in "${ANNOTATIONS[@]}"; do
        TMP_FILE=$(mktemp bed.XXXXXX)

        # don't use .bed extension as we'll use the column value of the output for Neo4j
        BED_FILE="${REGIONS_DIR}/${TISSUE}___${METHOD}___${ANNOTATION}"

        # merge all the part files together into the temp file
        hadoop fs -getmerge -skip-empty-file "${S3_DIR}/regions/biosample=${TISSUE}/method=${METHOD}/annotation=${ANNOTATION}/part-*" "${TMP_FILE}"

        # only keep the first 3 columns (chrom, start, end)
        cut --complement -f1,2,3 "${TMP_FILE}" > "${BED_FILE}"

        # add the bed file to the index
        echo "${BED_FILE}" >> "${BED_INDEX_FILE}"
    done
  done
done
