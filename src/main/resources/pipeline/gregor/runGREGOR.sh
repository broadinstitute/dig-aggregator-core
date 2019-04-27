#!/bin/bash -xe
#
# usage: runGREGOR.sh <ancestry> [r2]
#           where
#               ancestry = "AFR" | "AMR" | "ASN" | "EUR" | "SAN"
#               r2       = "0.2" | "0.7" (default = "0.7")
#

ANCESTRY=$1
R2=${2:-"0.7"}

# where GREGOR is installed locally
GREGOR_ROOT=/mnt/var/gregor

# various files and directories for the configuration
CONFIG_FILE="${GREGOR_ROOT}/config.txt"
SNP_FILE="${GREGOR_ROOT}/snplist.txt"
BED_INDEX_FILE="${GREGOR_ROOT}/bed.file.index"
REGIONS_DIR="${GREGOR_ROOT}/regions"
REF_DIR="${GREGOR_ROOT}/ref"
OUT_DIR="${GREGOR_ROOT}/out"
S3_DIR="s3://dig-analysis-data/out/gregor"

# clear any existing output previously generated
if [[ -d "${OUT_DIR}" ]]; then
    rm -rf "${OUT_DIR}"
fi

# clear whatever BED files exist from a previous run
if [[ -d "${REGIONS_DIR}" ]]; then
    rm -rf "${REGIONS_DIR}"
fi

# clear the BED index file if it exists from a previous run
if [[ -e "${BED_INDEX_FILE}" ]]; then
    rm "${BED_INDEX_FILE}"
fi

# ensure that the regions directory exists and bed index file
mkdir -p "${REGIONS_DIR}"
touch "${BED_INDEX_FILE}"

# get all unique datasets to process
DATASETS=($(hadoop fs -ls -C "${S3_DIR}/regions/*/part-*" | xargs dirname | xargs -I @ basename "@" | uniq))

# for each dataset, merge all the part files into a single BED
for DATASET in "${!DATASETS[@]}"; do
    ./getmerge-strip-headers.sh "${S3_DIR}/regions/${DATASET}/*.csv" "${REGIONS_DIR}/${DATASET}.bed"

    # append the BED file to the index file
    echo "${REGIONS_DIR}/${DATASET}.bed" >> "${BED_INDEX_FILE}"
done

# write the configuration file for GREGOR
cat > "${CONFIG_FILE}" <<EOF
INDEX_SNP_FILE      = ${SNP_FILE}
BED_FILE_INDEX      = ${BED_INDEX_FILE}
REF_DIR             = ${REF_DIR}
POPULATION          = ${ANCESTRY}
R2THRESHOLD         = ${R2}
OUT_DIR             = ${OUT_DIR}
LDWINDOWSIZE        = 1000000
MIN_NEIGHBOR_NUM    = 500
BEDFILE_IS_SORTED   = True
TOPNBEDFILES        = 2
JOBNUMBER           = 10
BATCHTYPE           = local
EOF

# run GREGOR
perl "${GREGOR_ROOT}/GREGOR/GREGOR.pl" --conf "${CONFIG_FILE}"

# upload output back to S3
aws s3 cp "${OUT_DIR}/StatisticSummaryFile.txt" "${S3_DIR}/summary/${ANCESTRY}/statistics.txt"
