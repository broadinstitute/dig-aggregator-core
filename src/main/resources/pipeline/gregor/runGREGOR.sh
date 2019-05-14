#!/bin/bash -xe
#
# usage: runGREGOR.sh <ancestry> [r2]
#           where
#               ancestry       = "AFR" | "AMR" | "ASN" | "EUR" | "SAN"
#               r2             = "0.2" | "0.7"
#               phenotype      = "T2D" | "FI" | ...
#               t2dkp_ancestry = "AA" | "HS" | "EA" | "EU" | "SA"
#

ANCESTRY=$1
R2=$2
PHENOTYPE=$3
T2DKP_ANCESTRY=$4

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

# clear the snp list file if it exists from a previous run
if [[ -e "${SNP_FILE}" ]]; then
    rm -rf "${SNP_FILE}"
fi

# clear whatever BED files exist from a previous run
if [[ -d "${REGIONS_DIR}" ]]; then
    rm -rf "${REGIONS_DIR}"
fi

# clear the BED index file if it exists from a previous run
if [[ -e "${BED_INDEX_FILE}" ]]; then
    rm "${BED_INDEX_FILE}"
fi

# source location of SNP part files
SNPS="${S3_DIR}/snp/${PHENOTYPE}/ancestry=${T2DKP_ANCESTRY}/part-*"

# if there are no SNPs for this ancestry and phenotype, just skip it
if ! hadoop fs -test -e "${SNPS}"; then
    exit 0
fi

# Download the SNP list for this phenotype and ancestry
hadoop fs -getmerge -skip-empty-file "${SNPS}" "${SNP_FILE}"

# ensure that the regions directory exists and bed index file
mkdir -p "${REGIONS_DIR}"
touch "${BED_INDEX_FILE}"

# find all the unique tissues from the part files
TISSUES=($(hadoop fs -ls -C "${S3_DIR}/regions/chromatin_state/*/*/part-*" | xargs dirname | xargs dirname | xargs -I @ basename "@" | sed -r 's/^biosample=//' | sort | uniq))

# debug to STDOUT all the unique tissues
echo "Unique bio-samples:"
echo "-------------------"
echo "${TISSUES[@]}" | tr ' ' '\n'
echo "-------------------"

# for each tissue, get all the annotations for it, and join them into tissue+annotation bed files
for TISSUE in "${TISSUES[@]}"; do
    ANNOTATIONS=($(hadoop fs -ls -C "${S3_DIR}/regions/chromatin_state/biosample=${TISSUE}/*/part-*" | xargs dirname | xargs -I @ basename "@" | sed -r 's/^name=([0-9]+_)?//' | sort | uniq))

    # for each annotation, merge all the part files into a single BED
    for ANNOTATION in "${ANNOTATIONS[@]}"; do
        # don't use .bed extension as we'll use the column value of the output for Neo4j
        BED_FILE="${REGIONS_DIR}/${TISSUE}___${ANNOTATION}"

        # merge all the part files together into a single glob
        hadoop fs -getmerge -skip-empty-file "${S3_DIR}/regions/chromatin_state/biosample=${TISSUE}/name=${ANNOTATION}/part-*" "${BED_FILE}"
        echo "${BED_FILE}" >> "${BED_INDEX_FILE}"
    done
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

# for debugging, dump the config file to STDOUT...
cat "${CONFIG_FILE}"

# run GREGOR
cd "${GREGOR_ROOT}/GREGOR/script"
perl GREGOR.pl --conf "${CONFIG_FILE}"

# dump the GREGOR.log file to STDOUT so it's in the log
if [[ -e "${OUT_DIR}/GREGOR.log" ]]; then
    cat "${OUT_DIR}/GREGOR.log"
fi

# upload output back to S3
aws s3 cp "${OUT_DIR}/StatisticSummaryFile.txt" "${S3_DIR}/summary/${PHENOTYPE}/${T2DKP_ANCESTRY}/statistics.txt"
