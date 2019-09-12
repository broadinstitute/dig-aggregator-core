#!/bin/bash -xe

PHENOTYPE="$1"

# output HDFS location
S3_PATH="s3://dig-analysis-data/out/metaanalysis"

# working directory
LOCAL_DIR="/mnt/var/metal"

# read and output directories
SRCDIR="${S3_PATH}/ancestry-specific/${PHENOTYPE}"
OUTDIR="${LOCAL_DIR}/trans-ethnic/${PHENOTYPE}"

# local scripts
RUN_METAL="/home/hadoop/bin/runMETAL.sh"
GET_MERGE="/home/hadoop/bin/getmerge-strip-headers.sh"

# start with a clean working directory
rm -rf "${OUTDIR}"
mkdir -p "${OUTDIR}"

# find all the variants processed by the ancestry-specific step
PARTS=($(hadoop fs -ls -C "${SRCDIR}/*/part-*"))

# find all the ancestries processed
ANCESTRIES=($(printf '%s\n' "${PARTS[@]}" | xargs dirname | awk -F "=" '{print $NF}' | sort | uniq))

# if no variants/ancestries found then exit (nothing to do)
if [[ "${#ANCESTRIES[@]}" -eq 0 ]]; then
    exit 0
fi

# for each ancestry, merge all the results into a single file
for ANCESTRY in "${ANCESTRIES[@]}"; do
    GLOB="${SRCDIR}/ancestry=${ANCESTRY}/part-*"
    ANCESTRY_DIR="${OUTDIR}/ancestry=${ANCESTRY}"

    # create the destination directory and merge variants there
    mkdir -p "${ANCESTRY_DIR}"
    bash "${GET_MERGE}" "${GLOB}" "${ANCESTRY_DIR}/variants.csv"
done

# where to run the analysis
ANALYSIS_DIR="${OUTDIR}/_analysis"

# collect all the input files together into an array
INPUT_FILES=($(find "${OUTDIR}" -name "variants.csv" | xargs realpath))

# run METAL across all ancestries with OVERLAP OFF
bash "${RUN_METAL}" "SAMPLESIZE" "OFF" "${ANALYSIS_DIR}" "${INPUT_FILES[@]}"
bash "${RUN_METAL}" "STDERR" "OFF" "${ANALYSIS_DIR}" "${INPUT_FILES[@]}"