#!/bin/bash -xe

PHENOTYPE="$1"

# nuke the aws folder, get ready for new data
aws s3 rm "s3://dig-analysis-data/out/metaanalysis/plots/$PHENOTYPE/" --recursive

# download the shell script to download the associations
aws s3 cp "s3://dig-analysis-data/resources/scripts/getmerge-strip-headers.sh" .
chmod +x ./getmerge-strip-headers.sh

# download the R program that makes the plot(s)
aws s3 cp "s3://dig-analysis-data/resources/pipeline/metaanalysis/manhattan.R" .
chmod +x ./manhattan.R

# where the bottom-line analysis for this phenotype is located
SRCDIR="s3://dig-analysis-data/out/metaanalysis/trans-ethnic/${PHENOTYPE}/*.csv"
OUTDIR="s3://dig-analysis-data/out/metaanalysis/plots/${PHENOTYPE}"

# make sure associations for this phenotype exist
if ! hadoop fs -test -e "${SRCDIR}"; then
  exit 0
fi

# download the associations
./getmerge-strip-headers.sh "${SRCDIR}" METAANALYSIS1.tbl

# write the header: CHR BP P SNP
printf "SNP\tCHR\tBP\tP\n" > associations.tbl

# reformat the associations table
tail -n +2 METAANALYSIS1.tbl | cut --fields=1,2,3,7 >> associations.tbl

# build the plot(s)
if ! ./manhattan.R; then
  aws s3 cp manhattan.tbl "${OUTDIR}/manhattan.tbl"
  exit 1
else
  touch _SUCCESS

  # copy the source data, final plots, and success file to S3
  aws s3 cp manhattan.tbl "${OUTDIR}/manhattan.tbl"
  aws s3 cp manhattan.png "${OUTDIR}/manhattan.png"
  aws s3 cp qq.png "${OUTDIR}/qq.png"
  aws s3 cp _SUCCESS "${OUTDIR}/_SUCCESS"

  # clean up files for future steps
  rm METAANALYSIS1.tbl
  rm associations.tbl
fi
