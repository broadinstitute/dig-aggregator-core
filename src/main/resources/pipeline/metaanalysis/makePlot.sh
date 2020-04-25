#!/bin/bash -xe

PHENOTYPE="$1"

# download the R program that makes the plot(s)
aws s3 cp "s3://dig-analysis-data/resources/pipeline/metaanalysis/manhattan.R" .
chmod +x ./manhattan.R

# download only the p-value meta-analysis results for this phenotype
if ! aws s3 cp "s3://dig-analysis-data/out/metaanalysis/staging/trans-ethnic/$PHENOTYPE/scheme=SAMPLESIZE/METAANALYSIS1.tbl" .; then
    exit 0
fi

# write the header: CHR BP P SNP
printf "CHR\tBP\tP\tSNP\n" > associations.tbl

# reformat the associations table
tail -n +2 METAANALYSIS1.tbl | cut --fields=1,6 | awk '{split($1,m,":"); print m[1]"\t"m[2]"\t"$2"\t"$1}' >> associations.tbl

# build the plot(s)
./manhattan.R

# done
touch _SUCCESS

# copy the final plot(s) and success file to S3
aws s3 cp manhattan.png "s3://dig-analysis-data/out/metaanalysis/plots/$PHENOTYPE/manhattan.png"
aws s3 cp qq.png "s3://dig-analysis-data/out/metaanalysis/plots/$PHENOTYPE/qq.png"
aws s3 cp _SUCCESS "s3://dig-analysis-data/out/metaanalysis/plots/$PHENOTYPE/_SUCCESS"

# clean up files for future steps
rm METAANALYSIS1.tbl
rm associations.tbl
