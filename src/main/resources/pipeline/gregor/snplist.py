#!/usr/bin/python3

import argparse
import platform
import subprocess

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, input_file_name, regexp_extract  # pylint: disable=E0611
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

s3dir = 's3://dig-analysis-data'

# GREGOR doesn't work on XY, M, or MT chromosomes.
valid_chromosomes = list(map(lambda c: str(c+1), range(22))) + ['X', 'Y']

# output schema of meta-analysis
meta_analysis_schema = StructType(
    [
        StructField('varId', StringType(), nullable=False),
        StructField('chromosome', StringType(), nullable=False),
        StructField('position', IntegerType(), nullable=False),
        StructField('reference', StringType(), nullable=False),
        StructField('alt', StringType(), nullable=False),
        StructField('phenotype', StringType(), nullable=False),
        StructField('pValue', DoubleType(), nullable=False),
        StructField('beta', DoubleType(), nullable=False),
        StructField('stdErr', DoubleType(), nullable=False),
        StructField('n', DoubleType(), nullable=False),
    ]
)


def test_glob(glob):
    """
    Searches for part files using a path glob.
    """
    cmd = ['hadoop', 'fs', '-test', '-e', glob]
    print('Running: ' + str(cmd))

    # if it returns non-zero, then the files didn't exist
    status = subprocess.call(cmd)
    print('Return code: ' + str(status))

    return status == 0


# entry point
if __name__ == '__main__':
    """
    Arguments: <phenotype>
    """
    print('Python version: %s' % platform.python_version())

    opts = argparse.ArgumentParser()
    opts.add_argument('phenotype')

    # get command line arguments
    args = opts.parse_args()

    # get the source and output directories
    srcdir = '%s/out/metaanalysis/ancestry-specific/%s/*/part-*' % (s3dir, args.phenotype)
    outdir = '%s/out/gregor/snp/%s' % (s3dir, args.phenotype)

    spark = SparkSession.builder.appName('gregor').getOrCreate()

    # The algorithm here is to find all the best SNPs on each chromosome, but only
    # SNPs within a given range of each other (e.g. 500 kb). The following
    # algorithm is used:
    #
    #   1. Create an empty DataFrame of SNPs that will serve as the output
    #   2. For each chromosome:
    #      a. find all SNPs < p-value threshold (source DF)
    #      b. find the best SNP (lowest p-value) a put it into the output DF
    #      c. remove all SNPs within the given range of (b) from the source DF
    #      d. if the source DF is not empty, goto 2b.
    #   3. The output DF now contains all the SNPs to use.

    output_schema = StructType([
        StructField('SNP', StringType()),
        StructField('ancestry', StringType()),
    ])

    # create the output SNP data frame
    output_SNPs = []

    # NOTE: It's possible that srcdir doesn't actually exist or contain anything!
    #       This is because meta-analysis filters data. For example, perhaps a
    #       dataset doesn't contain any BETA or P-VALUE data, in which case it
    #       will be successfully run through the meta-analysis processor, but it
    #       won't write anything out or get through ancestry-specific analysis.
    #
    #       When that happens, it's OK to just have an empty output SNP list.

    if test_glob(srcdir):
        df = spark.read.csv(srcdir, sep='\t', header=True, schema=meta_analysis_schema) \
            .withColumn('filename', input_file_name()) \
            .withColumn('ancestry', regexp_extract('filename', r'/ancestry=([^/]+)/', 1)) \
            .filter(col('chromosome').isin(valid_chromosomes)) \
            .filter(col('pValue') < 5.0e-8)

        # get a distinct list of each ancestries
        ancestries = df.select(df.ancestry) \
            .distinct() \
            .collect()

        # loop over each chromosome and ancestry to get the SNPs for it
        for chromosome in valid_chromosomes:
            for a_row in ancestries:
                ancestry = a_row.ancestry

                # Collect all the SNPs on this chromosome for this ancestry.
                #
                # Sort all the SNPs by p-value so the lowest p-value is last. This improves
                # performance considerably over using `min()` each iteration as we can just
                # grab the last SNP in the list, then `filter()`, which is stable.
                source_SNPs = df.filter((df.chromosome == chromosome) & (df.ancestry == ancestry)) \
                    .select(df.chromosome, df.position, df.pValue) \
                    .sort(df.pValue.desc()) \
                    .collect()

                # find the lowest p-value SNP
                while len(source_SNPs) > 0:
                    best = source_SNPs.pop()

                    # add the best SNP to the list of output SNPs
                    output_SNPs.append(Row(SNP='%s:%d' % (best.chromosome, best.position), ancestry=ancestry))

                    # remove all SNPs from the source list within a given range
                    source_SNPs = list(filter(lambda row: abs(row.position - best.position) > 500000, source_SNPs))

    # output the variants as CSV part files
    spark.createDataFrame(output_SNPs, output_schema) \
        .write \
        .mode('overwrite') \
        .partitionBy('ancestry') \
        .csv(outdir, sep='\t')

    # done
    spark.stop()
