#!/usr/bin/python3

import argparse
import platform

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf  # pylint: disable=E0611
from pyspark.sql.types import IntegerType

s3dir = 's3://dig-analysis-data'

# the BED files need to be sorted by chrom/start, this orders the chromosomes
chrom_sort_index = list(map(lambda c: str(c+1), range(22))) + ['X', 'Y', 'XY', 'M']

def chrom_index(c):
    return chrom_sort_index.index(c)

# entry point
if __name__ == '__main__':
    """
    Arguments: <dataset>
    """
    print('Python version: %s' % platform.python_version())

    opts = argparse.ArgumentParser()
    opts.add_argument('dataset', help='Dataset name to create a BED file for')

    # parse command line arguments
    args = opts.parse_args()

    # get the source and output directories
    srcdir = '%s/chromatin_state/%s' % (s3dir, args.dataset)
    outdir = '%s/out/gregor/regions/%s' % (s3dir, args.dataset)

    # create a spark session
    spark = SparkSession.builder.appName('gregor').getOrCreate()

    # custom function used for sorting chromosomes properly
    chrom_index_udf = udf(chrom_index, IntegerType())

    # read all the fields needed across the regions for the dataset
    df = spark.read.json('%s/part-*' % srcdir) \
        .withColumn('chromIndex', chrom_index_udf('chromosome')) \
        .sort('chromIndex', 'start', ascending=True) \
        .select(
            col('chromosome').alias('chrom'),
            col('start').alias('chromStart'),
            col('end').alias('chromEnd'),
            col('name').alias('name'),
        )

    # output the variants in BED format (TSV)
    df.write.mode('overwrite').csv(outdir, sep='\t')

    # done
    spark.stop()
