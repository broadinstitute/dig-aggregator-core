#!/usr/bin/python3

import platform

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, regexp_replace  # pylint: disable=E0611
from pyspark.sql.types import IntegerType

s3dir = 's3://dig-analysis-data'

# BED files need to be sorted by chrom/start, this orders the chromosomes
chrom_sort_index = list(map(lambda c: str(c+1), range(22))) + ['X', 'Y']

def chrom_index(c):
    return chrom_sort_index.index(c)

# entry point
if __name__ == '__main__':
    """
    Arguments: none
    """
    print('Python version: %s' % platform.python_version())

    # get the source and output directories
    srcdir = '%s/chromatin_state/*/part-*' % s3dir
    outdir = '%s/out/gregor/regions/chromatin_state' % s3dir

    # create a spark session
    spark = SparkSession.builder.appName('gregor').getOrCreate()

    # custom function used for sorting chromosomes properly
    chrom_index_udf = udf(chrom_index, IntegerType())

    # read all the fields needed across the regions for the dataset
    df = spark.read.json(srcdir) \
        .filter(col('chromosome').isin(chrom_sort_index)) \
        .withColumn('chromIndex', chrom_index_udf('chromosome')) \
        .sort('chromIndex', 'start', ascending=True) \
        .select(
            col('chromosome').alias('chrom'),
            col('start').alias('chromStart'),
            col('end').alias('chromEnd'),
            regexp_replace(col('biosample'), ':', '_').alias('biosample'),
            col('name'),
        )

    # output the variants in BED format (TSV)
    df.write\
        .mode('overwrite') \
        .partitionBy('biosample', 'name') \
        .csv(outdir, sep='\t')

    # done
    spark.stop()
