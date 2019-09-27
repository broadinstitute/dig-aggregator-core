#!/usr/bin/python3

import argparse
import platform

from pyspark.sql import SparkSession
from pyspark.sql.functions import col  # pylint: disable=E0611

s3dir = 's3://dig-analysis-data'

# entry point
if __name__ == '__main__':
    """
    Arguments: none
    """
    print('Python version: %s' % platform.python_version())

    # create a spark session
    spark = SparkSession.builder.appName('overlapRegions').getOrCreate()

    # previously run overlapRegions output
    srcdir = '%s/out/overlapregions/overlapped' % s3dir
    outdir = '%s/out/overlapregions/unique' % s3dir

    # load the annotated regions output, but only keep the overlapregion data
    df = spark.read.csv('%s/*/*/part-*' % srcdir, header=True, sep='\t').select(
        col('name'),
        col('chromosome'),
        col('start'),
        col('end'),
    )

    # union, keep distinct, and write then out
    df.distinct() \
        .write \
        .mode('overwrite') \
        .csv('%s/unique' % s3dir, header=True, sep='\t')

    # done
    spark.stop()
