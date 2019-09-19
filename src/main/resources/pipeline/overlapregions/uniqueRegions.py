#!/usr/bin/python3

import argparse
import platform

from pyspark.sql import SparkSession
from pyspark.sql.functions import col  # pylint: disable=E0611

s3dir = 's3://dig-analysis-data/out/overlapregions'

# entry point
if __name__ == '__main__':
    """
    Arguments: none
    """
    print('Python version: %s' % platform.python_version())

    # create a spark session
    spark = SparkSession.builder.appName('overlapRegions').getOrCreate()

    # previously run overlapRegions output
    regions_dir = '%s/regions' % s3dir
    variants_dir = '%s/variants' % s3dir

    # load the regions output, but only keep the overlapregion data
    df_r = spark.read.csv('%s/*/part-*' % regions_dir, header=True, sep='\t').select(
        col('name'),
        col('chromosome'),
        col('start'),
        col('end'),
    )

    # load the variants output, but only keep the overlapregion data
    df_v = spark.read.csv('%s/*/part-*' % variants_dir, header=True, sep='\t').select(
        col('name'),
        col('chromosome'),
        col('start'),
        col('end'),
    )

    # union, keep distinct, and write then out
    df_r.union(df_v).distinct() \
        .write \
        .mode('overwrite') \
        .csv('%s/unique' % s3dir, header=True, sep='\t')

    # done
    spark.stop()
