#!/usr/bin/python3

import argparse
import math
import platform

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col, concat, lit  # pylint: disable=E0611

efsdir = '/mnt/efs'
s3dir = 's3://dig-analysis-data'

# entry point
if __name__ == '__main__':
    """
    @param dataset e.g. `ExChip_CAMP`
    @param phenotype e.g. `T2D`
    """
    print('Python version: %s' % platform.python_version())

    opts = argparse.ArgumentParser()
    opts.add_argument('dataset')
    opts.add_argument('phenotype')

    # parse the command line parameters
    args = opts.parse_args()

    # get the source and output directories
    srcdir = 's3://dig-analysis-data/variants/%s/%s' % (args.dataset, args.phenotype)
    outdir = 'file://%s/metaanalysis/%s/variants/%s' % (efsdir, args.phenotype, args.dataset)

    # create a spark session
    spark = SparkSession.builder.appName('metaanalysis').getOrCreate()

    # slurp all the variant batches
    df = spark.read.json('%s/part-*' % srcdir)

    # remove all null pValue, beta values
    df = df \
        .filter(df.pValue.isNotNull()) \
        .filter(df.beta.isNotNull())

    # split the variants into rare and common buckets
    rare = df.filter(df.maf.isNotNull() & (df.maf < 0.05))
    common = df.filter(df.maf.isNull() | (df.maf >= 0.05))

    # output the rare variants as a single CSV
    rare.repartition(1) \
        .write \
        .mode('overwrite') \
        .partitionBy('ancestry') \
        .csv('%s/rare' % outdir, sep='\t', header=True)

    # output the common variants as a single CSV
    common.repartition(1) \
        .write \
        .mode('overwrite') \
        .partitionBy('ancestry') \
        .csv('%s/common' % outdir, sep='\t', header=True)

    # done
    spark.stop()
