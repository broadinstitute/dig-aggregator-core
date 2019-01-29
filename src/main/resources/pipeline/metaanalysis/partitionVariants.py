#!/usr/bin/python3

import argparse
import math
import platform

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col, concat, lit, when  # pylint: disable=E0611

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
    srcdir = '%s/variants/%s/%s' % (s3dir, args.dataset, args.phenotype)
    outdir = '%s/out/metaanalysis/variants/%s/%s' % (s3dir, args.phenotype, args.dataset)

    # create a spark session
    spark = SparkSession.builder.appName('metaanalysis').getOrCreate()

    # slurp all the variant batches
    df = spark.read.json('%s/part-*' % srcdir)

    # if ancestry isn't set, assume it's "Mixed"
    ancestry = when(df.ancestry.isNotNull(), df.ancestry).otherwise(lit('Mixed'))

    # remove all multi-allelic variants, mixed-ancestry variants, and any with
    # null p or beta values, select the order of the columns so when they are
    # written out in part files without a header it will be known exactly what
    # order they are in
    df = df \
        .filter(df.multiAllelic.isNull() | (df.multiAllelic == False)) \
        .filter(df.pValue.isNotNull()) \
        .filter(df.beta.isNotNull()) \
        .select(
            df.varId,
            df.chromosome,
            df.position,
            df.reference,
            df.alt,
            df.phenotype,
            ancestry.alias('ancestry'),
            df.pValue,
            df.beta,
            df.eaf,
            df.maf,
            df.stdErr,
            df.n,
        )

    # split the variants into rare and common buckets
    rare = df.filter(df.maf.isNotNull() & (df.maf < 0.05))
    common = df.filter(df.maf.isNull() | (df.maf >= 0.05))

    # output the rare variants as CSV part files
    rare.write \
        .mode('overwrite') \
        .partitionBy('ancestry') \
        .csv('%s/rare' % outdir, sep='\t', header=True)

    # output the common variants as CSV part files
    common.write \
        .mode('overwrite') \
        .partitionBy('ancestry') \
        .csv('%s/common' % outdir, sep='\t', header=True)

    # done
    spark.stop()
