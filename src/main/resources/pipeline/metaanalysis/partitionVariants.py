#!/usr/bin/env python

import argparse
import math
import platform
import scipy.stats

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col, concat, lit, log, udf  # pylint: disable=E0611

efsdir = '/mnt/efs'


def qnorm(beta, p):
    """
    Quantized function.

    See: https://www.rdocumentation.org/packages/stats/versions/3.5.1/topics/Normal
    """
    return -float(abs(beta) / scipy.stats.norm.ppf(p / 2))


# entry point
if __name__ == '__main__':
    """
    @param dataset e.g. `ExChip_CAMP/T2D`
    """
    print('Python version: %s' % platform.python_version())

    opts = argparse.ArgumentParser()
    opts.add_argument('dataset')

    # parse the command line parameters
    args = opts.parse_args()

    # get the root of the dataset and the phenotype
    root, phenotype = args.dataset.split('/')

    # used often enough that we don't want it screwed up...
    dataset = '%s/%s' % (root, phenotype)

    # get the source and output directories
    srcdir = 's3://dig-analysis-data/variants/%s/%s/variants-*' % (root, phenotype)
    outdir = 'file://%s/metaanalysis/%s/%s' % (efsdir, phenotype, root)

    # create a spark session
    spark = SparkSession.builder.appName('metaanalysis').getOrCreate()

    # load the metadata for the dataset (to get cases, controls, and samples)
    meta = spark.read.json('s3://dig-analysis-data/variants/%s/metadata' % dataset)

    # slurp all the variant batches
    df = spark.read.json(srcdir)

    # make sure that every row has a BETA column
    if 'beta' not in df.columns:
        if 'oddsRatio' in df.columns:
            df = df.withColumn('beta', log('oddsRatio'))
        else:
            df = df.withColumn('beta', None)

    # add the metadata for the dataset to every single variant
    df = df.withColumn('sampleSize', lit(meta.take(1)[0].subjects))

    # create a unique identifier for each variant
    sep = lit(':')
    var_id = concat(
        df.chromosome,
        sep,
        df.position.cast('string'),
        sep,
        df.reference,
        sep,
        df.allele,
    )

    # later the variants will be partitioned by dataset
    dataset = lit(root)

    # creates a unique ID for each variant
    df = df.select(
        var_id.alias('varId'),
        dataset.alias('dataset'),
        col('chromosome'),
        col('position'),
        col('reference'),
        col('allele'),
        col('phenotype'),
        col('ancestry'),
        col('pValue'),
        col('beta'),
        col('freq'),
        col('sampleSize'),
    )

    # calculate the standard error from beta and p-value
    err = udf(qnorm, DoubleType())

    # remove all null pValue and beta values
    df = df \
        .filter(df.pValue.isNotNull()) \
        .filter(df.beta.isNotNull()) \
        .filter(df.freq.isNotNull())

    # calculate the standard error from the beta and p-value
    df = df.withColumn('stderr', err(df.beta, df.pValue))

    # split the variants into rare and common buckets
    rare = df.filter(df.freq < 0.05)
    common = df.filter(df.freq >= 0.05)

    # output the rare variants as a single CSV
    rare.repartition(1) \
        .write \
        .mode('overwrite') \
        .partitionBy('ancestry') \
        .csv('%s/rare' % outdir, sep='\t', header=True, mode='overwrite')

    # output the common variants as a single CSV
    common.repartition(1) \
        .write \
        .mode('overwrite') \
        .partitionBy('ancestry') \
        .csv('%s/common' % outdir, sep='\t', header=True, mode='overwrite')

    # done
    spark.stop()
