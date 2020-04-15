#!/usr/bin/python3

import argparse
import platform

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, isnan, lit  # pylint: disable=E0611

s3dir = 's3://dig-analysis-data'


def calc_freq(df, ancestry):
    variants = df.filter(df.ancestry == ancestry)

    # no variants exist for this ancestry?
    if variants.rdd.isEmpty():
        return

    # find all variants with EAF or MAF
    eaf = variants.filter(variants.eaf.isNotNull() & (~isnan(variants.eaf)))
    maf = variants.filter(variants.maf.isNotNull() & (~isnan(variants.maf)))

    # find the max N per dataset
    n = variants.groupBy('dataset').max('n') \
        .select(
            col('dataset'),
            col('max(n)').alias('n')
        )

    # calculate the average EAF across traits per variant/dataset pair
    eaf = eaf.groupBy('varId', 'dataset').avg('eaf') \
        .select(
            col('varId'),
            col('dataset'),
            col('avg(eaf)').alias('eaf')
        )

    # calculate the average MAF across traits per variant/dataset pair
    maf = maf.groupBy('varId', 'dataset').avg('maf') \
        .select(
            col('varId'),
            col('dataset'),
            col('avg(maf)').alias('maf')
        )

    # calculate the weighted EAF average across datasets
    eaf = eaf.join(n, 'dataset') \
        .rdd \
        .keyBy(lambda v: v.varId) \
        .aggregateByKey(
            (0, 0),
            lambda a, b: (a[0] + (b.eaf * b.n), a[1] + b.n),
            lambda a, b: (a[0] + b[0], a[1] + b[1])
        ) \
        .map(lambda v: Row(varId=v[0], eaf=v[1][0] / v[1][1])) \
        .toDF()

    # calculate the weighted MAF average across datasets
    maf = maf.join(n, 'dataset') \
        .rdd \
        .keyBy(lambda v: v.varId) \
        .aggregateByKey(
            (0, 0),
            lambda a, b: (a[0] + (b.maf * b.n), a[1] + b.n),
            lambda a, b: (a[0] + b[0], a[1] + b[1])
        ) \
        .map(lambda v: Row(varId=v[0], maf=v[1][0] / v[1][1])) \
        .toDF()

    # MAF should always be present, EAF is optional
    comb_df = maf.join(eaf, 'varId', 'left_outer')

    # final frame for this ancestry
    return comb_df.select(
        comb_df.varId,
        comb_df.eaf,
        comb_df.maf,
        lit(ancestry).alias('ancestry'),
    )


# entry point
if __name__ == '__main__':
    """
    Arguments: ancestry
    """
    print('Python version: %s' % platform.python_version())

    # parse arguments
    opts = argparse.ArgumentParser()
    opts.add_argument('ancestry', help='Ancestry')
    args = opts.parse_args()

    # get the source and output directories
    srcdir = '%s/variants/*/*' % s3dir
    outdir = '%s/out/frequencyanalysis' % s3dir

    # create a spark session
    spark = SparkSession.builder.appName('frequencyanalysis').getOrCreate()

    # load variants from all datasets (returns None if ancestry has no variants)
    freq_df = calc_freq(spark.read.json('%s/part-*' % srcdir), args.ancestry)

    if freq_df:
        freq_df.write \
            .mode('overwrite') \
            .json('%s/%s' % (outdir, args.ancestry))

    # done
    spark.stop()
