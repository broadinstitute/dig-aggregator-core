#!/usr/bin/python3

import argparse
import functools
import platform

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, isnan, lit  # pylint: disable=E0611

s3dir = 's3://dig-analysis-data'


def calc_freq(df, ancestry):
    variants = df.filter(df.ancestry == ancestry)

    # find all variants with EAF
    eaf = variants.select(variants.varId, variants.eaf) \
        .filter(variants.eaf.isNotNull() & (~isnan(variants.eaf)))

    # find all variants with MAF
    maf = variants.select(variants.varId, variants.maf) \
        .filter(variants.maf.isNotNull() & (~isnan(variants.maf)))

    # locus information for each variant (to merge later)
    loci = variants.select(
        variants.varId,
        variants.chromosome,
        variants.position,
        variants.reference,
        variants.alt,
    )

    # calculate the average EAF
    if not eaf.rdd.isEmpty():
        eaf = eaf.rdd \
            .keyBy(lambda v: v.varId) \
            .aggregateByKey(
                (0, 0),
                lambda a, b: (a[0] + b.eaf, a[1] + 1),
                lambda a, b: (a[0] + b[0], a[1] + b[1])
            ) \
            .map(lambda v: Row(varId=v[0], eaf=v[1][0] / v[1][1])) \
            .toDF()

    # calculate the average MAF
    if not maf.rdd.isEmpty():
        maf = maf.rdd \
            .keyBy(lambda v: v.varId) \
            .aggregateByKey(
                (0, 0),
                lambda a, b: (a[0] + b.maf, a[1] + 1),
                lambda a, b: (a[0] + b[0], a[1] + b[1])
            ) \
            .map(lambda v: Row(varId=v[0], maf=v[1][0] / v[1][1])) \
            .toDF()

    # MAF should always be present, EAF is optional
    comb_df = maf \
        .join(eaf, 'varId', 'left_outer') \
        .join(loci, 'varId') \
        .distinct()

    # final dataframe for this ancestry
    return comb_df.select(
        comb_df.varId,
        comb_df.chromosome,
        comb_df.position,
        comb_df.reference,
        comb_df.alt,
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

    # load variants from all datasets
    calc_freq(spark.read.json('%s/part-*' % srcdir), args.ancestry).write \
        .mode('overwrite') \
        .csv('%s/%s' % (outdir, args.ancestry), sep='\t', header=True)

    # done
    spark.stop()
