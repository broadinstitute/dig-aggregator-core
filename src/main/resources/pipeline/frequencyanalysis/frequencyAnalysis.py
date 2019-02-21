#!/usr/bin/python3

import argparse
import functools
import platform

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import isnan, lit, when  # pylint: disable=E0611

s3dir = 's3://dig-analysis-data'


def calc_freq(df, ancestry):
    variants = df.filter(df.ancestry == ancestry)

    # find all variants with a value EAF
    eaf = variants.select(variants.varId, variants.eaf) \
        .filter(variants.eaf.isNotNull() & (~isnan(variants.eaf)))

    # find all variants with MAF (or use EAF to calculate MAF)
    maf = variants.select(
            variants.varId,
            when(variants.maf.isNotNull(), variants.maf) \
                .otherwise(
                    when(variants.eaf < 0.5, variants.eaf).otherwise(1.0 - variants.eaf)
                ) \
                .alias('maf')
        ) \
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
                lambda a,b: (a[0] + b.eaf, a[1] + 1),
                lambda a,b: (a[0] + b[0], a[1] + b[1])
            ) \
            .map(lambda v: Row(varId=v[0], eaf=v[1][0] / v[1][1])) \
            .toDF()

    # calculate the average MAF
    if not maf.rdd.isEmpty():
        maf = maf.rdd \
            .keyBy(lambda v: v.varId) \
            .aggregateByKey(
                (0, 0),
                lambda a,b: (a[0] + b.maf, a[1] + 1),
                lambda a,b: (a[0] + b[0], a[1] + b[1])
            ) \
            .map(lambda v: Row(varId=v[0], maf=v[1][0] / v[1][1])) \
            .toDF()

    # join the EAF and MAF values together (outer) and then join locus (inner)
    comb_df = maf.join(eaf, 'varId', 'outer') \
        .join(loci, 'varId', 'inner')

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
    @param phenotype e.g. `T2D`
    """
    print('Python version: %s' % platform.python_version())

    opts = argparse.ArgumentParser()
    opts.add_argument('phenotype')

    # parse the command line parameters
    args = opts.parse_args()

    # get the source and output directories
    srcdir = '%s/variants/*/%s' % (s3dir, args.phenotype)
    outdir = '%s/out/frequencyanalysis/%s' % (s3dir, args.phenotype)

    # create a spark session
    spark = SparkSession.builder.appName('frequencyanalysis').getOrCreate()

    # slurp all the variant batches
    df = spark.read.json('%s/part-*' % srcdir)

    # remove any variants with no (or mixed) ancestry
    df = df.filter(df.ancestry.isNotNull() & (df.ancestry != 'Mixed'))

    # get a list of all the distinct ancestries
    ancestries = df.select(df.ancestry) \
        .distinct() \
        .collect()

    # for each ancestry, calculate average EAF and MAF per variant
    ancestry_dfs = [calc_freq(df, row[0]) for row in ancestries]

    # join all the ancestries together into a single dataframe and write them
    if len(ancestry_dfs) > 0:
        all_freqs = functools.reduce(lambda a, b: a.union(b), ancestry_dfs)

        # write out all the frequencies
        all_freqs \
            .withColumn('phenotype', lit(args.phenotype)) \
            .write \
            .mode('overwrite') \
            .csv(outdir, sep='\t', header=True)

    # done
    spark.stop()
