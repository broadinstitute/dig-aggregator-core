#!/usr/bin/python3

import argparse
import math
import platform

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import concat_ws, length, lit, when  # pylint: disable=E0611

s3dir = 's3://dig-analysis-data'

# entry point
if __name__ == '__main__':
    """
    By default all datasets will be used, but optionally can be limited to 
    a single study and phenotype.
    """
    print('Python version: %s' % platform.python_version())

    opts = argparse.ArgumentParser()
    opts.add_argument('study', default='*')
    opts.add_argument('phenotype', default='*')

    # parse command line arguments
    args = opts.parse_args()

    # get the source and output directories
    srcdir = '%s/variants/%s/%s' % (s3dir, args.study, args.phenotype)
    outdir = '%s/out/varianteffect/variants' % s3dir

    # create a spark session
    spark = SparkSession.builder.appName('varianteffect').getOrCreate()

    # slurp all the variants across ALL datasets, but only locus information
    df = spark.read.json('%s/part-*' % srcdir) \
        .select(
            'varId',
            'chromosome',
            'position',
            'reference',
            'alt',
        )

    # only keep each variant once, doesn't matter what dataset it came from
    df = df.rdd \
        .keyBy(lambda v: v.varId) \
        .reduceByKey(lambda a, b: a) \
        .map(lambda v: v[1]) \
        .toDF()

    # get the length of the reference and alternate alleles
    ref_len = length(df.reference)
    alt_len = length(df.alt)

    # Calculate the end position from the start and whether there was an
    # insertion or deletion.
    #
    # See: https://useast.ensembl.org/info/docs/tools/vep/vep_formats.html
    #
    end = when(alt_len > ref_len, df.position + alt_len - 1) \
        .otherwise(df.position + ref_len - 1)

    # for inserts, start = end-1
    start = when(alt_len > ref_len, end - 1) \
        .otherwise(df.position)

    # join the reference and alternate alleles together
    allele = concat_ws('/', df.reference, df.alt)
    strand = lit('+')

    # extract only the fields necessary for the VCF file
    df = df.select(
        df.chromosome,
        start,
        end,
        allele,
        strand,
        df.varId,
    )

    # output the variants as CSV part files
    df.write.mode('overwrite').csv(outdir, sep='\t')

    # done
    spark.stop()
