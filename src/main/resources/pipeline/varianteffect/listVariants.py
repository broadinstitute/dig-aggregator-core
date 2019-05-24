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
    No parameters.
    """
    print('Python version: %s' % platform.python_version())

    # get the source and output directories
    srcdir = '%s/variants/*/*' % s3dir
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
        ) \
        .dropDuplicates(['varId'])

    # get the length of the reference and alternate alleles
    ref_len = length(df.reference)
    alt_len = length(df.alt)

    # Calculate the end position from the start and whether there was an
    # insertion or deletion.
    #
    # See: https://useast.ensembl.org/info/docs/tools/vep/vep_formats.html
    #
    end = when(ref_len == 0, df.position + alt_len - 1) \
        .otherwise(df.position + ref_len - 1)

    # check for insertion
    start = when(ref_len == 0, end + 1) \
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
