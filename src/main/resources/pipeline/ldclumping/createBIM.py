#!/usr/bin/python3

import argparse
import glob
import os.path
import platform
import re
import shutil
import subprocess
import sys

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, isnan, lit, when  # pylint: disable=E0611

# s3 path to 1000 genomes
s3_path = 's3://dig-analysis-data'

# this is the schema written out by the variant partition process
variants_schema = StructType(
    [
        StructField('varId', StringType(), nullable=False),
        StructField('chromosome', StringType(), nullable=False),
        StructField('position', IntegerType(), nullable=False),
        StructField('reference', StringType(), nullable=False),
        StructField('alt', StringType(), nullable=False),
        StructField('phenotype', StringType(), nullable=False),
        StructField('pValue', DoubleType(), nullable=False),
        StructField('beta', DoubleType(), nullable=False),
        StructField('eaf', DoubleType(), nullable=False),
        StructField('maf', DoubleType(), nullable=False),
        StructField('stdErr', DoubleType(), nullable=False),
        StructField('n', DoubleType(), nullable=False),
    ]
)

# entry point
if __name__ == '__main__':
    """
    Arguments: <phenotype>
    """
    print('python version=%s' % platform.python_version())
    print('user=%s' % os.getenv('USER'))

    opts = argparse.ArgumentParser()
    opts.add_argument('phenotype', help='the phenotype to create a BIM for')

    # parse command line arguments
    args = opts.parse_args()

    # create the spark context
    spark = SparkSession.builder.appName('LDClumping').getOrCreate()

    # where to pull variants from and where to write them out
    srcdir = '%s/out/metaanalysis/ancestry-specifc/%s/*' % (s3_path, args.phenotype)
    outdir = '%s/out/ldclumping/%s' % (s3_path, args.phenotype)

    # load the ancestry-specific meta-analysis for this phenotype
    df = spark.read.csv(srcdir, sep='\t', schema=variants_schema)

    # select the variants for the BIM file
    bim = df.select(
        df.chromosome,
        df.varId,
        lit(0),
        df.position,
        df.reference,
        df.alt,
    )

    # select the effects for the annotation file
    annotation = df.select(
        df.chromosome.alias('CHROM'),
        df.varId.alias('SNP'),
        df.position.alias('BP'),
        df.reference.alias('A1'),
        lit(0).alias('F_A'),
        lit(0).alias('F_U'),
        df.alt.alias('A2'),
        lit(0).alias('CHISQ'),
        df.pValue.alias('P'),
        df.beta.alias('BETA'),
    )

    # write out the BIM
    bim.write.csv('%s/bim' % outdir, sep='\t')

    # write out the annoation
    annotation.write.csv('%s/annotation' % outdir, sep='\t')
