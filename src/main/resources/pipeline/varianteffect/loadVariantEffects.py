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
from pyspark.sql.functions import concat_ws, split, explode, col  # pylint: disable=E0611

# where in S3 VEP data (input and output) is
s3dir = 's3://dig-analysis-data/out/varianteffect'

# local directory where the analysis will be performed
localdir = '/mnt/efs/varianteffect'

# entry point
if __name__ == '__main__':
    """
    No arguments.
    """
    print('python version=%s' % platform.python_version())
    print('user=%s' % os.getenv('USER'))

    # source location in S3 where the input and output tables are
    srcdir = '%s/effects' % localdir
    outdir = '%s/effects' % s3dir

    # create the spark context
    spark = SparkSession.builder.appName('VariantEffect').getOrCreate()

    # read all the JSON files output by VEP, keep only the consequence data
    df = spark.read.json('file://%s/part-*' % srcdir)

    # explode transcript consequences
    transcript_consequences = df.select(df.id, df.transcript_consequences) \
        .withColumn('cqs', explode(col('transcript_consequences'))) \
        .select(
            col('id'),
            col('cqs.*'),
        )

    # explode regulatory consequences
    regulatory_feature_consequences = df.select(df.id, df.regulatory_feature_consequences) \
        .withColumn('cqs', explode(col('regulatory_feature_consequences'))) \
        .select(
            col('id'),
            col('cqs.*'),
        )

    # drop any consequences that aren't "picked" to be the most severe
    # df = df.filter(df.pick == 1)

    # comma-separate array fields
    transcript_consequences = transcript_consequences \
        .drop(col('domains')) \
        .withColumn('flags', concat_ws(',', col('flags'))) \
        .withColumn('consequence_terms', concat_ws(',', col('consequence_terms')))

    # comma-separate array fields
    regulatory_feature_consequences = regulatory_feature_consequences \
        .withColumn('consequence_terms', concat_ws(',', col('consequence_terms')))

    # output them to HDFS as a CSV file
    transcript_consequences.write \
        .mode('overwrite') \
        .csv('%s/transcript_consequences' % outdir, sep='\t', header=True)

    # output them to HDFS as a CSV file
    regulatory_feature_consequences.write \
        .mode('overwrite') \
        .csv('%s/regulatory_feature_consequences' % outdir, sep='\t', header=True)

    # done
    spark.stop()
