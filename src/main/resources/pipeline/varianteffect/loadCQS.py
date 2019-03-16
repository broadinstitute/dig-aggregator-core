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
from pyspark.sql.functions import concat_ws, input_file_name, split, explode, col  # pylint: disable=E0611

# where in S3 VEP data (input and output) is
s3dir = 's3://dig-analysis-data/out/varianteffect'

# entry point
if __name__ == '__main__':
    """
    No arguments. Joins across all datasets.
    """
    print('python version=%s' % platform.python_version())
    print('user=%s' % os.getenv('USER'))

    # create the spark context
    spark = SparkSession.builder.appName('varianteffect').getOrCreate()

    # read all the JSON files output by VEP, keep only the consequence data
    df = spark.read.json('%s/effects/*.json' % s3dir)

    # explode transcript consequences
    transcript_consequences = df.select(df.id, df.transcript_consequences) \
        .withColumn('cqs', explode(col('transcript_consequences'))) \
        .select(
            col('id'),
            col('cqs.*'),
        )

    # explode regulatory features
    regulatory_feature_consequences = df.select(df.id, df.regulatory_feature_consequences) \
        .withColumn('cqs', explode(col('regulatory_feature_consequences'))) \
        .select(
            col('id'),
            col('cqs.*'),
        )

    # drop any that aren't "picked" to be the most severe
    transcript_consequences = transcript_consequences \
      .filter(transcript_consequences.pick == 1)
    regulatory_feature_consequences = regulatory_feature_consequences \
      .filter(regulatory_feature_consequences.pick == 1)

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
        .csv('%s/transcript_consequences' % s3dir, sep='\t', header=True)

    # output them to HDFS as a CSV file
    regulatory_feature_consequences.write \
        .mode('overwrite') \
        .csv('%s/regulatory_feature_consequences' % s3dir, sep='\t', header=True)

    # done
    spark.stop()
