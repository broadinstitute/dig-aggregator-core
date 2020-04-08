#!/usr/bin/python3

import os.path
import platform

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import split, explode, col, substring  # pylint: disable=E0611

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
    df = spark.read.json('%s/effects/*.json' % s3dir) \
        .withColumn('var', explode(col('colocated_variants'))) \
        .filter(substring(col('var.id'), 0, 2) == 'rs') \
        .filter(col('var.allele_string') == col('allele_string')) \
        .select(
            col('id').alias('varId'),
            col('var.id').alias('dbSNP'),
            col('seq_region_name').alias('chromosome'),
            col('start').alias('position'),
            col('allele_string'),
        )

    # split the reference and alternate allele string
    allele = split(df.allele_string, '/')

    # break it up into two columns
    df = df \
        .withColumn('reference', allele.getItem(0)) \
        .withColumn('alt', allele.getItem(1)) \
        .drop(col('allele_string'))

    # output them to HDFS as a CSV file
    df.filter(df.dbSNP.isNotNull()) \
        .write \
        .mode('overwrite') \
        .csv('%s/dbsnp' % s3dir, sep='\t', header=True)

    # done
    spark.stop()
