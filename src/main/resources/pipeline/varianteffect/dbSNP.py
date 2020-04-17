#!/usr/bin/python3

import os.path
import platform

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode_outer, lit, when

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

    # read all the variant effects
    df = spark.read.json('%s/effects/part-*.json' % s3dir)

    # explode the existing variants
    df = df.select(df.id, df.most_severe_consequence, df.colocated_variants, df.allele_string) \
        .withColumn('snp', explode_outer(df.colocated_variants))

    # keep only dbSNP refs
    df = df \
        .filter(df.snp.id.startswith('rs') & (df.snp.allele_string == df.allele_string)) \
        .select(
            df.id.alias('varId'),
            df.snp.id.alias('dbSNP'),
            df.most_severe_consequence.alias('mostSevereConsequence'),
        )

    # output the dbSNP in CSV format for intake validation
    df.repartition(1000) \
        .write \
        .mode('overwrite') \
        .csv('%s/dbsnp' % s3dir, sep='\t', header=True)

    # done
    spark.stop()
