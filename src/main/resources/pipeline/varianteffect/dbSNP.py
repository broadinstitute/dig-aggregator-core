#!/usr/bin/python3

import os.path
import platform

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode_outer

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

    # explode the transcript consequence and existing variants
    df = df \
        .withColumn('cqs', explode_outer(df.transcript_consequences)) \
        .withColumn('snp', explode_outer(df.colocated_variants))

    # is this a dbSNP reference with the same alleles?
    dbSNP_ref = df.snp.id.isNull() | (df.snp.id.startswith('rs') & (df.snp.allele_string == df.allele_string))

    # remove existing variants we don't care about
    df = df.filter(dbSNP_ref)

    # keep only the columns we care about
    df = df.select(
        df.id.alias('varId'),
        df.snp.id.alias('dbSNP'),
        df.most_severe_consequence.alias('mostSevereConsequence'),
        df.cqs.gene_id.alias('gene'),
    )

    # join and output the common effect data
    df.write \
        .mode('overwrite') \
        .json('%s/common' % s3dir)

    # output the dbSNP in CSV format for intake validation
    df.write \
        .mode('overwrite') \
        .csv('%s/dbsnp' % s3dir, sep='\t', header=True)

    # done
    spark.stop()
