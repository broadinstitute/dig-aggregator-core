#!/usr/bin/python3

import argparse
import os.path
import platform

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode_outer, lit, when

# where in S3 VEP data (input and output) is
s3dir = 's3://dig-analysis-data/out/varianteffect'

# entry point
if __name__ == '__main__':
    """
    Arguments: chromosome
    """
    print('Python version: %s' % platform.python_version())
    print('user=%s' % os.getenv('USER'))

    opts = argparse.ArgumentParser()
    opts.add_argument('chromosome')

    args = opts.parse_args()

    # create the spark context
    spark = SparkSession.builder.appName('varianteffect').getOrCreate()

    # read all the variant effects
    df = spark.read.json('%s/effects/part-*.json' % s3dir) \
        .filter(col('seq_region_name') == args.chromosome)

    # explode the existing variants
    df = df.select(df.id, df.most_severe_consequence, df.colocated_variants, df.allele_string) \
        .withColumn('snp', explode_outer(df.colocated_variants)) \
        .withColumn('csq', explode_outer(df.transcript_consequences))

    # the existing dbSNP reference
    has_dbSNP = df.snp.id.startswith('rs') & (df.snp.allele_string == df.allele_string)
    dbSNP = when(has_dbSNP, df.snp.id).otherwise(lit(None))

    # keep only dbSNP refs
    df = df \
        .filter(df.snp.id.startswith('rs') & (df.snp.allele_string == df.allele_string)) \
        .select(
            df.id.alias('varId'),
            dbSNP.alias('dbSNP'),
            df.cqs.gene_id.alias('gene'),
            df.cqs.transcription_id.alias('transcription'),
            df.most_severe_consequence.alias('mostSevereConsequence'),
        )

    # output the dbSNP in CSV format for intake validation
    df.write \
        .mode('overwrite') \
        .json('%s/dbsnp/%s' % (s3dir, args.chromosome))

    # done
    spark.stop()
