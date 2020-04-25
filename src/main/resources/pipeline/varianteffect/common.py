#!/usr/bin/python3

import os.path
import platform

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode_outer, struct

# where in S3 VEP data (input and output) is
s3dir = 's3://dig-analysis-data/out/varianteffect'

# entry point
if __name__ == '__main__':
    """
    Arguments: chromosome
    """
    print('Python version: %s' % platform.python_version())
    print('user=%s' % os.getenv('USER'))

    # create the spark context
    spark = SparkSession.builder.appName('varianteffect').getOrCreate()

    # load the common effect data
    df = spark.read.json('%s/effects/*.json' % s3dir).select(
        col('id'),
        col('allele_string'),
        col('most_severe_consequence'),
        col('colocated_variants'),
        col('transcript_consequences'),
    )

    # extract common data
    df = df \
        .withColumn('snp', explode_outer(df.colocated_variants)) \
        .withColumn('cqs', explode_outer(df.transcript_consequences)) \
        .select(
            col('id'),
            col('allele_string'),
            col('most_severe_consequence').alias('consequence'),
            col('snp.id').alias('dbSNP'),
            col('snp.allele_string').alias('dbSNP_allele'),
            col('cqs.gene_id').alias('gene'),
            col('cqs.transcript_id').alias('transcript'),
            col('cqs.impact').alias('impact'),
        )

    # condition for removing extraneous, existing variants
    is_dbSNP = col('dbSNP').startswith('rs') & (col('dbSNP_allele') == col('allele_string'))

    # consequence structure
    cqs = struct(
        col('consequence').alias('term'),
        col('gene'),
        col('transcript'),
        col('impact'),
    )

    # remove non-dbSNP rows; add consequence structure
    df = df.filter(col('dbSNP').isNull() | is_dbSNP) \
        .select(
            col('id').alias('varId'),
            col('dbSNP'),
            col('consequence'),
            col('gene'),
            col('transcript'),
            col('impact'),
        )

    # create a frame of just dbSNPs for intake processor comparison
    # dbSNP_ref = df.filter(col('dbSNP').isNotNull()) \
    #     .select(
    #         col('varId'),
    #         col('dbSNP'),
    #     )

    # output the common data in json format
    df.write \
        .mode('overwrite') \
        .csv('%s/common' % s3dir, sep='\t', header=True)

    # output the dbSNP data in csv format
    # dbSNP_ref.write \
    #     .mode('overwrite') \
    #     .csv('%s/dbsnp' % s3dir, sep='\t')

    # done
    spark.stop()
