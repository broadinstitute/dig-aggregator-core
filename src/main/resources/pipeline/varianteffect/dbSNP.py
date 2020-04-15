#!/usr/bin/python3

import os.path
import platform

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, lit, when  # pylint: disable=E0611

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
    vep = spark.read.json('%s/effects/*.json' % s3dir)

    # transcript consequence gene (if present)
    gene = when(vep.transcript_consequences.isNotNull(), vep.transcript_consequences[0].gene_id) \
        .otherwise(lit(None)) \
        .alias('gene')

    # pull out all the "common" columns
    common = vep.select(
        col('id').alias('varId'),
        col('most_severe_consequence').alias('mostSevereConsequence'),
        gene,
    )

    # create a frame of just the dbSNPs
    dbSNPs = vep \
        .withColumn('existing', explode(col('colocated_variants'))) \
        .filter(col('existing.id').startswith('rs') & (col('existing.allele_string') == col('allele_string'))) \
        .select(
            col('id').alias('varId'),
            col('existing.id').alias('dbSNP'),
        )

    # join the common data and dbSNPs together
    common = common.join(dbSNPs, 'varId', 'left_outer')

    # join and output the common effect data
    common.write \
        .mode('overwrite') \
        .json('%s/common' % s3dir)

    # output dbSNP in CSV format for intake validation
    dbSNPs.write \
        .mode('overwrite') \
        .csv('%s/dbsnp' % s3dir, sep='\t', header=True)

    # done
    spark.stop()
