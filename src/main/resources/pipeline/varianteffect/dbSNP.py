#!/usr/bin/python3

import os.path
import platform

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import explode, col, lit, substring, when  # pylint: disable=E0611

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
        .otherwise(lit(None))

    # extract the common effect data
    common = vep.select(
        vep.id.alias('varId'),
        gene.alias('gene'),
        vep.most_severe_consequence.alias('mostSevereConsequence'),
    )

    # find existing variants (dbSNP IDs)
    existing = vep \
        .withColumn('var', explode(col('colocated_variants'))) \
        .filter(substring(col('var.id'), 0, 2) == 'rs') \
        .filter(col('var.allele_string') == col('allele_string')) \
        .select(
            col('id').alias('varId'),
            col('var.id').alias('dbSNP'),
        )

    # join and output the common effect data
    common.join(existing, 'varId', how='left_outer') \
        .write \
        .mode('overwrite') \
        .json('%s/common' % s3dir)

    # output dbSNP in CSV format for intake validation
    existing.write \
        .mode('overwrite') \
        .csv('%s/dbsnp' % s3dir, sep='\t', header=True)

    # done
    spark.stop()
