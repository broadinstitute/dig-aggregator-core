#!/usr/bin/python3

import platform

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws  # pylint: disable=E0611

s3dir = 's3://dig-analysis-data'

# entry point
if __name__ == '__main__':
    """
    No arguments.
    """
    print('Python version: %s' % platform.python_version())

    # get the source and output directories
    srcdir = '%s/out/metaanalysis/trans-ethnic/*/part-*' % s3dir
    outdir = '%s/out/gregor/snp' % s3dir

    # create a spark session
    spark = SparkSession.builder.appName('gregor').getOrCreate()

    # slurp all the variants across ALL phenotypes
    df = spark.read.csv(srcdir, sep='\t', header=True) \
        .filter(col('chromosome') != 'MT') \
        .select(concat_ws(':', col('chromosome'), col('position')).alias('SNP')) \
        .distinct()

    # output the variants as CSV part files
    df.write.mode('overwrite').csv(outdir, sep='\t')

    # done
    spark.stop()
