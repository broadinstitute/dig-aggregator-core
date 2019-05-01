#!/usr/bin/python3

import argparse
import platform
import subprocess

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, input_file_name, regexp_extract  # pylint: disable=E0611
from pyspark.sql.types import StructType, StructField, StringType

s3dir = 's3://dig-analysis-data'

# GREGOR doesn't work on XY, M, or MT chromosomes.
valid_chromosomes = list(map(lambda c: str(c+1), range(22))) + ['X', 'Y']


def test_glob(glob):
    """
    Searches for part files using a path glob.
    """
    cmd = ['hadoop', 'fs', '-test', '-e', glob]
    print('Running: ' + str(cmd))

    # if it returns non-zero, then the files didn't exist
    status = subprocess.call(cmd)
    print('Return code: ' + str(status))

    return status == 0


# entry point
if __name__ == '__main__':
    """
    Arguments: <phenotype>
    """
    print('Python version: %s' % platform.python_version())

    opts = argparse.ArgumentParser()
    opts.add_argument('phenotype')

    # get command line arguments
    args = opts.parse_args()

    # get the source and output directories
    srcdir = '%s/out/metaanalysis/ancestry-specific/%s/*/part-*' % (s3dir, args.phenotype)
    outdir = '%s/out/gregor/snp/%s' % (s3dir, args.phenotype)

    spark = SparkSession.builder.appName('gregor').getOrCreate()

    # NOTE: It's possible that srcdir doesn't actually exist or contain anything!
    #       This is because meta-analysis filters data. For example, perhaps a
    #       dataset doesn't contain any BETA or P-VALUE data, in which case it
    #       will be successfully run through the meta-analysis processor, but it
    #       won't write anything out or get through ancestry-specific analysis.
    #
    #       When that happens, it's OK to just ignore it.

    if test_glob(srcdir):
        df = spark.read.csv(srcdir, sep='\t', header=True) \
            .withColumn('filename', input_file_name()) \
            .withColumn('ancestry', regexp_extract('filename', r'/ancestry=([^/]+)/', 1)) \
            .filter(col('chromosome').isin(valid_chromosomes)) \
            .filter(col('pValue') < 1.0e-8) \
            .select(
                concat_ws(':', col('chromosome'), col('position')).alias('SNP'),
                col('ancestry'),
            )
    else:
        schema = StructType([
            StructField('SNP', StringType()),
            StructField('ancestry', StringType()),
        ])

        # so _SUCCESS is written
        df = spark.createDataFrame([], schema)

    # output the variants as CSV part files
    df.write \
        .mode('overwrite') \
        .partitionBy('ancestry') \
        .csv(outdir, sep='\t')

    # done
    spark.stop()
