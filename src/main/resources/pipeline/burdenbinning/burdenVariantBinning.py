#!/usr/bin/python3

import argparse
import platform

# from pyspark.sql import SparkSession, Row
# from pyspark.sql.types import StructType, StringType, IntegerType, StructField
# from pyspark.sql.functions import broadcast, col, lit, concat_ws  # pylint: disable=E0611

# set the S3 directories
s3dir = 's3://dig-analysis-data'
s3out = '%s/out/overlapregions' % s3dir

# will only use chromosome, position, and varId
variants_schema = StructType(
    [
        StructField('chromosome', StringType(), nullable=False),
        StructField('position', IntegerType(), nullable=False),
        StructField('reference', StringType(), nullable=False),
        StructField('alternate', StringType(), nullable=False),
        StructField('prediction', StringType(), nullable=False),
        StructField('varId', StringType(), nullable=False),
    ]
)


def

# entry point
if __name__ == '__main__':
    """
    Arguments: [--variants | --annotated-regions] chromosome
    """
    print('Python version: %s' % platform.python_version())

    opts = argparse.ArgumentParser()
    opts.add_argument('--variants', action='store_true', default=False)
    opts.add_argument('--annotated-regions', action='store_true', default=False)
    opts.add_argument('chromosome')

    # parse command line arguments
    args = opts.parse_args()

    # create a spark session
    spark = SparkSession.builder.appName('overlapRegions').getOrCreate()

    # run the process
    if args.variants:
        overlap_variants(args.chromosome)
    if args.annotated_regions:
        overlap_annotated_regions(args.chromosome)

    # done
    spark.stop()
