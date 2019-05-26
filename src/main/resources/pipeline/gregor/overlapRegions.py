#!/usr/bin/python3

import argparse
import platform

from pyspark.sql import SparkSession
from pyspark.sql.functions import col  # pylint: disable=E0611
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

s3dir = 's3://dig-analysis-data'

# entry point
if __name__ == '__main__':
    """
    Arguments: chromosome
    """
    print('Python version: %s' % platform.python_version())

    opts = argparse.ArgumentParser()
    opts.add_argument('chromosome', help='chromosome of regions and variants')

    # parse arguments
    args = opts.parse_args()

    # source locations
    regions_src = '%s/chromatin_state/*/part-*' % s3dir
    variants_src = '%s/out/varianteffect/variants/part-*' % s3dir

    # output location
    outdir = '%s/out/gregor/overlapped-variants' % s3dir

    # create a spark session
    spark = SparkSession.builder.appName('gregor').getOrCreate()

    # format of the variants part files
    variants_schema = StructType([
        StructField('chromosome', StringType()),
        StructField('position', IntegerType()),
        StructField('end', IntegerType()),
        StructField('ref_alt', StringType()),
        StructField('strand', StringType()),
        StructField('id', StringType()),
    ])

    # load all the source regions for the given chromosome and give them a unique ID
    regions = spark.read.json(regions_src) \
        .filter(col('chromosome') == args.chromosome) \
        .select(
            col('chromosome'),
            col('start'),
            col('end'),
        )

    # load all the variants, just keep locus information
    variants = spark.read.csv(variants_src, sep='\t', header=None, schema=variants_schema) \
        .filter(col('chromosome') == args.chromosome) \
        .select(
            col('id'),
            col('chromosome'),
            col('position'),
        )

    # alias the frame for different names for the join
    r = regions.alias('r')
    v = variants.alias('v')

    # test if a region overlaps the given variant
    overlap = (v.chromosome == r.chromosome) & (v.position >= r.start) & (v.position < r.end)

    # find all the variants that overlap each region
    final = r.join(v, overlap, 'left_outer') \
        .select(
            col('r.chromosome').alias('chromosome'),
            col('r.start').alias('start'),
            col('r.end').alias('end'),
            col('v.id').alias('overlappedVariant'),
        )

    # output the regions to be loaded into Neo4j
    final.write \
        .mode('overwrite') \
        .csv('%s/chromosome=%s' % (outdir, args.chromosome), sep='\t', header=True)

    # done
    spark.stop()
