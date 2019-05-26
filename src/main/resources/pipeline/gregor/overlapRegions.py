#!/usr/bin/python3

import argparse
import platform

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, lit, monotonically_increasing_id  # pylint: disable=E0611
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
        .withColumn('id', monotonically_increasing_id()) \
        .filter(col('chromosome') == args.chromosome)

    # load all the variants for the given chromosome
    variants = spark.read \
        .csv(variants_src, sep='\t', header=None, schema=variants_schema) \
        .filter(col('chromosome') == args.chromosome)

    # alias the frame for different names for the join
    r = regions.alias('r')
    v = variants.alias('v').select('id', 'chromosome', 'position')

    # find all the variants that overlap each region
    p = r.join(v, (v.position >= r.start) & (v.position < r.end), 'left_outer') \
        .select(
            col('r.id').alias('id'),
            col('v.id').alias('overlappedVariant'),
        )

    # join the overlapped variants into the regions table
    final = regions.join(p, 'id')

    # output the regions to be loaded into Neo4j
    final.write \
        .mode('overwrite') \
        .csv('%s/chromosome=%s' % (outdir, args.chromosome), sep='\t', header=True)

    # done
    spark.stop()
