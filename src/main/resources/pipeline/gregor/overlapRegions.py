#!/usr/bin/python3

import platform

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, monotonically_increasing_id  # pylint: disable=E0611
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

s3dir = 's3://dig-analysis-data'

# entry point
if __name__ == '__main__':
    """
    Arguments: none
    """
    print('Python version: %s' % platform.python_version())

    # source locations
    regions_src = '%s/chromatin_state/*/part-*' % s3dir
    variants_src = '%s/out/varianteffect/variants/part-*' % s3dir

    # output location
    # srcdir = '%s/chromatin_state/*/part-*' % s3dir
    # outdir = '%s/out/gregor/overlapped-regions/chromatin_state' % s3dir
    outdir = '%s/out/gregor/overlapped-variants/chromatin_state' % s3dir

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

    # load all the source regions and give them a unique ID
    regions = spark.read.json(regions_src).withColumn('id', monotonically_increasing_id())

    # load all the variants and keep only what's needed
    variants = spark.read.csv(variants_src, sep='\t', header=None, schema=variants_schema)

    # alias the frame for different names for the join
    df1 = regions.alias('df1')
    df2 = variants.alias('df2').select('id', 'chromosome', 'position')

    # Find all the variants that overlap each region
    cond = \
        (df2.chromosome == df1.chromosome) & \
        (df2.position >= df1.start) & \
        (df2.position < df1.end)

    p = df1.join(df2, cond, 'left_outer') \
        .select(
            col('df1.id').alias('id'),
            col('df2.id').alias('var_id'),
        )

    # the condition to join on
    # cond = \
    #     (df2.chromosome == df1.chromosome) & (
    #         ((df2.start >= df1.start) & (df2.start < df1.end)) |
    #         ((df2.end >= df1.start) & (df2.end < df1.end)) |
    #         ((df2.start <= df1.start) & (df2.end > df1.end))
    #     )

    # join the frame with itself, only on overlapping regions
    # p = df1.join(df2, cond, 'left_outer').select(
    #     col('df1.id').alias('id'),
    #     col('df2.id').alias('overlapId'),
    # )

    # aggregate all the overlap IDs into a single value per region
    overlaps = p.rdd \
        .keyBy(lambda r: r.id) \
        .aggregateByKey(
            [],
            lambda acc, r: acc + [r.var_id] if r.var_id else acc,
            lambda acc, ids: acc + ids
        ) \
        .map(lambda r: Row(id=r[0], overlappedVariants=','.join(str(o) for o in r[1]))) \
        .toDF()

    # aggregate all the overlap IDs into a single value per region ID
    # overlaps = p.rdd \
    #     .keyBy(lambda r: r.id) \
    #     .aggregateByKey(
    #         [],
    #         lambda acc, r: acc if r.overlapId == r.id else acc + [r.overlapId],
    #         lambda acc, ids: acc + ids
    #     ) \
    #     .map(lambda r: Row(id=r[0], overlapIds=','.join(str(o) for o in r[1]))) \
    #     .toDF()

    # join the overlap IDs into the original set of regions
    final = regions.join(overlaps, 'id')

    # output the regions to be loaded into Neo4j
    final.write \
        .mode('overwrite') \
        .csv(outdir, sep='\t', header=True)

    # done
    spark.stop()
