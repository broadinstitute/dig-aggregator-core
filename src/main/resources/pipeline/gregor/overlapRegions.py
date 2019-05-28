#!/usr/bin/python3

import argparse
import platform

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import broadcast, col, lit, concat_ws  # pylint: disable=E0611

s3dir = 's3://dig-analysis-data'

# The size of each overlapped region, increasing this will result in fewer
# database nodes and faster Spark processing, but result in slower queries.

overlappedRegionSize = 100000

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
    srcdir = '%s/chromatin_state/*/part-*' % s3dir
    outdir = '%s/out/gregor/overlapped-regions' % s3dir

    # create a spark session
    spark = SparkSession.builder.appName('overlappedRegions').getOrCreate()

    # load all the source regions for the given chromosome and give them a unique ID
    df = spark.read.json(srcdir) \
        .filter(col('chromosome') == args.chromosome) \
        .select(
            col('chromosome'),
            col('start'),
            col('end'),
            col('biosample'),
            col('name'),
            col('itemRgb'),
        )

    # find the first and the last overlapped region
    min_start = df.agg({'start': 'min'}).head()[0]
    max_end = df.agg({'end': 'max'}).head()[0]

    # create a frame with all overlapped regions for this chromosome
    if min_start is None or max_end is None:
        df = df.withColumn('overlappedRegion', lit(''))
    else:
        regions = []

        # calculate the first and last overlapped region for this chromosome
        first = min_start // overlappedRegionSize
        last = max_end // overlappedRegionSize

        # compute all the overlapping region boundaries
        for i in range(first, last + 1):
            regions.append(Row(
                name='%s:%d' % (args.chromosome, i * overlappedRegionSize),
                start=i * overlappedRegionSize,
                end=(i + 1) * overlappedRegionSize,
            ))

        # create the overlapping regions frame
        overlappedRegions = spark.createDataFrame(regions)

        # .. is the start within the overlapped region?
        # or is the end within the overlapped region?
        # or does it completely contain the overlapped region?
        cond = \
            ((df.start >= overlappedRegions.start) & (df.start < overlappedRegions.end)) | \
            ((df.end >= overlappedRegions.start) & (df.end < overlappedRegions.end)) | \
            ((df.start <= overlappedRegions.start) & (df.end >= overlappedRegions.end))

        # join the regions with the overlapped regions
        df = df.alias('region')\
            .join(broadcast(overlappedRegions).alias('overlapped'), cond, 'left_outer') \
            .select(
                concat_ws(':', col('region.chromosome'), col('region.start'), col('region.end')).alias('name'),
                col('region.chromosome').alias('chromosome'),
                col('region.start').alias('start'),
                col('region.end').alias('end'),
                col('region.biosample').alias('tissue'),
                col('region.name').alias('annotation'),
                col('region.itemRgb').alias('itemRgb'),
                col('overlapped.name').alias('overlappedRegion'),
            )

    # output the regions to be loaded into Neo4j
    df.write \
        .mode('overwrite') \
        .csv('%s/chromosome=%s' % (outdir, args.chromosome), sep='\t', header=True)

    # done
    spark.stop()
