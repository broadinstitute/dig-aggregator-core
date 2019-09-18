#!/usr/bin/python3

import argparse
import platform

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StringType, IntegerType, StructField
from pyspark.sql.functions import broadcast, col, lit, concat_ws  # pylint: disable=E0611

s3dir = 's3://dig-analysis-data'
s3out = '%s/out/overlapregions' % s3dir

# The size of each overlap region. Increasing this will result in fewer
# database nodes and faster Spark processing, but result in slower queries
# as there will be more relationships made.
#
# However, the smaller the size is, the more overlap regions will be
# created, meaning the initial lookup for them will be slower. 100 kb
# seems to be a pretty good size.

overlappedRegionSize = 100000


# only need the first 3 columns of the region data
regions_schema = StructType(
    [
        StructField('chromosome', StringType(), nullable=False),
        StructField('start', IntegerType(), nullable=False),
        StructField('end', IntegerType(), nullable=False),
    ]
)


# will only use chromosome, position, and varId
variants_schema = StructType(
    [
        StructField('chromosome', StringType(), nullable=False),
        StructField('position', IntegerType(), nullable=False),
        StructField('end', IntegerType(), nullable=False),
        StructField('allele', StringType(), nullable=False),
        StructField('strand', StringType(), nullable=False),
        StructField('varId', StringType(), nullable=False),
    ]
)


def overlap_regions(chromosome):
    """
    Create overlapped regions for annotation regions.
    """
    srcdir = '%s/annotations/*/*/part-*' % s3dir
    outdir = '%s/regions' % s3out

    # load all the source regions for the given chromosome and give them a unique ID
    df = spark.read.json(srcdir) \
        .filter(col('chromosome') == chromosome) \
        .select(
            col('chromosome'),
            col('start'),
            col('end'),
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
            start = i * overlappedRegionSize
            end = start + overlappedRegionSize

            regions.append(Row(
                name='%s:%d-%d' % (chromosome, start, end),
                chromosome=chromosome,
                start=start,
                end=end,
            ))

        # create the overlapping regions frame
        overlapped_regions = spark.createDataFrame(regions)

        # .. is the start within the overlapped region?
        # or is the end within the overlapped region?
        # or does it completely contain the overlapped region?
        cond = \
            ((df.start >= overlapped_regions.start) & (df.start < overlapped_regions.end)) | \
            ((df.end >= overlapped_regions.start) & (df.end < overlapped_regions.end)) | \
            ((df.start <= overlapped_regions.start) & (df.end >= overlapped_regions.end))

        # unique naming of regions: CHROMOSOME:START-END
        region_name = concat_ws('-', concat_ws(':', col('region.chromosome'), col('region.start')), col('region.end'))

        # join the regions with the overlapped regions
        df = df.alias('region') \
            .join(broadcast(overlapped_regions).alias('overlapped'), cond, 'left_outer') \
            .select(
                col('overlapped.name').alias('name'),
                col('overlapped.chromosome').alias('chromosome'),
                col('overlapped.start').alias('start'),
                col('overlapped.end').alias('end'),
                region_name.alias('region'),
            ) \
            .distinct()

    # output the regions to be loaded into Neo4j
    df.write \
        .mode('overwrite') \
        .csv('%s/chromosome=%s' % (outdir, chromosome), sep='\t', header=True)


def overlap_variants(chromosome):
    """
    Create overlapped regions for variants.
    """
    srcdir = '%s/out/varianteffect/variants/part-*' % s3dir
    outdir = '%s/variants' % s3out

    # load all the source variants
    df = spark.read.csv(srcdir, header=False, sep='\t', schema=variants_schema) \
        .filter(col('chromosome') == chromosome) \
        .select(
            col('varId'),
            col('chromosome'),
            col('position'),
        )

    # find the first and the last overlapped region
    min_pos = df.agg({'position': 'min'}).head()[0]
    max_pos = df.agg({'position': 'max'}).head()[0]

    # create a frame with all overlapped regions for this chromosome
    if min_pos is None or max_pos is None:
        df = df.withColumn('overlappedRegion', lit(''))
    else:
        regions = []

        # calculate the first and last overlapped region for this chromosome
        first = min_pos // overlappedRegionSize
        last = max_pos // overlappedRegionSize

        # compute all the overlapping region boundaries
        for i in range(first, last + 1):
            start = i * overlappedRegionSize
            end = start + overlappedRegionSize

            regions.append(Row(
                name='%s:%d-%d' % (chromosome, start, end),
                chromosome=chromosome,
                start=start,
                end=end,
            ))

        # create the overlapping regions frame
        overlapped_regions = spark.createDataFrame(regions)

        # .. is the position within the overlapped region?
        cond = (df.position >= overlapped_regions.start) & (df.position < overlapped_regions.end)

        # join the regions with the overlapped regions
        df = df.alias('variant') \
            .join(broadcast(overlapped_regions).alias('overlapped'), cond, 'left_outer') \
            .select(
                col('overlapped.name').alias('name'),
                col('overlapped.chromosome').alias('chromosome'),
                col('overlapped.start').alias('start'),
                col('overlapped.end').alias('end'),
                col('variant.varId').alias('varId'),
            ) \
            .distinct()

    # output the regions to be loaded into Neo4j
    df.write \
        .mode('overwrite') \
        .csv('%s/chromosome=%s' % (outdir, chromosome), sep='\t', header=True)


# entry point
if __name__ == '__main__':
    """
    Arguments: [--variants | --regions] chromosome
    """
    print('Python version: %s' % platform.python_version())

    opts = argparse.ArgumentParser()
    opts.add_argument('--variants', action='store_true', default=False)
    opts.add_argument('--regions', action='store_true', default=False)
    opts.add_argument('chromosome')

    # parse command line arguments
    args = opts.parse_args()

    # --variants or --regions must be provided, but not both!
    assert args.variants != args.regions

    # create a spark session
    spark = SparkSession.builder.appName('overlapRegions').getOrCreate()

    # run the process
    if args.variants:
        overlap_variants(args.chromosome)
    else:
        overlap_regions(args.chromosome)

    # done
    spark.stop()
