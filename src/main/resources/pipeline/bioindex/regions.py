from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, lit, regexp_replace, when

# load and output directory
srcdir = 's3://dig-analysis-data/out/gregor/regions/unsorted/part-*'
outdir = 's3://dig-bio-index/regions'

# this is the schema written out by the regions processor
regions_schema = StructType(
    [
        StructField('chromosome', StringType(), nullable=False),
        StructField('start', IntegerType(), nullable=False),
        StructField('end', IntegerType(), nullable=False),
        StructField('tissue', StringType(), nullable=False),
        StructField('annotation', StringType(), nullable=False),
        StructField('method', StringType(), nullable=False),
        StructField('predictedTargetGene', StringType(), nullable=False),
        StructField('targetStart', IntegerType(), nullable=False),
        StructField('targetEnd', IntegerType(), nullable=False),
        StructField('transcriptionStartSite', IntegerType(), nullable=False),
        StructField('itemRgb', StringType(), nullable=False),
        StructField('score', DoubleType(), nullable=False),
    ]
)

# regexp for extracting biosample, method, and annotation
bed_re = r'([A-Z]+_\d+)___([^_]+)___(.*)'


if __name__ == '__main__':
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load all regions, fix the tissue ID, sort, and write
    df = spark.read.csv(srcdir, sep='\t', header=True, schema=regions_schema)

    # fix tissue name and convert NA method to null
    tissue = regexp_replace(df.tissue, '_', ':')
    method = when(df.method == 'NA', lit(None)).otherwise(df.method)

    # fix the tissue and method columns
    df = df \
        .withColumn('tissue', tissue) \
        .withColumn('method', method) \
        .orderBy(['chromosome', 'start']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()
