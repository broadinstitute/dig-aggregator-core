from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, input_file_name, regexp_replace, regexp_extract, array

# load and output directory
src_regions = 's3://dig-analysis-data/out/gregor/regions/unsorted/part-*'
src_gregor = 's3://dig-analysis-data/out/gregor/summary/*/*/statistics.txt'

outdir = 's3://dig-bio-index/regions'

# this is the schema written out by the regions processor
regions_schema = StructType(
    [
        StructField('chromosome', StringType(), nullable=False),
        StructField('start', IntegerType(), nullable=False),
        StructField('end', IntegerType(), nullable=False),
        StructField('biosample', StringType(), nullable=False),
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

# schema used for gregor summary output
summary_schema = StructType(
    [
        StructField('bedFile', StringType(), nullable=False),
        StructField('inBedIndexSNP', DoubleType(), nullable=False),
        StructField('expectedNumInBedSNP', DoubleType(), nullable=False),
        StructField('pValue', DoubleType(), nullable=False),
    ]
)

# regexp for extracting biosample, method, and annotation
bed_re = r'([A-Z]+_\d+)___([^_]+)___(.*)'
summary_re = r'/gregor/summary/([^/]+)/([^/]+)/statistics.txt'


if __name__ == '__main__':
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load all regions, fix biosample, sort, and write
    spark.read.csv(src_regions, sep='\t', header=True, schema=regions_schema) \
        .withColumn('biosample', regexp_replace(col('biosample'), '_', ':')) \
        .orderBy(['chromosome', 'start']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()
