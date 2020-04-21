from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, lit, regexp_replace, struct, when

# load and output directory
srcdir = 's3://dig-analysis-data/out/gregor/regions/unsorted/part-*'
outdir = 's3://dig-bio-index/regions'

# tissue ontology to join
tissue_ontology = 's3://dig-analysis-data/tissues/ontology'

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

    # load the tissue ontology to join
    tissues = spark.read.json(tissue_ontology)

    # put all the tissue data into a single column for tissues for the join
    tissues = tissues.select(tissues.id.alias('tissueId'), struct('*').alias('tissue'))

    # fix the tissue and method columns
    df = df \
        .withColumn('method', when(df.method == 'NA', lit(None)).otherwise(df.method)) \
        .withColumn('tissue', regexp_replace(df.tissue, '_', ':'))

    # rename the tissue column to tissueId
    df = df.withColumnRenamed('tissue', 'tissueId')

    # join with the tissue ontology
    df = df.join(tissues, 'tissueId', how='left_outer') \
        .drop('tissueId')

    # sort by locus and write
    df.orderBy(['chromosome', 'start']) \
        .write \
        .mode('overwrite') \
        .json('%s/locus' % outdir)

    # sort by annotation and method
    df.orderBy(['annotation', 'method']) \
        .write \
        .mode('overwrite') \
        .json('%s/annotation' % outdir)

    # done
    spark.stop()
