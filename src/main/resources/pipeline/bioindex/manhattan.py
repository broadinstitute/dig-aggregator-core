from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType, IntegerType
from pyspark.sql.functions import col, rank
from pyspark.sql.window import Window


# load and output directory
srcdir = 's3://dig-analysis-data/out/metaanalysis/trans-ethnic/*/part-*'
outdir = 's3://dig-bio-index/manhattan'

# this is the schema written out by the variant partition process
variants_schema = StructType(
    [
        StructField('varId', StringType(), nullable=False),
        StructField('chromosome', StringType(), nullable=False),
        StructField('position', IntegerType(), nullable=False),
        StructField('reference', StringType(), nullable=False),
        StructField('alt', StringType(), nullable=False),
        StructField('phenotype', StringType(), nullable=False),
        StructField('pValue', DoubleType(), nullable=False),
        StructField('beta', DoubleType(), nullable=False),
        StructField('zScore', DoubleType(), nullable=False),
        StructField('stdErr', DoubleType(), nullable=False),
        StructField('n', DoubleType(), nullable=False),
        StructField('top', BooleanType(), nullable=False),
    ]
)


if __name__ == '__main__':
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load the trans-ethnic meta-analysis
    df = spark.read.csv(srcdir, sep='\t', header=True, schema=variants_schema) \
        .filter(col('top'))

    # create a window into the data, key by phenotype, ranked by p-value
    w = Window.partitionBy(df.phenotype).orderBy(df.pValue)

    # take the top N variants per phenotype
    ranked = df.select('*', rank().over(w).alias('rank')).filter(col('rank') <= 5000)

    # write them out, partitioned by phenotype
    ranked.write \
        .mode('overwrite') \
        .partitionBy(['phenotype']) \
        .json(outdir)

    # done
    spark.stop()
