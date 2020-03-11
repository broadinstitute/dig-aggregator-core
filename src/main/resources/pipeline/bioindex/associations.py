from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType, IntegerType

# load and output directory
srcdir = 's3://dig-analysis-data/out/metaanalysis/trans-ethnic/*/part-*'
outdir = 's3://dig-bio-index/associations'

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

    # load the trans-ethnic, meta-analysis, top variants and write them sorted
    df = spark.read.csv(srcdir, sep='\t', header=True, schema=variants_schema)
    top = df.filter(df.top | (df.pValue < 1e-5))

    # all associations indexed by locus
    df.drop(df.top) \
        .orderBy(['phenotype', 'chromosome', 'position']) \
        .write \
        .mode('overwrite') \
        .json('%s/locus' % outdir)

    # top associations indexed by locus
    top.drop(top.top) \
        .orderBy(['chromosome', 'position']) \
        .write \
        .mode('overwrite') \
        .json('%s/top' % outdir)

    # done
    spark.stop()
