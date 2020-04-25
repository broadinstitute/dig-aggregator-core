from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType, IntegerType
from pyspark.sql.functions import rand


# load and output directory
srcdir = 's3://dig-analysis-data/out/metaanalysis/trans-ethnic/*/part-*'
outdir = 's3://dig-bio-index/associations'

# common vep data
common_dir = 's3://dig-analysis-data/out/varianteffect/common'
genes_dir = 's3://dig-analysis-data/genes/GRCh37'

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
    common = spark.read.csv('%s/part-*' % common_dir, sep='\t', header=True)
    genes = spark.read.json(genes_dir)

    # join the common data into the associations
    df = df.join(common, 'varId', how='left_outer')

    # write associations indexed by phenotype and locus
    df.drop('top') \
        .orderBy(['phenotype', 'chromosome', 'position']) \
        .write \
        .mode('overwrite') \
        .json('%s/locus' % outdir)

    # write out just the top associations, indexed by locus
    df.filter(df.top) \
        .drop('top') \
        .orderBy(['chromosome', 'position']) \
        .write \
        .mode('overwrite') \
        .json('%s/top' % outdir)

    # add a uniform random value to each record
    df = df.withColumn('r', rand())

    # filter associations based on p-value and the uniform random value
    df = df.filter((df.pValue * df.r) <= 1.e-6)

    # drop the random column, sort, and write the associations
    df.drop('r', 'top') \
        .orderBy(['phenotype', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json('%s/phenotype' % outdir)

    # done
    spark.stop()
