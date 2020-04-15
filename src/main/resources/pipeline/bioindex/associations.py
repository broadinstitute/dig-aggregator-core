from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType, IntegerType


# load and output directory
srcdir = 's3://dig-analysis-data/out/metaanalysis/trans-ethnic/*/part-*'
outdir = 's3://dig-bio-index/associations'

# common vep data
vepdir = 's3://dig-analysis-data/out/varianteffect/common/part-*'

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
    common = spark.read.json(vepdir)

    # join the dbSNP id with the associations
    df = df.join(common, 'varId', how='left_outer')
    top = df.filter(df.top)

    # find the variants most significant across the genome per phenotype, this
    # assumes a manhattan plot 4000 pixels wide, which means a maximum of 4000
    # "positions" to display, so 3B positions / 4000 = 750,000 to keep the best
    # variant in (or significant ones)
    by_phenotype = df.rdd \
        .keyBy(lambda v: (v.phenotype, v.chromosome, v.position // 750000)) \
        .reduceByKey(lambda a, b: b if b.pValue < a.pValue else a) \
        .map(lambda v: v[1]) \
        .toDF() \
        .union(df.filter(df.pValue < 1e-5))

    # all associations indexed by locus
    df.drop('top') \
        .orderBy(['phenotype', 'chromosome', 'position']) \
        .write \
        .mode('overwrite') \
        .json('%s/locus' % outdir)

    # top associations indexed by locus
    top.drop('top') \
        .orderBy(['chromosome', 'position']) \
        .write \
        .mode('overwrite') \
        .json('%s/top' % outdir)

    # write out the genome-wide associations by phenotype
    by_phenotype.drop(['rank', 'top']) \
        .orderBy(['phenotype', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json('%s/phenotype' % outdir)

    # done
    spark.stop()
