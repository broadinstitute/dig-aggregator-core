import operator

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import struct

# load and output directory
srcdir = 's3://dig-analysis-data/out/metaanalysis/trans-ethnic/*/part-*'
outdir = 's3://dig-bio-index/variants'

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
    ]
)


if __name__ == '__main__':
    spark = SparkSession.builder.appName('MetaAnalysis').getOrCreate()

    # load the trans-ethnic meta-analysis
    df = spark.read.csv(srcdir, sep='\t', header=True, schema=variants_schema)

    # select just the unique set of variants
    variants = df.select(df.varId, df.chromosome, df.position, df.reference, df.alt) \
        .dropDuplicates(['varId'])

    # combine meta-analysis results into a struct for aggregation
    agg = df.select(
        df.varId,

        # reduce the meta-analysis results into a single column
        struct(df.phenotype, df.pValue, df.beta, df.zScore, df.stdErr, df.n) \
            .alias('metaAnalysis'),
    )

    # aggregate all the meta-analyses for each variant into a single row (PheWAS)
    agg = agg.rdd \
        .keyBy(lambda r: r.varId) \
        .aggregateByKey([], lambda a, r: a + [r.metaAnalysis], operator.add) \
        .map(lambda r: Row(varId=r[0], metaAnalysis=r[1])) \
        .toDF()

    # join the results, sort, and write
    variants.join(agg, 'varId') \
        .orderBy(['chromosome', 'position']) \
        .write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()
