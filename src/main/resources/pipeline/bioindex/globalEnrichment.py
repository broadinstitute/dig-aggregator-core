import re

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType, IntegerType
from pyspark.sql.functions import col, input_file_name, lit, struct, udf, when

# load and output directory
srcdir = 's3://dig-analysis-data/out/gregor/summary/*/*/statistics.txt'
outdir = 's3://dig-bio-index/global_enrichment'

# tissue ontology to join
tissue_ontology = 's3://dig-analysis-data/tissues/ontology'

# this is the schema written out by the variant partition process
statistics_schema = StructType(
    [
        StructField('bed', StringType(), nullable=False),
        StructField('SNPs', IntegerType(), nullable=False),
        StructField('expectedSNPs', DoubleType(), nullable=False),
        StructField('pValue', DoubleType(), nullable=False),
    ]
)

# input filename -> phenotype and ancestry
src_re = r'/out/gregor/summary/([^/]+)/([^/]+)/'


if __name__ == '__main__':
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # udf functions
    phenotype_of_source = udf(lambda s: s and re.search(src_re, s).group(1))
    ancestry_of_source = udf(lambda s: s and re.search(src_re, s).group(2))
    tissue_of_bed = udf(lambda s: s and s.split('___')[0].replace('_', ':'))
    method_of_bed = udf(lambda s: s and s.split('___')[1])
    annotation_of_bed = udf(lambda s: s and s.split('___')[2])

    # load the trans-ethnic, meta-analysis, top variants and write them sorted
    df = spark.read.csv(srcdir, sep='\t', header=True, schema=statistics_schema) \
        .select('*', input_file_name().alias('source')) \
        .select(
            phenotype_of_source('source').alias('phenotype'),
            ancestry_of_source('source').alias('ancestry'),
            tissue_of_bed('bed').alias('tissueId'),
            method_of_bed('bed').alias('method'),
            annotation_of_bed('bed').alias('annotation'),
            col('SNPs'),
            col('expectedSNPs'),
            col('pValue'),
        )

    # load the tissue ontology to join
    tissues = spark.read.json(tissue_ontology)

    # make sure each record is valid
    isValid = \
        df.phenotype.isNotNull() & \
        df.ancestry.isNotNull() & \
        df.tissue.isNotNull() & \
        df.method.isNotNull() & \
        df.annotation.isNotNull()

    # convert NA method to null
    method = when(df.method == 'NA', lit(None)).otherwise(df.method)

    # put all the tissue data into a single column for tissues for the join
    tissues = tissues.select(tissues.id, struct('*').alias('tissue'))

    # add th method and join the tissue ontology
    df = df.filter(isValid) \
        .withColumn('method', method) \
        .join(tissues, df.tissueId == tissues.id, how='left_outer') \
        .drop('tissueId')

    # write out the global enrichment data sorted by phenotype
    df.orderBy(['phenotype', 'pValue']) \
        .write \
        .mode('overwrite') \
        .json('%s/phenotype' % outdir)

    # done
    spark.stop()
