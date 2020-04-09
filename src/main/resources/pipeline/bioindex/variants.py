from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType, IntegerType
from pyspark.sql.functions import col, struct, explode


# source directories
all_srcdir = 's3://dig-analysis-data/out/varianteffect/variants/part-*'
freq_srcdir = 's3://dig-analysis-data/out/frequencyanalysis/'
tf_srcdir = 's3://dig-analysis-data/out/transcriptionfactors/part-*'
vep_srcdir = 's3://dig-analysis-data/out/varianteffect/effects/part-*'
assoc_srcdir = 's3://dig-analysis-data/out/metaanalysis/trans-ethnic/*/part-*'
snp_srcdir = 's3://dig-analysis-data/out/varianteffect/dbsnp/part-*'

# output directory
outdir = 's3://dig-bio-index/variants'

# variant list schema
all_schema = StructType(
    [
        StructField('chromosome', StringType(), nullable=False),
        StructField('position', IntegerType(), nullable=False),
        StructField('end', IntegerType(), nullable=False),
        StructField('allele', StringType(), nullable=False),
        StructField('strand', StringType(), nullable=False),
        StructField('varId', StringType(), nullable=False),
    ]
)

# this is the schema written out by the frequency analysis processor
frequency_schema = StructType(
    [
        StructField('varId', StringType(), nullable=False),
        StructField('chromosome', StringType(), nullable=False),
        StructField('position', IntegerType(), nullable=False),
        StructField('reference', StringType(), nullable=False),
        StructField('alt', StringType(), nullable=False),
        StructField('eaf', DoubleType(), nullable=False),
        StructField('maf', DoubleType(), nullable=False),
        StructField('ancestry', StringType(), nullable=False),
    ]
)

# transcription factor schema
tf_schema = StructType(
    [
        StructField('varId', StringType(), nullable=False),
        StructField('positionWeightMatrix', StringType(), nullable=False),
        StructField('delta', DoubleType(), nullable=False),
        StructField('position', IntegerType(), nullable=False),
        StructField('strand', StringType(), nullable=False),
        StructField('refScore', DoubleType(), nullable=False),
        StructField('altScore', DoubleType(), nullable=False),
    ]
)

# this is the schema written out by the meta-analysis processor
assoc_schema = StructType(
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

# this is the schema written out by the dbSNP processor
dbSNP_schema = StructType(
    [
        StructField('varId', StringType(), nullable=False),
        StructField('dbSNP', StringType(), nullable=False),
    ]
)


def load_freq(ancestry_name):
    return spark.read \
        .csv('%s/%s/part-*' % (freq_srcdir, ancestry_name), sep='\t', header=True, schema=frequency_schema) \
        .select(col('varId'), struct('eaf', 'maf').alias(ancestry_name))


if __name__ == '__main__':
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load all the unique variants
    variants = spark.read.csv(all_srcdir, sep='\t', header=False, schema=all_schema) \
        .select('varId', 'chromosome', 'position')

    # frequency outputs by ancestry
    ancestries = ['AA', 'AF', 'EA', 'EU', 'HS', 'SA']
    freq = None

    # load frequencies by variant ID
    for ancestry in ancestries:
        df = load_freq(ancestry)

        # final, joined frequencies
        freq = df if freq is None else freq.join(df, 'varId', how='outer')

    # pull all the frequencies together into a single map
    freq = freq.select(freq.varId, struct(*ancestries).alias('frequency'))

    # load transcription factors and group them by varId
    tfs = spark.read.csv(tf_srcdir, sep='\t', header=True, schema=tf_schema) \
        .rdd \
        .keyBy(lambda r: r.varId) \
        .combineByKey(lambda r: [r], lambda c, r: c + [r], lambda c, rs: c + rs) \
        .toDF() \
        .select(
            col('_1').alias('varId'),
            col('_2').alias('transcriptionFactors'),
        )

    # dbsnp ids
    db_snp = spark.read.csv(snp_srcdir, sep='\t', header=True, schema=dbSNP_schema)

    # load effects
    vep = spark.read.json(vep_srcdir)

    # get the most severe consequence
    most_severe_consequence = vep.select(
        vep.id.alias('varId'),
        vep.most_severe_consequence.alias('mostSevereConsequence'),
    )

    # extract the picked transcript consequence terms
    transcript_consequence = vep.select(vep.id, vep.transcript_consequences) \
        .withColumn('cqs', explode(col('transcript_consequences'))) \
        .select(
            col('id').alias('varId'),
            struct('cqs.*').alias('transcriptConsequence'),
        )

    # extract the picked intergenic consequence terms
    intergenic_consequence = vep.select(vep.id, vep.intergenic_consequences) \
        .withColumn('cqs', explode(col('intergenic_consequences'))) \
        .select(
            col('id').alias('varId'),
            struct('cqs.*').alias('intergenicConsequence'),
        )

    # extract the picked regulator feature consequence terms
    regulatory_consequence = vep.select(vep.id, vep.regulatory_feature_consequences) \
        .withColumn('cqs', explode(col('regulatory_feature_consequences'))) \
        .select(
            col('id').alias('varId'),
            struct('cqs.*').alias('regulatoryConsequence'),
        )

    # load the bottom-line results, join them together by varId
    bottom_line = spark.read.csv(assoc_srcdir, sep='\t', header=True, schema=assoc_schema) \
        .select(
            col('varId'),
            col('phenotype'),
            col('pValue'),
            col('beta'),
            col('zScore'),
            col('stdErr'),
            col('n'),
        ) \
        .rdd \
        .keyBy(lambda r: r.varId) \
        .aggregateByKey([], lambda a, b: a + [b], lambda a, b: a + b) \
        .toDF() \
        .select(
            col('_1').alias('varId'),
            col('_2').alias('associations'),
        )

    # remove empty records, join everything together, then sort
    df = variants \
        .filter(col('varId').isNotNull()) \
        .join(db_snp, 'varId', how='left_outer') \
        .join(freq, 'varId', how='left_outer') \
        .join(tfs, 'varId', how='left_outer') \
        .join(most_severe_consequence, 'varId', how='left_outer') \
        .join(transcript_consequence, 'varId', how='left_outer') \
        .join(intergenic_consequence, 'varId', how='left_outer') \
        .join(regulatory_consequence, 'varId', how='left_outer') \
        .join(bottom_line, 'varId', how='left_outer') \
        .orderBy(['chromosome', 'position'])

    # write out variant data by ID
    df.write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()
