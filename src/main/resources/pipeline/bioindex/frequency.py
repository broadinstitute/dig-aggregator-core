from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, struct


# directories
srcdir = 's3://dig-analysis-data/out/frequencyanalysis/'
outdir = 's3://dig-bio-index/frequency'

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


def load_freq(ancestry_name):
    return spark.read \
        .csv('%s/%s/part-*' % (srcdir, ancestry_name), sep='\t', header=True, schema=frequency_schema) \
        .select(col('varId'), struct('eaf', 'maf').alias(ancestry_name))


if __name__ == '__main__':
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # frequency outputs by ancestry
    ancestries = ['AA', 'AF', 'EA', 'EU', 'HS', 'SA']
    freq = None

    # load frequencies by variant ID
    for ancestry in ancestries:
        df = load_freq(ancestry)

        # final, joined frequencies
        freq = df if freq is None else freq.join(df, 'varId', how='outer')

    # pull all the frequencies together into a single map
    freq = freq.select(freq.varId, *ancestries)

    # write out variant data by ID
    freq.write \
        .mode('overwrite') \
        .json(outdir)

    # done
    spark.stop()
