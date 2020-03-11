from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# load and output directory
srcdir = 's3://dig-analysis-data/genes/GRCh37/part-*'
outdir = 's3://dig-bio-index/genes'


# all valid chromosomes
chromosomes = list(map(str, range(1, 23))) + ['X', 'Y', 'XY', 'M', 'MT']


if __name__ == '__main__':
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load the genes and write them sorted
    df = spark.read.json(srcdir) \
        .filter(col('chromosome').isin(*chromosomes))

    # write the genes by region
    df.coalesce(1) \
        .orderBy(['chromosome', 'start']) \
        .write \
        .mode('overwrite') \
        .json('%s/region' % outdir)

    # write the genes by name
    df.coalesce(1) \
        .orderBy(['name']) \
        .write \
        .mode('overwrite') \
        .json('%s/name' % outdir)

    # done
    spark.stop()
