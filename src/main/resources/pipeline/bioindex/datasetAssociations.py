from pyspark.sql import SparkSession

# load and output directory
srcdir = 's3://dig-analysis-data/variants/*/*/part-*'
outdir = 's3://dig-bio-index/associations'


if __name__ == '__main__':
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load all dataset information
    df = spark.read.json(srcdir)

    # write out the datasets by phenotype
    df.filter(df.pValue <= 0.05) \
        .orderBy(['varId']) \
        .write \
        .mode('overwrite') \
        .json('%s/dataset' % outdir)

    # done
    spark.stop()
