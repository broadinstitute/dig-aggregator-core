from pyspark.sql import SparkSession

# load and output directory
srcdir = 's3://dig-analysis-data/variants/*/*/metadata'
outdir = 's3://dig-bio-index/datasets'


if __name__ == '__main__':
    spark = SparkSession.builder.appName('bioindex').getOrCreate()

    # load all dataset information
    df = spark.read.json(srcdir)

    # keep only specific fields
    df = df.select(
        df.name,
        df.phenotype,
        df.ancestry,
        df.tech,
        df.cases,
        df.controls,
        df.subjects,
    )

    # write out the datasets by phenotype
    df.orderBy(['phenotype']) \
        .repartition(1) \
        .write \
        .mode('overwrite') \
        .json('%s/phenotype' % outdir)

    # done
    spark.stop()
