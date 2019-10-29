#!/usr/bin/python3

import functools
import platform

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col  # pylint: disable=E0611

s3dir = 's3://dig-analysis-data'


# entry point
if __name__ == '__main__':
    """
    Arguments: none
    """
    print('Python version: %s' % platform.python_version())

    # TF and variant sources
    tfdir = '%s/transcription_factors/*/part-*' % s3dir
    vdir = '%s/out/varianteffect/variants/part-*' % s3dir

    # output directory
    outdir = '%s/out/transcriptionfactors/' % s3dir

    # create a spark session
    spark = SparkSession.builder.appName('transcriptfactors').getOrCreate()

    # load all the TFs
    tfs = spark.read.json(tfdir)

    # load all the variants, only keep the variant IDs
    variants = spark.read.csv(vdir, header=False, sep='\t') \
        .select(col('_6').alias('varId'))

    # join variants and TFs (limit TFs to variants we have)
    df = variants.alias('variants') \
        .join(tfs.alias('tfs'), 'varId') \
        .select(
            col('tfs.varId').alias('varId'),
            col('tfs.positionWeightMatrix').alias('positionWeightMatrix'),
            col('tfs.delta').alias('delta'),
            col('tfs.position').alias('position'),
            col('tfs.strand').alias('strand'),
            col('tfs.refScore').alias('refScore'),
            col('tfs.altScore').alias('altScore'),
        )

    # write out all the TFs
    df.write \
        .mode('overwrite') \
        .csv(outdir, sep='\t', header=True)

    # done
    spark.stop()
