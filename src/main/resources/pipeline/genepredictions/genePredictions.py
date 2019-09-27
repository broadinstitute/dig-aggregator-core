#!/usr/bin/python3

import argparse
import platform

from pyspark.sql import SparkSession
from pyspark.sql.functions import col  # pylint: disable=E0611

s3dir = 's3://dig-analysis-data'


# entry point
if __name__ == '__main__':
    """
    Arguments: none
    """
    print('Python version: %s' % platform.python_version())

    # opts = argparse.ArgumentParser()
    #
    # # parse the command line parameters
    # args = opts.parse_args()

    # get the source and output directories
    src_regions = '%s/gene_predictions/*' % s3dir
    src_genes = '%s/genes/GRCh37' % s3dir
    outdir = '%s/out/genepredictions/regions' % s3dir

    # create a spark session
    spark = SparkSession.builder.appName('genepredictions').getOrCreate()

    # slurp the dataset and genes
    regions = spark.read.json('%s/part-*' % src_regions)
    genes = spark.read.json('%s/part-*' % src_genes)

    # join condition
    cond = (regions.predictedTargetGene == genes.name) | (regions.predictedTargetGene == genes.ensemblId)

    # inner join the regions and genes, prefer the EnsemblID over gene name
    df = regions.alias('regions') \
        .join(genes.alias('genes'), cond) \
        .select(
            col('regions.chromosome').alias('chromosome'),
            col('regions.start').alias('start'),
            col('regions.end').alias('end'),
            col('genes.ensemblId').alias('predictedTargetGene'),
            col('regions.targetStart').alias('targetStart'),
            col('regions.targetEnd').alias('targetEnd'),
            col('regions.value').alias('value'),
        )

    # write out all the regions
    df.write.mode('overwrite').csv(outdir, sep='\t', header=True)

    # done
    spark.stop()
