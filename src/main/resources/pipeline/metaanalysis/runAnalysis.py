#!/usr/bin/python3

import argparse
import glob
import os.path
import platform
import re
import subprocess

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, isnan, lit, when  # pylint: disable=E0611

efsdir = '/mnt/efs'
bindir = '/mnt/efs/bin'

# where metal is located in S3
metal_s3path = 's3://dig-analysis-data/bin/generic-metal/metal'

# where metal is installed to locally
metal_local = '%s/metal' % bindir

# this is the schema written out by the variant partition process
variants_schema = StructType(
    [
        StructField('varId', StringType(), nullable=False),
        StructField('dataset', StringType(), nullable=False),
        StructField('chromosome', StringType(), nullable=False),
        StructField('position', IntegerType(), nullable=False),
        StructField('reference', StringType(), nullable=False),
        StructField('alt', StringType(), nullable=False),
        StructField('phenotype', StringType(), nullable=False),
        StructField('pValue', DoubleType(), nullable=False),
        StructField('beta', DoubleType(), nullable=False),
        StructField('maf', DoubleType(), nullable=False),
        StructField('n', IntegerType(), nullable=False),
        StructField('stdErr', DoubleType(), nullable=False),
    ]
)

# this is the schema written out by the METAL app
metaanalysis_schema = StructType(
    [
        StructField('MarkerName', StringType(), nullable=False),
        StructField('Allele1', StringType(), nullable=False),
        StructField('Allele2', StringType(), nullable=False),
        StructField('Freq1', DoubleType(), nullable=False),
        StructField('FreqSE', DoubleType(), nullable=False),
        StructField('MinFreq', DoubleType(), nullable=False),
        StructField('MaxFreq', DoubleType(), nullable=False),
        StructField('Effect', DoubleType(), nullable=False),
        StructField('StdErr', DoubleType(), nullable=False),
        StructField('P-value', DoubleType(), nullable=False),
        StructField('Direction', StringType(), nullable=False),
        StructField('TotalSampleSize', DoubleType(), nullable=False),
    ]
)


def install_metal():
    """
    Checks to see if METAL (in EFS) doesn't exist and should be copied from S3.
    """
    if not os.path.isfile(metal_local):
        subprocess.check_call(['mkdir', '-p', bindir])
        subprocess.check_call(['aws', 's3', 'cp', metal_s3path, metal_local])
        subprocess.check_call(['chmod', '+x', metal_local])


def read_analysis(spark, path):
    """
    Read the METAANALYSIS file in `path` and transform it before returning
    a RDD for it. This transformation will end up being what's put in the
    database for querying.
    """

    def transform(row):
        var = row.MarkerName.split(':')

        # NOTE: If the reference allele is pointing to the effect allele, then
        #       the z-score, effect, direction, and MAF need to be flipped.

        flip = var[2].upper() == row.Allele1.upper()

        # transform to correct format
        return Row(
            varId=row.MarkerName,
            chromosome=var[0],
            position=int(var[1]),
            reference=var[2].upper(),
            alt=var[3].upper(),
            pValue=row['P-value'],
            n=row.TotalSampleSize,
            beta=row.Effect if not flip else -row.Effect,
            maf=row.Freq1 if not flip else 1.0 - row.Freq1,
            maxFreq=row.MaxFreq if not flip else 1.0 - row.MaxFreq,
            stdErr=row.StdErr,
        )

    # read the METAANALYSIS file,filter out rows that have bad computations
    df = spark.read.csv(path, header=True, sep='\t', schema=metaanalysis_schema) \
        .filter(col('MarkerName').isNotNull()) \
        .filter(isnan(col('Freq1')) == False) \
        .filter(isnan(col('MaxFreq')) == False) \
        .filter(isnan(col('P-Value')) == False) \
        .filter(isnan(col('Effect')) == False) \
        .filter(isnan(col('StdErr')) == False)

    # transform the output into the proper format
    return df.rdd.map(transform)


def run_metal(workdir, parts, overlap=False, freq=True):
    """
    Run the METAL program at a given location with a set of part files.
    """
    subprocess.call(['mkdir', '-p', workdir])
    subprocess.call(['rm', '-rf', '%s/*' % workdir])

    # header used for all input files
    script = [
        'SEPARATOR TAB',
        'SCHEME %s' % ('SAMPLESIZE' if overlap else 'STDERR'),
        'MARKERLABEL varId',
        'ALLELELABELS reference alt',
        'PVALUELABEL pValue',
        'EFFECTLABEL beta',
        'WEIGHTLABEL n',
        'FREQLABEL maf',
        'STDERRLABEL stdErr',
        'CUSTOMVARIABLE TotalSampleSize',
        'LABEL TotalSampleSize AS n',
        'OVERLAP %s' % ('ON' if overlap else 'OFF'),
        'AVERAGEFREQ %s' % ('ON' if freq else 'OFF'),
        'MINMAXFREQ %s' % ('ON' if freq else 'OFF'),
        'COLUMNCOUNTING LENIENT',
    ]

    # add all the parts
    for part in parts:
        script.append('PROCESS %s' % part)

    # add the footer
    script += [
        'OUTFILE %s/METAANALYSIS .csv' % workdir,
        'ANALYZE',
        'QUIT',
    ]

    # turn the script into a single string
    script = '\n'.join(script)

    # write the script to a file for posterity and debugging
    with open('%s/metal.script' % workdir, 'w') as fp:
        fp.write(script)

    # send all the commands to METAL
    pipe = subprocess.Popen([metal_local], stdin=subprocess.PIPE)

    # send the metal script through stdin
    pipe.communicate(input=script.encode('ascii'))
    pipe.stdin.close()

    # TODO: Verify by parsing .info file and checking for errors, etc.

    # the final output file
    return '%s/METAANALYSIS1.csv' % workdir


def variants_path(phenotype):
    """
    Where in EFS are the partitioned variants stored for a given dataset.
    """
    return '%s/metaanalysis/%s/variants' % (efsdir, phenotype)


def common_variants_path(phenotype, ancestry):
    """
    Where the common variants for a given ancestry are stored.
    """
    return '%s/*/common/ancestry=%s' % (variants_path(phenotype), ancestry)


def rare_variants_path(phenotype, ancestry):
    """
    Where the rare variants for a given ancestry are stored.
    """
    return '%s/*/rare/ancestry=%s' % (variants_path(phenotype), ancestry)


def ancestry_specific_path(phenotype, ancestry):
    """
    Where the ancestry-specific analysis (per ancestry) is run.
    """
    return '%s/metaanalysis/%s/ancestry-specific/%s' % (efsdir, phenotype, ancestry)


def trans_ethnic_path(phenotype):
    """
    Where the trans-ethnic analysis (per ancestry) is run.
    """
    return '%s/metaanalysis/%s/trans-ethnic/analysis' % (efsdir, phenotype)


def run_ancestry_specific_analysis(spark, phenotype):
    """
    Runs METAL for each individual ancestry within a phenotype with OVERLAP ON,
    then union the output with the rare variants across all datasets for each
    ancestry.
    """
    srcdir = variants_path(phenotype)

    # dataset part files by ancestry
    ancestries = {}

    # find all the common variant part files
    for part in glob.glob('%s/*/common/ancestry=*/part-*' % srcdir):
        ancestry = re.search(r'/ancestry=([^/]+)/', part).group(1)
        ancestries.setdefault(ancestry, []) \
            .append(part)

    # for each ancestry, run METAL across all the datasets with OVERLAP ON
    for ancestry, parts in ancestries.items():
        outdir = ancestry_specific_path(phenotype, ancestry)

        # run METAL with OVERLAP, keep track of where the output was saved to
        output_file = run_metal('%s/analysis' % outdir, parts, overlap=True)

        # NOTE: The columns from the analysis and rare variants need to be
        #       in the same order. To guarantee this, we'll select from each
        #       using this list of columns!
        columns = [
            col('varId'),
            col('dataset'),
            col('phenotype'),
            col('chromosome'),
            col('position'),
            col('reference'),
            col('alt'),
            col('pValue'),
            col('beta'),
            col('maf'),
            col('n'),
            col('stderr'),
        ]

        # read the METAANALYSIS file from EFS into spark and transform it
        analysis = read_analysis(spark, 'file://%s' % output_file).toDF()

        # get the ancestry-specific frequencies for each variant
        freqs = analysis \
            .select(
                col('varId'),
                col('chromosome'),
                col('position'),
                col('reference'),
                col('alt'),
                col('maf'),
                lit(ancestry).alias('ancestry'),
                lit(phenotype).alias('phenotype'),
            )

        # read the rare variants across all for this ancestry
        rare_variants = spark.read.csv(
            'file://%s' % rare_variants_path(phenotype, ancestry),
            header=True,
            sep='\t',
            schema=variants_schema,
        )

        # combine the results with the rare variants for this ancestry
        variants = analysis \
            .withColumn('dataset', lit('METAANALYSIS')) \
            .withColumn('phenotype', lit(phenotype)) \
            .select(*columns) \
            .union(rare_variants.select(*columns))

        # keep only the variants from the largest dataset, output results
        variants.rdd \
            .keyBy(lambda v: v.varId) \
            .reduceByKey(lambda a, b: b if b.n > a.n else a) \
            .map(lambda v: v[1]) \
            .toDF() \
            .repartition(1) \
            .write \
            .mode('overwrite') \
            .csv('file://%s/combined' % outdir, sep='\t', header=True)

        # write the ancestry-specific frequencies back to S3 if not Mixed
        if ancestry.upper() in ['AA', 'AF', 'EA', 'EU', 'HS', 'SA']:
            path = 's3://dig-analysis-data/out/metaanalysis/%s/ancestry-specific/%s' % (phenotype, ancestry)

            # write the frequencies to be uploaded to Neo4j
            freqs.write.csv(path, sep='\t', header=True, mode='overwrite')


def run_trans_ethnic_analysis(spark, phenotype):
    """
    The output from each ancestry-specific analysis is pulled together and
    processed with OVERLAP OFF. Once done, the results are uploaded back to
    HDFS (S3) where they can be kept and uploaded to a database.
    """
    outdir = 's3://dig-analysis-data/out/metaanalysis/%s/trans-ethnic' % phenotype

    # part files across the results from analyzing each ancestry
    paths = '%s/combined/part-*' % ancestry_specific_path(phenotype, '*')
    parts = [part for part in glob.glob(paths)]

    # run METAL without OVERLAP joining all ancestries together
    output_file = run_metal(trans_ethnic_path(phenotype), parts, overlap=False)

    # read the METAANALYSIS file from EFS into spark and transform it
    variants = read_analysis(spark, 'file://%s' % output_file)

    # filter the top variants across the genome for this phenotype
    top = variants \
        .keyBy(lambda v: (v.chromosome, v.position // 20000)) \
        .reduceByKey(lambda a, b: b if b.pValue < a.pValue else a) \
        .map(lambda v: v[1]) \
        .toDF() \
        .select(
            col('varId'),
            lit(True).alias('top'),
        )

    # for every variant, set a flag for whether or not it is a "top" variant
    variants = variants \
        .toDF() \
        .withColumn('phenotype', lit(phenotype)) \
        .join(top, 'varId', 'left_outer')

    # define the 'top' column as either being 'true' or 'false'
    top_col = when(variants.top.isNotNull(), variants.top).otherwise(lit(False))

    # overwrite the results of the join operation and save
    variants.withColumn('top', top_col) \
        .write \
        .mode('overwrite') \
        .csv(outdir, sep='\t', header=True)


# entry point
if __name__ == '__main__':
    """
    Arguments: [--ancestry-specific | --trans-ethnic] <phenotype>

    Either --ancestry-specific or --trans-ethnic is required to be passed on
    the command line, but they are also mutually exclusive.
    """
    print(platform.python_version())

    opts = argparse.ArgumentParser()
    opts.add_argument('--ancestry-specific', action='store_true', default=False)
    opts.add_argument('--trans-ethnic', action='store_true', default=False)
    opts.add_argument('phenotype')

    # parse command line arguments
    args = opts.parse_args()

    # --ancestry-specific or --trans-ethnic must be provided, but not both!
    assert args.ancestry_specific != args.trans_ethnic

    # make sure metal is installed locally
    install_metal()

    # create the spark context
    spark = SparkSession.builder.appName('MetaAnalysis').getOrCreate()

    # either run the trans-ethnic analysis or ancestry-specific analysis
    if args.ancestry_specific:
        run_ancestry_specific_analysis(spark, args.phenotype)
    else:
        run_trans_ethnic_analysis(spark, args.phenotype)

    # done
    spark.stop()
