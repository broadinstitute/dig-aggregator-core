#!/usr/bin/python3

import argparse
import functools
import glob
import os.path
import platform
import re
import shutil
import subprocess
import sys

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, isnan, lit, when  # pylint: disable=E0611

# where in S3 meta-analysis data is
s3_path = 's3://dig-analysis-data/out/metaanalysis'

# where local analysis happens
localdir = '/mnt/efs/metaanalysis'

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
        StructField('eaf', DoubleType(), nullable=False),
        StructField('maf', DoubleType(), nullable=False),
        StructField('stdErr', DoubleType(), nullable=False),
        StructField('n', DoubleType(), nullable=False),
    ]
)


def metaanalysis_schema(samplesize=True, freq=False, overlap=False):
    """
    Create the CSV schema used for the METAANALYSIS output file.
    """
    schema = [
        StructField('MarkerName', StringType(), nullable=False),
        StructField('Allele1', StringType(), nullable=False),
        StructField('Allele2', StringType(), nullable=False),
    ]

    # add frequency columns
    if freq:
        schema += [
            StructField('Freq1', DoubleType(), nullable=False),
            StructField('FreqSE', DoubleType(), nullable=False),
            StructField('MinFreq', DoubleType(), nullable=False),
            StructField('MaxFreq', DoubleType(), nullable=False),
        ]

    # add samplesize or stderr
    if samplesize:
        schema += [
            StructField('Weight', DoubleType(), nullable=False),
            StructField('Zscore', DoubleType(), nullable=False),
        ]
    else:
        schema += [
            StructField('Effect', DoubleType(), nullable=False),
            StructField('StdErr', DoubleType(), nullable=False),
        ]

    # add N if overlap was ON
    if samplesize and overlap:
        schema += [
            StructField('N', DoubleType(), nullable=False),
        ]

    # add p-value and direction
    schema += [
        StructField('Pvalue', DoubleType(), nullable=False),
        StructField('Direction', StringType(), nullable=False),
        StructField('TotalSampleSize', DoubleType(), nullable=False),
    ]

    return StructType(schema)


def read_samplesize_analysis(spark, path, overlap):
    """
    Read the output of METAL when run with OVERLAP ON and SCHEMA SAMPLESIZE.
    """

    def transform(row):
        chrom, pos, ref, alt = row.MarkerName.split(':')

        # sometimes METAL will flip the alleles
        flip = alt == row.Allele1.upper()

        return Row(
            varId=row.MarkerName,
            chromosome=chrom,
            position=int(pos),
            reference=ref,
            alt=alt,
            pValue=row.Pvalue,
            zScore=row.Zscore if not flip else -row.Zscore,
            n=row.TotalSampleSize,
        )

    # load into spark and transform
    return spark.read \
        .csv(
            path,
            sep='\t',
            header=True,
            schema=metaanalysis_schema(samplesize=True, freq=True, overlap=overlap),
        ) \
        .filter(col('MarkerName').isNotNull()) \
        .rdd \
        .map(transform) \
        .toDF() \
        .filter(isnan(col('pValue')) == False) \
        .filter(isnan(col('zScore')) == False)


def read_stderr_analysis(spark, path):
    """
    Read the output of METAL when run with OVERLAP OFF and SCHEMA STDERR.
    """

    def transform(row):
        _, _, _, alt = row.MarkerName.upper().split(':')

        # sometimes METAL will flip the alleles
        flip = alt == row.Allele1.upper()

        # get the effect allele frequency and minor allele frequency
        eaf = row.Freq1 if not flip else 1.0 - row.Freq1
        maf = eaf if eaf < 0.5 else 1.0 - eaf

        return Row(
            varId=row.MarkerName.upper(),
            eaf=eaf,
            maf=maf,
            beta=row.Effect if not flip else -row.Effect,
            stdErr=row.StdErr,
        )

    # load into spark and transform
    return spark.read \
        .csv(
            path,
            sep='\t',
            header=True,
            schema=metaanalysis_schema(samplesize=False, freq=True),
        ) \
        .filter(col('MarkerName').isNotNull()) \
        .rdd \
        .map(transform) \
        .toDF() \
        .filter(isnan(col('beta')) == False) \
        .filter(isnan(col('eaf')) == False) \
        .filter(isnan(col('stdErr')) == False)


def load_analysis(spark, path, overlap=False):
    """
    Load the SAMPLESIZE and STDERR analysis and join them together.
    """
    samplesize_outfile = 'file://%s/scheme=SAMPLESIZE/METAANALYSIS1.tbl' % path
    stderr_outfile = 'file://%s/scheme=STDERR/METAANALYSIS1.tbl' % path

    # load both files into data frames
    samplesize_analysis = read_samplesize_analysis(spark, samplesize_outfile, overlap)
    stderr_analysis = read_stderr_analysis(spark, stderr_outfile)

    # join the two analyses together by id
    return samplesize_analysis.join(stderr_analysis, 'varId')


def test_path(path):
    """
    Run `hadoop fs -test -s` to see if any files exist matching the pathspec.
    """
    try:
        subprocess.check_call(['hadoop', 'fs', '-test', '-s', path])
    except subprocess.CalledProcessError:
        return False

    # a exit-code of 0 is success
    return True


def find_parts(path):
    """
    Run `hadoop fs -ls -C` to find all the files that match a particular path.
    """
    try:
        return subprocess.check_output(['hadoop', 'fs', '-ls', '-C', path]) \
            .decode('UTF-8') \
            .strip() \
            .split('\n')
    except subprocess.CalledProcessError:
        return []


def load_ancestry_specific_analysis(spark, phenotype):
    """
    Load the METAL results for each ancestry into a single DataFrame.
    """
    srcdir = '%s/ancestry-specific/%s' % (localdir, phenotype)
    outdir = '%s/ancestry-specific/%s' % (s3_path, phenotype)

    # the final dataframe for each ancestry
    df = []

    # find all the _analysis directories
    for path in glob.glob('%s/*/_analysis' % srcdir):
        ancestry = re.search(r'/ancestry=([^/]+)/_analysis', path).group(1)

        # NOTE: The columns from the analysis and rare variants need to be
        #       in the same order before unioning the sets together. To
        #       guarantee this, we'll select from each using the schema written
        #       by the partition variants script.

        columns = [col(field.name) for field in variants_schema]

        # read and join the analyses
        analysis = load_analysis(spark, path, overlap=True) \
            .withColumn('phenotype', lit(phenotype)) \
            .select(*columns)

        # location of rare variants for this phenotype+ancestry
        rare_path = '%s/variants/%s/*/rare/ancestry=%s' % (s3_path, phenotype, ancestry)

        # are there rare variants to merge with the analysis?
        if test_path(rare_path):
            rare_variants = spark.read \
                .csv(
                    '%s/variants/%s/*/rare/ancestry=%s' % (s3_path, phenotype, ancestry),
                    sep='\t',
                    header=True,
                    schema=variants_schema,
                ) \
                .select(*columns)

            # update the analysis and keep variants with the largest N
            analysis = analysis.union(rare_variants) \
                .rdd \
                .keyBy(lambda v: v.varId) \
                .reduceByKey(lambda a, b: b if b.n > a.n else a) \
                .map(lambda v: v[1]) \
                .toDF()

        # add the ancestry back in so it may be partitioned on write
        analysis = analysis.withColumn('ancestry', lit(ancestry))

        # union all the variants together into a single data frame
        df.append(analysis)

    # union all the ancestries together
    if len(df) > 0:
        all_variants = functools.reduce(lambda a, b: a.union(b), df)

        # NOTE: The column ordering is still the same as it was when we joined,
        #       but the ancestry column is added on the end. This will be stripped
        #       off with .partitionBy(), leaving all the other column ordering
        #       untouched.

        # write out all the processed variants, rejoined with rare variants
        all_variants.write \
            .mode('overwrite') \
            .partitionBy('ancestry') \
            .csv(outdir, sep='\t', header=True)


def load_trans_ethnic_analysis(spark, phenotype):
    """
    The output from each ancestry-specific analysis is pulled together and
    processed with OVERLAP OFF. Once done, the results are uploaded back to
    HDFS (S3) where they can be kept and uploaded to a database.
    """
    srcdir = '%s/trans-ethnic/%s/_analysis' % (localdir, phenotype)
    outdir = '%s/trans-ethnic/%s' % (s3_path, phenotype)

    # NOTE: After loading the analysis, we need to make sure that the columns
    #       are in the correct (expected) order for loading into Neo4j. To
    #       do this, we select based on the same order output by the variant
    #       partition script.

    columns = [col(field.name) for field in variants_schema]

    # load the analyses
    variants = load_analysis(spark, srcdir, overlap=False) \
        .withColumn('phenotype', lit(phenotype)) \
        .select(*columns)

    # filter the top variants across the genome for this phenotype
    top = variants \
        .rdd \
        .keyBy(lambda v: (v.chromosome, v.position // 20000)) \
        .reduceByKey(lambda a, b: b if b.pValue < a.pValue else a) \
        .map(lambda v: v[1]) \
        .toDF() \
        .select(
            col('varId'),
            lit(True).alias('top'),
        )

    # for every variant, set a flag for whether or not it is a "top" variant
    variants = variants.join(top, 'varId', 'left_outer')

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
    print('python version=%s' % platform.python_version())
    print('user=%s' % os.getenv('USER'))

    opts = argparse.ArgumentParser()
    opts.add_argument('--ancestry-specific', action='store_true', default=False)
    opts.add_argument('--trans-ethnic', action='store_true', default=False)
    opts.add_argument('phenotype')

    # parse command line arguments
    args = opts.parse_args()

    # --ancestry-specific or --trans-ethnic must be provided, but not both!
    assert args.ancestry_specific != args.trans_ethnic

    # create the spark context
    spark = SparkSession.builder.appName('MetaAnalysis').getOrCreate()

    # either run the trans-ethnic analysis or ancestry-specific analysis
    if args.ancestry_specific:
        load_ancestry_specific_analysis(spark, args.phenotype)
    else:
        load_trans_ethnic_analysis(spark, args.phenotype)

    # done
    spark.stop()
