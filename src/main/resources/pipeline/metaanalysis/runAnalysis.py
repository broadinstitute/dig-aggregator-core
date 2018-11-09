#!/usr/bin/python3

import argparse
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

# where metal is installed locally
metal_local = '/home/hadoop/bin/metal'

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


def run_metal_script(workdir, parts, stderr=False, overlap=False, freq=False):
    """
    Run the METAL program at a given location with a set of part files.
    """
    scheme = 'STDERR' if stderr else 'SAMPLESIZE'
    path = '%s/scheme=%s' % (workdir, scheme)

    # make sure the path exists and is empty
    subprocess.call(['rm', '-rf', path])
    subprocess.call(['mkdir', '-p', path])

    # header used for all input files
    script = [
        'SCHEME %s' % scheme,
        'SEPARATOR TAB',
        'COLUMNCOUNTING LENIENT',
        'MARKERLABEL varId',
        'ALLELELABELS reference alt',
        'PVALUELABEL pValue',
        'EFFECTLABEL beta',
        'WEIGHTLABEL n',
        'FREQLABEL eaf',
        'STDERRLABEL stdErr',
        'CUSTOMVARIABLE TotalSampleSize',
        'LABEL TotalSampleSize AS n',
        'AVERAGEFREQ ON',
        'MINMAXFREQ ON',
        'OVERLAP %s' % ('ON' if overlap else 'OFF'),
    ]

    # add all the parts
    for part in parts:
        script.append('PROCESS %s' % part)

    # add the footer
    script += [
        'OUTFILE %s/METAANALYSIS .tbl' % path,
        'ANALYZE',
        'QUIT',
    ]

    # turn the script into a single string
    script = '\n'.join(script)

    # write the script to a file for posterity and debugging
    with open('%s/metal.script' % path, 'w') as fp:
        fp.write(script)

    # send all the commands to METAL
    pipe = subprocess.Popen([metal_local], stdin=subprocess.PIPE)

    # send the metal script through stdin
    pipe.communicate(input=script.encode('ascii'))
    pipe.stdin.close()

    # TODO: Verify by parsing the .info file and checking for errors.

    return '%s/METAANALYSIS1.tbl' % path


def run_metal(spark, workdir, parts, overlap=False):
    """
    Run METAL twice: once for SAMPLESIZE (pValue + zScore) and once for STDERR,
    then load and join the results together in an DataFrame and return it.
    """
    samplesize_outfile = run_metal_script(workdir, parts, stderr=False, overlap=overlap)
    stderr_outfile = run_metal_script(workdir, parts, stderr=True, overlap=False)

    # load both files into data frames
    samplesize_analysis = read_samplesize_analysis(spark, 'file://' + samplesize_outfile, overlap)
    stderr_analysis = read_stderr_analysis(spark, 'file://' + stderr_outfile)

    # join the two analyses together by id
    return samplesize_analysis.join(stderr_analysis, 'varId')


def merge_parts(path, outfile, add_header_schema=None):
    """
    Run `hadoop fs -getmerge` to join multiple CSV part files together.
    """
    subprocess.check_call(['hadoop', 'fs', '-getmerge', '-nl', '-skip-empty-file', path, outfile])

    # after merging, add a header row to the resulting CSV
    if add_header_schema is not None:
        headers = '\t'.join(field.name for field in add_header_schema.fields)

        # add the header to the top of the file
        subprocess.check_call(['sed', '-i', '1i%s' % headers, outfile])


def find_parts(path):
    """
    Run `hadoop fs -ls -C` to find all the files that match a particular path.
    """
    print('Collecting files from %s' % path)

    return subprocess \
        .check_output(['hadoop', 'fs', '-ls', '-C', path]) \
        .split('\n')


def run_ancestry_specific_analysis(spark, phenotype):
    """
    Runs METAL for each individual ancestry within a phenotype with OVERLAP ON,
    then union the output with the rare variants across all datasets for each
    ancestry.
    """
    srcdir = '%s/variants/%s' % (s3_path, phenotype)
    outdir = '%s/ancestry-specific/%s' % (s3_path, phenotype)

    # where analysis for this phenotype will take place
    workdir = '%s/ancestry-specific/%s' % (localdir, phenotype)

    # completely nuke any pre-existing data that may be lying around...
    shutil.rmtree(workdir, ignore_errors=True)

    # ancestry -> [dataset] map
    ancestries = dict()
    df = None

    # the path format is .../<dataset>/(common|rare)/ancestry=?
    r = re.compile(r'/([^/]+)/(?:common|rare)/ancestry=(.+)$')

    # find all the unique ancestries across this phenotype
    for part in find_parts('%s/*/*' % srcdir):
        m = r.search(part)

        if m is not None:
            dataset, ancestry = m.groups()
            ancestries.setdefault(ancestry, set()) \
                .add(dataset)

    # for each ancestry, run METAL across all the datasets with OVERLAP ON
    for ancestry, datasets in ancestries.items():
        ancestrydir = '%s/%s' % (workdir, ancestry)
        analysisdir = '%s/_analysis' % ancestrydir

        # collect all the dataset files created
        dataset_files = []

        # datasets need to be merged into a single file for METAL to EFS
        for dataset in datasets:
            parts = '%s/%s/common/ancestry=%s' % (srcdir, dataset, ancestry)
            dataset_file = '%s/%s/common.csv' % (ancestrydir, dataset)

            # merge the common variants for the dataset together
            merge_parts(parts, dataset_file, add_header_schema=variants_schema)

            # tally all the
            dataset_files.append(dataset_file)

        # run METAL across all datasets with OVERLAP ON
        analysis = run_metal(spark, analysisdir, dataset_files, overlap=True) \
            .withColumn('phenotype', lit(phenotype))

        # NOTE: The columns from the analysis and rare variants need to be
        #       in the same order before unioning the sets together. To
        #       guarantee this, we'll select from each using this list of
        #       columns!

        columns = [
            col('varId'),
            col('chromosome'),
            col('position'),
            col('reference'),
            col('alt'),
            col('phenotype'),
            col('pValue'),
            col('beta'),
            col('eaf'),
            col('maf'),
            col('stdErr'),
            col('n'),
        ]

        # read the rare variants across all datasets for this ancestry
        rare_variants = spark.read \
            .csv(
                '%s/*/rare/ancestry=%s' % (srcdir, ancestry),
                sep='\t',
                schema=variants_schema,
            ) \
            .select(*columns)

        # combine the results with the rare variants for this ancestry
        variants = analysis.select(*columns).union(rare_variants) \
            .withColumn('ancestry', lit(ancestry))

        # keep only the variants from the largest sample size
        variants.rdd \
            .keyBy(lambda v: v.varId) \
            .reduceByKey(lambda a, b: b if b.n > a.n else a) \
            .map(lambda v: v[1]) \
            .toDF()

        # union all the variants together into a single data frame
        df = variants if df is None else df.union(variants)

    # write out all the processed variants, rejoined with rare variants
    df.write \
        .mode('overwrite') \
        .partitionBy('ancestry') \
        .csv(outdir, sep='\t')


def run_trans_ethnic_analysis(spark, phenotype):
    """
    The output from each ancestry-specific analysis is pulled together and
    processed with OVERLAP OFF. Once done, the results are uploaded back to
    HDFS (S3) where they can be kept and uploaded to a database.
    """
    srcdir = '%s/ancestry-specific/%s' % (s3_path, phenotype)
    outdir = '%s/trans-ethnic/%s' % (s3_path, phenotype)

    # where analysis for this phenotype will take place
    workdir = '%s/trans-ethnic/%s' % (localdir, phenotype)

    # completely nuke any pre-existing data that may be lying around...
    shutil.rmtree(workdir, ignore_errors=True)

    # list of all ancestries for this analysis
    ancestries = []
    local_files = []

    # the path format is .../<dataset>/(common|rare)/ancestry=?
    r = re.compile(r'/ancestry=(.+)$')

    # find all the unique ancestries across this phenotype
    for part in find_parts(srcdir):
        m = r.search(part)

        if m is not None:
            ancestries.append(m.group(1))

    # for each ancestry, merge all the results into a single file
    for ancestry in ancestries:
        parts = '%s/ancestry=%s' % (srcdir, ancestry)
        local_file = '%s/%s/variants.csv' % (workdir, ancestry)

        merge_parts(parts, local_file, add_header_schema=variants_schema)
        local_files.append(local_file)

    # run METAL across all ancestries with OVERLAP OFF
    analysisdir = '%s/_analysis' % workdir
    variants = run_metal(spark, analysisdir, local_files, overlap=False) \
        .withColumn('phenotype', lit(phenotype))

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
    variants = variants \
        .withColumn('phenotype', lit(phenotype)) \
        .join(top, 'varId', 'left_outer')

    # define the 'top' column as either being 'true' or 'false'
    top_col = when(variants.top.isNotNull(), variants.top).otherwise(lit(False))

    # overwrite the results of the join operation and save
    variants.withColumn('top', top_col) \
        .write \
        .mode('overwrite') \
        .csv(outdir, sep='\t')


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

    # create the spark context
    spark = SparkSession.builder.appName('MetaAnalysis').getOrCreate()

    # either run the trans-ethnic analysis or ancestry-specific analysis
    if args.ancestry_specific:
        run_ancestry_specific_analysis(spark, args.phenotype)
    else:
        run_trans_ethnic_analysis(spark, args.phenotype)

    # done
    spark.stop()
