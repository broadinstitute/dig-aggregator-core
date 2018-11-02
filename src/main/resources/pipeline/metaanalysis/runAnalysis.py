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
        StructField('eaf', DoubleType(), nullable=False),
        StructField('n', DoubleType(), nullable=False),
        StructField('stdErr', DoubleType(), nullable=False),
    ]
)


def install_metal():
    """
    Install metal from S3.
    """
    if not os.path.isfile(metal_local):
        subprocess.check_call(['mkdir', '-p', bindir])
        subprocess.check_call(['aws', 's3', 'cp', metal_s3path, metal_local])
        subprocess.check_call(['chmod', '+x', metal_local])


def variants_path(phenotype):
    "Where in EFS are the partitioned variants stored for a given dataset."
    return '%s/metaanalysis/%s/variants' % (efsdir, phenotype)


def common_variants_path(phenotype, ancestry):
    "Where the common variants for a given ancestry are stored."
    return '%s/*/common/ancestry=%s' % (variants_path(phenotype), ancestry)


def rare_variants_path(phenotype, ancestry):
    "Where the rare variants for a given ancestry are stored."
    return '%s/*/rare/ancestry=%s' % (variants_path(phenotype), ancestry)


def ancestry_specific_path(phenotype, ancestry):
    "Where the ancestry-specific analysis (per ancestry) is run."
    return '%s/metaanalysis/%s/ancestry-specific/ancestry=%s' % (efsdir, phenotype, ancestry)


def trans_ethnic_path(phenotype):
    "Where the trans-ethnic analysis (per ancestry) is run."
    return '%s/metaanalysis/%s/trans-ethnic/analysis' % (efsdir, phenotype)


def metaanalysis_schema(samplesize=True, freq=False):
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

    # add p-value and direction
    schema += [
        StructField('Pvalue', DoubleType(), nullable=False),
        StructField('Direction', StringType(), nullable=False),
        StructField('TotalSampleSize', DoubleType(), nullable=False),
    ]

    return StructType(schema)


def read_samplesize_analysis(spark, path):
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
            schema=metaanalysis_schema(samplesize=True, freq=True),
        ) \
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

        return Row(
            varId=row.MarkerName.upper(),
            beta=row.Effect if not flip else -row.Effect,
            eaf=row.Freq1 if not flip else 1.0 - row.Freq1,
            maxFreq=row.MaxFreq if not flip else 1.0 - row.MaxFreq,
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
        .rdd \
        .map(transform) \
        .toDF() \
        .filter(isnan(col('beta')) == False) \
        .filter(isnan(col('eaf')) == False) \
        .filter(isnan(col('stdErr')) == False)


def run_metal_script(workdir, parts, scheme, overlap=False, freq=False):
    """
    Run the METAL program at a given location with a set of part files.
    """
    path = '%s/scheme=%s' % (workdir, scheme)

    # make sure the path exists and is empty
    subprocess.call(['rm', '-rf', path])
    subprocess.call(['mkdir', '-p', path])

    # header used for all input files
    script = [
        'SCHEME %s' % scheme.upper(),
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
        'OUTFILE %s/METAANALYSIS .csv' % path,
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

    return '%s/METAANALYSIS1.csv' % path


def run_metal(spark, workdir, parts, overlap=False):
    """
    Run METAL twice: once for SAMPLESIZE (pValue + zScore) and once for STDERR,
    then load and join the results together in an DataFrame and return it.
    """
    samplesize_outfile = run_metal_script(workdir, parts, 'SAMPLESIZE', overlap=overlap)
    stderr_outfile = run_metal_script(workdir, parts, 'STDERR', overlap=False)

    # load both files into data frames
    samplesize_analysis = read_samplesize_analysis(spark, 'file://' + samplesize_outfile)
    stderr_analysis = read_stderr_analysis(spark, 'file://' + stderr_outfile)

    # join the two analyses together by id
    return samplesize_analysis.join(stderr_analysis, 'varId')


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

        # run METAL with OVERLAP load the results into a data frame
        analysis = run_metal(spark, '%s/analysis' % outdir, parts, overlap=True) \
            .withColumn('phenotype', lit(phenotype)) \
            .withColumn('ancestry', lit(ancestry))

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
            col('n'),
            col('stdErr'),
        ]

        # read the rare variants across all datasets for this ancestry
        rare_variants = spark.read \
            .csv(
                'file://%s' % rare_variants_path(phenotype, ancestry),
                sep='\t',
                header=True,
                schema=variants_schema,
            ) \
            .select(*columns)

        # combine the results with the rare variants for this ancestry
        variants = analysis.select(*columns).union(rare_variants)

        # keep only the variants from the largest sample size
        variants.rdd \
            .keyBy(lambda v: v.varId) \
            .reduceByKey(lambda a, b: b if b.n > a.n else a) \
            .map(lambda v: v[1]) \
            .toDF() \
            .repartition(1) \
            .write \
            .mode('overwrite') \
            .csv('file://%s/analysis+rare' % outdir, sep='\t', header=True)

        # write ancestry-specific frequencies for known ancestries
        if ancestry in ['AA', 'AF', 'EU', 'HS', 'EA', 'SA']:
            path = 's3://dig-analysis-data/out/metaanalysis/%s/ancestry-specific/ancestry=%s' % (phenotype, ancestry)
            freq = analysis.select(
                analysis.varId,
                analysis.chromosome,
                analysis.reference,
                analysis.alt,
                analysis.eaf,
                analysis.phenotype,
                analysis.ancestry,
            )

            # write out the frequency data for this ancestry/phenotype
            freq.write.mode('overwrite').csv(path, sep='\t', header=True)


def run_trans_ethnic_analysis(spark, phenotype):
    """
    The output from each ancestry-specific analysis is pulled together and
    processed with OVERLAP OFF. Once done, the results are uploaded back to
    HDFS (S3) where they can be kept and uploaded to a database.
    """
    outdir = 's3://dig-analysis-data/out/metaanalysis/%s/trans-ethnic' % phenotype

    # part files across the results from analyzing each ancestry
    paths = '%s/analysis+rare/part-*' % ancestry_specific_path(phenotype, '*')
    parts = [part for part in glob.glob(paths)]

    # run METAL joining all ancestries together
    variants = run_metal(spark, trans_ethnic_path(phenotype), parts, overlap=False)

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
