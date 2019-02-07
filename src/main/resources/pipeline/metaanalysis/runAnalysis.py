#!/usr/bin/python3

# NOTE: This script is run via script-runner, which REQUIRES that the script be
#       saved with Unix line-endings (LF, not CRLF)!

import argparse
import glob
import os
import os.path
import platform
import re
import shutil
import subprocess
import sys
import tempfile

# where in S3 meta-analysis data is
s3_path = 's3://dig-analysis-data/out/metaanalysis'

# where local analysis happens
localdir = '/mnt/efs/metaanalysis'

# where metal is installed locally
metal_local = '/home/hadoop/bin/metal'

# getmerge-strip-headers script installed locally
getmerge = '/home/hadoop/bin/getmerge-strip-headers.sh'


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


def run_metal(path, input_files, overlap=False):
    """
    Run METAL twice: once for SAMPLESIZE (pValue + zScore) and once for STDERR,
    then load and join the results together in an DataFrame and return it.
    """
    run_metal_script(path, input_files, stderr=False, overlap=overlap)
    run_metal_script(path, input_files, stderr=True, overlap=False)


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
    print('Collecting files from %s' % path)

    try:
        return subprocess.check_output(['hadoop', 'fs', '-ls', '-C', path]) \
            .decode('UTF-8') \
            .strip() \
            .split('\n')
    except subprocess.CalledProcessError:
        return []


def merge_parts(path, outfile):
    """
    Run `hadoop fs -getmerge` to join multiple CSV part files together. 
    
    This works by using the bin/scripts/getmerge-strip-headers.sh script in
    S3 which will use AWK to remove all the extraneous headers from the
    final, merged CSV file.
    """
    subprocess.check_call(['mkdir', '-p', os.path.dirname(outfile)])
    subprocess.check_call([getmerge, path, outfile])


def run_ancestry_specific_analysis(phenotype):
    """
    Runs METAL for each individual ancestry within a phenotype with OVERLAP ON,
    then union the output with the rare variants across all datasets for each
    ancestry.
    """
    srcdir = '%s/variants/%s' % (s3_path, phenotype)
    outdir = '%s/ancestry-specific/%s' % (localdir, phenotype)

    # completely nuke any pre-existing data that may be lying around...
    if os.path.isdir(outdir):
        shutil.rmtree(outdir)

    # ancestry -> [dataset] map
    ancestries = dict()

    # the path format is .../<dataset>/(common|rare)/ancestry=?
    r = re.compile(r'/([^/]+)/(?:common|rare)/ancestry=(.+)$')

    # find all the unique ancestries across this phenotype
    for part in find_parts('%s/*/*' % srcdir):
        m = r.search(part)

        if m is not None:
            dataset, ancestry = m.groups()
            ancestries.setdefault(ancestry, set()) \
                .add(dataset)

    # if there is more than 1 ancestry, delete the "Mixed" or no ancestry
    if len(ancestries) > 1 and 'Mixed' in ancestries:
        del ancestries['Mixed']

    # for each ancestry, run METAL across all the datasets with OVERLAP ON
    for ancestry, datasets in ancestries.items():
        ancestrydir = '%s/ancestry=%s' % (outdir, ancestry)
        analysisdir = '%s/_analysis' % ancestrydir

        # collect all the dataset files created
        dataset_files = []

        # datasets need to be merged into a single file for METAL to EFS
        for dataset in datasets:
            parts = '%s/%s/common/ancestry=%s' % (srcdir, dataset, ancestry)
            dataset_file = '%s/%s/common.csv' % (ancestrydir, dataset)

            # merge the common variants for the dataset together
            merge_parts(parts, dataset_file)

            # tally all the dataset files
            dataset_files.append(dataset_file)

        # run METAL across all datasets with OVERLAP ON
        run_metal(analysisdir, dataset_files, overlap=True)


def run_trans_ethnic_analysis(phenotype):
    """
    The output from each ancestry-specific analysis is pulled together and
    processed with OVERLAP OFF. Once done, the results are uploaded back to
    HDFS (S3) where they can be kept and uploaded to a database.
    """
    srcdir = '%s/ancestry-specific/%s' % (s3_path, phenotype)
    outdir = '%s/trans-ethnic/%s' % (localdir, phenotype)

    # completely nuke any pre-existing data that may be lying around...
    if os.path.isdir(outdir):
        shutil.rmtree(outdir)

    # list of all ancestries for this analysis
    ancestries = []
    input_files = []

    # the path format is .../<dataset>/(common|rare)/ancestry=?
    r = re.compile(r'/ancestry=(.+)$')

    # find all the unique ancestries across this phenotype
    for part in find_parts(srcdir):
        m = r.search(part)

        if m is not None:
            ancestries.append(m.group(1))

    # if there is no data, then don't try to load+process
    if len(ancestries) == 0:
        return

    # for each ancestry, merge all the results into a single file
    for ancestry in ancestries:
        parts = '%s/ancestry=%s' % (srcdir, ancestry)
        input_file = '%s/ancestry=%s/variants.csv' % (outdir, ancestry)

        merge_parts(parts, input_file)
        input_files.append(input_file)

    # run METAL across all ancestries with OVERLAP OFF
    run_metal('%s/_analysis' % outdir, input_files, overlap=False)


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

    # either run the trans-ethnic analysis or ancestry-specific analysis
    if args.ancestry_specific:
        run_ancestry_specific_analysis(args.phenotype)
    else:
        run_trans_ethnic_analysis(args.phenotype)
