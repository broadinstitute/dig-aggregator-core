#!/usr/bin/python3

# NOTE: This script is run via script-runner, which REQUIRES that the script be
#       saved with Unix line-endings (LF, not CRLF)!

import argparse
import glob
import os.path
import platform
import re
import shutil
import subprocess
import sys

# where in S3 VEP data (input and output) is
s3dir = 's3://dig-analysis-data/out/varianteffect'

# local directory where the analysis will be performed
localdir = '/mnt/efs/varianteffect'

# local directory where the VEP cache is located
cache = '%s/vep_data' % localdir


def docker_cmd(input_pathname, output_pathname):
    """
    Construct the command line for VEP to run in Docker.
    """
    in_path, in_file = os.path.split(input_pathname)
    out_path, out_file = os.path.split(output_pathname)

    return [
        'sudo',
        'docker',
        'run',

        # interactive mode
        '-i',

        # mount input directory to /data in container
        '-v',
        '%s:/data' % in_path,

        # mount output directory to /out in container
        '-v',
        '%s:/out' % out_path,

        # mount VEP genome cache
        '-v',
        '%s/vep_data:/opt/vep/.vep' % localdir,

        # the image name
        'ensemblorg/ensembl-vep',

        # run VEP in offline mode (use cache)
        './vep',
        '-offline',
        '-i',
        '/data/%s' % in_file,
        '-o',
        '/out/%s' % out_file,
    ]


def run_vep(input_path, output_path):
    """
    Run VEP over each of the part files in the directory.
    """
    parts = glob.glob('%s/part-*' % input_path)

    # run VEP over each part file (note: use the mounted directory for docker!)
    for i, part in enumerate(parts):
        out = '%s/part-%d' % (output_path, i + 1)
        cmd = docker_cmd(part, out)

        # log the docker command
        print('Running: `%s`' % ' '.join(cmd))

        # run the docker command
        subprocess.check_call(cmd)


if __name__ == '__main__':
    """
    Arguments: <study> <phenotype>

    @param study e.g. `ExChip_CAMP`
    @param phenotype e.g. `T2D`
    """
    print('python version=%s' % platform.python_version())
    print('user=%s' % os.getenv('USER'))

    opts = argparse.ArgumentParser()
    opts.add_argument('study')
    opts.add_argument('phenotype')

    # parse command line arguments
    args = opts.parse_args()

    # source location in S3 where the input and output tables are
    srcdir = '%s/%s/%s/variants' % (s3dir, args.study, args.phenotype)
    outdir = '%s/%s/%s/effects' % (s3dir, args.study, args.phenotype)

    # local fs location where VEP will read data from
    datadir = '%s/variants/%s/%s' % (localdir, args.study, args.phenotype)

    # local fs location where VEP will write to
    vepdir = '%s/out' % datadir

    # if the data directory already exists, nuke it
    subprocess.check_call(['rm', '-rf', datadir])

    # ensure the local directories exist
    subprocess.check_call(['mkdir', '-p', datadir])
    subprocess.check_call(['mkdir', '-p', vepdir])

    # docker needs permissions to write to the output directory
    subprocess.check_call(['chmod', 'a+rwx', vepdir])

    # copy all the source part files to the temp location
    subprocess.check_call(['aws', 's3', 'cp', srcdir, datadir, '--recursive', '--exclude=_SUCCESS'])

    # go
    run_vep(datadir, vepdir)

    # TODO: upload the output back to S3
