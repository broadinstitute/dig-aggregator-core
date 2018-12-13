#!/usr/bin/python3

# NOTE: This script is run via script-runner, which REQUIRES that the script be
#       saved with Unix line-endings (LF, not CRLF)!

import argparse
import concurrent.futures
import glob
import os.path
import platform
import re
import shutil
import subprocess
import sys
import vcf

# where in S3 VEP data (input and output) is
s3dir = 's3://dig-analysis-data/out/varianteffect'

# local directory where the analysis will be performed
localdir = '/mnt/efs/varianteffect'

# local directory where 3rd party binaries and data are located
bindir = '/mnt/efs/bin'

# mounted docker volume path where local vep_data lives
voldir = '/opt/vep/.vep'

# fields in the INFO column of the VCF from the CSQ key
#
# see: https://useast.ensembl.org/info/docs/tools/vep/vep_formats.html#vcf
#
csq_fields = [
    'Uploaded_variation',
    'Location',
    'Allele',
    'Gene',
    'Feature',
    'Feature_type',
    'Consequence',
    'cDNA_position',
    'CDS_position',
    'Protein_position',
    'Amino_acids',
    'Codons',
    'Existing_variation',
    'CCDS',
    'TSL',
    'APPRIS',
    'BIOTYPE',
    'CANONICAL',
    'HGNC',
    'ENSP',
    'DOMAINS',
    'MOTIF_NAME',
    'MOTIF_POS',
    'HIGH_INF_POS',
    'MOTIF_SCORE_CHANGE',
    'SIFT',
    'PolyPhen',
    'Condel',
    'IMPACT',
    'PICK',
]

# arguments to pass into the LoF plugin
lof_args = [
    'loftee_path:%s/Plugins/loftee-0.3-beta' % voldir,
    'human_ancestor_fa:%s/fasta/GRCh37.primary_assembly.genome.fa' % voldir,
]

# list of fields returned from LoF
lof_fields = [
    'LoF',
    'LoF_filter',
    'LoF_flags',
]

# list of fields from dbNSFP desired
dbnsfp_fields = [
    'CADD_phred',
    'CADD_raw_rankscore',
    'CADD_raw',
    'clinvar_clnsig',
    'clinvar_golden_stars',
    'clinvar_rs',
    'clinvar_trait',
    'DANN_rankscore',
    'DANN_score',
    'Eigen-PC-raw_rankscore',
    'Eigen-PC-raw',
    'Eigen-phred',
    'Eigen-raw',
    'FATHMM_converted_rankscore',
    'FATHMM_pred',
    'FATHMM_score',
    'fathmm-MKL_coding_group',
    'fathmm-MKL_coding_pred',
    'fathmm-MKL_coding_rankscore',
    'fathmm-MKL_coding_score',
    'GenoCanyon_score_rankscore',
    'GenoCanyon_score',
    'GERP++_NR',
    'GERP++_RS_rankscore',
    'GERP++_RS',
    'GM12878_confidence_value',
    'GM12878_fitCons_score_rankscore',
    'GM12878_fitCons_score',
    'GTEx_V6p_gene',
    'GTEx_V6p_tissue',
    'H1-hESC_confidence_value',
    'H1-hESC_fitCons_score_rankscore',
    'H1-hESC_fitCons_score',
    'HUVEC_confidence_value',
    'HUVEC_fitCons_score_rankscore',
    'HUVEC_fitCons_score',
    'integrated_confidence_value',
    'integrated_fitCons_score_rankscore',
    'integrated_fitCons_score',
    'Interpro_domain',
    'LRT_converted_rankscore',
    'LRT_Omega',
    'LRT_pred',
    'LRT_score',
    'MetaLR_pred',
    'MetaLR_rankscore',
    'MetaLR_score',
    'MetaSVM_pred',
    'MetaSVM_rankscore',
    'MetaSVM_score',
    'MutationAssessor_pred',
    'MutationAssessor_score_rankscore',
    'MutationAssessor_score',
    'MutationAssessor_UniprotID',
    'MutationAssessor_variant',
    'MutationTaster_AAE',
    'MutationTaster_converted_rankscore',
    'MutationTaster_model',
    'MutationTaster_pred',
    'MutationTaster_score',
    'phastCons100way_vertebrate_rankscore',
    'phastCons100way_vertebrate',
    'phastCons20way_mammalian_rankscore',
    'phastCons20way_mammalian',
    'phyloP100way_vertebrate_rankscore',
    'phyloP100way_vertebrate',
    'phyloP20way_mammalian_rankscore',
    'phyloP20way_mammalian',
    'Polyphen2_HDIV_pred',
    'Polyphen2_HDIV_rankscore',
    'Polyphen2_HDIV_score',
    'Polyphen2_HVAR_pred',
    'Polyphen2_HVAR_rankscore',
    'Polyphen2_HVAR_score',
    'PROVEAN_converted_rankscore',
    'PROVEAN_pred',
    'PROVEAN_score',
    'Reliability_index',
    'SIFT_converted_rankscore',
    'SIFT_pred',
    'SIFT_score',
    'SiPhy_29way_logOdds_rankscore',
    'SiPhy_29way_logOdds',
    'SiPhy_29way_pi',
    'Transcript_id_VEST3',
    'Transcript_var_VEST3',
    'VEST3_rankscore',
    'VEST3_score',
]

# arguments to pass into the dbNSFP plugin
dbnsfp_args = ['%s/dbNSFP/dbNSFP_hg19.gz' % voldir] + dbnsfp_fields


def docker_cmd(input_pathname, output_pathname):
    """
    Construct the command line for VEP to run in Docker.
    """
    in_path, in_file = os.path.split(input_pathname)
    out_path, out_file = os.path.split(output_pathname)

    os.getenv

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
        '%s/vep_data:%s' % (bindir, voldir),

        # the image name
        'ensemblorg/ensembl-vep',

        # add a bin folder in vep_data to the path
        'export',
        'PATH=%s/bin:$PATH' % voldir,

        # perl flags
        'perl',
        '-I',
        '%s/Plugins/loftee-0.3-beta' % voldir,

        # run VEP in offline mode (use cache)
        './vep',
        '--json',
        '--no_stats',
        '--fasta',
        '%s/fasta/GRCh37.primary_assembly.genome.fa' % voldir,
        '--polyphen',
        'b',
        '--sift',
        'b',
        '--ccds',
        '--canonical',
        '--appris',
        '--tsl',
        '--biotype',
        '--regulatory',
        '--plugin',
        'dbNSFP,%s' % ','.join(dbnsfp_args),
        '--plugin',
        'LoF,%s' % ','.join(lof_args),
        '-offline',
        '--flag_pick_allele',
        '--pick_order',
        'tsl,biotype,appris,rank,ccds,canonical,length',
        '--domains',
        'flags',
        '--fields',
        ','.join(csq_fields + lof_fields + dbnsfp_fields),
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
    parts.sort()

    # run VEP over each part file (note: use the mounted directory for docker!)
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        jobs = []

        # run VEP in parallel over all the input part files
        for part in parts:
            out_basename, _ = os.path.splitext(os.path.basename(part))

            # VEP outputs a JSON, which is loaded by Spark in a later step to S3
            out_json = '%s/%s.json' % (output_path, out_basename)

            # run VEP
            cmd = docker_cmd(part, out_json)

            # log the docker command
            print('Running: `%s`' % ' '.join(cmd))

            # run the docker command
            job = executor.submit(subprocess.check_call, cmd)
            jobs.append(job)

        # wait for all the jobs to finish (or for one to fail)
        concurrent.futures.wait(jobs, return_when=concurrent.futures.FIRST_EXCEPTION)


if __name__ == '__main__':
    """
    No arguments.
    """
    print('python version=%s' % platform.python_version())
    print('user=%s' % os.getenv('USER'))

    # source location in S3 where the input and output tables are
    srcdir = '%s/variants' % s3dir

    # local fs location where VEP will read data from and write to
    variantdir = '%s/variants' % localdir
    effectdir = '%s/effects' % localdir

    # if the data directory already exists, nuke it
    subprocess.check_call(['rm', '-rf', variantdir])
    subprocess.check_call(['rm', '-rf', effectdir])

    # ensure the local directories exist
    subprocess.check_call(['mkdir', '-p', variantdir])
    subprocess.check_call(['mkdir', '-p', effectdir])

    # docker needs permissions to write to the output directory
    subprocess.check_call(['chmod', 'a+w', effectdir])

    # copy all the source part files to the variant input directory
    subprocess.check_call(['aws', 's3', 'cp', srcdir, variantdir, '--recursive', '--exclude=_SUCCESS'])

    # run VEP over each part file, generate a matching part file
    run_vep(variantdir, effectdir)
